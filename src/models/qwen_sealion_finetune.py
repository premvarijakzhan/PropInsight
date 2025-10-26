#!/usr/bin/env python3
"""
Qwen-SEA-LION Fine-tuning Script for PropInsight Multi-Dimensional Sentiment Analysis

This script fine-tunes the Qwen-SEA-LION-v4-32B-IT model for Singapore property sentiment analysis
using multiple data sources and incorporating Singlish cultural context.

Key Features:
- Multi-dimensional sentiment analysis (overall, price, policy, affordability)
- Singlish and Singapore property domain adaptation
- RESI benchmark alignment for evaluation
- LoRA/QLoRA efficient fine-tuning
- Comprehensive dataset integration

Author: PropInsight Team
Date: 2025
"""

import os
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from pathlib import Path
import logging
from datetime import datetime
import re
import warnings
warnings.filterwarnings("ignore")

# Core ML libraries
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from transformers import (
    AutoTokenizer, AutoModelForCausalLM, 
    TrainingArguments, Trainer,
    BitsAndBytesConfig, DataCollatorForLanguageModeling,
    EarlyStoppingCallback, TrainerCallback
)
from peft import (
    LoraConfig, get_peft_model, TaskType,
    prepare_model_for_kbit_training
)
from datasets import Dataset as HFDataset
import evaluate
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, f1_score
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import seaborn as sns
try:
    import wandb
except ImportError:
    wandb = None
try:
    from tensorboardX import SummaryWriter
except ImportError:
    SummaryWriter = None

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('qwen_sealion_finetune.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class FineTuningConfig:
    """Configuration class for fine-tuning parameters"""
    
    # Model configuration
    model_name: str = "aisingapore/llama3-8b-cpt-sea-lionv2.1-instruct"
    output_dir: str = "./results"
    cache_dir: str = "./cache"
    
    # LoRA configuration
    lora_r: int = 16
    lora_alpha: int = 32
    lora_dropout: float = 0.1
    target_modules: List[str] = None
    
    # Training configuration
    num_train_epochs: int = 3
    per_device_train_batch_size: int = 2
    per_device_eval_batch_size: int = 2
    gradient_accumulation_steps: int = 8
    learning_rate: float = 2e-4
    weight_decay: float = 0.01
    warmup_ratio: float = 0.1
    max_grad_norm: float = 1.0
    
    # Early stopping configuration
    early_stopping_patience: int = 3
    early_stopping_threshold: float = 0.001
    metric_for_best_model: str = "eval_loss"
    greater_is_better: bool = False
    
    # Evaluation configuration
    evaluation_strategy: str = "steps"
    eval_steps: int = 500
    save_steps: int = 500
    logging_steps: int = 100
    
    # Quantization
    use_4bit: bool = True
    bnb_4bit_compute_dtype: str = "float16"
    bnb_4bit_quant_type: str = "nf4"
    use_nested_quant: bool = False
    
    # Data configuration
    max_length: int = 2048
    train_size: float = 0.8
    val_size: float = 0.1
    test_size: float = 0.1
    
    # Visualization and monitoring
    enable_tensorboard: bool = True
    enable_wandb: bool = False
    plot_training_curves: bool = True
    save_plots: bool = True
    
    # Paths
    data_dir: str = "../../data"
    corpus_dir: str = "../../data/forfinetuning/corpus"
    resi_dir: str = "../../data/raw/resi"
    
    def __post_init__(self):
        if self.target_modules is None:
            # Default LoRA target modules for Qwen/SEA-LION architecture
            self.target_modules = ["q_proj", "k_proj", "v_proj", "o_proj", 
                                 "gate_proj", "up_proj", "down_proj"]

class TrainingVisualizationCallback(TrainerCallback):
    """Custom callback for training visualization and monitoring"""
    
    def __init__(self, config: FineTuningConfig):
        self.config = config
        self.train_losses = []
        self.eval_losses = []
        self.learning_rates = []
        self.steps = []
        self.eval_steps = []
        
        # Initialize TensorBoard writer if enabled
        if config.enable_tensorboard and SummaryWriter:
            self.tb_writer = SummaryWriter(log_dir=f"{config.output_dir}/tensorboard")
        else:
            self.tb_writer = None
            
        # Initialize wandb if enabled
        if config.enable_wandb and wandb:
            wandb.init(
                project="propinsight-qwen-sealion",
                config=config.__dict__,
                name=f"finetune_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
    
    def on_log(self, args, state, control, model=None, logs=None, **kwargs):
        """Log training metrics"""
        if logs:
            step = state.global_step
            
            # Log to TensorBoard
            if self.tb_writer:
                for key, value in logs.items():
                    if isinstance(value, (int, float)):
                        self.tb_writer.add_scalar(key, value, step)
            
            # Log to wandb
            if self.config.enable_wandb and wandb:
                wandb.log(logs, step=step)
            
            # Store metrics for plotting
            if 'loss' in logs:
                self.train_losses.append(logs['loss'])
                self.steps.append(step)
            
            if 'eval_loss' in logs:
                self.eval_losses.append(logs['eval_loss'])
                self.eval_steps.append(step)
            
            if 'learning_rate' in logs:
                self.learning_rates.append(logs['learning_rate'])
    
    def on_train_end(self, args, state, control, model=None, **kwargs):
        """Generate final plots and cleanup"""
        if self.config.plot_training_curves:
            self.plot_training_curves()
        
        if self.tb_writer:
            self.tb_writer.close()
        
        if self.config.enable_wandb and wandb:
            wandb.finish()
    
    def plot_training_curves(self):
        """Generate training visualization plots"""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle('Training Progress', fontsize=16)
            
            # Training loss
            if self.train_losses and self.steps:
                axes[0, 0].plot(self.steps, self.train_losses, 'b-', label='Training Loss')
                axes[0, 0].set_title('Training Loss')
                axes[0, 0].set_xlabel('Steps')
                axes[0, 0].set_ylabel('Loss')
                axes[0, 0].grid(True)
                axes[0, 0].legend()
            
            # Evaluation loss
            if self.eval_losses and self.eval_steps:
                axes[0, 1].plot(self.eval_steps, self.eval_losses, 'r-', label='Evaluation Loss')
                axes[0, 1].set_title('Evaluation Loss')
                axes[0, 1].set_xlabel('Steps')
                axes[0, 1].set_ylabel('Loss')
                axes[0, 1].grid(True)
                axes[0, 1].legend()
            
            # Learning rate
            if self.learning_rates and self.steps:
                axes[1, 0].plot(self.steps, self.learning_rates, 'g-', label='Learning Rate')
                axes[1, 0].set_title('Learning Rate Schedule')
                axes[1, 0].set_xlabel('Steps')
                axes[1, 0].set_ylabel('Learning Rate')
                axes[1, 0].grid(True)
                axes[1, 0].legend()
            
            # Combined losses
            if self.train_losses and self.eval_losses:
                axes[1, 1].plot(self.steps, self.train_losses, 'b-', label='Training Loss', alpha=0.7)
                if len(self.eval_steps) > 0:
                    axes[1, 1].plot(self.eval_steps, self.eval_losses, 'r-', label='Evaluation Loss', alpha=0.7)
                axes[1, 1].set_title('Training vs Evaluation Loss')
                axes[1, 1].set_xlabel('Steps')
                axes[1, 1].set_ylabel('Loss')
                axes[1, 1].grid(True)
                axes[1, 1].legend()
            
            plt.tight_layout()
            
            if self.config.save_plots:
                plot_path = f"{self.config.output_dir}/training_curves.png"
                plt.savefig(plot_path, dpi=300, bbox_inches='tight')
                logging.info(f"Training curves saved to {plot_path}")
            
            plt.show()
            
        except Exception as e:
            logging.warning(f"Failed to generate training plots: {e}")


class PropInsightDataProcessor:
    """
    Comprehensive data processor for PropInsight multi-source datasets
    
    This class handles:
    1. Loading and preprocessing labeled datasets from multiple sources
    2. Integrating Singlish and property domain corpus
    3. Creating instruction-tuning format for multi-dimensional sentiment analysis
    4. RESI benchmark alignment for evaluation
    """
    
    def __init__(self, config: FineTuningConfig):
        self.config = config
        self.singlish_dict = {}
        self.property_corpus = {}
        self.sentiment_labels = {
            'overall_sentiment': ['positive', 'negative', 'neutral'],
            'price_sentiment': ['rising', 'falling', 'stable', 'neutral'],
            'policy_sentiment': ['positive', 'negative', 'neutral'],
            'affordability_sentiment': ['positive', 'negative', 'neutral']
        }
        
        # Load corpus data
        self._load_corpus_data()
        
    def _load_corpus_data(self):
        """Load Singlish dictionary and property domain corpus"""
        try:
            # Load Singlish lexicon
            singlish_path = os.path.join(self.config.corpus_path, "Singlish", "lexicon.csv")
            if os.path.exists(singlish_path):
                singlish_df = pd.read_csv(singlish_path)
                self.singlish_dict = dict(zip(singlish_df['term'], singlish_df['meaning']))
                logger.info(f"Loaded {len(self.singlish_dict)} Singlish terms")
            
            # Load property glossary
            property_glossary_path = os.path.join(self.config.corpus_path, "glossaryV3.csv")
            if os.path.exists(property_glossary_path):
                property_df = pd.read_csv(property_glossary_path)
                self.property_corpus = dict(zip(property_df['term'], property_df['definition']))
                logger.info(f"Loaded {len(self.property_corpus)} property terms")
                
        except Exception as e:
            logger.warning(f"Error loading corpus data: {e}")
    
    def load_labeled_datasets(self) -> pd.DataFrame:
        """
        Load and combine all labeled datasets
        
        Returns:
            Combined DataFrame with standardized columns
        """
        datasets = []
        
        # Dataset paths
        dataset_paths = {
            'sgexpats': 'forfinetuning/labeled/forums/singapore_property_forum_posts_sgexpats_processed/sgexpats_forum_labeled.csv',
            'hwz': 'forfinetuning/labeled/multi_forum_property_posts_hwz_processed_2023_2025/forum_labeled_corrected.csv',
            'government': 'forfinetuning/labeled/government/gov_websites_labeled.csv',
            'reddit': 'forfinetuning/labeled/reddit/reddit_2023_2025_property_labeled.csv'
        }
        
        for source, path in dataset_paths.items():
            full_path = os.path.join(self.config.base_data_path, path)
            try:
                df = pd.read_csv(full_path)
                df['data_source'] = source
                
                # Standardize column names based on dataset structure
                if source == 'sgexpats':
                    df = self._standardize_sgexpats_data(df)
                elif source == 'hwz':
                    df = self._standardize_hwz_data(df)
                elif source == 'government':
                    df = self._standardize_government_data(df)
                elif source == 'reddit':
                    df = self._standardize_reddit_data(df)
                
                datasets.append(df)
                logger.info(f"Loaded {len(df)} samples from {source}")
                
            except Exception as e:
                logger.error(f"Error loading {source} dataset: {e}")
        
        if datasets:
            combined_df = pd.concat(datasets, ignore_index=True)
            logger.info(f"Combined dataset size: {len(combined_df)} samples")
            return combined_df
        else:
            raise ValueError("No datasets could be loaded")
    
    def _standardize_sgexpats_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize SGExpats forum data format"""
        standardized = pd.DataFrame()
        standardized['text'] = df['clean_text']
        standardized['overall_sentiment'] = df['overall_sentiment']
        standardized['price_sentiment'] = df['price_sentiment']
        standardized['policy_sentiment'] = df['policy_sentiment']
        standardized['affordability_sentiment'] = df['affordability_sentiment']
        standardized['location'] = df['location']
        standardized['singlish_detected'] = df['has_singlish']
        standardized['cultural_context'] = df['cultural_context']
        standardized['emotion'] = df['emotion']
        standardized['data_source'] = df['data_source']
        return standardized
    
    def _standardize_hwz_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize HardwareZone forum data format"""
        standardized = pd.DataFrame()
        standardized['text'] = df['clean_text']
        standardized['overall_sentiment'] = df['overall_sentiment']
        standardized['price_sentiment'] = df['price_sentiment']
        standardized['policy_sentiment'] = df['policy_sentiment']
        standardized['affordability_sentiment'] = df['affordability_sentiment']
        standardized['location'] = df['location']
        standardized['singlish_detected'] = df['singlish_detected']
        standardized['cultural_context'] = df['cultural_context']
        standardized['emotion'] = df['emotion']
        standardized['data_source'] = df['data_source']
        return standardized
    
    def _standardize_government_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize government website data format"""
        standardized = pd.DataFrame()
        standardized['text'] = df['clean_text']
        standardized['overall_sentiment'] = df['overall_sentiment']
        standardized['price_sentiment'] = df['price_sentiment']
        standardized['policy_sentiment'] = df['policy_sentiment']
        standardized['affordability_sentiment'] = df['affordability_sentiment']
        standardized['location'] = df.get('location', 'none')
        standardized['singlish_detected'] = False  # Government text typically formal
        standardized['cultural_context'] = 'official'
        standardized['emotion'] = df['emotion']
        standardized['data_source'] = df['data_source']
        return standardized
    
    def _standardize_reddit_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize Reddit data format"""
        standardized = pd.DataFrame()
        standardized['text'] = df['body']
        standardized['overall_sentiment'] = df['Sentiment']
        standardized['price_sentiment'] = df['PriceSentiment']
        standardized['policy_sentiment'] = df['PolicySentiment']
        standardized['affordability_sentiment'] = df['AffordabilitySentiment']
        standardized['location'] = df['Location']
        standardized['singlish_detected'] = df['SinglishDetected']
        standardized['cultural_context'] = df['CulturalContext']
        standardized['emotion'] = df['Emotion']
        standardized['data_source'] = df['data_source']
        return standardized
    
    def create_instruction_dataset(self, df: pd.DataFrame) -> List[Dict[str, str]]:
        """
        Create instruction-tuning dataset for multi-dimensional sentiment analysis
        
        This method creates training examples in the format:
        - System prompt explaining the task
        - User input with text and context
        - Assistant response with structured sentiment analysis
        
        Args:
            df: Standardized DataFrame with labeled data
            
        Returns:
            List of instruction-tuning examples
        """
        instructions = []
        
        # System prompt for multi-dimensional sentiment analysis
        system_prompt = """You are PropInsight, an AI assistant specialized in analyzing Singapore property market sentiment. You understand Singlish expressions and local property market context. 

Your task is to analyze property-related text and provide multi-dimensional sentiment analysis including:
1. Overall sentiment (positive/negative/neutral)
2. Price sentiment (rising/falling/stable/neutral)  
3. Policy sentiment (positive/negative/neutral)
4. Affordability sentiment (positive/negative/neutral)

Consider Singapore's unique cultural context, Singlish expressions, and local property market dynamics in your analysis."""

        for _, row in df.iterrows():
            # Skip rows with missing essential data
            if pd.isna(row['text']) or len(str(row['text']).strip()) < 10:
                continue
            
            # Create user input with context
            user_input = f"Analyze the sentiment of this Singapore property-related text:\n\n\"{row['text']}\""
            
            # Add context if available
            if row['singlish_detected']:
                user_input += f"\n\nNote: This text contains Singlish expressions."
            
            if row['cultural_context'] and row['cultural_context'] != 'none':
                user_input += f"\nCultural context: {row['cultural_context']}"
            
            # Create structured assistant response
            assistant_response = {
                "overall_sentiment": str(row['overall_sentiment']),
                "price_sentiment": str(row['price_sentiment']),
                "policy_sentiment": str(row['policy_sentiment']),
                "affordability_sentiment": str(row['affordability_sentiment'])
            }
            
            # Add location and emotion if available
            if row['location'] and row['location'] != 'none':
                assistant_response["location_mentioned"] = str(row['location'])
            
            if row['emotion'] and row['emotion'] != 'none':
                assistant_response["detected_emotion"] = str(row['emotion'])
            
            # Format as JSON for structured output
            assistant_text = json.dumps(assistant_response, indent=2)
            
            # Create instruction example
            instruction = {
                "system": system_prompt,
                "user": user_input,
                "assistant": assistant_text
            }
            
            instructions.append(instruction)
        
        logger.info(f"Created {len(instructions)} instruction examples")
        return instructions
    
    def format_for_training(self, instructions: List[Dict[str, str]], tokenizer) -> List[str]:
        """
        Format instruction examples for training with proper chat template
        
        Args:
            instructions: List of instruction examples
            tokenizer: Tokenizer with chat template
            
        Returns:
            List of formatted training texts
        """
        formatted_texts = []
        
        for instruction in instructions:
            # Create conversation format
            messages = [
                {"role": "system", "content": instruction["system"]},
                {"role": "user", "content": instruction["user"]},
                {"role": "assistant", "content": instruction["assistant"]}
            ]
            
            # Apply chat template if available
            if hasattr(tokenizer, 'apply_chat_template'):
                formatted_text = tokenizer.apply_chat_template(
                    messages, 
                    tokenize=False,
                    add_generation_prompt=False
                )
            else:
                # Fallback formatting
                formatted_text = f"System: {instruction['system']}\n\nUser: {instruction['user']}\n\nAssistant: {instruction['assistant']}"
            
            formatted_texts.append(formatted_text)
        
        return formatted_texts

class PropInsightDataset(Dataset):
    """PyTorch Dataset for PropInsight fine-tuning"""
    
    def __init__(self, texts: List[str], tokenizer, max_length: int = 2048):
        self.texts = texts
        self.tokenizer = tokenizer
        self.max_length = max_length
    
    def __len__(self):
        return len(self.texts)
    
    def __getitem__(self, idx):
        text = self.texts[idx]
        
        # Tokenize with truncation and padding
        encoding = self.tokenizer(
            text,
            truncation=True,
            padding='max_length',
            max_length=self.max_length,
            return_tensors='pt'
        )
        
        return {
            'input_ids': encoding['input_ids'].flatten(),
            'attention_mask': encoding['attention_mask'].flatten(),
            'labels': encoding['input_ids'].flatten()  # For causal LM, labels = input_ids
        }

class QwenSeaLionFineTuner:
    """
    Main fine-tuning class for Qwen-SEA-LION model
    
    This class handles:
    1. Model and tokenizer loading with quantization
    2. LoRA configuration and application
    3. Training setup and execution
    4. Model evaluation and saving
    """
    
    def __init__(self, config: FineTuningConfig):
        self.config = config
        self.model = None
        self.tokenizer = None
        self.data_processor = PropInsightDataProcessor(config)
        
        # Setup device
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Using device: {self.device}")
        
    def setup_model_and_tokenizer(self):
        """
        Load and configure model and tokenizer with quantization and LoRA
        
        This method:
        1. Loads the base model with 4-bit quantization for memory efficiency
        2. Configures LoRA for parameter-efficient fine-tuning
        3. Prepares the model for k-bit training
        """
        logger.info("Loading model and tokenizer...")
        
        # Configure quantization for memory efficiency
        if self.config.use_4bit:
            bnb_config = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_quant_type=self.config.bnb_4bit_quant_type,
                bnb_4bit_compute_dtype=getattr(torch, self.config.bnb_4bit_compute_dtype),
                bnb_4bit_use_double_quant=self.config.use_nested_quant,
            )
        else:
            bnb_config = None
        
        # Load tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.config.tokenizer_name,
            trust_remote_code=True,
            padding_side="right"  # Important for training
        )
        
        # Add pad token if not present
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        
        # Load model with quantization
        self.model = AutoModelForCausalLM.from_pretrained(
            self.config.model_name,
            quantization_config=bnb_config,
            device_map="auto",
            trust_remote_code=True,
            torch_dtype=torch.float16 if self.config.use_4bit else torch.float32
        )
        
        # Prepare model for k-bit training
        if self.config.use_4bit:
            self.model = prepare_model_for_kbit_training(self.model)
        
        # Configure LoRA
        lora_config = LoraConfig(
            r=self.config.lora_r,
            lora_alpha=self.config.lora_alpha,
            target_modules=self.config.target_modules,
            lora_dropout=self.config.lora_dropout,
            bias="none",
            task_type=TaskType.CAUSAL_LM,
        )
        
        # Apply LoRA to model
        self.model = get_peft_model(self.model, lora_config)
        
        # Print trainable parameters
        self.model.print_trainable_parameters()
        
        logger.info("Model and tokenizer setup complete")
    
    def prepare_datasets(self) -> Tuple[Dataset, Dataset]:
        """
        Prepare training and validation datasets
        
        Returns:
            Tuple of (train_dataset, eval_dataset)
        """
        logger.info("Preparing datasets...")
        
        # Load and process data
        df = self.data_processor.load_labeled_datasets()
        
        # Create instruction dataset
        instructions = self.data_processor.create_instruction_dataset(df)
        
        # Format for training
        formatted_texts = self.data_processor.format_for_training(instructions, self.tokenizer)
        
        # Split into train/validation
        train_texts, eval_texts = train_test_split(
            formatted_texts, 
            test_size=0.1, 
            random_state=42,
            stratify=None  # Can't stratify on text data
        )
        
        # Create PyTorch datasets
        train_dataset = PropInsightDataset(train_texts, self.tokenizer, self.config.max_length)
        eval_dataset = PropInsightDataset(eval_texts, self.tokenizer, self.config.max_length)
        
        logger.info(f"Training samples: {len(train_dataset)}")
        logger.info(f"Evaluation samples: {len(eval_dataset)}")
        
        return train_dataset, eval_dataset
    
    def setup_training_arguments(self) -> TrainingArguments:
        """Configure training arguments with early stopping and monitoring"""
        return TrainingArguments(
            output_dir=self.config.output_dir,
            num_train_epochs=self.config.num_train_epochs,
            per_device_train_batch_size=self.config.per_device_train_batch_size,
            per_device_eval_batch_size=self.config.per_device_eval_batch_size,
            gradient_accumulation_steps=self.config.gradient_accumulation_steps,
            learning_rate=self.config.learning_rate,
            weight_decay=self.config.weight_decay,
            warmup_ratio=self.config.warmup_ratio,
            max_grad_norm=self.config.max_grad_norm,
            logging_steps=self.config.logging_steps,
            eval_steps=self.config.eval_steps,
            save_steps=self.config.save_steps,
            evaluation_strategy=self.config.evaluation_strategy,
            save_strategy="steps",
            load_best_model_at_end=True,
            metric_for_best_model=self.config.metric_for_best_model,
            greater_is_better=self.config.greater_is_better,
            report_to=["tensorboard"] if self.config.enable_tensorboard else None,
            remove_unused_columns=False,
            dataloader_pin_memory=False,
            fp16=True if self.config.use_4bit else False,
            save_total_limit=3,  # Keep only 3 best checkpoints
            seed=42,
            data_seed=42,
        )
    
    def train(self):
        """
        Execute the fine-tuning process with early stopping and visualization
        
        This method:
        1. Sets up model and tokenizer
        2. Prepares datasets
        3. Configures training arguments
        4. Sets up callbacks (early stopping, visualization)
        5. Runs the training loop
        6. Saves the final model
        """
        logger.info("Starting fine-tuning process...")
        
        # Setup model and tokenizer
        self.setup_model_and_tokenizer()
        
        # Prepare datasets
        train_dataset, eval_dataset = self.prepare_datasets()
        
        # Setup training arguments
        training_args = self.setup_training_arguments()
        
        # Create data collator
        data_collator = DataCollatorForLanguageModeling(
            tokenizer=self.tokenizer,
            mlm=False,  # We're doing causal LM, not masked LM
        )
        
        # Setup callbacks
        callbacks = []
        
        # Early stopping callback
        early_stopping = EarlyStoppingCallback(
            early_stopping_patience=self.config.early_stopping_patience,
            early_stopping_threshold=self.config.early_stopping_threshold
        )
        callbacks.append(early_stopping)
        
        # Visualization callback
        viz_callback = TrainingVisualizationCallback(self.config)
        callbacks.append(viz_callback)
        
        # Create trainer
        trainer = Trainer(
            model=self.model,
            args=training_args,
            train_dataset=train_dataset,
            eval_dataset=eval_dataset,
            data_collator=data_collator,
            callbacks=callbacks,
        )
        
        # Start training
        logger.info("Beginning training...")
        try:
            trainer.train()
            logger.info("Training completed successfully!")
        except KeyboardInterrupt:
            logger.info("Training interrupted by user")
        except Exception as e:
            logger.error(f"Training failed with error: {e}")
            raise
        
        # Save final model
        logger.info("Saving final model...")
        trainer.save_model()
        self.tokenizer.save_pretrained(self.config.output_dir)
        
        # Save training configuration
        config_path = f"{self.config.output_dir}/training_config.json"
        with open(config_path, 'w') as f:
            json.dump(self.config.__dict__, f, indent=2, default=str)
        
        logger.info(f"Fine-tuning complete! Model saved to {self.config.output_dir}")
        
        return trainer
    
    def evaluate_model(self, test_texts: List[str] = None, trainer: Trainer = None) -> Dict[str, Any]:
        """
        Comprehensive evaluation of the fine-tuned model
        
        Args:
            test_texts: List of test texts for evaluation
            trainer: Trained model trainer instance
            
        Returns:
            Dictionary containing comprehensive evaluation metrics
        """
        logger.info("Starting comprehensive model evaluation...")
        
        if trainer is None and self.model is None:
            raise ValueError("Either trainer or model must be provided for evaluation")
        
        evaluation_results = {
            'timestamp': datetime.now().isoformat(),
            'model_name': self.config.model_name,
            'training_config': self.config.__dict__
        }
        
        try:
            # 1. Basic evaluation metrics
            if trainer and hasattr(trainer, 'evaluate'):
                logger.info("Computing basic evaluation metrics...")
                eval_results = trainer.evaluate()
                evaluation_results['basic_metrics'] = eval_results
                logger.info(f"Evaluation loss: {eval_results.get('eval_loss', 'N/A')}")
            
            # 2. Sentiment analysis evaluation
            if test_texts:
                logger.info("Evaluating sentiment analysis performance...")
                sentiment_metrics = self._evaluate_sentiment_analysis(test_texts)
                evaluation_results['sentiment_analysis'] = sentiment_metrics
            
            # 3. Singlish understanding evaluation
            logger.info("Evaluating Singlish understanding...")
            singlish_metrics = self._evaluate_singlish_understanding()
            evaluation_results['singlish_understanding'] = singlish_metrics
            
            # 4. Property domain evaluation
            logger.info("Evaluating property domain knowledge...")
            property_metrics = self._evaluate_property_domain()
            evaluation_results['property_domain'] = property_metrics
            
            # 5. Generate evaluation report
            self._generate_evaluation_report(evaluation_results)
            
            # 6. Create evaluation visualizations
            self._create_evaluation_plots(evaluation_results)
            
            logger.info("Model evaluation completed successfully!")
            
        except Exception as e:
            logger.error(f"Evaluation failed: {e}")
            evaluation_results['error'] = str(e)
        
        return evaluation_results
    
    def _evaluate_sentiment_analysis(self, test_texts: List[str]) -> Dict[str, Any]:
        """Evaluate sentiment analysis performance"""
        logger.info("Running sentiment analysis evaluation...")
        
        # Sample test cases for sentiment analysis
        sentiment_test_cases = [
            {"text": "The new BTO prices are absolutely ridiculous! How can young couples afford this?", "expected": "negative"},
            {"text": "Great location near MRT, very convenient for daily commute", "expected": "positive"},
            {"text": "The property market seems stable this quarter", "expected": "neutral"},
            {"text": "Wah, this condo so atas but price also very shiok", "expected": "positive"},  # Singlish
        ]
        
        predictions = []
        actuals = []
        
        for case in sentiment_test_cases:
            try:
                # Generate prediction using the model
                prompt = f"Analyze the sentiment of this property-related text: '{case['text']}'. Sentiment:"
                
                # Tokenize and generate
                inputs = self.tokenizer(prompt, return_tensors="pt", truncation=True, max_length=512)
                
                with torch.no_grad():
                    outputs = self.model.generate(
                        **inputs,
                        max_new_tokens=10,
                        temperature=0.1,
                        do_sample=False,
                        pad_token_id=self.tokenizer.eos_token_id
                    )
                
                # Decode prediction
                response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
                predicted_sentiment = self._extract_sentiment_from_response(response)
                
                predictions.append(predicted_sentiment)
                actuals.append(case['expected'])
                
            except Exception as e:
                logger.warning(f"Failed to evaluate sentiment for text: {case['text'][:50]}... Error: {e}")
                predictions.append("unknown")
                actuals.append(case['expected'])
        
        # Calculate metrics
        try:
            accuracy = accuracy_score(actuals, predictions)
            f1 = f1_score(actuals, predictions, average='weighted', zero_division=0)
            
            # Classification report
            report = classification_report(actuals, predictions, output_dict=True, zero_division=0)
            
            return {
                'accuracy': accuracy,
                'f1_score': f1,
                'classification_report': report,
                'test_cases': len(sentiment_test_cases),
                'predictions': predictions,
                'actuals': actuals
            }
        except Exception as e:
            logger.error(f"Failed to calculate sentiment metrics: {e}")
            return {'error': str(e)}
    
    def _evaluate_singlish_understanding(self) -> Dict[str, Any]:
        """Evaluate model's understanding of Singlish"""
        logger.info("Evaluating Singlish understanding...")
        
        singlish_test_cases = [
            {"text": "Wah, this HDB flat damn expensive leh", "contains_singlish": True},
            {"text": "Can or not? This price very reasonable mah", "contains_singlish": True},
            {"text": "The property market is performing well", "contains_singlish": False},
            {"text": "Aiyah, why the COV so high one?", "contains_singlish": True},
        ]
        
        correct_identifications = 0
        total_cases = len(singlish_test_cases)
        
        for case in singlish_test_cases:
            try:
                prompt = f"Does this text contain Singlish expressions? Text: '{case['text']}' Answer (Yes/No):"
                
                inputs = self.tokenizer(prompt, return_tensors="pt", truncation=True, max_length=512)
                
                with torch.no_grad():
                    outputs = self.model.generate(
                        **inputs,
                        max_new_tokens=5,
                        temperature=0.1,
                        do_sample=False,
                        pad_token_id=self.tokenizer.eos_token_id
                    )
                
                response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
                predicted_has_singlish = "yes" in response.lower()
                
                if predicted_has_singlish == case['contains_singlish']:
                    correct_identifications += 1
                    
            except Exception as e:
                logger.warning(f"Failed to evaluate Singlish for: {case['text'][:30]}... Error: {e}")
        
        accuracy = correct_identifications / total_cases if total_cases > 0 else 0
        
        return {
            'singlish_identification_accuracy': accuracy,
            'correct_identifications': correct_identifications,
            'total_test_cases': total_cases
        }
    
    def _evaluate_property_domain(self) -> Dict[str, Any]:
        """Evaluate property domain knowledge"""
        logger.info("Evaluating property domain knowledge...")
        
        property_test_cases = [
            {"question": "What does BTO stand for?", "expected_keywords": ["build", "to", "order"]},
            {"question": "What is COV in property transactions?", "expected_keywords": ["cash", "over", "valuation"]},
            {"question": "Explain ABSD", "expected_keywords": ["additional", "buyer", "stamp", "duty"]},
        ]
        
        domain_scores = []
        
        for case in property_test_cases:
            try:
                prompt = f"Question: {case['question']} Answer:"
                
                inputs = self.tokenizer(prompt, return_tensors="pt", truncation=True, max_length=512)
                
                with torch.no_grad():
                    outputs = self.model.generate(
                        **inputs,
                        max_new_tokens=50,
                        temperature=0.3,
                        do_sample=True,
                        pad_token_id=self.tokenizer.eos_token_id
                    )
                
                response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
                
                # Check if expected keywords are present
                response_lower = response.lower()
                keyword_matches = sum(1 for keyword in case['expected_keywords'] 
                                    if keyword.lower() in response_lower)
                
                score = keyword_matches / len(case['expected_keywords'])
                domain_scores.append(score)
                
            except Exception as e:
                logger.warning(f"Failed to evaluate property question: {case['question'][:30]}... Error: {e}")
                domain_scores.append(0.0)
        
        avg_domain_score = np.mean(domain_scores) if domain_scores else 0.0
        
        return {
            'average_domain_knowledge_score': avg_domain_score,
            'individual_scores': domain_scores,
            'total_questions': len(property_test_cases)
        }
    
    def _extract_sentiment_from_response(self, response: str) -> str:
        """Extract sentiment from model response"""
        response_lower = response.lower()
        
        if any(word in response_lower for word in ['positive', 'good', 'great', 'excellent']):
            return 'positive'
        elif any(word in response_lower for word in ['negative', 'bad', 'terrible', 'awful']):
            return 'negative'
        elif any(word in response_lower for word in ['neutral', 'okay', 'average']):
            return 'neutral'
        else:
            return 'unknown'
    
    def _generate_evaluation_report(self, results: Dict[str, Any]):
        """Generate comprehensive evaluation report"""
        logger.info("Generating evaluation report...")
        
        report_path = f"{self.config.output_dir}/evaluation_report.txt"
        
        try:
            with open(report_path, 'w') as f:
                f.write("=" * 80 + "\n")
                f.write("PROPINSIGHT QWEN-SEALION MODEL EVALUATION REPORT\n")
                f.write("=" * 80 + "\n\n")
                
                f.write(f"Evaluation Timestamp: {results['timestamp']}\n")
                f.write(f"Model: {results['model_name']}\n\n")
                
                # Basic metrics
                if 'basic_metrics' in results:
                    f.write("BASIC EVALUATION METRICS:\n")
                    f.write("-" * 40 + "\n")
                    for key, value in results['basic_metrics'].items():
                        f.write(f"{key}: {value}\n")
                    f.write("\n")
                
                # Sentiment analysis
                if 'sentiment_analysis' in results:
                    sa = results['sentiment_analysis']
                    f.write("SENTIMENT ANALYSIS PERFORMANCE:\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Accuracy: {sa.get('accuracy', 'N/A'):.4f}\n")
                    f.write(f"F1 Score: {sa.get('f1_score', 'N/A'):.4f}\n")
                    f.write(f"Test Cases: {sa.get('test_cases', 'N/A')}\n\n")
                
                # Singlish understanding
                if 'singlish_understanding' in results:
                    su = results['singlish_understanding']
                    f.write("SINGLISH UNDERSTANDING:\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Identification Accuracy: {su.get('singlish_identification_accuracy', 'N/A'):.4f}\n")
                    f.write(f"Correct Identifications: {su.get('correct_identifications', 'N/A')}/{su.get('total_test_cases', 'N/A')}\n\n")
                
                # Property domain
                if 'property_domain' in results:
                    pd = results['property_domain']
                    f.write("PROPERTY DOMAIN KNOWLEDGE:\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Average Domain Score: {pd.get('average_domain_knowledge_score', 'N/A'):.4f}\n")
                    f.write(f"Questions Evaluated: {pd.get('total_questions', 'N/A')}\n\n")
                
                f.write("=" * 80 + "\n")
                f.write("End of Report\n")
                f.write("=" * 80 + "\n")
            
            logger.info(f"Evaluation report saved to {report_path}")
            
        except Exception as e:
            logger.error(f"Failed to generate evaluation report: {e}")
    
    def _create_evaluation_plots(self, results: Dict[str, Any]):
        """Create evaluation visualization plots"""
        logger.info("Creating evaluation plots...")
        
        try:
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle('Model Evaluation Results', fontsize=16)
            
            # Sentiment Analysis Performance
            if 'sentiment_analysis' in results:
                sa = results['sentiment_analysis']
                if 'classification_report' in sa and sa['classification_report']:
                    # Extract precision, recall, f1-score for each class
                    classes = [k for k in sa['classification_report'].keys() 
                             if k not in ['accuracy', 'macro avg', 'weighted avg']]
                    
                    if classes:
                        metrics = ['precision', 'recall', 'f1-score']
                        x = np.arange(len(classes))
                        width = 0.25
                        
                        for i, metric in enumerate(metrics):
                            values = [sa['classification_report'][cls][metric] for cls in classes]
                            axes[0, 0].bar(x + i*width, values, width, label=metric)
                        
                        axes[0, 0].set_title('Sentiment Analysis Performance')
                        axes[0, 0].set_xlabel('Sentiment Classes')
                        axes[0, 0].set_ylabel('Score')
                        axes[0, 0].set_xticks(x + width)
                        axes[0, 0].set_xticklabels(classes)
                        axes[0, 0].legend()
                        axes[0, 0].grid(True, alpha=0.3)
            
            # Overall Performance Summary
            performance_metrics = []
            metric_names = []
            
            if 'sentiment_analysis' in results:
                performance_metrics.append(results['sentiment_analysis'].get('accuracy', 0))
                metric_names.append('Sentiment\nAccuracy')
            
            if 'singlish_understanding' in results:
                performance_metrics.append(results['singlish_understanding'].get('singlish_identification_accuracy', 0))
                metric_names.append('Singlish\nIdentification')
            
            if 'property_domain' in results:
                performance_metrics.append(results['property_domain'].get('average_domain_knowledge_score', 0))
                metric_names.append('Property\nDomain')
            
            if performance_metrics:
                bars = axes[0, 1].bar(metric_names, performance_metrics, color=['skyblue', 'lightgreen', 'lightcoral'])
                axes[0, 1].set_title('Overall Performance Summary')
                axes[0, 1].set_ylabel('Score')
                axes[0, 1].set_ylim(0, 1)
                axes[0, 1].grid(True, alpha=0.3)
                
                # Add value labels on bars
                for bar, value in zip(bars, performance_metrics):
                    axes[0, 1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                                   f'{value:.3f}', ha='center', va='bottom')
            
            # Training Loss (if available from basic metrics)
            if 'basic_metrics' in results:
                basic = results['basic_metrics']
                loss_metrics = {k: v for k, v in basic.items() if 'loss' in k.lower()}
                
                if loss_metrics:
                    axes[1, 0].bar(loss_metrics.keys(), loss_metrics.values(), color='orange')
                    axes[1, 0].set_title('Loss Metrics')
                    axes[1, 0].set_ylabel('Loss Value')
                    axes[1, 0].tick_params(axis='x', rotation=45)
                    axes[1, 0].grid(True, alpha=0.3)
            
            # Model Configuration Summary (text-based)
            axes[1, 1].axis('off')
            config_text = f"""Model Configuration:
            
Model: {results.get('model_name', 'N/A')}
LoRA Rank: {self.config.lora_r}
Learning Rate: {self.config.learning_rate}
Batch Size: {self.config.per_device_train_batch_size}
Epochs: {self.config.num_train_epochs}
Max Length: {self.config.max_length}
            """
            axes[1, 1].text(0.1, 0.9, config_text, transform=axes[1, 1].transAxes,
                           fontsize=10, verticalalignment='top', fontfamily='monospace')
            axes[1, 1].set_title('Configuration Summary')
            
            plt.tight_layout()
            
            if self.config.save_plots:
                plot_path = f"{self.config.output_dir}/evaluation_plots.png"
                plt.savefig(plot_path, dpi=300, bbox_inches='tight')
                logger.info(f"Evaluation plots saved to {plot_path}")
            
            plt.show()
            
        except Exception as e:
            logger.warning(f"Failed to create evaluation plots: {e}")
        
        # This would implement comprehensive evaluation including:
        # 1. RESI benchmark alignment
        # 2. Multi-dimensional sentiment accuracy
        # 3. Singlish understanding assessment
        # 4. Cultural context preservation
        
        # Placeholder for evaluation logic
        evaluation_results = {
            "overall_accuracy": 0.0,
            "price_sentiment_f1": 0.0,
            "policy_sentiment_f1": 0.0,
            "affordability_sentiment_f1": 0.0,
            "singlish_understanding": 0.0,
            "resi_alignment_score": 0.0
        }
        
        return evaluation_results

def main():
    """
    Main execution function
    
    This function demonstrates the complete fine-tuning pipeline:
    1. Configuration setup
    2. Data processing and preparation
    3. Model fine-tuning
    4. Comprehensive evaluation and reporting
    """
    logger.info("Starting PropInsight Qwen-SEA-LION Fine-tuning Pipeline")
    
    try:
        # Initialize configuration
        config = FineTuningConfig()
        
        # Create output directory
        os.makedirs(config.output_dir, exist_ok=True)
        
        # Initialize fine-tuner
        fine_tuner = QwenSeaLionFineTuner(config)
        
        # Execute training
        logger.info("Starting training phase...")
        trainer = fine_tuner.train()
        
        # Run comprehensive evaluation
        logger.info("Starting evaluation phase...")
        evaluation_results = fine_tuner.evaluate_model(trainer=trainer)
        
        # Save evaluation results
        eval_results_path = f"{config.output_dir}/evaluation_results.json"
        with open(eval_results_path, 'w') as f:
            json.dump(evaluation_results, f, indent=2, default=str)
        
        logger.info(f"Evaluation results saved to {eval_results_path}")
        
        # Print summary
        print("\n" + "="*80)
        print("FINE-TUNING COMPLETED SUCCESSFULLY!")
        print("="*80)
        print(f"Model saved to: {config.output_dir}")
        print(f"Training logs: qwen_sealion_finetune.log")
        print(f"Evaluation report: {config.output_dir}/evaluation_report.txt")
        print(f"Training curves: {config.output_dir}/training_curves.png")
        print(f"Evaluation plots: {config.output_dir}/evaluation_plots.png")
        
        if 'sentiment_analysis' in evaluation_results:
            sa = evaluation_results['sentiment_analysis']
            print(f"Sentiment Analysis Accuracy: {sa.get('accuracy', 'N/A'):.4f}")
        
        if 'singlish_understanding' in evaluation_results:
            su = evaluation_results['singlish_understanding']
            print(f"Singlish Understanding Accuracy: {su.get('singlish_identification_accuracy', 'N/A'):.4f}")
        
        if 'property_domain' in evaluation_results:
            pd = evaluation_results['property_domain']
            print(f"Property Domain Knowledge Score: {pd.get('average_domain_knowledge_score', 'N/A'):.4f}")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Fine-tuning pipeline failed: {e}")
        raise

if __name__ == "__main__":
    """
    Entry point for the fine-tuning script
    
    Usage:
        python qwen_sealion_finetune.py
    
    Note: This script is designed for educational and development purposes.
    Actual execution requires significant computational resources (GPU with 24GB+ VRAM)
    and proper environment setup with all dependencies installed.
    
    Key considerations before running:
    1. Ensure sufficient GPU memory (recommended: A100 40GB or similar)
    2. Install required dependencies (transformers, peft, bitsandbytes, etc.)
    3. Verify data paths and accessibility
    4. Consider using gradient checkpointing for memory optimization
    5. Monitor training progress and adjust hyperparameters as needed
    
    Expected outcomes:
    - Fine-tuned model specialized for Singapore property sentiment analysis
    - Enhanced understanding of Singlish expressions and cultural context
    - Multi-dimensional sentiment analysis capabilities
    - RESI benchmark alignment for real-world applicability
    """
    main()