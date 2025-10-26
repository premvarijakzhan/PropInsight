#!/usr/bin/env python3
"""
Standalone Evaluation Script for PropInsight Qwen-SEA-LION Fine-tuned Model

This script provides comprehensive evaluation capabilities for the fine-tuned model,
including detailed metrics, visualizations, and RESI benchmark alignment.

Author: PropInsight Team
Date: 2024
"""

import os
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
import logging
from datetime import datetime
import argparse
import warnings
warnings.filterwarnings("ignore")

# ML and NLP imports
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from sklearn.metrics import (
    classification_report, confusion_matrix, accuracy_score, 
    f1_score, precision_score, recall_score, roc_auc_score
)
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
import seaborn as sns

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('model_evaluation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ModelEvaluator:
    """
    Comprehensive evaluation framework for PropInsight fine-tuned model
    """
    
    def __init__(self, model_path: str, output_dir: str = "./evaluation_results"):
        """
        Initialize the evaluator
        
        Args:
            model_path: Path to the fine-tuned model
            output_dir: Directory to save evaluation results
        """
        self.model_path = model_path
        self.output_dir = output_dir
        self.model = None
        self.tokenizer = None
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Evaluation metrics storage
        self.results = {}
        
        logger.info(f"Initialized ModelEvaluator with model: {model_path}")
    
    def load_model(self):
        """Load the fine-tuned model and tokenizer"""
        try:
            logger.info("Loading model and tokenizer...")
            
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_path)
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_path,
                torch_dtype=torch.float16,
                device_map="auto",
                trust_remote_code=True
            )
            
            # Ensure pad token is set
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token
            
            logger.info("Model and tokenizer loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def generate_response(self, prompt: str, max_length: int = 512) -> str:
        """
        Generate response from the model
        
        Args:
            prompt: Input prompt
            max_length: Maximum response length
            
        Returns:
            Generated response
        """
        try:
            inputs = self.tokenizer(
                prompt, 
                return_tensors="pt", 
                truncation=True, 
                max_length=1024
            )
            
            with torch.no_grad():
                outputs = self.model.generate(
                    **inputs,
                    max_length=max_length,
                    num_return_sequences=1,
                    temperature=0.7,
                    do_sample=True,
                    pad_token_id=self.tokenizer.eos_token_id
                )
            
            response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
            
            # Extract only the generated part (remove input prompt)
            if prompt in response:
                response = response.replace(prompt, "").strip()
            
            return response
            
        except Exception as e:
            logger.warning(f"Failed to generate response for prompt: {e}")
            return ""
    
    def evaluate_sentiment_analysis(self, test_data: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Evaluate sentiment analysis performance
        
        Args:
            test_data: List of test samples with 'text' and 'sentiment' keys
            
        Returns:
            Sentiment analysis evaluation results
        """
        logger.info("Evaluating sentiment analysis performance...")
        
        predictions = []
        true_labels = []
        
        sentiment_prompts = {
            'positive': 'positive',
            'negative': 'negative', 
            'neutral': 'neutral'
        }
        
        for sample in test_data:
            text = sample['text']
            true_sentiment = sample['sentiment'].lower()
            
            # Create evaluation prompt
            prompt = f"""Analyze the sentiment of this Singapore property-related text:

Text: "{text}"

Sentiment (positive/negative/neutral):"""
            
            response = self.generate_response(prompt, max_length=50)
            
            # Extract sentiment from response
            predicted_sentiment = self._extract_sentiment(response)
            
            predictions.append(predicted_sentiment)
            true_labels.append(true_sentiment)
        
        # Calculate metrics
        accuracy = accuracy_score(true_labels, predictions)
        f1_macro = f1_score(true_labels, predictions, average='macro')
        f1_weighted = f1_score(true_labels, predictions, average='weighted')
        
        # Classification report
        class_report = classification_report(
            true_labels, predictions, 
            output_dict=True, 
            zero_division=0
        )
        
        # Confusion matrix
        cm = confusion_matrix(true_labels, predictions)
        
        results = {
            'accuracy': accuracy,
            'f1_macro': f1_macro,
            'f1_weighted': f1_weighted,
            'classification_report': class_report,
            'confusion_matrix': cm.tolist(),
            'predictions': predictions,
            'true_labels': true_labels
        }
        
        logger.info(f"Sentiment Analysis - Accuracy: {accuracy:.4f}, F1 (macro): {f1_macro:.4f}")
        
        return results
    
    def evaluate_singlish_understanding(self) -> Dict[str, Any]:
        """
        Evaluate Singlish understanding capabilities
        
        Returns:
            Singlish evaluation results
        """
        logger.info("Evaluating Singlish understanding...")
        
        # Singlish test cases
        singlish_tests = [
            {
                'text': "Wah, this condo very chio leh! But price damn ex sia.",
                'expected_understanding': 'positive_with_price_concern',
                'singlish_terms': ['wah', 'chio', 'leh', 'ex', 'sia']
            },
            {
                'text': "Aiyo, HDB flat so cramped, cannot tahan already.",
                'expected_understanding': 'negative_space_complaint',
                'singlish_terms': ['aiyo', 'tahan']
            },
            {
                'text': "This area got good makan places, shiok for family.",
                'expected_understanding': 'positive_amenities',
                'singlish_terms': ['makan', 'shiok']
            },
            {
                'text': "Confirm plus chop this location very convenient.",
                'expected_understanding': 'positive_location',
                'singlish_terms': ['confirm plus chop']
            },
            {
                'text': "Alamak, the agent never tell me got construction noise!",
                'expected_understanding': 'negative_agent_issue',
                'singlish_terms': ['alamak']
            }
        ]
        
        correct_interpretations = 0
        singlish_identifications = 0
        total_singlish_terms = 0
        
        detailed_results = []
        
        for test in singlish_tests:
            prompt = f"""Analyze this Singapore property comment and explain what it means:

Comment: "{test['text']}"

Please explain:
1. The overall sentiment
2. Any Singlish terms used and their meanings
3. The main message about the property

Analysis:"""
            
            response = self.generate_response(prompt, max_length=200)
            
            # Check if Singlish terms are identified
            identified_terms = 0
            for term in test['singlish_terms']:
                if term.lower() in response.lower():
                    identified_terms += 1
            
            singlish_identifications += identified_terms
            total_singlish_terms += len(test['singlish_terms'])
            
            # Simple sentiment extraction for correctness check
            sentiment = self._extract_sentiment(response)
            expected_sentiment = 'positive' if 'positive' in test['expected_understanding'] else 'negative'
            
            if sentiment == expected_sentiment:
                correct_interpretations += 1
            
            detailed_results.append({
                'text': test['text'],
                'response': response,
                'identified_terms': identified_terms,
                'total_terms': len(test['singlish_terms']),
                'correct_sentiment': sentiment == expected_sentiment
            })
        
        # Calculate metrics
        interpretation_accuracy = correct_interpretations / len(singlish_tests)
        singlish_identification_rate = singlish_identifications / total_singlish_terms
        
        results = {
            'interpretation_accuracy': interpretation_accuracy,
            'singlish_identification_rate': singlish_identification_rate,
            'total_tests': len(singlish_tests),
            'correct_interpretations': correct_interpretations,
            'detailed_results': detailed_results
        }
        
        logger.info(f"Singlish Understanding - Accuracy: {interpretation_accuracy:.4f}, "
                   f"Term Identification: {singlish_identification_rate:.4f}")
        
        return results
    
    def evaluate_property_domain_knowledge(self) -> Dict[str, Any]:
        """
        Evaluate property domain knowledge
        
        Returns:
            Property domain evaluation results
        """
        logger.info("Evaluating property domain knowledge...")
        
        # Property domain test cases
        domain_tests = [
            {
                'question': "What is the difference between HDB and private condominiums in Singapore?",
                'key_points': ['public housing', 'private property', 'eligibility', 'price', 'facilities']
            },
            {
                'question': "Explain the Additional Buyer's Stamp Duty (ABSD) in Singapore.",
                'key_points': ['stamp duty', 'additional', 'foreign buyers', 'multiple properties', 'rates']
            },
            {
                'question': "What factors affect property prices in Singapore?",
                'key_points': ['location', 'transport', 'amenities', 'government policies', 'market demand']
            },
            {
                'question': "What is the Minimum Occupation Period (MOP) for HDB flats?",
                'key_points': ['5 years', 'occupation period', 'resale', 'HDB rules']
            },
            {
                'question': "Explain the concept of leasehold vs freehold properties in Singapore.",
                'key_points': ['99 years', 'lease', 'freehold', 'ownership', 'value']
            }
        ]
        
        knowledge_scores = []
        detailed_results = []
        
        for test in domain_tests:
            prompt = f"""Question about Singapore property market:

{test['question']}

Please provide a comprehensive answer:"""
            
            response = self.generate_response(prompt, max_length=300)
            
            # Score based on key points mentioned
            points_covered = 0
            for point in test['key_points']:
                if point.lower() in response.lower():
                    points_covered += 1
            
            score = points_covered / len(test['key_points'])
            knowledge_scores.append(score)
            
            detailed_results.append({
                'question': test['question'],
                'response': response,
                'points_covered': points_covered,
                'total_points': len(test['key_points']),
                'score': score
            })
        
        # Calculate overall metrics
        average_score = np.mean(knowledge_scores)
        
        results = {
            'average_knowledge_score': average_score,
            'individual_scores': knowledge_scores,
            'detailed_results': detailed_results,
            'total_questions': len(domain_tests)
        }
        
        logger.info(f"Property Domain Knowledge - Average Score: {average_score:.4f}")
        
        return results
    
    def _extract_sentiment(self, text: str) -> str:
        """
        Extract sentiment from model response
        
        Args:
            text: Model response text
            
        Returns:
            Extracted sentiment (positive/negative/neutral)
        """
        text_lower = text.lower()
        
        # Look for explicit sentiment words
        if any(word in text_lower for word in ['positive', 'good', 'great', 'excellent', 'happy']):
            return 'positive'
        elif any(word in text_lower for word in ['negative', 'bad', 'poor', 'terrible', 'unhappy']):
            return 'negative'
        else:
            return 'neutral'
    
    def create_visualizations(self):
        """Create comprehensive evaluation visualizations"""
        logger.info("Creating evaluation visualizations...")
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('PropInsight Model Evaluation Results', fontsize=16, fontweight='bold')
        
        # 1. Sentiment Analysis Confusion Matrix
        if 'sentiment_analysis' in self.results:
            sa_results = self.results['sentiment_analysis']
            cm = np.array(sa_results['confusion_matrix'])
            
            sns.heatmap(
                cm, 
                annot=True, 
                fmt='d', 
                cmap='Blues',
                ax=axes[0, 0],
                xticklabels=['Negative', 'Neutral', 'Positive'],
                yticklabels=['Negative', 'Neutral', 'Positive']
            )
            axes[0, 0].set_title('Sentiment Analysis Confusion Matrix')
            axes[0, 0].set_xlabel('Predicted')
            axes[0, 0].set_ylabel('Actual')
        
        # 2. Overall Performance Metrics
        metrics_data = []
        metric_names = []
        
        if 'sentiment_analysis' in self.results:
            metrics_data.append(self.results['sentiment_analysis']['accuracy'])
            metric_names.append('Sentiment\nAccuracy')
        
        if 'singlish_understanding' in self.results:
            metrics_data.append(self.results['singlish_understanding']['interpretation_accuracy'])
            metric_names.append('Singlish\nUnderstanding')
        
        if 'property_domain' in self.results:
            metrics_data.append(self.results['property_domain']['average_knowledge_score'])
            metric_names.append('Domain\nKnowledge')
        
        if metrics_data:
            bars = axes[0, 1].bar(metric_names, metrics_data, color=['#1f77b4', '#ff7f0e', '#2ca02c'])
            axes[0, 1].set_title('Overall Performance Metrics')
            axes[0, 1].set_ylabel('Score')
            axes[0, 1].set_ylim(0, 1)
            
            # Add value labels on bars
            for bar, value in zip(bars, metrics_data):
                axes[0, 1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                               f'{value:.3f}', ha='center', va='bottom')
        
        # 3. Singlish Term Identification
        if 'singlish_understanding' in self.results:
            su_results = self.results['singlish_understanding']
            detailed = su_results['detailed_results']
            
            identification_rates = [r['identified_terms'] / r['total_terms'] for r in detailed]
            test_labels = [f"Test {i+1}" for i in range(len(identification_rates))]
            
            axes[1, 0].bar(test_labels, identification_rates, color='orange', alpha=0.7)
            axes[1, 0].set_title('Singlish Term Identification by Test')
            axes[1, 0].set_ylabel('Identification Rate')
            axes[1, 0].set_ylim(0, 1)
            axes[1, 0].tick_params(axis='x', rotation=45)
        
        # 4. Property Domain Knowledge Scores
        if 'property_domain' in self.results:
            pd_results = self.results['property_domain']
            scores = pd_results['individual_scores']
            question_labels = [f"Q{i+1}" for i in range(len(scores))]
            
            axes[1, 1].bar(question_labels, scores, color='green', alpha=0.7)
            axes[1, 1].set_title('Property Domain Knowledge by Question')
            axes[1, 1].set_ylabel('Knowledge Score')
            axes[1, 1].set_ylim(0, 1)
        
        plt.tight_layout()
        
        # Save the plot
        plot_path = f"{self.output_dir}/evaluation_visualizations.png"
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        logger.info(f"Visualizations saved to {plot_path}")
        
        plt.show()
    
    def generate_evaluation_report(self):
        """Generate comprehensive evaluation report"""
        logger.info("Generating evaluation report...")
        
        report_path = f"{self.output_dir}/evaluation_report.txt"
        
        with open(report_path, 'w') as f:
            f.write("="*80 + "\n")
            f.write("PROPINSIGHT QWEN-SEA-LION MODEL EVALUATION REPORT\n")
            f.write("="*80 + "\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Model Path: {self.model_path}\n\n")
            
            # Sentiment Analysis Results
            if 'sentiment_analysis' in self.results:
                sa = self.results['sentiment_analysis']
                f.write("SENTIMENT ANALYSIS PERFORMANCE\n")
                f.write("-" * 40 + "\n")
                f.write(f"Accuracy: {sa['accuracy']:.4f}\n")
                f.write(f"F1 Score (Macro): {sa['f1_macro']:.4f}\n")
                f.write(f"F1 Score (Weighted): {sa['f1_weighted']:.4f}\n\n")
                
                f.write("Classification Report:\n")
                for label, metrics in sa['classification_report'].items():
                    if isinstance(metrics, dict):
                        f.write(f"  {label}: Precision={metrics.get('precision', 0):.3f}, "
                               f"Recall={metrics.get('recall', 0):.3f}, "
                               f"F1={metrics.get('f1-score', 0):.3f}\n")
                f.write("\n")
            
            # Singlish Understanding Results
            if 'singlish_understanding' in self.results:
                su = self.results['singlish_understanding']
                f.write("SINGLISH UNDERSTANDING PERFORMANCE\n")
                f.write("-" * 40 + "\n")
                f.write(f"Interpretation Accuracy: {su['interpretation_accuracy']:.4f}\n")
                f.write(f"Singlish Term Identification Rate: {su['singlish_identification_rate']:.4f}\n")
                f.write(f"Total Tests: {su['total_tests']}\n")
                f.write(f"Correct Interpretations: {su['correct_interpretations']}\n\n")
            
            # Property Domain Knowledge Results
            if 'property_domain' in self.results:
                pd = self.results['property_domain']
                f.write("PROPERTY DOMAIN KNOWLEDGE PERFORMANCE\n")
                f.write("-" * 40 + "\n")
                f.write(f"Average Knowledge Score: {pd['average_knowledge_score']:.4f}\n")
                f.write(f"Total Questions: {pd['total_questions']}\n\n")
                
                f.write("Individual Question Scores:\n")
                for i, score in enumerate(pd['individual_scores']):
                    f.write(f"  Question {i+1}: {score:.3f}\n")
                f.write("\n")
            
            # Summary and Recommendations
            f.write("SUMMARY AND RECOMMENDATIONS\n")
            f.write("-" * 40 + "\n")
            
            overall_scores = []
            if 'sentiment_analysis' in self.results:
                overall_scores.append(self.results['sentiment_analysis']['accuracy'])
            if 'singlish_understanding' in self.results:
                overall_scores.append(self.results['singlish_understanding']['interpretation_accuracy'])
            if 'property_domain' in self.results:
                overall_scores.append(self.results['property_domain']['average_knowledge_score'])
            
            if overall_scores:
                avg_performance = np.mean(overall_scores)
                f.write(f"Overall Average Performance: {avg_performance:.4f}\n\n")
                
                if avg_performance >= 0.8:
                    f.write("✓ EXCELLENT: Model shows strong performance across all evaluation dimensions.\n")
                elif avg_performance >= 0.6:
                    f.write("✓ GOOD: Model shows satisfactory performance with room for improvement.\n")
                else:
                    f.write("⚠ NEEDS IMPROVEMENT: Model requires additional training or fine-tuning.\n")
            
            f.write("\nRecommendations:\n")
            f.write("- Continue monitoring performance on real-world data\n")
            f.write("- Consider additional fine-tuning on specific weak areas\n")
            f.write("- Implement continuous evaluation pipeline\n")
            f.write("- Gather user feedback for iterative improvements\n")
        
        logger.info(f"Evaluation report saved to {report_path}")
    
    def run_comprehensive_evaluation(self, test_data_path: Optional[str] = None):
        """
        Run comprehensive evaluation pipeline
        
        Args:
            test_data_path: Optional path to test data JSON file
        """
        logger.info("Starting comprehensive evaluation pipeline...")
        
        # Load model
        self.load_model()
        
        # Load test data if provided
        test_data = []
        if test_data_path and os.path.exists(test_data_path):
            with open(test_data_path, 'r') as f:
                test_data = json.load(f)
            logger.info(f"Loaded {len(test_data)} test samples from {test_data_path}")
        else:
            # Create sample test data
            test_data = [
                {'text': 'This condo is amazing with great facilities!', 'sentiment': 'positive'},
                {'text': 'The HDB flat is too small and overpriced.', 'sentiment': 'negative'},
                {'text': 'The location is decent, nothing special.', 'sentiment': 'neutral'},
                {'text': 'Wah this place very nice leh!', 'sentiment': 'positive'},
                {'text': 'Alamak, so expensive for what?', 'sentiment': 'negative'}
            ]
            logger.info("Using sample test data for evaluation")
        
        # Run evaluations
        self.results['sentiment_analysis'] = self.evaluate_sentiment_analysis(test_data)
        self.results['singlish_understanding'] = self.evaluate_singlish_understanding()
        self.results['property_domain'] = self.evaluate_property_domain_knowledge()
        
        # Save results
        results_path = f"{self.output_dir}/evaluation_results.json"
        with open(results_path, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        # Generate report and visualizations
        self.generate_evaluation_report()
        self.create_visualizations()
        
        logger.info("Comprehensive evaluation completed successfully!")
        
        return self.results

def main():
    """Main function for standalone evaluation"""
    parser = argparse.ArgumentParser(description='Evaluate PropInsight Fine-tuned Model')
    parser.add_argument('--model_path', type=str, required=True,
                       help='Path to the fine-tuned model directory')
    parser.add_argument('--test_data', type=str, default=None,
                       help='Path to test data JSON file (optional)')
    parser.add_argument('--output_dir', type=str, default='./evaluation_results',
                       help='Output directory for evaluation results')
    
    args = parser.parse_args()
    
    # Initialize evaluator
    evaluator = ModelEvaluator(args.model_path, args.output_dir)
    
    # Run evaluation
    results = evaluator.run_comprehensive_evaluation(args.test_data)
    
    # Print summary
    print("\n" + "="*60)
    print("EVALUATION COMPLETED SUCCESSFULLY!")
    print("="*60)
    print(f"Results saved to: {args.output_dir}")
    
    if 'sentiment_analysis' in results:
        sa = results['sentiment_analysis']
        print(f"Sentiment Analysis Accuracy: {sa['accuracy']:.4f}")
    
    if 'singlish_understanding' in results:
        su = results['singlish_understanding']
        print(f"Singlish Understanding: {su['interpretation_accuracy']:.4f}")
    
    if 'property_domain' in results:
        pd = results['property_domain']
        print(f"Domain Knowledge Score: {pd['average_knowledge_score']:.4f}")
    
    print("="*60)

if __name__ == "__main__":
    main()