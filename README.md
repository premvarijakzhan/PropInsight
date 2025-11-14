# PropInsight

PropInsight is a practical, end-to-end framework for collecting, preprocessing, fine-tuning, and evaluating language models on Singapore real estate domain data. It focuses on sentiment analysis, Singlish/cultural context understanding, and domain knowledge grounded in government policies and market conditions.

## Academic Context

This project was developed for the Graduate Certificate in Practical Language Processing Practice module (STK) 
## Team Contributions

**Prem Varijakzhan (A0291913B)**
- Preprocessing pipelines
- Data labeling workflows
- Government dataset scraping
- Model fine-tuning implementation
- Evaluation framework
- Report writing
- Presentation slides
- Property Domain Corpus

**Ong Wee Yang (A0017030A)**
- Forum and Reddit data scraping
- Report writing
- Demo video production
- Dashboard development

All large datasets, processed files, checkpoints, and model artifacts are available on Google Drive:

Google Drive: https://drive.google.com/drive/folders/1HiABarjnxIutrmpsY2b8eVDGweu5Xyfi?usp=sharing

Place the downloaded folders/files into the project root preserving the structure shown below.

## Key Features
- Robust fine-tuning script with automatic checkpointing and best model selection
- Comprehensive evaluation suite (accuracy, precision, recall, F1, confusion matrices)
- Preprocessing pipelines for Reddit, forums, and government sources
- Dashboard for visual exploration of policies and signals

## Project Structure
- `src/models/qwen_sealion_finetune.py` — main fine-tuning entrypoint
- `src/models/evaluate_model.py` — evaluation of trained/baseline models
- `src/models/resi_evaluation.py` — RESI alignment and reasoning checks
- `src/preprocessing/*` — data preprocessing pipelines (Reddit, government agencies, forums)
- `dashboard/streamlit_app.py` — interactive dashboard
- `data/` — corpus, raw, and processed datasets (download from Drive)
- `results/` — metrics, predictions, visualizations, and training configs

## Getting Started

### Prerequisites
- Windows or Linux with Python 3.10+
- NVIDIA GPU with CUDA recommended for training
- Recommended Python packages: `torch`, `transformers`, `datasets`, `peft`, `accelerate`, `scikit-learn`, `pandas`, `numpy`, `pyarrow`, `matplotlib`, `seaborn`, `tensorboard`, `wandb`

Example installation (adjust CUDA as needed):
```
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121
pip install transformers datasets peft accelerate scikit-learn pandas numpy pyarrow matplotlib seaborn tensorboard wandb
```

### Download Data and Models
- Visit the Google Drive folder:
  - https://drive.google.com/drive/folders/1HiABarjnxIutrmpsY2b8eVDGweu5Xyfi?usp=sharing
- Download the datasets, processed files, model checkpoints, and any large artifacts.
- Place them under `data/` and `results/` following this structure:
  - `data/raw/` (forums, government, Reddit JSONs)
  - `data/processed/` (cleaned corpora and parquet/CSV)
  - `data/corpus/` (Singlish , Singlish is from (https://github.com/SingDict/singdict) and property domain lexicons is developed by ourselves)
  - `results/` (metrics, predictions, visualizations, tensorboard logs)

## Usage


### Dashboard
Explore a simple dashboard:
```
streamlit run dashboard/streamlit_app.py
```
Shows policy events, panel views, and basic analytics.

## Notes & Tips
- Large files are intentionally distributed via Google Drive. If you encounter GitHub push/pull limits, use the Drive link above instead.
- Keep directory names consistent with the project structure; many scripts assume relative paths under `data/` and `results/`.
- Enable TensorBoard or Weights & Biases for real-time training monitoring (optional).
- For reproducibility, the training script writes out a configuration JSON with the exact arguments used.

## Acknowledgements
This project integrates public datasets, community forums, and government information to tailor language models to the Singapore property context. Thanks to open-source contributors in the ecosystem (PyTorch, Hugging Face, scikit-learn, Streamlit, etc.).
