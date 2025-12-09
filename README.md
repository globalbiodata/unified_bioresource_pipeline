# Unified Bioresource Pipeline

Pipeline for extracting bioresource mentions from scientific literature.

## Overview

This pipeline processes scientific papers through multiple phases:

1. **Classification**: Identify papers describing bioresources (V2 RoBERTa + PyCaret)
2. **NER**: Extract entity mentions (V2 BERT + spaCy Hybrid)
3. **Linguistic Scoring**: Score entities linguistically
4. **SetFit Classification**: Classify introduction vs usage papers
5. **Entity Mapping**: Map entities to resources
6. **URL Scanning**: Extract and validate URLs
7. **Deduplication**: Deduplicate resources
8. **URL Recovery**: Recover missing URLs from abstracts/fulltext/web
9. **Finalization**: Generate final inventory

## Requirements

- Python 3.10+
- Git LFS (for model files)

## Installation

```bash
# Clone with LFS
git lfs install
git clone https://github.com/globalbiodata/unified_bioresource_pipeline.git
cd unified_bioresource_pipeline

# Install dependencies
pip install -r requirements.txt

# For spaCy NER
python -m spacy download en_core_web_sm
```

## Usage

```bash
# Run full pipeline
python run_pipeline.py --input data/input/papers.csv --output-dir results/

# Run specific phases
python run_pipeline.py --start-phase 5 --end-phase 9
```

## Directory Structure

```
unified_bioresource_pipeline/
├── config/              # Pipeline configuration
├── lib/                 # Library modules
├── src/                 # Core source files
├── scripts/             # Phase scripts
├── notebooks/           # Colab notebooks
└── models/              # Pre-trained models (LFS)
```

## Models

Models are stored using Git LFS (~1.4 GB total):

| Model | Size | Purpose |
|-------|------|---------|
| v2_classifier | 476 MB | Paper classification |
| v2_ner | 473 MB | Named entity recognition |
| setfit_introduction_classifier | 419 MB | Introduction/usage classification |
| spacy_hybrid_ner | 34 MB | Hybrid NER (entity ruler + statistical) |
| pycaret | 184 KB | Metadata-based classification |

## License

See LICENSE file.
