# Unified Bioresource Pipeline

A comprehensive ML pipeline for discovering bioresource mentions (databases, repositories, tools) from scientific literature.

## Overview

This pipeline processes scientific papers from Europe PMC through 9 phases to identify and catalog novel bioresources:

```
Papers (EPMC) → Classification → NER → Linguistic Scoring → SetFit →
Mapping → Deduplication → URL Scanning → URL Recovery → Final Inventory
```

**Key Features:**
- **Dual-model classification**: V2 RoBERTa + PyCaret ensemble for high recall
- **Hybrid NER**: V2 BERT + spaCy EntityRuler for comprehensive entity extraction
- **Multi-profile deduplication**: Conservative, balanced, or aggressive filtering
- **Session-based execution**: Reproducible runs with checkpointing
- **URL validation**: Live checking with Wayback Machine fallback

## Quick Start

```bash
# Clone with LFS (for model files)
git lfs install
git clone https://github.com/globalbiodata/unified_bioresource_pipeline.git
cd unified_bioresource_pipeline

# Install dependencies
pip install -r requirements.txt
python -m spacy download en_core_web_sm

# Run full pipeline
python run_pipeline.py --input-dir data/input/ --profiles aggressive

# Resume from specific phase
python run_pipeline.py --session-id 2025-12-04-143052-abc12 --start-phase 5
```

## Documentation

| Document | Description |
|----------|-------------|
| [QUICK_START.md](docs/QUICK_START.md) | 5-minute getting started guide |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design and data flow |
| [INSTALLATION.md](docs/INSTALLATION.md) | Environment setup instructions |
| [CONFIGURATION.md](docs/CONFIGURATION.md) | Config file reference |
| [PHASES.md](docs/PHASES.md) | Detailed phase documentation |
| [MODELS.md](docs/MODELS.md) | ML model descriptions |
| [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common issues and solutions |
| [GLOSSARY.md](docs/GLOSSARY.md) | Terminology definitions |

## Pipeline Phases

| Phase | Name | Description | GPU |
|-------|------|-------------|-----|
| 0 | Data Acquisition | Fetch papers from Europe PMC | No |
| 1 | Classification | Identify bioresource papers (V2 + PyCaret) | Yes |
| 2 | NER | Extract entity mentions (V2 + spaCy) | Yes |
| 3 | Linguistic Scoring | Score papers with linguistic patterns | No |
| 4 | SetFit | Classify medium-confidence papers | Yes |
| 5 | Mapping | Create paper sets and extract URLs | No |
| 6 | URL Scanning | Validate URLs with web scanner | No |
| 7 | Deduplication | Merge duplicate resources | No |
| 8 | URL Recovery | Recover missing URLs from text | No |
| 9 | Finalization | Generate final inventory | No |

**Note:** Phase 6 runs after Phase 7 to avoid scanning URLs that will be merged.

## Directory Structure

```
unified_bioresource_pipeline/
├── run_pipeline.py          # Main orchestrator
├── config/
│   └── pipeline_config.yaml # Pipeline configuration
├── lib/
│   ├── session_utils.py     # Session management
│   └── url_scanner/         # URL validation module
├── src/
│   └── inventory_utils/     # Core utilities
├── scripts/
│   ├── phase0_data_acquisition/
│   ├── phase1_classification/
│   ├── phase2_ner/
│   ├── phase3_linguistic/
│   ├── phase4_setfit/
│   ├── phase5_mapping/
│   ├── phase6_scanning/
│   ├── phase7_deduplication/
│   ├── phase8_url_recovery/
│   └── phase9_finalization/
├── notebooks/               # Google Colab notebooks
├── models/                  # Pre-trained models (Git LFS)
└── docs/                    # Documentation
```

## Models

Models are stored using Git LFS (~1.4 GB total):

| Model | Size | Purpose |
|-------|------|---------|
| v2_classifier | 400 MB | Paper classification (RoBERTa) |
| v2_ner | 400 MB | Named entity recognition (RoBERTa) |
| pycaret | 184 KB | Metadata-based classification |
| spacy_hybrid_ner | 34 MB | Hybrid NER (EntityRuler + statistical) |
| setfit_introduction_classifier | 419 MB | Introduction vs usage classification |

## Requirements

- Python 3.10+
- Git LFS (for model files)
- GPU recommended for Phases 1, 2, 4 (Google Colab supported)

## Deduplication Profiles

| Profile | Description | Use Case |
|---------|-------------|----------|
| **aggressive** | Maximum recall, accepts more candidates | Recommended - comprehensive discovery, requires more filtering |
| **balanced** | Good precision/recall trade-off | Less manual filtering needed |
| **conservative** | High precision, strict filtering | When false positives are costly |

## Session Management

Each pipeline run creates a session with unique ID (`YYYY-MM-DD-HHMMSS-xxxxx`):

```
results/{session_id}/
├── input/              # Input files
├── 02_ner/             # NER outputs
├── 03_linguistic/      # Linguistic scoring
├── 04_setfit/          # SetFit classification
├── 05_mapping/         # Paper sets and URLs
├── 06_scanning/        # URL scan results
├── 07_deduplication/   # Dedup by profile
│   ├── conservative/
│   ├── balanced/
│   └── aggressive/
├── 08_url_recovery/    # Recovered URLs
└── 09_finalization/    # Final inventory
```


## Acknowledgments

The **V2 models** (v2_classifier and v2_ner) used in this pipeline are the original machine learning models developed in 2021 by **Ana Maria Istrate** and **Kenneth Schackart** as part of the Global Biodata Coalition's bioresource inventory project. The original implementation is available at [github.com/globalbiodata/inventory_2022](https://github.com/globalbiodata/inventory_2022).

## Author

Warren Emmett <warren.emmett@gmail.com>
