# Installation Guide

## System Requirements

### Hardware

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| **RAM** | 8 GB | 16 GB |
| **Disk Space** | 5 GB | 10 GB |
| **GPU** | - | NVIDIA with 8GB+ VRAM |

**GPU Requirements by Phase:**

| Phase | GPU Required | Notes |
|-------|--------------|-------|
| Phase 1 (V2 Classification) | Yes | T4 minimum, A100 recommended |
| Phase 1 (PyCaret) | No | CPU-only |
| Phase 2 (V2 NER) | Yes | T4 minimum |
| Phase 2 (spaCy) | No | CPU-only (EntityRuler) |
| Phase 4 (SetFit) | Recommended | Can run on CPU (slower) |
| Phases 3, 5-9 | No | CPU-only |

### Software

- Python 3.10 or higher
- Git with LFS support
- Google account (for Colab notebooks)

## Installation Steps

### 1. Install Git LFS

Git LFS is required for downloading model files.

**macOS:**
```bash
brew install git-lfs
git lfs install
```

**Ubuntu/Debian:**
```bash
sudo apt-get install git-lfs
git lfs install
```

**Windows:**
```bash
# Download from https://git-lfs.github.com/
git lfs install
```

### 2. Clone Repository

```bash
git clone https://github.com/globalbiodata/unified_bioresource_pipeline.git
cd unified_bioresource_pipeline
```

Verify LFS files downloaded:
```bash
git lfs ls-files
# Should show model files (~1.4 GB total)
```

### 3. Create Virtual Environment

**Using venv:**
```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate     # Windows
```

**Using conda:**
```bash
conda create -n bioresource python=3.10
conda activate bioresource
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

### 5. Install spaCy Model

```bash
python -m spacy download en_core_web_sm
```

### 6. Verify Installation

```bash
# Check Python version
python --version  # Should be 3.10+

# Check key packages
python -c "import torch; print(f'PyTorch: {torch.__version__}')"
python -c "import transformers; print(f'Transformers: {transformers.__version__}')"
python -c "import spacy; print(f'spaCy: {spacy.__version__}')"

# Check models exist
ls -la models/
```

## Environment Setup for Different Phases

### Local Environment (Phases 3, 5-9)

The base installation above is sufficient for CPU-only phases.

### PyCaret Environment (Phase 1 PyCaret)

PyCaret may require a separate environment due to dependency conflicts:

```bash
# Create separate environment
python -m venv pycaret_env
source pycaret_env/bin/activate

# Install PyCaret
pip install pycaret[full]
```

### spaCy Environment (Phase 2 spaCy)

For the spaCy hybrid NER:

```bash
# Activate main environment
source venv/bin/activate

# Ensure spaCy is installed
pip install spacy
python -m spacy download en_core_web_sm
```

### Google Colab (Phases 1, 2, 4)

GPU-intensive phases are best run on Google Colab:

1. Upload notebook from `notebooks/` directory
2. Enable GPU: Runtime → Change runtime type → GPU
3. Upload input data files
4. Run all cells
5. Download results

**Colab Notebooks:**
- `notebooks/phase1_classification/v2_classification_colab.ipynb`
- `notebooks/phase1_classification/pycaret_classification_colab.ipynb`
- `notebooks/phase2_ner/v2_ner_colab.ipynb`
- `notebooks/phase4_setfit/setfit_inference_colab.ipynb`

## Directory Setup

Create required directories:

```bash
mkdir -p data/input
mkdir -p results
```

## Verifying Model Files

Check that all models downloaded correctly:

```bash
# List model directories
ls -la models/

# Expected output:
# models/
# ├── pycaret/           (~184 KB)
# ├── setfit_introduction_classifier/  (~419 MB)
# ├── spacy_hybrid_ner/  (~34 MB)
# ├── v2_classifier/     (~400 MB)
# └── v2_ner/            (~400 MB)
```

If models are missing or show as pointer files:
```bash
git lfs pull
```

## Troubleshooting Installation

### Git LFS Issues

**Models show as small text files:**
```bash
git lfs pull
# or
git lfs fetch --all
git lfs checkout
```

### Dependency Conflicts

**PyCaret conflicts with other packages:**
Use a separate virtual environment for PyCaret (see above).

### CUDA/GPU Issues

**PyTorch not detecting GPU:**
```bash
python -c "import torch; print(torch.cuda.is_available())"
```

If False, reinstall PyTorch with CUDA support:
```bash
pip install torch --index-url https://download.pytorch.org/whl/cu118
```

### spaCy Model Issues

**Model not found error:**
```bash
python -m spacy download en_core_web_sm
# or for larger model:
python -m spacy download en_core_web_lg
```

## Next Steps

After installation:

1. Read [QUICK_START.md](QUICK_START.md) to run your first pipeline
2. See [CONFIGURATION.md](CONFIGURATION.md) to customize settings
3. Check [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for common issues
