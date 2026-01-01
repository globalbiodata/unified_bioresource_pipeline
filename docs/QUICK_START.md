# Quick Start Guide

Get the pipeline running in 5 minutes.

## Prerequisites

- Python 3.10+
- Git with LFS support
- Google account (for Colab notebooks)

## 1. Clone and Setup

```bash
# Install Git LFS first
git lfs install

# Clone repository
git clone https://github.com/globalbiodata/unified_bioresource_pipeline.git
cd unified_bioresource_pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

## 2. Fetch Papers from EPMC (Phase 0)

Query Europe PMC to get candidate papers:

```bash
# Fetch papers from a specific date range
python scripts/phase0_data_acquisition/query_epmc.py \
    config/epmc_query_v5.1.txt \
    -f 2022-01-01 \
    -t 2024-06-30 \
    -o data/my_batch/
```

This creates:
- `data/my_batch/query_results.csv` - Papers with metadata
- `data/my_batch/last_query_dates.txt` - Date range record

**Alternative:** If you already have papers from EPMC, skip this step and place your CSV in `data/my_batch/`. Your CSV should contain:
- `id` or `pmid` - Paper identifier
- `title` - Paper title
- `abstract` - Paper abstract
- Optional: `pubYear`, `citedByCount`, `hasDbCrossReferences`, etc.

## 3. Run Classification (Phase 1)

**Option A: Google Colab (Recommended for GPU)**

1. Upload `notebooks/phase1_classification/v2_classification_colab.ipynb` to Colab
2. Upload your input CSV
3. Run all cells
4. Download results

**Option B: Local with PyCaret only**

```bash
python scripts/phase1_classification/02_run_pycaret_local.py \
    --input data/my_batch/papers.csv \
    --output data/my_batch/pycaret_results.csv
```

## 4. Run NER (Phase 2)

**Google Colab:**
1. Upload `notebooks/phase2_ner/v2_ner_colab.ipynb`
2. Process classification-positive papers
3. Download NER results

**Local spaCy:**
```bash
python scripts/phase2_ner/run_spacy_full_hybrid_local.py \
    --input data/my_batch/classification_positives.csv \
    --output data/my_batch/spacy_ner_results.csv
```

## 5. Run Remaining Phases (Local)

Once you have classification and NER results:

```bash
# Create session and run phases 2-9
python run_pipeline.py \
    --input-dir data/my_batch/ \
    --profiles aggressive \
    --start-phase 2
```

Or run specific phases:
```bash
# Just phases 3-5
python run_pipeline.py \
    --session-id YOUR_SESSION_ID \
    --start-phase 3 \
    --end-phase 5
```

## 6. Find Your Results

Results are in `results/{session_id}/`:

```
results/2025-12-04-143052-abc12/
├── 07_deduplication/aggressive/set_c_final.csv  # Deduplicated resources
└── 09_finalization/final_inventory.csv          # Final output
```

## Common Commands

```bash
# Dry run (preview without executing)
python run_pipeline.py --input-dir data/my_batch/ --dry-run

# Use balanced profile (less filtering needed)
python run_pipeline.py --input-dir data/my_batch/ --profiles balanced

# Skip web search breakpoint in Phase 8
python run_pipeline.py --session-id SESSION --start-phase 8 --skip-web-search

# Verbose output
python run_pipeline.py --input-dir data/my_batch/ --verbose
```

## Next Steps

- Read [PHASES.md](PHASES.md) for detailed phase documentation
- See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) if you encounter issues
- Check [CONFIGURATION.md](CONFIGURATION.md) to customize settings
