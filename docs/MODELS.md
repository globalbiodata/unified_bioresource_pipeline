# Model Documentation

## Overview

The pipeline uses five ML models stored in the `models/` directory via Git LFS.

| Model | Size | Phase | GPU Required |
|-------|------|-------|--------------|
| v2_classifier | ~400 MB | 1 | Yes |
| v2_ner | ~400 MB | 2 | Yes |
| pycaret | ~184 KB | 1 | No |
| spacy_hybrid_ner | ~34 MB | 2 | No |
| setfit_introduction_classifier | ~419 MB | 4 | Recommended |

**Total size:** ~1.4 GB

---

## V2 Models Attribution

The **V2 models** (v2_classifier and v2_ner) are the original machine learning models developed in 2021 as part of the Global Biodata Coalition's bioresource inventory project. These models were trained and developed by:

- **Ana Maria Istrate** (Harvard T.H. Chan School of Public Health)
- **Kenneth Schackart** (University of Arizona)

The original training code, data, and full implementation are available in the [inventory_2022 repository](https://github.com/globalbiodata/inventory_2022).

### Original Work

The V2 models were created for the 2022 Global Biodata Resource (GBC) inventory update and represent significant work in:

- **Domain-Adapted Pre-training:** Using AllenAI's `dsp_roberta_base_dapt_biomed_tapt_rct_500` model, which was pre-trained on biomedical literature
- **Multi-task Learning:** Training both classification and NER models with shared architecture
- **Annotation Methodology:** Developing gold-standard datasets for training through manual annotation

### Integration in This Pipeline

This unified pipeline integrates the V2 models alongside newer models (PyCaret, spaCy Hybrid NER, SetFit) to improve recall and coverage. The V2 models remain the core components for text-based classification and entity extraction.

For detailed information on the original development, training procedures, and evaluation, please refer to the [inventory_2022 repository documentation](https://github.com/globalbiodata/inventory_2022).

---

## V2 RoBERTa Classifier

**Location:** `models/v2_classifier/`

**Purpose:** Binary classification of papers as bioresource-related or not.

### Architecture

| Property | Value |
|----------|-------|
| Base Model | `allenai/dsp_roberta_base_dapt_biomed_tapt_rct_500` |
| Architecture | RobertaForSequenceClassification |
| Hidden Dimensions | 768 |
| Parameters | ~125M |
| Labels | 2 (bioresource / not-bioresource) |
| Max Sequence Length | 256 tokens |

### Training

| Property | Value |
|----------|-------|
| Training Data | ~1,600 manually classified papers |
| Split | 70% train / 15% val / 15% test |
| Epochs | 10 |
| Batch Size | 16 (adjustable by GPU memory) |
| Learning Rate | 2e-5 |
| Optimizer | AdamW |

### Performance

| Metric | Score |
|--------|-------|
| F1 | 0.990 |
| Precision | 0.999 |
| Recall | 0.981 |
| Accuracy | 0.980 |

### Hardware Requirements

| GPU Memory | Batch Size |
|------------|------------|
| ≥ 24 GB | 32 |
| ≥ 16 GB | 24 |
| ≥ 12 GB | 16 |
| < 12 GB | 8 |

**Recommended:** Google Colab with T4 or A100 GPU.

### Usage

```python
from transformers import AutoModelForSequenceClassification, AutoTokenizer

model = AutoModelForSequenceClassification.from_pretrained("models/v2_classifier")
tokenizer = AutoTokenizer.from_pretrained("models/v2_classifier")

inputs = tokenizer(title + " " + abstract, return_tensors="pt", truncation=True, max_length=256)
outputs = model(**inputs)
prediction = outputs.logits.argmax(-1).item()
```

---

## V2 BERT NER

**Location:** `models/v2_ner/`

**Purpose:** Extract bioresource entity mentions using token classification.

### Architecture

| Property | Value |
|----------|-------|
| Base Model | `allenai/dsp_roberta_base_dapt_biomed_tapt_rct_500` |
| Architecture | RobertaForTokenClassification |
| Hidden Dimensions | 768 |
| Labels | 5 (BIO tagging) |
| Max Sequence Length | 512 tokens |

### Label Schema

| Label | Description | Example |
|-------|-------------|---------|
| O | Outside (not an entity) | - |
| B-COM | Beginning of compound/abbreviation | "PDB" |
| I-COM | Inside compound/abbreviation | - |
| B-FUL | Beginning of full name | "Protein" in "Protein Data Bank" |
| I-FUL | Inside full name | "Data Bank" in "Protein Data Bank" |

### Training

| Property | Value |
|----------|-------|
| Training Data | 554 manually annotated papers |
| Split | 70% train / 15% val / 15% test |
| Epochs | 10 |
| Batch Size | 16 |
| Learning Rate | 2e-5 |

### Performance

| Metric | Score |
|--------|-------|
| F1 | 0.749 |
| Precision | 0.779 |
| Recall | 0.722 |

### Usage

```python
from transformers import AutoModelForTokenClassification, AutoTokenizer

model = AutoModelForTokenClassification.from_pretrained("models/v2_ner")
tokenizer = AutoTokenizer.from_pretrained("models/v2_ner")

inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
outputs = model(**inputs)
predictions = outputs.logits.argmax(-1)
```

---

## PyCaret Classifier

**Location:** `models/pycaret/`

**Purpose:** Metadata-based classification using engineered features.

### Architecture

| Property | Value |
|----------|-------|
| Type | AutoML Ensemble |
| Algorithm | Blended top 3 models (soft voting) |
| Features | 91-112 engineered features |
| Model Size | ~184 KB |

### Feature Categories

| Category | Count | Description |
|----------|-------|-------------|
| MeSH Terms | 30-50 | Target-encoded with Bayesian smoothing |
| Citation Metrics | 5 | Count, log-transform, age-ratio, percentiles |
| Access Flags | 8 | isOpenAccess, inPMC, hasData, etc. |
| Journal Encoding | 20-30 | Target-encoded top journals |
| Text Length | 3 | Title length, abstract length, has_abstract |
| Keywords | 3 | Keyword count, database keyword presence |

### Training

| Property | Value |
|----------|-------|
| Training Data | ~1,600 papers with metadata |
| CV Folds | 5-10 |
| Tuning Iterations | 10-50 |
| Imbalance Handling | BorderlineSMOTE (0.7 ratio) |
| Normalization | Z-score |

### Hardware Requirements

- **CPU only** - no GPU required
- Training time: 10-50 minutes
- Inference: seconds

### Usage

```python
from pycaret.classification import load_model, predict_model

model = load_model("models/pycaret/pycaret_metadata_classifier")
predictions = predict_model(model, data=df)
```

---

## spaCy Hybrid NER

**Location:** `models/spacy_hybrid_ner/`

**Purpose:** Pattern-based entity extraction using EntityRuler.

### Architecture

| Property | Value |
|----------|-------|
| Type | EntityRuler (pattern matching) |
| Patterns | ~3,000-5,000 patterns |
| Pattern Types | Token patterns + phrase patterns |
| Labels | COM, FUL |

### Pattern Generation Process

1. **Extract Dictionary:** Read labeled bioresource papers (~3,700 resources)
2. **Enrich Full Names:** Regex on abstracts to find full names for abbreviations
3. **Generate Patterns:** Create token and phrase patterns as JSONL
4. **Relabel COM/FUL:** Convert to match V2 entity types

### Pattern Types

**Token Patterns** (for short names):
```json
{"label": "COM", "pattern": [{"TEXT": "PDB"}], "id": "PDB"}
```

**Phrase Patterns** (for full names):
```json
{"label": "FUL", "pattern": "Protein Data Bank", "id": "PDB"}
```

### Hardware Requirements

- **CPU only** - no GPU required
- Fast inference (pattern matching)

### Usage

```python
import spacy

nlp = spacy.load("models/spacy_hybrid_ner")
doc = nlp(text)
for ent in doc.ents:
    print(ent.text, ent.label_)
```

---

## SetFit Introduction Classifier

**Location:** `models/setfit_introduction_classifier/`

**Purpose:** Few-shot classification of medium-confidence papers as introduction or usage.

### Architecture

| Property | Value |
|----------|-------|
| Base Model | `sentence-transformers/all-mpnet-base-v2` |
| Max Sequence Length | 384 tokens |
| Classes | 2 (introduction=1, usage=0) |
| Training Examples | 40 (20 positive, 20 negative) |

### Training Configuration

| Property | Value |
|----------|-------|
| Batch Size | 16 |
| Epochs | 1 |
| Body Learning Rate | 2e-5 |
| Head Learning Rate | 0.01 |
| Loss | CosineSimilarityLoss |
| Distance Metric | cosine_distance |
| Margin | 0.25 |

### Training Data Sources

**Positive Examples (20):**
- Papers with PubMed types: "Database", "Software", "Introductory Journal Article"
- Papers with linguistic score ≥ 7

**Negative Examples (20):**
- Papers with linguistic score ≤ -2

### Performance

| Metric | Value |
|--------|-------|
| Training Accuracy | 100% |
| Training Time | ~2 minutes |
| Inference Time | ~6 minutes for 16K papers |

### Hardware Requirements

- **GPU recommended** for faster inference
- Can run on CPU (slower)

### Usage

```python
from setfit import SetFitModel

model = SetFitModel.from_pretrained("models/setfit_introduction_classifier")
predictions = model.predict(texts)
probabilities = model.predict_proba(texts)
```

---

## Model Versioning

Models are versioned using Git LFS. To update models:

```bash
# Pull latest models
git lfs pull

# Check model files
git lfs ls-files

# Verify model integrity
ls -la models/
```

## Retraining Models

For retraining procedures, see the original training notebooks:
- V2 Classifier: `full_training_pipeline.ipynb`
- V2 NER: `full_training_pipeline.ipynb`
- PyCaret: `pycaret_metadata_training.ipynb`
- spaCy: `spacy_hybrid_ner/scripts/`
- SetFit: `advanced_paper_filtering/scripts/04_train_setfit.py`
