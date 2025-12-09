#!/usr/bin/env python3
"""
PyCaret Classification - Local Script for 2022-mid2025 Fresh Run

Runs PyCaret metadata classifier on 98,571 papers locally (no GPU required).
Expected runtime: ~10-15 minutes on CPU.

Usage:
    cd /Users/warren/development/GBC/inventory_2022
    source pycaret_env/bin/activate  # or biodata_modern_env
    python unified_bioresource_pipeline/scripts/phase1_classification/02_run_pycaret_local.py

Output:
    unified_bioresource_pipeline/data/phase1_classification/pycaret_classification_98k_{session_id}.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import warnings
import time
import random
import string
import os

warnings.filterwarnings('ignore')

print("=" * 80)
print("PYCARET CLASSIFICATION - 2022-mid2025 FRESH RUN")
print("=" * 80)

# ============================================
# CONFIGURATION
# ============================================

BASE_DIR = Path(__file__).parent.parent.parent.parent  # inventory_2022
PIPELINE_DIR = BASE_DIR / "unified_bioresource_pipeline"

# Input data - 2022-mid2025
INPUT_FILE = BASE_DIR / "data/final_query_v5.1_2022_mid2025/query_results.csv"

# Model path
MODEL_PATH = BASE_DIR / "pycaret_models/test_mode_true/pycaret_metadata_classifier_v1"

# Output path
OUTPUT_DIR = PIPELINE_DIR / "data/phase1_classification"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Session ID
SESSION_ID = f"{datetime.now().strftime('%Y-%m-%d')}-{''.join(random.choices(string.ascii_lowercase + string.digits, k=6))}"
TIMESTAMP = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

print(f"\nConfiguration:")
print(f"   Session ID: {SESSION_ID}")
print(f"   Input: {INPUT_FILE}")
print(f"   Model: {MODEL_PATH}.pkl")
print(f"   Output: {OUTPUT_DIR}")
print("=" * 80)

# ============================================
# VERIFY FILES EXIST
# ============================================

print("\nVerifying files...")

if not INPUT_FILE.exists():
    raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")
print(f"   Input file: {INPUT_FILE.stat().st_size / 1024 / 1024:.1f} MB")

if not MODEL_PATH.with_suffix('.pkl').exists():
    raise FileNotFoundError(f"Model not found: {MODEL_PATH}.pkl")
print(f"   Model file: {MODEL_PATH.with_suffix('.pkl').stat().st_size / 1024:.1f} KB")

# ============================================
# LOAD DATA
# ============================================

print("\nLoading input data...")
start_load = time.time()

df = pd.read_csv(INPUT_FILE, low_memory=False)
df['pmid'] = df['id'].astype(str)

# Deduplicate
n_before = len(df)
df = df.drop_duplicates(subset=['id'], keep='first')
n_after = len(df)

print(f"   Loaded: {n_before:,} papers")
if n_before > n_after:
    print(f"   Deduplicated: {n_after:,} unique papers ({n_before - n_after:,} duplicates removed)")

# Check abstract coverage
has_abstract = df['abstract'].notna() & (df['abstract'] != '')
print(f"   Papers with abstracts: {has_abstract.sum():,} ({100*has_abstract.sum()/len(df):.1f}%)")
print(f"   Papers without abstracts: {(~has_abstract).sum():,} ({100*(~has_abstract).sum()/len(df):.1f}%)")

load_time = time.time() - start_load
print(f"   Load time: {load_time:.1f}s")

# ============================================
# FEATURE ENGINEERING FUNCTION
# ============================================

def engineer_features_for_model(df, model):
    """Engineer features to match EXACTLY what the model expects."""
    expected_features = list(model.feature_names_in_)
    print(f"\nEngineering {len(expected_features)} features...")

    df = df.copy()

    def convert_to_binary(val):
        if pd.isna(val) or val == '' or val == 'N' or val == False or val == 'false':
            return 0
        elif val == 'Y' or val == True or val == 'true':
            return 1
        else:
            try:
                return int(val)
            except:
                return 0

    # Parse list columns
    def safe_parse_list(x):
        if pd.isna(x) or x == '':
            return []
        if isinstance(x, str) and x.startswith('['):
            try:
                parsed = json.loads(x)
                # Filter out None values that come from JSON null
                return [item for item in parsed if item is not None]
            except:
                return []
        return []

    df['meshTerms_list'] = df['meshTerms'].apply(safe_parse_list) if 'meshTerms' in df.columns else [[]] * len(df)
    df['pubType_list'] = df['pubType'].apply(safe_parse_list) if 'pubType' in df.columns else [[]] * len(df)
    df['keywords_list'] = df['keywords'].apply(safe_parse_list) if 'keywords' in df.columns else [[]] * len(df)

    # Base features
    df['citedByCount'] = df['citedByCount'].fillna(0).astype(int) if 'citedByCount' in df.columns else 0
    df['pubYear'] = df['pubYear'].fillna(2023).astype(int) if 'pubYear' in df.columns else 2023

    # Binary access features
    binary_features = ['inEPMC', 'inPMC', 'hasDbCrossReferences', 'hasData', 'hasSuppl', 'isOpenAccess']
    for feat in binary_features:
        if feat in df.columns:
            df[feat] = df[feat].apply(convert_to_binary)
        else:
            df[feat] = 0

    # Computed features - with safeguards against infinity/NaN
    df['log_citations'] = np.log1p(df['citedByCount'].clip(lower=0))
    df['years_since_pub'] = (2025 - df['pubYear']).clip(lower=0, upper=100)  # Clip to reasonable range
    df['citation_age_ratio'] = df['citedByCount'] / (df['years_since_pub'] + 1)
    df['citation_age_ratio'] = df['citation_age_ratio'].replace([np.inf, -np.inf], 0).fillna(0).clip(upper=10000)
    df['is_highly_cited'] = (df['citedByCount'] > df['citedByCount'].quantile(0.75)).astype(int)
    df['is_uncited'] = (df['citedByCount'] == 0).astype(int)
    df['access_score'] = df[['hasDbCrossReferences', 'hasData', 'hasSuppl', 'isOpenAccess']].sum(axis=1)
    df['is_recent'] = (df['pubYear'] >= 2018).astype(int)
    df['is_old'] = (df['pubYear'] < 2010).astype(int)

    # MeSH features
    df['mesh_term_count'] = df['meshTerms_list'].apply(len)

    # Pubtype features
    df['pubtype_count'] = df['pubType_list'].apply(len)
    df['is_review'] = df['pubType_list'].apply(lambda x: 1 if 'Review' in x else 0)
    df['is_letter'] = df['pubType_list'].apply(lambda x: 1 if 'Letter' in x else 0)

    # Text features
    df['title_length'] = df['title'].fillna('').apply(len)
    df['abstract_length'] = df['abstract'].fillna('').apply(len)
    df['has_abstract'] = (df['abstract_length'] > 0).astype(int)

    # Keyword features
    df['keyword_count'] = df['keywords_list'].apply(len)
    database_keywords = ['database', 'repository', 'resource', 'portal', 'knowledgebase',
                         'archive', 'registry', 'catalog', 'collection']
    df['has_database_keyword'] = df['keywords_list'].apply(
        lambda x: 1 if any(kw.lower() in [k.lower() for k in x if k and isinstance(k, str)] for kw in database_keywords) else 0
    )

    # Create specific MeSH features
    for feat in expected_features:
        if feat.startswith('mesh_') and feat != 'mesh_term_count':
            mesh_name = feat[5:].replace('_', ' ').replace(',', ', ')
            df[feat] = df['meshTerms_list'].apply(
                lambda x: 1 if any(mesh_name.lower() in term.lower() for term in x if term and isinstance(term, str)) else 0
            )

    # Create specific pubtype features
    pubtype_mapping = {
        'pubtype_Journal_Article': 'Journal Article',
        'pubtype_Review': 'Review',
        'pubtype_Research_Support_NIH_Extramura': 'Research Support, N.I.H., Extramural',
        'pubtype_Research_Support_Non-US_Govt': "Research Support, Non-U.S. Gov't",
        'pubtype_Research_Support_US_Govt_PHS': "Research Support, U.S. Gov't, P.H.S.",
        'pubtype_Comparative_Study': 'Comparative Study',
        'pubtype_Letter': 'Letter',
        'pubtype_Comment': 'Comment',
        'pubtype_Editorial': 'Editorial',
        'pubtype_Case_Reports': 'Case Reports',
    }
    for feat, pubtype in pubtype_mapping.items():
        if feat in expected_features:
            df[feat] = df['pubType_list'].apply(lambda x: 1 if pubtype in x else 0)

    # Create specific journal features
    for feat in expected_features:
        if feat.startswith('journal_'):
            journal_pattern = feat[8:]
            df[feat] = df['journalTitle'].fillna('').apply(
                lambda x: 1 if journal_pattern.lower().replace('_', ' ') in x.lower() else 0
            ) if 'journalTitle' in df.columns else 0

    # Add label column (required by PyCaret)
    df['label'] = 0.5

    # Ensure all expected features exist
    for feat in expected_features:
        if feat not in df.columns:
            df[feat] = 0

    # Select features in EXACT order
    X = df[expected_features].copy()
    X = X.fillna(0)

    # Final safeguard: replace any remaining inf values
    X = X.replace([np.inf, -np.inf], 0)

    # Check for any remaining problematic values
    if X.isin([np.inf, -np.inf]).any().any():
        print("   WARNING: Still have infinity values after cleanup!")
    if X.isna().any().any():
        print("   WARNING: Still have NaN values after cleanup!")

    print(f"   Created {len(expected_features)} features")
    return X

# ============================================
# LOAD PYCARET MODEL
# ============================================

from pycaret.classification import load_model, predict_model

print("\nLoading PyCaret model...")
model = load_model(str(MODEL_PATH))
n_features = len(model.feature_names_in_)
print(f"   Model loaded ({n_features} features)")

# ============================================
# RUN PREDICTIONS
# ============================================

print("\n" + "=" * 80)
print("RUNNING PYCARET CLASSIFICATION")
print("=" * 80)

start_time = time.time()

# Engineer features
X = engineer_features_for_model(df, model)

feature_time = time.time() - start_time
print(f"   Feature engineering: {feature_time:.1f}s")

# Make predictions
print(f"\nPredicting on {len(X):,} papers...")
predict_start = time.time()

predictions = predict_model(model, data=X)

predict_time = time.time() - predict_start
print(f"   Prediction time: {predict_time:.1f}s")
print(f"   Speed: {len(X)/predict_time:.0f} papers/second")

# ============================================
# SAVE RESULTS
# ============================================

# Create results dataframe
df_results = pd.DataFrame({
    'publication_id': df['id'].values,
    'title': df['title'].values,
    'abstract': df['abstract'].values,
    'pycaret_prediction': predictions['prediction_label'].values,
    'pycaret_score': predictions['prediction_score'].values,
    'pycaret_positive': (predictions['prediction_label'] == 1).astype(int).values
})

# Calculate statistics
total = len(df_results)
predicted_positive = (df_results['pycaret_positive'] == 1).sum()
predicted_negative = total - predicted_positive

total_time = time.time() - start_time

print(f"\nClassification Statistics:")
print(f"   Total papers: {total:,}")
print(f"   Predicted bio-resource: {predicted_positive:,} ({predicted_positive/total*100:.1f}%)")
print(f"   Predicted NOT bio-resource: {predicted_negative:,} ({predicted_negative/total*100:.1f}%)")

# Score distribution
print(f"\nPrediction Scores:")
print(f"   Mean: {df_results['pycaret_score'].mean():.3f}")
print(f"   Median: {df_results['pycaret_score'].median():.3f}")

# Save results
output_file = OUTPUT_DIR / f"pycaret_classification_98k_{SESSION_ID}.csv"
df_results.to_csv(output_file, index=False)

print(f"\nResults saved to: {output_file}")
print(f"   File size: {output_file.stat().st_size / 1024 / 1024:.1f} MB")

# Also save with simple name for pipeline
simple_output = OUTPUT_DIR / "pycaret_classification_98k.csv"
df_results.to_csv(simple_output, index=False)
print(f"   Also saved as: {simple_output}")

# ============================================
# SAVE SUMMARY
# ============================================

summary = {
    'session_id': SESSION_ID,
    'timestamp': datetime.now().isoformat(),
    'dataset': '2022-mid2025 Fresh Run',
    'input_file': str(INPUT_FILE),
    'total_papers': int(total),
    'predicted_positive': int(predicted_positive),
    'predicted_negative': int(predicted_negative),
    'positive_percentage': float(predicted_positive/total*100),
    'processing_time_seconds': float(total_time),
    'papers_per_second': float(total/total_time),
    'output_file': str(output_file)
}

summary_file = OUTPUT_DIR / f"pycaret_summary_{SESSION_ID}.json"
with open(summary_file, 'w') as f:
    json.dump(summary, f, indent=2)

print(f"   Summary: {summary_file}")

# ============================================
# COMPLETION
# ============================================

print(f"\n" + "=" * 80)
print("PYCARET CLASSIFICATION COMPLETE")
print("=" * 80)
print(f"\nSession ID: {SESSION_ID}")
print(f"Total time: {total_time:.1f}s ({total/total_time:.0f} papers/sec)")
print(f"\nNext steps:")
print(f"   1. Wait for V2 classification from Colab")
print(f"   2. Create classification union (V2 OR PyCaret)")
print(f"   3. Run Phase 2 NER")
print("=" * 80)
