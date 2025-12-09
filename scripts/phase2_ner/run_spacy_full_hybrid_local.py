#!/usr/bin/env python3
"""
Script: Run spaCy Full Hybrid NER - 2022-mid2025 Fresh Run (LOCAL)

EXACT COPY of validation_spacy_v_BERT/scripts/09b_run_spacy_full_hybrid_ner.py
Only paths changed for 2022-mid2025 fresh run.

Purpose: Extract biodata resource entities using FULL HYBRID (EntityRuler + Statistical NER)
Input: Classification union (27,975 papers)
Output: Full hybrid NER results CSV and benchmark JSON

Usage:
    cd /Users/warren/development/GBC/inventory_2022
    source spacy_hybrid_ner/venv/bin/activate
    python unified_bioresource_pipeline/scripts/phase2_ner/run_spacy_full_hybrid_local.py

Requirements:
    - spaCy environment: source spacy_hybrid_ner/venv/bin/activate
    - Input file: unified_bioresource_pipeline/data/phase1_classification/classification_union.csv
    - Model: spacy_hybrid_ner/models/ner_hybrid_v2_com_ful

Runtime: ~55 minutes (CPU, ~8.5 papers/sec for 27,975 papers)
"""

import pandas as pd
import spacy
from tqdm import tqdm
import json
import time
import datetime
import random
import string
from pathlib import Path
import sys

# Configuration - ADAPTED FOR 2022-mid2025 FRESH RUN
INPUT_FILE = "unified_bioresource_pipeline/data/phase1_classification/classification_union.csv"
MODEL_PATH = "spacy_hybrid_ner/models/ner_hybrid_v2_com_ful"
OUTPUT_DIR = "unified_bioresource_pipeline/data/phase2_ner"
BENCHMARK_DIR = "unified_bioresource_pipeline/data/phase2_ner/benchmarks"

# Processing parameters
BATCH_SIZE = 64
PROGRESS_INTERVAL = 1000

# Generate session ID
SESSION_ID = f"{datetime.datetime.now().strftime('%Y-%m-%d')}-{''.join(random.choices(string.ascii_lowercase + string.digits, k=6))}"

# Output files
OUTPUT_FILE = f"{OUTPUT_DIR}/spacy_ner_results_{SESSION_ID}.csv"
BENCHMARK_FILE = f"{BENCHMARK_DIR}/spacy_ner_benchmark_{SESSION_ID}.json"


def load_spacy_full_hybrid(model_path):
    """Load spaCy model with ALL components enabled (Full Hybrid mode)"""
    print("Loading spaCy model (Full Hybrid mode)...")

    # Check model exists
    if not Path(model_path).exists():
        raise FileNotFoundError(f"Model not found: {model_path}")

    # Load model with ALL components
    nlp = spacy.load(model_path)

    # Verify pipeline components
    print(f"\nActive pipeline: {nlp.pipe_names}")

    # Verify all required components are present
    required = ['entity_ruler', 'tok2vec', 'ner']
    for comp in required:
        if comp not in nlp.pipe_names:
            raise ValueError(f"Required component '{comp}' not found in pipeline!")

    print("  ✓ EntityRuler: High-precision pattern matching")
    print("  ✓ tok2vec: Word embeddings")
    print("  ✓ Statistical NER: ML-based discovery")
    print("\n✓ Model ready for full hybrid extraction\n")

    return nlp


def main():
    print("=" * 70)
    print("SPACY FULL HYBRID NER - 2022-MID2025 FRESH RUN")
    print("=" * 70)
    print(f"Session ID: {SESSION_ID}")
    print(f"Input: {INPUT_FILE}")
    print(f"Output: {OUTPUT_FILE}")
    print("=" * 70)
    print()

    # Load input data
    print("Loading input file...")
    df = pd.read_csv(INPUT_FILE)
    print(f"✓ Loaded {len(df):,} papers")

    # Detect ID column
    id_col = None
    for col in ['id', 'publication_id', 'pubmed_id', 'PMID', 'pmid']:
        if col in df.columns:
            id_col = col
            break

    if not id_col:
        raise ValueError(f"No ID column found. Available: {list(df.columns)}")

    print(f"✓ Using ID column: {id_col}")

    # Check for title and abstract
    if 'title' not in df.columns or 'abstract' not in df.columns:
        raise ValueError("Missing 'title' or 'abstract' columns")

    # Combine title + abstract
    df['text'] = df['title'].fillna('') + ' ' + df['abstract'].fillna('')
    print(f"✓ Prepared text for {len(df):,} papers\n")

    # Load model with ALL components enabled
    nlp = load_spacy_full_hybrid(MODEL_PATH)

    # Create output directories
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    Path(BENCHMARK_DIR).mkdir(parents=True, exist_ok=True)

    # Extract entities
    print(f"Extracting entities from {len(df):,} papers...")
    print("=" * 70)

    all_entities = []
    papers_with_entities = 0

    start_time = time.time()

    # Process with batch pipeline
    for i, doc in enumerate(tqdm(nlp.pipe(df['text'], batch_size=BATCH_SIZE),
                                  total=len(df),
                                  desc="Extracting entities")):

        paper_id = df.iloc[i][id_col]
        text = df.iloc[i]['text']

        # Extract entities from document
        doc_entities = []
        for ent in doc.ents:
            # In full hybrid mode, entities can come from EntityRuler (with ent_id_) or Statistical NER (without)
            source = 'ruler' if ent.ent_id_ else 'statistical'
            canonical_id = ent.ent_id_ if ent.ent_id_ else None

            entity = {
                'ID': str(paper_id),
                'text': text,
                'mention': ent.text,
                'label': ent.label_,
                'canonical_id': canonical_id,
                'source': source,
                'start_char': ent.start_char,
                'end_char': ent.end_char
            }
            doc_entities.append(entity)

        if doc_entities:
            papers_with_entities += 1

        all_entities.extend(doc_entities)

    # Calculate stats
    elapsed = time.time() - start_time
    speed = len(df) / elapsed

    print("\n" + "=" * 70)
    print("EXTRACTION COMPLETE")
    print("=" * 70)
    print(f"Papers processed: {len(df):,}")
    print(f"Total entities: {len(all_entities):,}")
    print(f"Papers with entities: {papers_with_entities:,} ({100*papers_with_entities/len(df):.1f}%)")
    print(f"Avg entities/paper: {len(all_entities)/len(df):.2f}")
    print(f"\nProcessing time: {elapsed/60:.2f} minutes")
    print(f"Speed: {speed:.1f} papers/second")
    print("=" * 70)
    print()

    # Save results
    df_results = pd.DataFrame(all_entities)

    print("Saving results...")
    df_results.to_csv(OUTPUT_FILE, index=False)
    print(f"✓ Saved: {OUTPUT_FILE}")
    print(f"  Rows: {len(df_results):,}")

    # Also save with simple name for pipeline
    simple_output = f"{OUTPUT_DIR}/spacy_ner_results.csv"
    df_results.to_csv(simple_output, index=False)
    print(f"✓ Also saved as: {simple_output}")

    # Display statistics
    if len(df_results) > 0:
        print("\nEntity source breakdown:")
        for source, count in df_results['source'].value_counts().items():
            print(f"  {source}: {count:,} ({100*count/len(df_results):.1f}%)")

        # Canonical IDs
        with_canonical = df_results['canonical_id'].notna().sum()
        print(f"\nEntities with canonical IDs: {with_canonical:,} ({100*with_canonical/len(df_results):.1f}%)")

        # Top entities
        print("\nTop 10 most frequent entities:")
        for i, (mention, count) in enumerate(df_results['mention'].value_counts().head(10).items(), 1):
            print(f"  {i:2d}. {mention}: {count:,}")
    else:
        print("\n⚠ WARNING: No entities extracted!")
        print("This suggests the hybrid pipeline is non-functional.")

    print()

    # Create benchmark
    benchmark = {
        "session_id": SESSION_ID,
        "timestamp": datetime.datetime.now().isoformat(),
        "model": {
            "name": "spacy_hybrid_ner_full_hybrid",
            "path": MODEL_PATH,
            "components": nlp.pipe_names,
            "disabled": list(nlp.disabled),
            "mode": "full_hybrid"
        },
        "input": {
            "file": INPUT_FILE,
            "papers": len(df)
        },
        "processing": {
            "batch_size": BATCH_SIZE,
            "time_seconds": round(elapsed, 2),
            "time_minutes": round(elapsed/60, 2),
            "speed_papers_per_sec": round(speed, 2)
        },
        "results": {
            "papers_processed": len(df),
            "papers_with_entities": papers_with_entities,
            "coverage_percent": round(100*papers_with_entities/len(df), 2),
            "total_entities": len(all_entities),
            "avg_entities_per_paper": round(len(all_entities)/len(df), 2),
            "unique_canonical_ids": int(df_results['canonical_id'].nunique()) if len(df_results) > 0 else 0,
            "entities_with_canonical_id": int(df_results['canonical_id'].notna().sum()) if len(df_results) > 0 else 0
        },
        "output": {
            "file": OUTPUT_FILE,
            "rows": len(df_results)
        }
    }

    # Save benchmark
    with open(BENCHMARK_FILE, 'w') as f:
        json.dump(benchmark, f, indent=2)

    print(f"✓ Saved benchmark: {BENCHMARK_FILE}")

    print("\n" + "=" * 70)
    print("✓ SPACY FULL HYBRID NER COMPLETE")
    print("=" * 70)
    print(f"Session: {SESSION_ID}")
    print(f"Entities extracted: {len(df_results):,}")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
