#!/usr/bin/env python3
"""
SetFit Inference Script - Phase 4

Run SetFit inference on medium-score papers using trained model.

Usage:
    # Session-based (PREFERRED):
    python 08_setfit_inference.py --session-dir results/2025-12-04-143052-abc12

    # Legacy mode:
    python 08_setfit_inference.py --auto

    # Custom paths:
    python 08_setfit_inference.py \
        --input-file data/medium_score_papers.csv \
        --model-dir models/setfit_model \
        --output-dir data/setfit_output

Session Mode:
    When --session-dir is provided:
    - Reads from: {session_dir}/03_linguistic/medium_score_papers.csv
    - Model from: models/setfit_introduction_classifier/ (or --model-dir)
    - Outputs to: {session_dir}/04_setfit/

Author: Pipeline Automation
Date: 2025-11-17
Updated: 2025-12-04 (added session-dir support)
"""

import argparse
import os
import sys
import time
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Requires: lib/session_utils.py (run from unified_bioresource_pipeline directory)
# Add lib to path for session utilities
SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

# Import from lib - will fail loudly if lib not found
from lib.session_utils import get_session_path, validate_session_dir

# Default model location
DEFAULT_MODEL_DIR = PIPELINE_ROOT / "models" / "setfit_introduction_classifier"
# Legacy model location
LEGACY_MODEL_DIR = PIPELINE_ROOT.parent / "advanced_paper_filtering" / "results" / "setfit_2025-11-17-134146" / "setfit_introduction_classifier"

def load_setfit_model(model_dir):
    """Load trained SetFit model"""
    from setfit import SetFitModel

    print(f"\nLoading SetFit model from: {model_dir}")
    model = SetFitModel.from_pretrained(model_dir)
    print("✓ Model loaded successfully")
    return model

def load_medium_score_papers(papers_file):
    """Load medium-score papers for classification"""
    print(f"\nLoading medium-score papers from: {papers_file}")
    df = pd.read_csv(papers_file)
    print(f"✓ Loaded {len(df):,} papers")

    # Verify required columns
    required_cols = ['pmid', 'title', 'abstract']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    return df

def prepare_texts(df):
    """Prepare text inputs for SetFit model"""
    print("\nPreparing text inputs...")

    texts = []
    for idx, row in df.iterrows():
        title = str(row['title']) if pd.notna(row['title']) else ""
        abstract = str(row['abstract']) if pd.notna(row['abstract']) else ""

        if abstract:
            text = f"{title}\n\n{abstract}"
        else:
            text = title

        texts.append(text)

    print(f"✓ Prepared {len(texts):,} text inputs")
    return texts

def run_inference(model, texts, batch_size=32):
    """Run SetFit inference on texts"""
    print(f"\nRunning inference on {len(texts):,} papers...")
    print(f"Batch size: {batch_size}")

    start_time = time.time()

    # Get predictions and probabilities
    predictions = model.predict(texts)
    proba = model.predict_proba(texts)

    # Extract confidence scores (probability of positive class)
    if len(proba.shape) == 2:
        # Binary classification: get probability of class 1
        confidence = proba[:, 1]
    else:
        # Single probability per prediction
        confidence = proba

    elapsed = time.time() - start_time
    print(f"✓ Inference complete in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    print(f"  Average: {elapsed/len(texts):.3f} sec/paper")

    return predictions, confidence

def categorize_by_confidence(predictions, confidence):
    """Categorize predictions by confidence tier"""
    print("\nCategorizing by confidence tier...")

    # Define tiers
    high_mask = confidence >= 0.70
    medium_mask = (confidence >= 0.60) & (confidence < 0.70)
    low_mask = confidence < 0.60

    # Count by tier and prediction
    intro_high = sum(predictions & high_mask)
    intro_medium = sum(predictions & medium_mask)
    intro_low = sum(predictions & low_mask)

    usage_high = sum((~predictions) & high_mask)
    usage_medium = sum((~predictions) & medium_mask)
    usage_low = sum((~predictions) & low_mask)

    print(f"\n  Introductions:")
    print(f"    High confidence (≥0.70):    {intro_high:,} ({intro_high/len(predictions)*100:.1f}%)")
    print(f"    Medium confidence (0.60-0.69): {intro_medium:,} ({intro_medium/len(predictions)*100:.1f}%)")
    print(f"    Low confidence (<0.60):     {intro_low:,} ({intro_low/len(predictions)*100:.1f}%)")
    print(f"    TOTAL:                      {sum(predictions):,}")

    print(f"\n  Usage:")
    print(f"    High confidence (≥0.70):    {usage_high:,} ({usage_high/len(predictions)*100:.1f}%)")
    print(f"    Medium confidence (0.60-0.69): {usage_medium:,} ({usage_medium/len(predictions)*100:.1f}%)")
    print(f"    Low confidence (<0.60):     {usage_low:,} ({usage_low/len(predictions)*100:.1f}%)")
    print(f"    TOTAL:                      {sum(~predictions):,}")

    return {
        'intro_high': intro_high,
        'intro_medium': intro_medium,
        'intro_low': intro_low,
        'usage_high': usage_high,
        'usage_medium': usage_medium,
        'usage_low': usage_low
    }

def save_results(df, predictions, confidence, output_dir):
    """Save classification results"""
    print(f"\nSaving results to: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    # Add predictions to dataframe
    results_df = df.copy()
    results_df['setfit_prediction'] = predictions.astype(int)
    results_df['setfit_confidence'] = confidence

    # Add confidence tier
    conditions = [
        confidence >= 0.70,
        (confidence >= 0.60) & (confidence < 0.70),
        confidence < 0.60
    ]
    choices = ['high', 'medium', 'low']
    results_df['confidence_tier'] = np.select(conditions, choices, default='unknown')

    # Add prediction label
    results_df['prediction_label'] = results_df['setfit_prediction'].map({
        1: 'INTRODUCTION',
        0: 'USAGE'
    })

    # Save introductions
    intro_df = results_df[results_df['setfit_prediction'] == 1].copy()
    intro_df = intro_df.sort_values('setfit_confidence', ascending=False)
    intro_file = os.path.join(output_dir, 'setfit_classified_introductions.csv')
    intro_df.to_csv(intro_file, index=False)
    print(f"✓ Saved {len(intro_df):,} introductions to: {intro_file}")

    # Save usage
    usage_df = results_df[results_df['setfit_prediction'] == 0].copy()
    usage_df = usage_df.sort_values('setfit_confidence', ascending=False)
    usage_file = os.path.join(output_dir, 'setfit_classified_usage.csv')
    usage_df.to_csv(usage_file, index=False)
    print(f"✓ Saved {len(usage_df):,} usage papers to: {usage_file}")

    # Save combined results
    combined_file = os.path.join(output_dir, 'setfit_all_results.csv')
    results_df = results_df.sort_values('setfit_confidence', ascending=False)
    results_df.to_csv(combined_file, index=False)
    print(f"✓ Saved {len(results_df):,} total results to: {combined_file}")

    return intro_df, usage_df, results_df

def generate_summary(stats, intro_df, usage_df, output_dir, elapsed_time):
    """Generate summary report"""
    print("\nGenerating summary report...")

    summary_file = os.path.join(output_dir, 'setfit_inference_summary.txt')

    with open(summary_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("SETFIT INFERENCE SUMMARY - PHASE 1\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total papers classified: {len(intro_df) + len(usage_df):,}\n")
        f.write(f"Inference time: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)\n")
        f.write("\n")

        f.write("CLASSIFICATION RESULTS\n")
        f.write("-" * 80 + "\n")
        total = len(intro_df) + len(usage_df)
        f.write(f"Introductions: {len(intro_df):,} ({len(intro_df)/total*100:.1f}%)\n")
        f.write(f"Usage:         {len(usage_df):,} ({len(usage_df)/total*100:.1f}%)\n")
        f.write("\n")

        f.write("CONFIDENCE TIERS - INTRODUCTIONS\n")
        f.write("-" * 80 + "\n")
        f.write(f"High (≥0.70):       {stats['intro_high']:,} ({stats['intro_high']/len(intro_df)*100:.1f}% of intros)\n")
        f.write(f"Medium (0.60-0.69): {stats['intro_medium']:,} ({stats['intro_medium']/len(intro_df)*100:.1f}% of intros)\n")
        f.write(f"Low (<0.60):        {stats['intro_low']:,} ({stats['intro_low']/len(intro_df)*100:.1f}% of intros)\n")
        f.write("\n")

        f.write("CONFIDENCE TIERS - USAGE\n")
        f.write("-" * 80 + "\n")
        f.write(f"High (≥0.70):       {stats['usage_high']:,} ({stats['usage_high']/len(usage_df)*100:.1f}% of usage)\n")
        f.write(f"Medium (0.60-0.69): {stats['usage_medium']:,} ({stats['usage_medium']/len(usage_df)*100:.1f}% of usage)\n")
        f.write(f"Low (<0.60):        {stats['usage_low']:,} ({stats['usage_low']/len(usage_df)*100:.1f}% of usage)\n")
        f.write("\n")

        f.write("CONFIDENCE STATISTICS - INTRODUCTIONS\n")
        f.write("-" * 80 + "\n")
        intro_conf = intro_df['setfit_confidence']
        f.write(f"Mean:   {intro_conf.mean():.4f}\n")
        f.write(f"Median: {intro_conf.median():.4f}\n")
        f.write(f"Min:    {intro_conf.min():.4f}\n")
        f.write(f"Max:    {intro_conf.max():.4f}\n")
        f.write(f"Std:    {intro_conf.std():.4f}\n")
        f.write("\n")

        f.write("CONFIDENCE STATISTICS - USAGE\n")
        f.write("-" * 80 + "\n")
        usage_conf = usage_df['setfit_confidence']
        f.write(f"Mean:   {usage_conf.mean():.4f}\n")
        f.write(f"Median: {usage_conf.median():.4f}\n")
        f.write(f"Min:    {usage_conf.min():.4f}\n")
        f.write(f"Max:    {usage_conf.max():.4f}\n")
        f.write(f"Std:    {usage_conf.std():.4f}\n")
        f.write("\n")

        f.write("=" * 80 + "\n")

    print(f"✓ Summary saved to: {summary_file}")

    # Also print to console
    print("\n" + "=" * 80)
    print("INFERENCE COMPLETE")
    print("=" * 80)
    print(f"Total papers: {total:,}")
    print(f"Introductions: {len(intro_df):,} ({len(intro_df)/total*100:.1f}%)")
    print(f"  High confidence: {stats['intro_high']:,}")
    print(f"  Medium confidence: {stats['intro_medium']:,}")
    print(f"Usage: {len(usage_df):,} ({len(usage_df)/total*100:.1f}%)")
    print(f"  High confidence: {stats['usage_high']:,}")
    print(f"  Medium confidence: {stats['usage_medium']:,}")
    print("=" * 80)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Phase 4: SetFit Inference on medium-score papers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Session-based mode (PREFERRED):
  python 08_setfit_inference.py --session-dir results/2025-12-04-143052-abc12

  # Legacy mode (auto-detect):
  python 08_setfit_inference.py --auto

  # Custom paths:
  python 08_setfit_inference.py \\
      --input-file data/medium_score_papers.csv \\
      --model-dir models/setfit_model \\
      --output-dir data/setfit_output
        """
    )

    # Session mode arguments
    parser.add_argument("--session-dir", type=Path,
                        help="Session directory path (e.g., results/2025-12-04-143052-abc12)")

    # Input/output arguments
    parser.add_argument("--input-file", type=Path, help="Path to medium-score papers CSV")
    parser.add_argument("--model-dir", type=Path, help="Path to trained SetFit model directory")
    parser.add_argument("--output-dir", type=Path, help="Output directory for results")
    parser.add_argument("--auto", action="store_true", help="Auto-detect files in legacy paths")

    # Inference options
    parser.add_argument("--batch-size", type=int, default=32, help="Batch size for inference (default: 32)")

    return parser.parse_args()


def find_model_dir():
    """Find SetFit model directory, checking multiple locations."""
    # Check default location in pipeline
    if DEFAULT_MODEL_DIR.exists():
        return DEFAULT_MODEL_DIR

    # Check legacy location
    if LEGACY_MODEL_DIR.exists():
        return LEGACY_MODEL_DIR

    return None


def main():
    """Main execution"""
    args = parse_args()
    start_time = time.time()

    print("=" * 80)
    print("SETFIT INFERENCE - PHASE 4")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Determine input/output paths
    if args.session_dir:
        # SESSION MODE (PREFERRED)
        print("MODE: Session-based")
        print(f"Session: {args.session_dir}\n")

        # Validate session directory
        session_path = Path(args.session_dir).resolve()
        if not session_path.exists():
            print(f"ERROR: Session directory not found: {session_path}")
            sys.exit(1)

        # Validate session structure
        try:
            validate_session_dir(session_path, required_phases=['03_linguistic'])
        except ValueError as e:
            print(f"ERROR: Invalid session directory: {e}")
            sys.exit(1)

        papers_file = get_session_path(args.session_dir, '03_linguistic', 'medium_score_papers.csv')
        output_dir = get_session_path(args.session_dir, '04_setfit')

        # Model directory (use provided or find)
        if args.model_dir:
            model_dir = args.model_dir
        else:
            model_dir = find_model_dir()
            if model_dir is None:
                print(f"ERROR: SetFit model not found in:")
                print(f"  - {DEFAULT_MODEL_DIR}")
                print(f"  - {LEGACY_MODEL_DIR}")
                print("\nPlease provide --model-dir or copy model to one of the above locations")
                sys.exit(1)

    elif args.input_file:
        # LEGACY: Explicit paths
        print("MODE: Legacy (explicit paths)")
        papers_file = args.input_file
        output_dir = args.output_dir if args.output_dir else PIPELINE_ROOT / "results" / "setfit_inference"
        model_dir = args.model_dir if args.model_dir else find_model_dir()

        if model_dir is None:
            print("ERROR: SetFit model not found. Please provide --model-dir")
            sys.exit(1)

    elif args.auto:
        # LEGACY: Auto-detect
        print("MODE: Legacy (auto-detect)")
        base_dir = PIPELINE_ROOT.parent

        # Try to find papers file in various locations
        candidate_paths = [
            base_dir / "advanced_paper_filtering" / "data" / "results" / "medium_score_papers.csv",
            PIPELINE_ROOT / "data" / "phase3_linguistic" / "medium_score_papers.csv",
        ]
        papers_file = None
        for path in candidate_paths:
            if path.exists():
                papers_file = path
                break

        if papers_file is None:
            print("ERROR: Could not find medium_score_papers.csv")
            print("Searched:")
            for p in candidate_paths:
                print(f"  - {p}")
            sys.exit(1)

        output_dir = PIPELINE_ROOT / "results" / "setfit_inference"
        model_dir = find_model_dir()

        if model_dir is None:
            print("ERROR: SetFit model not found. Please provide --model-dir")
            sys.exit(1)

    else:
        print("ERROR: Must provide --session-dir, --input-file, or use --auto")
        sys.exit(1)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Model directory: {model_dir}")
    print(f"Papers file: {papers_file}")
    print(f"Output directory: {output_dir}")

    # Check if model exists
    if not model_dir.exists():
        print(f"\nERROR: Model directory not found: {model_dir}")
        sys.exit(1)

    if not papers_file.exists():
        print(f"\nERROR: Papers file not found: {papers_file}")
        sys.exit(1)

    # Step 1: Load model
    model = load_setfit_model(str(model_dir))

    # Step 2: Load papers
    df = load_medium_score_papers(str(papers_file))

    # Step 3: Prepare texts
    texts = prepare_texts(df)

    # Step 4: Run inference
    predictions, confidence = run_inference(model, texts, batch_size=args.batch_size)

    # Step 5: Categorize by confidence
    stats = categorize_by_confidence(predictions, confidence)

    # Step 6: Save results
    intro_df, usage_df, results_df = save_results(df, predictions, confidence, str(output_dir))

    # Step 7: Generate summary
    elapsed = time.time() - start_time
    generate_summary(stats, intro_df, usage_df, str(output_dir), elapsed)

    print(f"\nTotal execution time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")

    # Next step guidance
    print("\n" + "=" * 80)
    print("NEXT STEP")
    print("=" * 80)
    if args.session_dir:
        print(f"\nRun Phase 5 - Create Paper Sets:")
        print(f"  python scripts/phase5_mapping/09_create_paper_sets.py --session-dir {args.session_dir}")
    else:
        print("\nRun Phase 5 - Create Paper Sets")

    print("\n✓ Phase 4 complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
