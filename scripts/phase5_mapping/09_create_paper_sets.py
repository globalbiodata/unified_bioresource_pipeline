#!/usr/bin/env python3
"""
Phase 5: Create Three Paper Sets for Filtering Strategy Comparison

Set A: Linguistic Only (ling_score >= threshold)
Set B: SetFit Only (confidence >= threshold, high + medium)
Set C: Union of A + B (deduplicated)

Usage:
    # Session-based (PREFERRED):
    python 09_create_paper_sets.py --session-dir results/2025-12-04-143052-abc12

    # Legacy mode:
    python 09_create_paper_sets.py --auto

    # Custom paths (legacy):
    python 09_create_paper_sets.py \
        --linguistic-file data/linguistic_scored.csv \
        --setfit-file data/setfit_introductions.csv \
        --output-dir data/paper_sets

Session Mode:
    When --session-dir is provided:
    - Reads from: {session_dir}/03_linguistic/all_scored_papers.csv
    - Reads from: {session_dir}/04_setfit/setfit_classified_introductions.csv
    - Outputs to: {session_dir}/05_mapping/

Author: Pipeline Automation
Date: 2025-11-18
Updated: 2025-12-04 (added session-dir support, argparse)
"""

import argparse
import pandas as pd
import json
import sys
from pathlib import Path
from datetime import datetime

# Requires: lib/session_utils.py (run from unified_bioresource_pipeline directory)
# Add lib to path for session utilities
SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

# Import from lib - will fail loudly if lib not found
from lib.session_utils import get_session_path, validate_session_dir

# Legacy paths
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent  # inventory_2022
LEGACY_LINGUISTIC_FILE = PROJECT_ROOT / "advanced_paper_filtering/data/results/final_classified_introductions.csv"
LEGACY_SETFIT_FILE = PROJECT_ROOT / "pipeline_synthesis_2025-11-18/results/setfit_inference/setfit_classified_introductions.csv"
LEGACY_OUTPUT_DIR = PROJECT_ROOT / "pipeline_synthesis_2025-11-18/data/paper_sets"

# Alternative legacy paths
ALT_LINGUISTIC_FILE = PIPELINE_ROOT / "data" / "phase3_linguistic" / "all_scored_papers.csv"
ALT_SETFIT_FILE = PIPELINE_ROOT / "data" / "phase4_setfit" / "setfit_classified_introductions.csv"

# Default thresholds
DEFAULT_LINGUISTIC_THRESHOLD = 3.0
DEFAULT_SETFIT_THRESHOLD = 0.60


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Phase 5: Create three paper sets for filtering strategy comparison",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Session-based mode (PREFERRED):
  python 09_create_paper_sets.py --session-dir results/2025-12-04-143052-abc12

  # Legacy mode (auto-detect):
  python 09_create_paper_sets.py --auto

  # Custom paths (legacy):
  python 09_create_paper_sets.py \\
      --linguistic-file data/linguistic_scored.csv \\
      --setfit-file data/setfit_introductions.csv \\
      --output-dir data/paper_sets
        """
    )

    # Session mode arguments
    parser.add_argument("--session-dir", type=Path,
                        help="Session directory path (e.g., results/2025-12-04-143052-abc12)")

    # Legacy mode arguments
    parser.add_argument("--linguistic-file", type=Path,
                        help="Path to linguistic scored papers CSV")
    parser.add_argument("--setfit-file", type=Path,
                        help="Path to SetFit classified introductions CSV")
    parser.add_argument("--output-dir", type=Path,
                        help="Output directory for paper sets")
    parser.add_argument("--auto", action="store_true",
                        help="Auto-detect files in legacy paths")

    # Threshold parameters
    parser.add_argument("--linguistic-threshold", type=float, default=DEFAULT_LINGUISTIC_THRESHOLD,
                        help=f"Linguistic score threshold for Set A (default: {DEFAULT_LINGUISTIC_THRESHOLD})")
    parser.add_argument("--setfit-threshold", type=float, default=DEFAULT_SETFIT_THRESHOLD,
                        help=f"SetFit confidence threshold for Set B (default: {DEFAULT_SETFIT_THRESHOLD})")

    return parser.parse_args()


def find_legacy_files():
    """Auto-detect legacy file paths."""
    print("=" * 80)
    print("AUTO-DETECTING LEGACY FILES")
    print("=" * 80)

    linguistic_file = None
    setfit_file = None
    output_dir = None

    # Try unified_bioresource_pipeline paths first
    if ALT_LINGUISTIC_FILE.exists() and ALT_SETFIT_FILE.exists():
        print(f"\nFound files in unified_bioresource_pipeline:")
        linguistic_file = ALT_LINGUISTIC_FILE
        setfit_file = ALT_SETFIT_FILE
        output_dir = PIPELINE_ROOT / "data" / "phase5_mapping"
        print(f"  Linguistic: {linguistic_file.name}")
        print(f"  SetFit: {setfit_file.name}")
        return linguistic_file, setfit_file, output_dir

    # Fall back to legacy paths
    if LEGACY_LINGUISTIC_FILE.exists() and LEGACY_SETFIT_FILE.exists():
        print(f"\nFound files in legacy paths:")
        linguistic_file = LEGACY_LINGUISTIC_FILE
        setfit_file = LEGACY_SETFIT_FILE
        output_dir = LEGACY_OUTPUT_DIR
        print(f"  Linguistic: {linguistic_file.name}")
        print(f"  SetFit: {setfit_file.name}")
        return linguistic_file, setfit_file, output_dir

    # Report what's missing
    print("\nERROR: Could not find required files!")
    print(f"\nSearched for linguistic file:")
    print(f"  {ALT_LINGUISTIC_FILE}")
    print(f"  {LEGACY_LINGUISTIC_FILE}")
    print(f"\nSearched for SetFit file:")
    print(f"  {ALT_SETFIT_FILE}")
    print(f"  {LEGACY_SETFIT_FILE}")
    sys.exit(1)


def main():
    args = parse_args()
    start_time = datetime.now()

    print("=" * 80)
    print("PHASE 5: CREATE THREE PAPER SETS")
    print("=" * 80)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Determine input/output paths
    if args.session_dir:
        # SESSION MODE (PREFERRED)
        print(f"\nMODE: Session-based")
        print(f"Session: {args.session_dir}\n")

        # Validate session directory
        session_path = Path(args.session_dir).resolve()
        if not session_path.exists():
            print(f"ERROR: Session directory not found: {session_path}")
            sys.exit(1)

        # Validate session structure
        try:
            validate_session_dir(session_path, required_phases=['03_linguistic', '04_setfit'])
        except ValueError as e:
            print(f"ERROR: Invalid session directory: {e}")
            sys.exit(1)

        linguistic_file = get_session_path(args.session_dir, '03_linguistic', 'all_scored_papers.csv')
        setfit_file = get_session_path(args.session_dir, '04_setfit', 'setfit_classified_introductions.csv')
        output_dir = get_session_path(args.session_dir, '05_mapping')

    elif args.linguistic_file and args.setfit_file:
        # LEGACY: Explicit paths
        print(f"\nMODE: Legacy (explicit paths)")
        linguistic_file = args.linguistic_file
        setfit_file = args.setfit_file
        output_dir = args.output_dir if args.output_dir else LEGACY_OUTPUT_DIR

    elif args.auto or not (args.linguistic_file or args.setfit_file):
        # LEGACY: Auto-detect
        print(f"\nMODE: Legacy (auto-detect)")
        linguistic_file, setfit_file, output_dir = find_legacy_files()

    else:
        print("ERROR: Must provide --session-dir, or both --linguistic-file and --setfit-file, or use --auto")
        sys.exit(1)

    # Use custom thresholds if provided
    ling_threshold = args.linguistic_threshold
    setfit_threshold = args.setfit_threshold

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Verify input files exist
    if not linguistic_file.exists():
        print(f"ERROR: Linguistic file not found: {linguistic_file}")
        sys.exit(1)
    if not setfit_file.exists():
        print(f"ERROR: SetFit file not found: {setfit_file}")
        sys.exit(1)

    print(f"\nInput files:")
    print(f"  Linguistic: {linguistic_file}")
    print(f"  SetFit: {setfit_file}")
    print(f"\nThresholds:")
    print(f"  Linguistic score: >= {ling_threshold}")
    print(f"  SetFit confidence: >= {setfit_threshold}")
    print(f"\nOutput directory: {output_dir}")

    # ========================================================================
    # SET A: LINGUISTIC ONLY
    # ========================================================================
    print("\n" + "=" * 80)
    print("Loading Set A: Linguistic Only (ling_score >= {})...".format(ling_threshold))
    df_linguistic = pd.read_csv(linguistic_file)
    print(f"  Loaded: {len(df_linguistic):,} papers")

    # Filter by linguistic score threshold
    df_linguistic_filtered = df_linguistic[df_linguistic['ling_score'] >= ling_threshold].copy()
    print(f"  After score filter (>= {ling_threshold}): {len(df_linguistic_filtered):,} papers")

    # Keep only pmid and source info
    set_a = df_linguistic_filtered[['pmid']].copy()
    set_a['source'] = 'linguistic'
    set_a['ling_score'] = df_linguistic_filtered['ling_score']

    print(f"  Set A papers: {len(set_a):,}")

    # ========================================================================
    # SET B: SETFIT ONLY (confidence >= threshold)
    # ========================================================================
    print("\n" + "=" * 80)
    print("Loading Set B: SetFit (confidence >= {})...".format(setfit_threshold))
    df_setfit_all = pd.read_csv(setfit_file)
    print(f"  Loaded: {len(df_setfit_all):,} SetFit introductions")

    # Filter for confidence >= threshold (high + medium)
    df_setfit_filtered = df_setfit_all[df_setfit_all['setfit_confidence'] >= setfit_threshold].copy()
    print(f"  After confidence filter (>= {setfit_threshold}): {len(df_setfit_filtered):,} papers")

    # Breakdown by tier
    high_conf = len(df_setfit_filtered[df_setfit_filtered['setfit_confidence'] >= 0.70])
    medium_conf = len(df_setfit_filtered[(df_setfit_filtered['setfit_confidence'] >= 0.60) &
                                         (df_setfit_filtered['setfit_confidence'] < 0.70)])
    print(f"    High (>= 0.70): {high_conf:,}")
    print(f"    Medium (0.60-0.69): {medium_conf:,}")

    # Keep only pmid and source info
    set_b = df_setfit_filtered[['pmid']].copy()
    set_b['source'] = 'setfit'
    set_b['setfit_confidence'] = df_setfit_filtered['setfit_confidence']

    print(f"  Set B papers: {len(set_b):,}")

    # ========================================================================
    # SET C: UNION OF A + B
    # ========================================================================
    print("\n" + "=" * 80)
    print("Creating Set C: Union of A + B...")

    # Mark papers in each set
    set_a_pmids = set(set_a['pmid'])
    set_b_pmids = set(set_b['pmid'])

    # Calculate overlaps
    both_sets = set_a_pmids & set_b_pmids
    only_a = set_a_pmids - set_b_pmids
    only_b = set_b_pmids - set_a_pmids
    union_pmids = set_a_pmids | set_b_pmids

    print(f"  Papers in A only: {len(only_a):,}")
    print(f"  Papers in B only: {len(only_b):,}")
    print(f"  Papers in both A and B: {len(both_sets):,}")
    print(f"  Union total: {len(union_pmids):,}")

    # Create Set C with source tracking
    set_c_data = []

    for pmid in union_pmids:
        in_a = pmid in set_a_pmids
        in_b = pmid in set_b_pmids

        row = {'pmid': pmid}

        if in_a and in_b:
            row['source'] = 'both'
            row['ling_score'] = set_a[set_a['pmid'] == pmid]['ling_score'].iloc[0]
            row['setfit_confidence'] = set_b[set_b['pmid'] == pmid]['setfit_confidence'].iloc[0]
        elif in_a:
            row['source'] = 'linguistic_only'
            row['ling_score'] = set_a[set_a['pmid'] == pmid]['ling_score'].iloc[0]
            row['setfit_confidence'] = None
        else:  # in_b
            row['source'] = 'setfit_only'
            row['ling_score'] = None
            row['setfit_confidence'] = set_b[set_b['pmid'] == pmid]['setfit_confidence'].iloc[0]

        set_c_data.append(row)

    set_c = pd.DataFrame(set_c_data)
    print(f"  Set C papers: {len(set_c):,}")

    # ========================================================================
    # SAVE PAPER SETS
    # ========================================================================
    print("\n" + "=" * 80)
    print("Saving paper sets...")

    set_a_path = output_dir / "set_a_linguistic.csv"
    set_b_path = output_dir / "set_b_setfit.csv"
    set_c_path = output_dir / "set_c_union.csv"

    set_a.to_csv(set_a_path, index=False)
    print(f"  Saved Set A: {set_a_path}")

    set_b.to_csv(set_b_path, index=False)
    print(f"  Saved Set B: {set_b_path}")

    set_c.to_csv(set_c_path, index=False)
    print(f"  Saved Set C: {set_c_path}")

    # ========================================================================
    # GENERATE SUMMARY STATISTICS
    # ========================================================================
    print("\n" + "=" * 80)
    print("Generating summary statistics...")

    summary = {
        'timestamp': datetime.now().isoformat(),
        'parameters': {
            'linguistic_threshold': ling_threshold,
            'setfit_threshold': setfit_threshold
        },
        'set_a': {
            'name': 'Linguistic Only',
            'criteria': f'ling_score >= {ling_threshold}',
            'paper_count': len(set_a),
            'source_file': str(linguistic_file.name)
        },
        'set_b': {
            'name': 'SetFit High+Medium',
            'criteria': f'confidence >= {setfit_threshold}',
            'paper_count': len(set_b),
            'high_confidence': high_conf,
            'medium_confidence': medium_conf,
            'source_file': str(setfit_file.name)
        },
        'set_c': {
            'name': 'Union (A + B)',
            'criteria': 'Linguistic OR SetFit',
            'paper_count': len(set_c),
            'linguistic_only': len(only_a),
            'setfit_only': len(only_b),
            'both': len(both_sets)
        },
        'overlap_analysis': {
            'linguistic_only': int(len(only_a)),
            'setfit_only': int(len(only_b)),
            'both_methods': int(len(both_sets)),
            'union_total': int(len(union_pmids)),
            'overlap_rate': round(len(both_sets) / len(union_pmids) * 100, 2) if len(union_pmids) > 0 else 0
        }
    }

    summary_path = output_dir / "paper_sets_summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"  Saved summary: {summary_path}")

    # ========================================================================
    # SUMMARY REPORT
    # ========================================================================
    print("\n" + "=" * 80)
    print("PAPER SETS SUMMARY")
    print("=" * 80)
    print(f"\nThresholds:")
    print(f"  Linguistic: >= {ling_threshold}")
    print(f"  SetFit: >= {setfit_threshold}")
    print(f"\nPaper sets created:")
    print(f"  Set A (Linguistic):     {len(set_a):,} papers")
    print(f"  Set B (SetFit):         {len(set_b):,} papers")
    print(f"  Set C (Union):          {len(set_c):,} papers")
    print(f"\nOVERLAP ANALYSIS:")
    if len(union_pmids) > 0:
        print(f"  Linguistic only:      {len(only_a):,} papers ({len(only_a)/len(union_pmids)*100:.1f}%)")
        print(f"  SetFit only:          {len(only_b):,} papers ({len(only_b)/len(union_pmids)*100:.1f}%)")
        print(f"  Both methods:         {len(both_sets):,} papers ({len(both_sets)/len(union_pmids)*100:.1f}%)")
        print(f"  Agreement rate:       {len(both_sets)/len(union_pmids)*100:.1f}%")
    else:
        print(f"  No papers in union")

    # Runtime
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 80)
    print("SUCCESS")
    print("=" * 80)
    print(f"\nCompleted: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")

    print("\n" + "=" * 80)
    print("NEXT STEP")
    print("=" * 80)
    if args.session_dir:
        print(f"\nRun Phase 6 - Primary Resource Mapping:")
        print(f"  python scripts/phase5_mapping/10_map_primary_resources.py --session-dir {args.session_dir}")
        print(f"  Input: {set_c_path}")
        print(f"  Papers to process: {len(set_c):,}")
    else:
        print("\nNext: Map primary resources from Set C")
        print(f"  Input: {set_c_path}")
        print(f"  Papers to process: {len(set_c):,}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
