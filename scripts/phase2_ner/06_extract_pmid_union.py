#!/usr/bin/env python3
"""
Extract All Unique Papers from NER Union

Extracts all unique PMIDs from spaCy Hybrid NER and V2 BERT NER results,
creating the complete union for input to downstream pipeline phases.

Usage:
    # Session-based (PREFERRED):
    python 06_extract_pmid_union.py --session-dir results/2025-12-04-143052-abc12

    # Legacy mode (auto-detect files):
    python 06_extract_pmid_union.py --auto

    # Specify custom paths (legacy):
    python 06_extract_pmid_union.py \
        --spacy-file ../data/phase2_ner/spacy_ner_results.csv \
        --v2-file ../data/phase2_ner/v2_ner_results.csv \
        --output ../data/phase2_ner/ner_union_pmids.txt

Session Mode:
    When --session-dir is provided:
    - Reads from: {session_dir}/input/spacy_ner_results.csv
    - Reads from: {session_dir}/input/v2_ner_results.csv
    - Outputs to: {session_dir}/02_ner/ner_union.csv
    - Outputs to: {session_dir}/02_ner/ner_union_pmids.txt

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-11-20
Updated: 2025-12-03 (added argparse, column auto-detection, entity CSV output)
Updated: 2025-12-04 (added session-dir support)
"""

import argparse
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# Requires: lib/session_utils.py (run from unified_bioresource_pipeline directory)
# Add lib to path for session utilities
SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

# Import from lib - will fail loudly if lib not found
from lib.session_utils import get_session_path, add_session_args, validate_session_dir

# Default paths (legacy)
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent  # Go up to inventory_2022
DEFAULT_NER_DIR = PROJECT_ROOT / "validation_spacy_v_BERT" / "results" / "phase2" / "ner"
DEFAULT_OUTPUT = PROJECT_ROOT / "advanced_paper_filtering" / "data" / "input" / "all_paper_pmids.txt"

# Alternative paths for unified_bioresource_pipeline
ALT_NER_DIR = SCRIPT_DIR.parent.parent / "data" / "phase2_ner"


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract unique PMIDs from NER union (spaCy + V2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Session-based mode (PREFERRED):
  python 06_extract_pmid_union.py --session-dir results/2025-12-04-143052-abc12

  # Use auto-detection (legacy):
  python 06_extract_pmid_union.py --auto

  # Specify exact files (legacy):
  python 06_extract_pmid_union.py \\
      --spacy-file ../data/phase2_ner/spacy_ner_results.csv \\
      --v2-file ../data/phase2_ner/v2_ner_results.csv \\
      --output ../data/phase2_ner/ner_union_pmids.txt
        """
    )

    # Session mode arguments
    parser.add_argument("--session-dir", type=Path,
                        help="Session directory path (e.g., results/2025-12-04-143052-abc12)")

    # Legacy mode arguments
    parser.add_argument("--spacy-file", type=Path, help="Path to spaCy NER results CSV")
    parser.add_argument("--v2-file", type=Path, help="Path to V2 BERT NER results CSV")
    parser.add_argument("--output", type=Path, help="Output path for PMID list (.txt)")
    parser.add_argument("--entity-csv", type=Path, help="Optional: output combined entity CSV")
    parser.add_argument("--auto", action="store_true",
                        help="Auto-detect files in unified_bioresource_pipeline or legacy paths")

    return parser.parse_args()


def find_id_column(df, filename):
    """Auto-detect the ID column (handles 'ID' vs 'publication_id')."""
    if 'ID' in df.columns:
        return 'ID'
    elif 'publication_id' in df.columns:
        return 'publication_id'
    elif 'id' in df.columns:
        return 'id'
    else:
        raise ValueError(f"Cannot find ID column in {filename}. Columns: {list(df.columns)}")


def find_ner_files_auto():
    """Auto-detect NER files, preferring unified_bioresource_pipeline paths."""
    print("=" * 80)
    print("AUTO-DETECTING NER FILES")
    print("=" * 80)

    spacy_file = None
    v2_file = None
    output_file = None

    # Try unified_bioresource_pipeline paths first
    if ALT_NER_DIR.exists():
        print(f"\nChecking: {ALT_NER_DIR}")

        # Look for spaCy file
        spacy_candidates = list(ALT_NER_DIR.glob("spacy_ner_results*.csv"))
        if spacy_candidates:
            spacy_file = max(spacy_candidates, key=lambda p: p.stat().st_mtime)
            print(f"  Found spaCy: {spacy_file.name}")

        # Look for V2 file
        v2_candidates = list(ALT_NER_DIR.glob("v2_ner_results*.csv"))
        if v2_candidates:
            v2_file = max(v2_candidates, key=lambda p: p.stat().st_mtime)
            print(f"  Found V2: {v2_file.name}")

        if spacy_file and v2_file:
            output_file = ALT_NER_DIR / "ner_union_pmids.txt"
            print(f"\n  Using unified_bioresource_pipeline paths")
            return spacy_file, v2_file, output_file

    # Fall back to legacy paths
    if DEFAULT_NER_DIR.exists():
        print(f"\nChecking legacy: {DEFAULT_NER_DIR}")

        spacy_candidates = list(DEFAULT_NER_DIR.glob("spacy_ner_full_hybrid_results_*.csv"))
        if spacy_candidates:
            spacy_file = max(spacy_candidates, key=lambda p: p.stat().st_mtime)
            print(f"  Found spaCy: {spacy_file.name}")

        v2_candidates = list(DEFAULT_NER_DIR.glob("v2_ner_results_*.csv"))
        if v2_candidates:
            v2_file = max(v2_candidates, key=lambda p: p.stat().st_mtime)
            print(f"  Found V2: {v2_file.name}")

        if spacy_file and v2_file:
            output_file = DEFAULT_OUTPUT
            print(f"\n  Using legacy paths")
            return spacy_file, v2_file, output_file

    # Report what's missing
    if not spacy_file:
        print("\nERROR: No spaCy NER files found!")
        print(f"  Searched: {ALT_NER_DIR}")
        print(f"  Searched: {DEFAULT_NER_DIR}")
    if not v2_file:
        print("\nERROR: No V2 NER files found!")
        print(f"  Searched: {ALT_NER_DIR}")
        print(f"  Searched: {DEFAULT_NER_DIR}")

    sys.exit(1)


def find_latest_ner_files():
    """Find the most recent spaCy and V2 NER result files (legacy function)."""
    print("=" * 80)
    print("FINDING LATEST NER RESULT FILES")
    print("=" * 80)

    # Find spaCy hybrid files
    spacy_files = list(DEFAULT_NER_DIR.glob("spacy_ner_full_hybrid_results_*.csv"))
    if not spacy_files:
        print("ERROR: No spaCy NER files found!")
        print(f"Searched in: {DEFAULT_NER_DIR}")
        sys.exit(1)

    spacy_file = max(spacy_files, key=lambda p: p.stat().st_mtime)
    print(f"\nspaCy NER file: {spacy_file.name}")
    print(f"  Size: {spacy_file.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"  Modified: {datetime.fromtimestamp(spacy_file.stat().st_mtime)}")

    # Find V2 BERT files
    v2_files = list(DEFAULT_NER_DIR.glob("v2_ner_results_*.csv"))
    if not v2_files:
        print("ERROR: No V2 NER files found!")
        print(f"Searched in: {DEFAULT_NER_DIR}")
        sys.exit(1)

    v2_file = max(v2_files, key=lambda p: p.stat().st_mtime)
    print(f"\nV2 NER file: {v2_file.name}")
    print(f"  Size: {v2_file.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"  Modified: {datetime.fromtimestamp(v2_file.stat().st_mtime)}")

    return spacy_file, v2_file


def extract_unique_pmids(spacy_file, v2_file, entity_csv_path=None):
    """Extract unique PMIDs from both NER files."""
    print("\n" + "=" * 80)
    print("EXTRACTING UNIQUE PMIDs")
    print("=" * 80)

    # Load spaCy NER results
    print(f"\nLoading spaCy NER results...")
    print(f"  File: {spacy_file}")
    spacy_df = pd.read_csv(spacy_file)
    print(f"  Loaded: {len(spacy_df):,} entity mentions")
    print(f"  Columns: {list(spacy_df.columns)}")

    # Auto-detect ID column
    spacy_id_col = find_id_column(spacy_df, spacy_file.name)
    print(f"  ID column: {spacy_id_col}")

    # Extract unique PMIDs
    spacy_pmids = set(spacy_df[spacy_id_col].dropna().astype(float).astype(int).astype(str).unique())
    print(f"  Unique papers: {len(spacy_pmids):,}")

    # Load V2 NER results
    print(f"\nLoading V2 BERT NER results...")
    print(f"  File: {v2_file}")
    v2_df = pd.read_csv(v2_file)
    print(f"  Loaded: {len(v2_df):,} entity mentions")
    print(f"  Columns: {list(v2_df.columns)}")

    # Auto-detect ID column
    v2_id_col = find_id_column(v2_df, v2_file.name)
    print(f"  ID column: {v2_id_col}")

    # Extract unique PMIDs
    v2_pmids = set(v2_df[v2_id_col].dropna().astype(float).astype(int).astype(str).unique())
    print(f"  Unique papers: {len(v2_pmids):,}")

    # Calculate union
    print("\n" + "-" * 80)
    print("UNION STATISTICS")
    print("-" * 80)

    both = spacy_pmids & v2_pmids
    spacy_only = spacy_pmids - v2_pmids
    v2_only = v2_pmids - spacy_pmids
    union = spacy_pmids | v2_pmids

    print(f"\nspaCy only:     {len(spacy_only):,} papers")
    print(f"V2 only:        {len(v2_only):,} papers")
    print(f"Both (overlap): {len(both):,} papers")
    print(f"Union (total):  {len(union):,} papers")

    # Optionally create combined entity CSV
    if entity_csv_path:
        print("\n" + "-" * 80)
        print("CREATING COMBINED ENTITY CSV")
        print("-" * 80)

        # Normalize spaCy columns
        spacy_norm = spacy_df.copy()
        spacy_norm['publication_id'] = spacy_norm[spacy_id_col]
        spacy_norm['entity_text'] = spacy_norm.get('mention', spacy_norm.get('text', ''))
        spacy_norm['entity_type'] = spacy_norm.get('label', '')
        spacy_norm['confidence'] = spacy_norm.get('confidence', 1.0)
        spacy_norm['source'] = spacy_norm.get('source', 'spacy_hybrid')
        spacy_out = spacy_norm[['publication_id', 'entity_text', 'entity_type', 'confidence', 'source']].copy()

        # Normalize V2 columns
        v2_norm = v2_df.copy()
        v2_norm['publication_id'] = v2_norm[v2_id_col]
        v2_norm['entity_text'] = v2_norm.get('mention', v2_norm.get('text', ''))
        v2_norm['entity_type'] = v2_norm.get('label', '')
        v2_norm['confidence'] = v2_norm.get('prob', v2_norm.get('confidence', 1.0))
        v2_norm['source'] = 'v2_bert'
        v2_out = v2_norm[['publication_id', 'entity_text', 'entity_type', 'confidence', 'source']].copy()

        # Combine
        combined = pd.concat([spacy_out, v2_out], ignore_index=True)
        print(f"  Combined entities (before dedup): {len(combined):,}")

        # Deduplicate
        combined = combined.drop_duplicates(
            subset=['publication_id', 'entity_text', 'entity_type'],
            keep='first'
        )
        print(f"  Combined entities (after dedup):  {len(combined):,}")

        # Save
        entity_csv_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(entity_csv_path, index=False)
        print(f"  Saved to: {entity_csv_path}")

    return sorted(union, key=int)


def save_pmids(pmids, output_file):
    """Save PMIDs to text file."""
    print("\n" + "=" * 80)
    print("SAVING PMIDs")
    print("=" * 80)

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Write PMIDs (one per line)
    print(f"\nWriting to: {output_file}")
    with open(output_file, 'w') as f:
        for pmid in pmids:
            f.write(f"{pmid}\n")

    print(f"✓ Saved {len(pmids):,} PMIDs")

    # Verify file
    file_size = output_file.stat().st_size / 1024
    print(f"✓ File size: {file_size:.1f} KB")


def find_session_ner_files(session_dir):
    """Find NER files in session input directory."""
    input_dir = get_session_path(session_dir, 'input')

    print("=" * 80)
    print("SESSION MODE - FINDING NER FILES")
    print("=" * 80)
    print(f"\nSession directory: {session_dir}")
    print(f"Input directory: {input_dir}")

    # Look for spaCy file (try multiple patterns)
    spacy_file = None
    for pattern in ['spacy_ner_results*.csv', 'spacy_ner_*.csv']:
        candidates = list(input_dir.glob(pattern))
        if candidates:
            spacy_file = max(candidates, key=lambda p: p.stat().st_mtime)
            break

    # Look for V2 file
    v2_file = None
    for pattern in ['v2_ner_results*.csv', 'v2_ner_*.csv']:
        candidates = list(input_dir.glob(pattern))
        if candidates:
            v2_file = max(candidates, key=lambda p: p.stat().st_mtime)
            break

    # Report findings
    if spacy_file:
        print(f"\n  Found spaCy: {spacy_file.name}")
    else:
        print(f"\n  ERROR: No spaCy NER file found in {input_dir}")
        print("    Expected: spacy_ner_results.csv or spacy_ner_results*.csv")

    if v2_file:
        print(f"  Found V2: {v2_file.name}")
    else:
        print(f"\n  ERROR: No V2 NER file found in {input_dir}")
        print("    Expected: v2_ner_results.csv or v2_ner_results*.csv")

    if not spacy_file or not v2_file:
        sys.exit(1)

    return spacy_file, v2_file


def main():
    """Main execution."""
    args = parse_args()
    start_time = datetime.now()

    print("\n" + "=" * 80)
    print("EXTRACT NER UNION PAPERS")
    print("=" * 80)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    try:
        # Determine input files and output path
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
                validate_session_dir(session_path, required_phases=['input'])
            except ValueError as e:
                print(f"ERROR: Invalid session directory: {e}")
                sys.exit(1)

            # Find input files in session/input/
            spacy_file, v2_file = find_session_ner_files(args.session_dir)

            # Set output paths
            output_dir = get_session_path(args.session_dir, '02_ner')
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / "ner_union_pmids.txt"
            entity_csv = output_dir / "ner_union.csv"

        elif args.spacy_file and args.v2_file:
            # LEGACY: Use explicitly provided paths
            print("MODE: Legacy (explicit paths)")
            spacy_file = args.spacy_file
            v2_file = args.v2_file
            output_file = args.output if args.output else ALT_NER_DIR / "ner_union_pmids.txt"
            entity_csv = args.entity_csv
            print(f"  spaCy: {spacy_file}")
            print(f"  V2: {v2_file}")

        elif args.auto or not (args.spacy_file or args.v2_file):
            # LEGACY: Auto-detect (default behavior)
            print("MODE: Legacy (auto-detect)")
            spacy_file, v2_file, output_file = find_ner_files_auto()
            if args.output:
                output_file = args.output
            entity_csv = args.entity_csv
            if entity_csv is None:
                # Default to creating entity CSV in auto mode
                entity_csv = output_file.parent / "ner_union.csv"

        else:
            print("ERROR: Must provide --session-dir, or both --spacy-file and --v2-file, or use --auto")
            sys.exit(1)

        # Extract unique PMIDs (and optionally create entity CSV)
        pmids = extract_unique_pmids(spacy_file, v2_file, entity_csv_path=entity_csv)

        # Save to output file
        save_pmids(pmids, output_file)

        # Summary
        end_time = datetime.now()
        duration = end_time - start_time

        print("\n" + "=" * 80)
        print("SUCCESS")
        print("=" * 80)
        print(f"\nCompleted: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration}")
        print(f"\nOutput PMID list: {output_file}")
        if entity_csv:
            print(f"Output entity CSV: {entity_csv}")
        print(f"Total papers: {len(pmids):,}")

        print("\n" + "=" * 80)
        print("NEXT STEP")
        print("=" * 80)
        if args.session_dir:
            print(f"\nRun Phase 3 - Linguistic Scoring:")
            print(f"  python scripts/phase3_linguistic/run_linguistic_scoring.py --session-dir {args.session_dir}")
        else:
            print("\nRun Phase 3 - Linguistic Scoring:")
            print("  python unified_bioresource_pipeline/scripts/phase3_linguistic/run_linguistic_scoring.py")

        return 0

    except KeyboardInterrupt:
        print("\nERROR: Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
