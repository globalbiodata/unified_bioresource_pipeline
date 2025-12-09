#!/usr/bin/env python3
"""
Phase 6: Prepare URLs for Scanning

Takes papers with extracted URLs and prepares a deduplicated URL list for scanning.

Input: Papers with resource_url column (from script 13)
Output: Prepared URL list with metadata (url_id, domain, path, etc.)

Usage:
    # Session-based (PREFERRED):
    python 14_prepare_urls.py --session-dir results/2025-12-04-143052-abc12

    # Legacy mode (auto-detect):
    python 14_prepare_urls.py --auto

    # Custom paths (legacy):
    python 14_prepare_urls.py \
        --input-file data/union_papers_with_urls.csv \
        --output-file data/prepared_urls.csv

Session Mode:
    When --session-dir is provided:
    - Reads from: {session_dir}/05_mapping/union_papers_with_urls.csv
    - Outputs to: {session_dir}/06_scanning/prepared_urls.csv

Author: Pipeline Automation
Date: 2025-11-18
Updated: 2025-12-04 (added session-dir support, argparse)
"""

import argparse
import pandas as pd
import sys
import json
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime

# Requires: lib/session_utils.py (run from unified_bioresource_pipeline directory)
# Add lib to path for session utilities
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import from lib - will fail loudly if lib not found
from lib.session_utils import get_session_path, validate_session_dir

# Legacy paths
LEGACY_INPUT_FILE = PROJECT_ROOT.parent / "pipeline_synthesis_2025-11-18/data/union_papers_with_urls.csv"
LEGACY_OUTPUT_FILE = PROJECT_ROOT.parent / "pipeline_synthesis_2025-11-18/data/prepared_urls.csv"

# Alternative legacy paths
ALT_INPUT_FILE = PROJECT_ROOT / "data" / "phase5_mapping" / "union_papers_with_urls.csv"
ALT_OUTPUT_FILE = PROJECT_ROOT / "data" / "phase6_scanning" / "prepared_urls.csv"


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Phase 6: Prepare URLs for scanning",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Session-based mode (PREFERRED):
  python 14_prepare_urls.py --session-dir results/2025-12-04-143052-abc12

  # Legacy mode (auto-detect):
  python 14_prepare_urls.py --auto

  # Custom paths (legacy):
  python 14_prepare_urls.py \\
      --input-file data/union_papers_with_urls.csv \\
      --output-file data/prepared_urls.csv
        """
    )

    # Session mode arguments
    parser.add_argument("--session-dir", type=Path,
                        help="Session directory path (e.g., results/2025-12-04-143052-abc12)")

    # Legacy mode arguments
    parser.add_argument("--input-file", type=Path,
                        help="Path to input CSV with extracted URLs")
    parser.add_argument("--output-file", type=Path,
                        help="Path to output prepared URLs CSV")
    parser.add_argument("--auto", action="store_true",
                        help="Auto-detect files in legacy paths")

    return parser.parse_args()


def find_legacy_files():
    """Auto-detect legacy file paths."""
    print("=" * 80)
    print("AUTO-DETECTING LEGACY FILES")
    print("=" * 80)

    input_file = None
    output_file = None

    # Try unified_bioresource_pipeline paths first
    if ALT_INPUT_FILE.exists():
        print(f"\nFound input file in unified_bioresource_pipeline:")
        input_file = ALT_INPUT_FILE
        output_file = ALT_OUTPUT_FILE
        print(f"  Input: {input_file.name}")
        return input_file, output_file

    # Fall back to legacy paths
    if LEGACY_INPUT_FILE.exists():
        print(f"\nFound input file in legacy paths:")
        input_file = LEGACY_INPUT_FILE
        output_file = LEGACY_OUTPUT_FILE
        print(f"  Input: {input_file.name}")
        return input_file, output_file

    # Report what's missing
    print("\nERROR: Could not find required input file!")
    print(f"\nSearched for:")
    print(f"  {ALT_INPUT_FILE}")
    print(f"  {LEGACY_INPUT_FILE}")
    sys.exit(1)


def main():
    args = parse_args()
    start_time = datetime.now()

    print("=" * 80)
    print("PHASE 6: PREPARE URLs FOR SCANNING")
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
            validate_session_dir(session_path, required_phases=['05_mapping'])
        except ValueError as e:
            print(f"ERROR: Invalid session directory: {e}")
            sys.exit(1)

        input_file = get_session_path(args.session_dir, '05_mapping', 'union_papers_with_urls.csv')
        output_dir = get_session_path(args.session_dir, '06_scanning')
        output_file = output_dir / 'prepared_urls.csv'

    elif args.input_file and args.output_file:
        # LEGACY: Explicit paths
        print(f"\nMODE: Legacy (explicit paths)")
        input_file = args.input_file
        output_file = args.output_file

    elif args.auto or not args.input_file:
        # LEGACY: Auto-detect
        print(f"\nMODE: Legacy (auto-detect)")
        input_file, output_file = find_legacy_files()

    else:
        print("ERROR: Must provide --session-dir, or both --input-file and --output-file, or use --auto")
        sys.exit(1)

    # Create output directory
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Verify input file exists
    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)

    print(f"\nInput file: {input_file}")
    print(f"Output file: {output_file}")

    # ========================================================================
    # LOAD DATA
    # ========================================================================
    print("\n" + "=" * 80)
    print("Loading papers with URLs...")
    df = pd.read_csv(input_file)
    print(f"  Loaded: {len(df):,} papers")

    # Check for required columns
    if 'resource_url' not in df.columns:
        print("ERROR: Input file missing 'resource_url' column!")
        print(f"Available columns: {', '.join(df.columns)}")
        sys.exit(1)

    # Filter to papers with URLs
    df_with_urls = df[df['resource_url'].notna()].copy()
    print(f"  Papers with URLs: {len(df_with_urls):,}")

    if len(df_with_urls) == 0:
        print("\nWARNING: No papers with URLs found!")
        print("Creating empty output file.")
        empty_df = pd.DataFrame(columns=['url_id', 'url', 'domain', 'path', 'pmid',
                                        'primary_entity_long', 'primary_entity_short',
                                        'title', 'source_file'])
        empty_df.to_csv(output_file, index=False)
        print(f"\nSaved empty file: {output_file}")
        return 0

    # ========================================================================
    # PREPARE URL DATA
    # ========================================================================
    print("\n" + "=" * 80)
    print("Preparing URL data...")

    # Extract URL components
    url_data = []
    for idx, row in df_with_urls.iterrows():
        url = row['resource_url']
        parsed = urlparse(url)

        url_entry = {
            'url_id': idx,
            'url': url,
            'domain': parsed.netloc,
            'path': parsed.path,
            'pmid': row.get('pmid', ''),
            'primary_entity_long': row.get('primary_entity_long', ''),
            'primary_entity_short': row.get('primary_entity_short', ''),
            'title': row.get('title', ''),
            'source_file': 'union_papers_with_urls'
        }
        url_data.append(url_entry)

    url_df = pd.DataFrame(url_data)
    print(f"  Prepared: {len(url_df):,} URL entries")

    # ========================================================================
    # DEDUPLICATE URLs
    # ========================================================================
    print("\n" + "=" * 80)
    print("Deduplicating URLs...")

    initial_count = len(url_df)
    url_df_dedup = url_df.drop_duplicates(subset=['url'], keep='first').copy()
    duplicates_removed = initial_count - len(url_df_dedup)

    print(f"  Initial URLs: {initial_count:,}")
    print(f"  Unique URLs: {len(url_df_dedup):,}")
    print(f"  Duplicates removed: {duplicates_removed:,}")

    # ========================================================================
    # STATISTICS
    # ========================================================================
    print("\n" + "=" * 80)
    print("URL STATISTICS")
    print("=" * 80)
    print(f"\nTotal unique URLs: {len(url_df_dedup):,}")
    print(f"Unique domains: {url_df_dedup['domain'].nunique():,}")

    # Domain distribution
    print(f"\nTop 10 domains:")
    top_domains = url_df_dedup['domain'].value_counts().head(10)
    for domain, count in top_domains.items():
        print(f"  {domain:40s}: {count:3d} URLs")

    # ========================================================================
    # SAVE OUTPUT
    # ========================================================================
    print("\n" + "=" * 80)
    print("Saving prepared URLs...")

    url_df_dedup.to_csv(output_file, index=False)
    print(f"  Saved: {output_file}")
    print(f"  Rows: {len(url_df_dedup):,}")

    # ========================================================================
    # SAVE SUMMARY
    # ========================================================================
    summary = {
        'timestamp': datetime.now().isoformat(),
        'input_file': str(input_file.name),
        'output_file': str(output_file.name),
        'statistics': {
            'input_papers': len(df),
            'papers_with_urls': len(df_with_urls),
            'total_url_entries': initial_count,
            'unique_urls': len(url_df_dedup),
            'duplicates_removed': duplicates_removed,
            'unique_domains': int(url_df_dedup['domain'].nunique())
        },
        'top_domains': top_domains.to_dict()
    }

    summary_path = output_file.parent / 'prepare_urls_summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"  Saved summary: {summary_path}")

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
        print(f"\nRun Phase 6 - URL Scanning:")
        print(f"  python scripts/phase6_scanning/15_scan_urls.py --session-dir {args.session_dir}")
        print(f"  Input: {output_file}")
        print(f"  URLs to scan: {len(url_df_dedup):,}")
    else:
        print("\nNext: Scan URLs for validity and metadata")
        print(f"  Input: {output_file}")
        print(f"  URLs to scan: {len(url_df_dedup):,}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
