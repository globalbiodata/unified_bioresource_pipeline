#!/usr/bin/env python3
"""
Phase 6: Merge URL Scanner Scores Back into Pipeline Dataset

This script bridges the URL scanner and deduplication phases by adding
scanner quality scores to the papers dataset.

Scanner scores added:
  - url_scanner_score: Total indicator score (0-500+)
  - url_likelihood: Classification (CRITICAL/HIGH/MEDIUM/LOW/VERY_LOW)
  - url_indicators: List of matching indicators
  - url_scan_status: Scan outcome (success/failed/not_scanned)

Usage:
    # Session-based (PREFERRED):
    python 16_merge_scan_scores.py --session-dir results/2025-12-04-143052-abc12

    # Legacy mode (auto-detect):
    python 16_merge_scan_scores.py --auto

    # Custom paths (legacy):
    python 16_merge_scan_scores.py \
        --papers-file data/papers_with_urls.csv \
        --scan-results data/scan_results.csv \
        --output-file data/papers_with_scanner_scores.csv

Session Mode:
    When --session-dir is provided:
    - Reads from: {session_dir}/05_mapping/papers_with_urls.csv (papers with URLs)
    - Reads from: {session_dir}/06_scanning/*scan_results*.csv (URL scan results)
    - Outputs to: {session_dir}/06_scanning/papers_with_scanner_scores.csv
    - Stats to: {session_dir}/06_scanning/scanner_merge_statistics.txt

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-11-18
Updated: 2025-12-04 (added session-dir support, argparse)
"""

import argparse
import pandas as pd
import numpy as np
import sys
from pathlib import Path
import json

# Add lib to path for session utilities
SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

# Import from lib - will fail loudly if lib not found
from lib.session_utils import get_session_path, validate_session_dir

# Legacy paths
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent  # inventory_2022
LEGACY_SYNTHESIS_DIR = PROJECT_ROOT / 'pipeline_synthesis_2025-11-18'
LEGACY_PAPERS_FILE = LEGACY_SYNTHESIS_DIR / 'data/union_papers_with_urls.csv'
LEGACY_SCAN_RESULTS = LEGACY_SYNTHESIS_DIR / 'results/gbc_scan_results.csv'
LEGACY_OUTPUT_FILE = LEGACY_SYNTHESIS_DIR / 'data/union_papers_with_scanner_scores.csv'
LEGACY_STATS_FILE = LEGACY_SYNTHESIS_DIR / 'results/scanner_merge_statistics.txt'


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Phase 6: Merge URL scanner scores into pipeline dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Session-based mode (PREFERRED):
  python 16_merge_scan_scores.py --session-dir results/2025-12-04-143052-abc12

  # Legacy mode (auto-detect):
  python 16_merge_scan_scores.py --auto

  # Custom paths (legacy):
  python 16_merge_scan_scores.py \\
      --papers-file data/papers_with_urls.csv \\
      --scan-results data/scan_results.csv \\
      --output-file data/papers_with_scanner_scores.csv
        """
    )

    # Session mode arguments
    parser.add_argument("--session-dir", type=Path,
                        help="Session directory path (e.g., results/2025-12-04-143052-abc12)")

    # Legacy mode arguments
    parser.add_argument("--papers-file", type=Path,
                        help="Path to papers with URLs CSV")
    parser.add_argument("--scan-results", type=Path,
                        help="Path to scan results CSV")
    parser.add_argument("--output-file", type=Path,
                        help="Output path for papers with scanner scores CSV")
    parser.add_argument("--stats-file", type=Path,
                        help="Output path for statistics report")
    parser.add_argument("--auto", action="store_true",
                        help="Auto-detect files in legacy paths")

    return parser.parse_args()


def find_legacy_files():
    """Auto-detect legacy file paths."""
    print("=" * 80)
    print("AUTO-DETECTING LEGACY FILES")
    print("=" * 80)

    papers_file = None
    scan_results = None
    output_file = None
    stats_file = None

    # Check legacy paths
    if LEGACY_PAPERS_FILE.exists():
        print(f"\nFound papers file: {LEGACY_PAPERS_FILE.name}")
        papers_file = LEGACY_PAPERS_FILE

    if LEGACY_SCAN_RESULTS.exists():
        print(f"Found scan results: {LEGACY_SCAN_RESULTS.name}")
        scan_results = LEGACY_SCAN_RESULTS

    output_file = LEGACY_OUTPUT_FILE
    stats_file = LEGACY_STATS_FILE

    # Check if we found required files
    if not papers_file:
        print(f"\nERROR: Could not find papers file at {LEGACY_PAPERS_FILE}")
        sys.exit(1)

    # Scan results are optional (will create placeholders if missing)
    print(f"\nUsing legacy paths:")
    print(f"  Papers:  {papers_file}")
    print(f"  Scan:    {scan_results if scan_results else 'Not found (will use placeholders)'}")
    print(f"  Output:  {output_file}")
    print(f"  Stats:   {stats_file}")

    return papers_file, scan_results, output_file, stats_file


def find_scan_results_file(session_dir: Path) -> Path:
    """
    Find scan results CSV in session scanning directory.

    Looks for files matching: *scan_results*.csv
    """
    scan_dir = get_session_path(session_dir, '06_scanning')

    # Look for scan results files
    scan_files = list(scan_dir.glob('*scan_results*.csv'))

    if not scan_files:
        return None

    # If multiple, use the most recent
    scan_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return scan_files[0]


def resolve_paths(args):
    """
    Resolve input/output paths based on mode.

    Returns:
        tuple: (papers_file, scan_results, output_file, stats_file)
    """
    # Session mode (PREFERRED)
    if args.session_dir:
        print("=" * 80)
        print("SESSION MODE")
        print("=" * 80)

        # Validate session directory
        try:
            validate_session_dir(args.session_dir, required_phases=['05_mapping'])
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)

        # Input from Phase 5
        papers_file = get_session_path(args.session_dir, '05_mapping', 'papers_with_urls.csv')

        if not papers_file.exists():
            print(f"ERROR: Papers file not found: {papers_file}")
            print(f"Please run Script 13 (extract_urls.py) first.")
            sys.exit(1)

        # Scan results from Phase 6 (optional)
        scan_results = find_scan_results_file(args.session_dir)

        # Outputs to Phase 6
        output_file = get_session_path(args.session_dir, '06_scanning', 'papers_with_scanner_scores.csv')
        stats_file = get_session_path(args.session_dir, '06_scanning', 'scanner_merge_statistics.txt')

        # Ensure scanning directory exists
        get_session_path(args.session_dir, '06_scanning').mkdir(parents=True, exist_ok=True)

        print(f"\nSession: {args.session_dir.name}")
        print(f"Papers:  {papers_file.relative_to(args.session_dir)}")
        if scan_results:
            print(f"Scan:    {scan_results.relative_to(args.session_dir)}")
        else:
            print(f"Scan:    Not found (will use placeholders)")
        print(f"Output:  {output_file.relative_to(args.session_dir)}")
        print(f"Stats:   {stats_file.relative_to(args.session_dir)}")

        return papers_file, scan_results, output_file, stats_file

    # Auto mode (legacy)
    elif args.auto:
        return find_legacy_files()

    # Custom paths mode (legacy)
    elif args.papers_file and args.output_file:
        print("=" * 80)
        print("CUSTOM PATHS MODE")
        print("=" * 80)

        if not args.papers_file.exists():
            print(f"ERROR: Papers file not found: {args.papers_file}")
            sys.exit(1)

        scan_results = args.scan_results if args.scan_results and args.scan_results.exists() else None
        stats_file = args.stats_file if args.stats_file else args.output_file.parent / 'scanner_merge_statistics.txt'

        print(f"\nPapers:  {args.papers_file}")
        if scan_results:
            print(f"Scan:    {scan_results}")
        else:
            print(f"Scan:    Not provided (will use placeholders)")
        print(f"Output:  {args.output_file}")
        print(f"Stats:   {stats_file}")

        return args.papers_file, scan_results, args.output_file, stats_file

    else:
        print("ERROR: Must specify either --session-dir, --auto, or --papers-file + --output-file")
        sys.exit(1)


def normalize_url(url):
    """Normalize URL for matching (lowercase, strip protocol/www)"""
    if pd.isna(url):
        return ''

    url = str(url).lower().strip()

    # Remove protocol
    url = url.replace('https://', '').replace('http://', '')

    # Remove www prefix
    if url.startswith('www.'):
        url = url[4:]

    # Remove trailing slash
    url = url.rstrip('/')

    return url


def merge_scanner_scores(papers_file: Path, scan_results_file: Path, output_file: Path, stats_file: Path):
    """
    Main processing function: merge scanner scores into papers dataset.

    Args:
        papers_file: Path to papers with URLs CSV
        scan_results_file: Path to scan results CSV (can be None)
        output_file: Path for output CSV
        stats_file: Path for statistics report
    """
    print("="*80)
    print("Merging URL Scanner Scores into Pipeline Dataset")
    print("="*80)

    # ============================================================================
    # LOAD DATA
    # ============================================================================

    print("\n1. Loading datasets...")

    # Load papers with URLs
    if not papers_file.exists():
        print(f"ERROR: Papers file not found: {papers_file}")
        print(f"Please run Script 13 (extract_urls.py) first.")
        sys.exit(1)

    df_papers = pd.read_csv(papers_file)
    print(f"   ✓ Loaded papers: {len(df_papers):,} papers")
    print(f"   ✓ Papers with URLs: {df_papers['has_resource_url'].sum():,}")

    # Load scan results
    if scan_results_file is None or not scan_results_file.exists():
        print(f"   ⚠ WARNING: Scan results not found")
        print(f"   Creating output without scanner scores.")
        print(f"   Run scanner scripts first to add URL validation scores.")
        has_scan_results = False
    else:
        df_scan = pd.read_csv(scan_results_file)
        print(f"   ✓ Loaded scan results: {len(df_scan):,} URLs")
        print(f"   ✓ Successfully scanned: {(df_scan['status'] == 'success').sum():,}")
        has_scan_results = True

    # ============================================================================
    # NORMALIZE URLS FOR MATCHING
    # ============================================================================

    if has_scan_results:
        print("\n2. Normalizing URLs for matching...")

        # Normalize URLs in both datasets
        df_papers['resource_url_normalized'] = df_papers['resource_url'].apply(normalize_url)
        df_scan['url_normalized'] = df_scan['url'].apply(normalize_url)

        print(f"   ✓ Normalized {len(df_papers):,} paper URLs")
        print(f"   ✓ Normalized {len(df_scan):,} scan URLs")

    # ============================================================================
    # MERGE SCANNER SCORES
    # ============================================================================

    if has_scan_results:
        print("\n3. Merging scanner scores...")

        # Prepare scan results for merge
        scan_cols = {
            'url_normalized': 'resource_url_normalized',
            'total_score': 'url_scanner_score',
            'likelihood': 'url_likelihood',
            'indicators': 'url_indicators',
            'status': 'url_scan_status',
            'error_type': 'url_scan_error'
        }

        df_scan_subset = df_scan[list(scan_cols.keys())].rename(columns=scan_cols)

        # Merge on normalized URL
        df_merged = df_papers.merge(
            df_scan_subset,
            on='resource_url_normalized',
            how='left'
        )

        # Fill missing scanner scores (URLs not scanned)
        df_merged['url_scanner_score'] = df_merged['url_scanner_score'].fillna(0)
        df_merged['url_likelihood'] = df_merged['url_likelihood'].fillna('NOT_SCANNED')
        df_merged['url_scan_status'] = df_merged['url_scan_status'].fillna('not_scanned')

        # Drop normalized URL column (temp column for matching)
        df_merged = df_merged.drop(columns=['resource_url_normalized'])

        print(f"   ✓ Merged scanner scores")
        print(f"   ✓ Matched URLs: {(df_merged['url_scan_status'] == 'success').sum():,}")
        print(f"   ✓ Failed scans: {(df_merged['url_scan_status'] == 'failed').sum():,}")
        print(f"   ✓ Not scanned: {(df_merged['url_scan_status'] == 'not_scanned').sum():,}")

    else:
        # No scan results - create placeholder columns
        print("\n3. Creating placeholder scanner columns...")
        df_merged = df_papers.copy()
        df_merged['url_scanner_score'] = 0
        df_merged['url_likelihood'] = 'NOT_SCANNED'
        df_merged['url_scan_status'] = 'not_scanned'
        df_merged['url_indicators'] = ''
        df_merged['url_scan_error'] = ''
        print(f"   ✓ Added placeholder columns (all NOT_SCANNED)")

    # ============================================================================
    # CALCULATE STATISTICS
    # ============================================================================

    print("\n4. Calculating statistics...")

    stats = {
        'total_papers': len(df_merged),
        'papers_with_urls': df_merged['has_resource_url'].sum(),
        'papers_without_urls': (~df_merged['has_resource_url']).sum(),
    }

    if has_scan_results:
        stats.update({
            'scanned_success': (df_merged['url_scan_status'] == 'success').sum(),
            'scanned_failed': (df_merged['url_scan_status'] == 'failed').sum(),
            'not_scanned': (df_merged['url_scan_status'] == 'not_scanned').sum(),
            'critical': (df_merged['url_likelihood'] == 'CRITICAL').sum(),
            'high': (df_merged['url_likelihood'] == 'HIGH').sum(),
            'medium': (df_merged['url_likelihood'] == 'MEDIUM').sum(),
            'low': (df_merged['url_likelihood'] == 'LOW').sum(),
            'very_low': (df_merged['url_likelihood'] == 'VERY_LOW').sum(),
            'mean_score': df_merged[df_merged['url_scanner_score'] > 0]['url_scanner_score'].mean(),
            'median_score': df_merged[df_merged['url_scanner_score'] > 0]['url_scanner_score'].median(),
        })

    # ============================================================================
    # SAVE OUTPUT
    # ============================================================================

    print("\n5. Saving enriched dataset...")
    df_merged.to_csv(output_file, index=False)
    print(f"   ✓ Saved to: {output_file}")
    print(f"   ✓ Shape: {df_merged.shape}")
    print(f"   ✓ New columns added: url_scanner_score, url_likelihood, url_scan_status, url_indicators")

    # ============================================================================
    # SAVE STATISTICS REPORT
    # ============================================================================

    print("\n6. Generating statistics report...")

    # Ensure parent directory exists
    stats_file.parent.mkdir(parents=True, exist_ok=True)

    with open(stats_file, 'w') as f:
        f.write("="*80 + "\n")
        f.write("URL SCANNER MERGE STATISTICS\n")
        f.write("="*80 + "\n\n")

        f.write(f"Total papers: {stats['total_papers']:,}\n")
        f.write(f"Papers with URLs: {stats['papers_with_urls']:,} ({stats['papers_with_urls']/stats['total_papers']*100:.1f}%)\n")
        f.write(f"Papers without URLs: {stats['papers_without_urls']:,} ({stats['papers_without_urls']/stats['total_papers']*100:.1f}%)\n\n")

        if has_scan_results:
            f.write("="*80 + "\n")
            f.write("SCANNER RESULTS\n")
            f.write("="*80 + "\n\n")

            f.write("Scan Status:\n")
            f.write(f"  Success:      {stats['scanned_success']:>6,} ({stats['scanned_success']/stats['papers_with_urls']*100:>5.1f}% of URLs)\n")
            f.write(f"  Failed:       {stats['scanned_failed']:>6,} ({stats['scanned_failed']/stats['papers_with_urls']*100:>5.1f}% of URLs)\n")
            f.write(f"  Not scanned:  {stats['not_scanned']:>6,}\n\n")

            f.write("URL Quality Classification:\n")
            f.write(f"  CRITICAL:     {stats['critical']:>6,} ({stats['critical']/stats['scanned_success']*100:>5.1f}% of successful scans)\n")
            f.write(f"  HIGH:         {stats['high']:>6,} ({stats['high']/stats['scanned_success']*100:>5.1f}%)\n")
            f.write(f"  MEDIUM:       {stats['medium']:>6,} ({stats['medium']/stats['scanned_success']*100:>5.1f}%)\n")
            f.write(f"  LOW:          {stats['low']:>6,} ({stats['low']/stats['scanned_success']*100:>5.1f}%)\n")
            f.write(f"  VERY_LOW:     {stats['very_low']:>6,} ({stats['very_low']/stats['scanned_success']*100:>5.1f}%)\n\n")

            high_quality = stats['critical'] + stats['high']
            f.write(f"High-quality URLs (CRITICAL + HIGH): {high_quality:,} ({high_quality/stats['scanned_success']*100:.1f}%)\n\n")

            f.write("Scanner Score Statistics:\n")
            f.write(f"  Mean score:   {stats['mean_score']:>6.1f}\n")
            f.write(f"  Median score: {stats['median_score']:>6.0f}\n\n")

        else:
            f.write("="*80 + "\n")
            f.write("NO SCANNER RESULTS AVAILABLE\n")
            f.write("="*80 + "\n\n")
            f.write("Run scanner scripts to generate URL quality scores:\n")
            f.write("  1. scripts/phase6_scanning/14_prepare_urls.py\n")
            f.write("  2. scripts/phase6_scanning/15_scan_urls.py\n")
            f.write("  3. Re-run this script to merge scores\n")

    print(f"   ✓ Saved statistics to: {stats_file}")

    # ============================================================================
    # SUMMARY
    # ============================================================================

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    print(f"\nPapers processed: {stats['total_papers']:,}")
    print(f"Papers with URLs: {stats['papers_with_urls']:,} ({stats['papers_with_urls']/stats['total_papers']*100:.1f}%)")

    if has_scan_results:
        print(f"\nSuccessfully scanned: {stats['scanned_success']:,}")
        high_quality = stats['critical'] + stats['high']
        print(f"High-quality URLs: {high_quality:,} ({high_quality/stats['scanned_success']*100:.1f}%)")
        print(f"Mean scanner score: {stats['mean_score']:.1f}")
    else:
        print(f"\n⚠ No scanner results merged (placeholder columns added)")

    print(f"\nOutput: {output_file}")
    print(f"Stats:  {stats_file}")

    print("\n" + "="*80)
    print("COMPLETE!")
    print("="*80)


def main():
    """Main entry point."""
    args = parse_args()
    papers_file, scan_results_file, output_file, stats_file = resolve_paths(args)
    merge_scanner_scores(papers_file, scan_results_file, output_file, stats_file)


if __name__ == "__main__":
    main()
