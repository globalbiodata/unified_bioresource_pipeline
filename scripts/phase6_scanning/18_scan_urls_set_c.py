#!/usr/bin/env python3
"""
URL Scanning for Set C (Union)

Scans all URLs in Set C using the integrated bioresource_url_scanner and adds
validation columns to the dataset.

Created: 2025-11-20
Updated: 2025-11-21 (Added session support)
Updated: 2025-12-04 (Refactored with argparse and session-dir integration)
Updated: 2025-12-05 (Integrated scanner into lib/url_scanner)
Estimated time: 75-90 minutes for full scan
"""

import argparse
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

# Add project root to path for lib imports
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from lib.session_utils import get_session_path, validate_session_dir
from lib.url_scanner import BioresourceScanner

# ============================================================================
# ARGUMENT PARSING
# ============================================================================

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Scan URLs in Set C using bioresource_url_scanner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Session mode (recommended):
  python 18_scan_urls_set_c.py --session-dir 2025-12-04-111420-z381s

  # Session mode with specific profile:
  python 18_scan_urls_set_c.py --session-dir 2025-12-04-111420-z381s --profile balanced

  # Legacy mode with auto-detection:
  python 18_scan_urls_set_c.py --auto

  # Custom timeout:
  python 18_scan_urls_set_c.py --session-dir results/session1 --timeout 7200
        """
    )

    # Input/output modes
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        '--session-dir',
        type=str,
        help='Session directory path (e.g., results/2025-12-04-143052-a3f9b)'
    )
    mode_group.add_argument(
        '--auto',
        action='store_true',
        help='Auto-detect legacy input files from pipeline_synthesis_2025-11-18'
    )

    # Profile selection (for deduplication output)
    parser.add_argument(
        '--profile',
        type=str,
        default='aggressive',
        choices=['conservative', 'balanced', 'aggressive'],
        help='Deduplication profile to use (default: aggressive)'
    )

    # Optional file overrides
    parser.add_argument(
        '--input-file',
        type=str,
        help='Custom input file path (overrides default)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Custom output directory (overrides default)'
    )

    # Scanner parameters
    parser.add_argument(
        '--timeout',
        type=int,
        default=20,
        help='Scanner timeout per request in seconds (default: 20)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='Number of concurrent workers (default: 10)'
    )
    parser.add_argument(
        '--domain-delay',
        type=float,
        default=1.0,
        help='Delay between requests to same domain in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--session-id',
        type=str,
        help='Session ID for scanner output file naming (auto-generated if not provided)'
    )

    return parser.parse_args()


# ============================================================================
# PATH CONFIGURATION
# ============================================================================

def configure_paths(args):
    """Configure input/output paths based on arguments."""
    paths = {}

    # Base directories
    BASE_DIR = PROJECT_ROOT.parent if PROJECT_ROOT.name == 'unified_bioresource_pipeline' else PROJECT_ROOT
    paths['base_dir'] = BASE_DIR

    # Mode-specific paths
    if args.session_dir:
        # Session mode
        session_dir = Path(args.session_dir).resolve()

        # Validate session exists
        if not session_dir.exists():
            print(f"ERROR: Session directory not found: {session_dir}")
            sys.exit(1)

        # Validate required phases
        try:
            validate_session_dir(session_dir, required_phases=['07_deduplication'])
        except ValueError as e:
            print(f"ERROR: Session validation failed: {e}")
            sys.exit(1)

        paths['session_dir'] = session_dir
        paths['mode'] = 'session'
        paths['profile'] = args.profile

        # Default paths within session - check multiple locations in order of preference
        if args.input_file:
            paths['input_file'] = Path(args.input_file)
        else:
            # Priority 1: URL recovery output (has recovered URLs)
            url_recovery_file = get_session_path(
                session_dir, '08_url_recovery/final', 'set_c_with_urls.csv'
            )
            # Priority 2: Dedup output with profile
            dedup_profile_file = get_session_path(
                session_dir, f'07_deduplication/{args.profile}', 'set_c_final.csv'
            )
            # Priority 3: Legacy dedup output (backward compat)
            dedup_legacy_file = get_session_path(
                session_dir, '07_deduplication', 'set_c_dedup.csv'
            )

            if url_recovery_file.exists():
                paths['input_file'] = url_recovery_file
                print(f"Using URL recovery output: {url_recovery_file}")
            elif dedup_profile_file.exists():
                paths['input_file'] = dedup_profile_file
                print(f"Using dedup output ({args.profile} profile): {dedup_profile_file}")
            elif dedup_legacy_file.exists():
                paths['input_file'] = dedup_legacy_file
                print(f"Using legacy dedup output: {dedup_legacy_file}")
            else:
                print(f"ERROR: No input file found. Checked:")
                print(f"  - {url_recovery_file}")
                print(f"  - {dedup_profile_file}")
                print(f"  - {dedup_legacy_file}")
                sys.exit(1)

        paths['output_dir'] = Path(args.output_dir) if args.output_dir else get_session_path(
            session_dir, '06_scanning'
        )

    elif args.auto:
        # Legacy auto-detection mode
        paths['mode'] = 'legacy'
        legacy_base = BASE_DIR / 'pipeline_synthesis_2025-11-18'

        paths['input_file'] = args.input_file if args.input_file else (
            legacy_base / 'results' / 'deduplicated' / 'set_c_union_dedup.csv'
        )
        paths['output_dir'] = Path(args.output_dir) if args.output_dir else (
            legacy_base / 'results' / 'url_scanned'
        )

    # Validate input file exists
    if not Path(paths['input_file']).exists():
        print(f"ERROR: Input file not found: {paths['input_file']}")
        sys.exit(1)

    # Create output directory
    Path(paths['output_dir']).mkdir(parents=True, exist_ok=True)

    # Output files
    paths['output_file'] = Path(paths['output_dir']) / 'set_c_with_url_scan.csv'
    paths['stats_file'] = Path(paths['output_dir']) / 'url_scan_statistics.txt'

    # Session ID for output naming
    if args.session_id:
        session_id = args.session_id
    elif args.session_dir:
        session_id = Path(args.session_dir).name
    else:
        session_id = datetime.now().strftime('%Y-%m-%d-%H%M%S')

    paths['session_id'] = session_id
    paths['scan_results_file'] = Path(paths['output_dir']) / f'scan_results_{session_id}.csv'

    return paths


# ============================================================================
# MAIN SCANNING WORKFLOW
# ============================================================================

def load_set_c(input_file):
    """Load Set C data."""
    print("1. Loading Set C...")
    df_c = pd.read_csv(input_file)
    print(f"   Total resources: {len(df_c)}")

    urls_present = df_c['resource_url'].notna().sum()
    print(f"   Resources with URLs: {urls_present}")

    return df_c, urls_present


def prepare_urls_for_scanner(df_c):
    """Prepare URLs for scanner."""
    print("\n2. Preparing URLs for scanner...")

    # Create URL dataset for scanner
    url_data = df_c[df_c['resource_url'].notna()][
        ['pmid', 'resource_url', 'primary_entity_long', 'primary_entity_short']
    ].copy()

    url_data = url_data.rename(columns={
        'pmid': 'id',
        'resource_url': 'url',
        'primary_entity_long': 'entity_long',
        'primary_entity_short': 'entity_short'
    })

    # Add domain column (required by scanner)
    url_data['domain'] = url_data['url'].apply(
        lambda x: urlparse(str(x)).netloc if pd.notna(x) else ''
    )

    print(f"   Prepared {len(url_data)} URLs")
    print(f"   Unique domains: {url_data['domain'].nunique()}")

    return url_data


def run_url_scanner(url_data, paths, args):
    """Run the integrated URL scanner."""
    print("\n3. Running URL scanner...")
    print(f"   URLs to scan: {len(url_data)}")
    print(f"   Estimated time: 75-90 minutes")
    print(f"   Timeout per request: {args.timeout}s")
    print(f"   Workers: {args.workers}")
    print(f"   Domain delay: {args.domain_delay}s")
    print(f"   Scanner: lib/url_scanner (integrated)")

    print(f"\n   Starting scan at {datetime.now().strftime('%H:%M:%S')}...")
    print(f"   (This will take approximately 75-90 minutes)\n")

    try:
        # Create scanner instance
        scanner = BioresourceScanner(
            max_workers=args.workers,
            domain_delay=args.domain_delay,
            timeout=args.timeout
        )

        # Convert DataFrame to list of dicts for scanner
        import time
        start_time = time.time()

        # Create checkpoint file path for incremental saves
        checkpoint_file = paths['output_dir'] / 'scan_checkpoint.csv'

        # Run scan with checkpoint file for robustness
        results = scanner.scan_batch(
            url_data.to_dict('records'),
            show_progress=True,
            checkpoint_file=str(checkpoint_file)
        )

        total_time = time.time() - start_time

        # Convert to DataFrame
        results_df = pd.DataFrame(results)
        results_df['indicators_found'] = results_df['indicators_found'].apply(
            lambda x: '; '.join(x) if isinstance(x, list) else ''
        )

        # Save results
        results_df.to_csv(paths['scan_results_file'], index=False)

        print(f"\n   Scan completed at {datetime.now().strftime('%H:%M:%S')}")
        print(f"   Total time: {total_time / 60:.1f} minutes")
        print(f"   Results saved to: {paths['scan_results_file'].name}")

        return paths['scan_results_file'], results_df

    except Exception as e:
        print(f"\nERROR: Scanner execution failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def print_scan_summary(scan_df):
    """Print summary of scan results."""
    print(f"\n4. Scan Summary...")
    print(f"   Scanned URLs: {len(scan_df)}")

    # Connectivity stats
    live_count = scan_df['is_live'].sum()
    failed_count = (~scan_df['is_live']).sum()
    print(f"   Live URLs: {live_count} ({live_count / len(scan_df) * 100:.1f}%)")
    print(f"   Failed: {failed_count} ({failed_count / len(scan_df) * 100:.1f}%)")

    # Wayback stats
    if 'wayback_used' in scan_df.columns:
        wayback_count = scan_df['wayback_used'].sum()
        if wayback_count > 0:
            print(f"   Wayback rescues: {wayback_count} ({wayback_count / len(scan_df) * 100:.1f}%)")

    # Likelihood distribution
    if 'likelihood' in scan_df.columns:
        print(f"\n   Likelihood Distribution:")
        for likelihood in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'VERY LOW']:
            count = (scan_df['likelihood'] == likelihood).sum()
            pct = count / len(scan_df) * 100
            print(f"     {likelihood:12s}: {count:4d} ({pct:5.1f}%)")


def merge_scan_results(df_c, scan_df):
    """Merge scan results with Set C."""
    print("\n5. Merging scan results with Set C...")

    # Prepare scan data for merge (using actual column names from scanner V4)
    scan_columns = ['url', 'status_code', 'final_url', 'total_score',
                   'is_live', 'likelihood', 'indicators_found', 'wayback_used']

    # Only use columns that exist in scan_df
    available_columns = [col for col in scan_columns if col in scan_df.columns]
    scan_data = scan_df[available_columns].copy()

    # Rename columns with url_ prefix
    rename_map = {
        'status_code': 'url_status',
        'final_url': 'url_final',
        'total_score': 'url_score',
        'is_live': 'url_is_live',
        'likelihood': 'url_likelihood',
        'indicators_found': 'url_indicators_found',
        'wayback_used': 'url_wayback_used'
    }

    scan_data = scan_data.rename(columns={
        k: v for k, v in rename_map.items() if k in scan_data.columns
    })

    # Merge with Set C
    df_c_scanned = df_c.merge(scan_data, left_on='resource_url', right_on='url', how='left')

    if 'url' in df_c_scanned.columns:
        df_c_scanned = df_c_scanned.drop(columns=['url'])

    # Fill NaN for resources without URLs
    url_cols = ['url_status', 'url_final', 'url_score', 'url_is_live',
               'url_likelihood', 'url_indicators_found', 'url_wayback_used']

    for col in url_cols:
        if col not in df_c_scanned.columns:
            df_c_scanned[col] = None

    print(f"   Merged: {len(df_c_scanned)} resources")
    print(f"   With URL scan data: {df_c_scanned['url_status'].notna().sum()}")

    return df_c_scanned


def save_output(df_c_scanned, output_file):
    """Save merged output."""
    print("\n6. Saving output...")
    df_c_scanned.to_csv(output_file, index=False)
    print(f"   Saved to: {output_file}")


def generate_statistics(df_c, df_c_scanned, scan_df, urls_present, stats_file):
    """Generate and save statistics."""
    print("\n7. Generating statistics...")

    stats = []
    stats.append("="*80)
    stats.append("SET C URL SCANNING STATISTICS")
    stats.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    stats.append("="*80)
    stats.append("")

    stats.append(f"Total resources in Set C: {len(df_c)}")
    stats.append(f"Resources with URLs: {urls_present}")
    stats.append(f"URLs scanned: {len(scan_df)}")
    stats.append("")

    # URL scan results breakdown
    if 'url_status' in df_c_scanned.columns:
        status_counts = df_c_scanned['url_status'].value_counts()
        stats.append("URL Scan Status:")
        for status, count in status_counts.items():
            stats.append(f"  {status}: {count}")
        stats.append("")

    # Score distribution
    if 'url_score' in df_c_scanned.columns:
        scored = df_c_scanned['url_score'].notna().sum()
        if scored > 0:
            avg_score = df_c_scanned['url_score'].mean()
            stats.append(f"URLs with scores: {scored}")
            stats.append(f"Average score: {avg_score:.2f}")
            stats.append("")

            # Score categories
            very_high = (df_c_scanned['url_score'] >= 0.8).sum()
            high = ((df_c_scanned['url_score'] >= 0.6) &
                   (df_c_scanned['url_score'] < 0.8)).sum()
            medium = ((df_c_scanned['url_score'] >= 0.4) &
                     (df_c_scanned['url_score'] < 0.6)).sum()
            low = (df_c_scanned['url_score'] < 0.4).sum()

            stats.append("Score Distribution:")
            stats.append(f"  Very High (≥0.8): {very_high}")
            stats.append(f"  High (0.6-0.8): {high}")
            stats.append(f"  Medium (0.4-0.6): {medium}")
            stats.append(f"  Low (<0.4): {low}")
            stats.append("")

    # Wayback usage
    if 'url_wayback_used' in df_c_scanned.columns:
        wayback_count = df_c_scanned['url_wayback_used'].sum()
        stats.append(f"Wayback Machine used: {wayback_count}")
        stats.append("")

    stats_text = '\n'.join(stats)

    with open(stats_file, 'w') as f:
        f.write(stats_text)

    print(stats_text)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    args = parse_args()
    paths = configure_paths(args)

    print("="*80)
    print("URL SCANNING FOR SET C (UNION)")
    print("="*80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {paths['mode']}")
    if paths['mode'] == 'session':
        print(f"Session: {paths['session_dir'].name}")
    print(f"Input: {paths['input_file']}")
    print(f"Output: {paths['output_dir']}")
    print()

    # Execute workflow
    df_c, urls_present = load_set_c(paths['input_file'])
    url_data = prepare_urls_for_scanner(df_c)
    scan_file, scan_df = run_url_scanner(url_data, paths, args)
    print_scan_summary(scan_df)
    df_c_scanned = merge_scan_results(df_c, scan_df)
    save_output(df_c_scanned, paths['output_file'])
    generate_statistics(df_c, df_c_scanned, scan_df, urls_present, paths['stats_file'])

    print("\n" + "="*80)
    print("COMPLETE!")
    print("="*80)
    print(f"\nOutput file: {paths['output_file']}")
    print(f"Statistics: {paths['stats_file']}")
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nERROR: Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
