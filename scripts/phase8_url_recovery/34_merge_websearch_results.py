#!/usr/bin/env python3
"""
Script 34: Filter and Finalize URL Recovery Results

Purpose: Apply URL exclusion filters to consolidated web search results
         and produce final output for Phase 9 (Finalization).

         Supports two modes:
         1. Merged mode (--input): Single pre-merged CSV with all sources
         2. Split mode (--recovered + --websearch-dir): Separate files to merge

Input:  Mode 1: Single merged CSV with columns:
            original_record_num, database_name, long_database_name,
            found_urls, url_source, match_quality, notes
        Mode 2:
            - recovered_urls.csv (from script 33)
            - websearch_chunks/websearch_results_*.csv (from agents)

Output: - final_url_recovery.csv (filtered results)
        - excluded_urls.csv (URLs filtered out by rules)
        - url_recovery_summary.json (statistics)

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-11-28
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# Add lib to path for session utilities
SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

# Add unified_bioresource_pipeline to path for session utils
pipeline_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(pipeline_root))
from lib.session_utils import validate_session_dir, get_session_path


# Required columns in agent output files
REQUIRED_COLUMNS = [
    'original_record_num',
    'database_name',
    'found_urls',
    'url_source',
    'match_quality',
]

# Valid values for validation
VALID_SOURCES = ['abstract', 'fulltext', 'web_search', 'not_available']
VALID_QUALITIES = ['HIGH', 'MEDIUM', 'LOW', '']

# =============================================================================
# URL EXCLUSION PATTERNS
# These URL types are automatically filtered out during merge
# =============================================================================

EXCLUDE_PATTERNS = [
    # Code repositories
    (r'github\.com/', 'GitHub repository'),
    (r'\.github\.io/', 'GitHub Pages'),
    (r'gitlab\.com/', 'GitLab repository'),
    (r'bitbucket\.org/', 'Bitbucket repository'),
    (r'sourceforge\.net/', 'SourceForge'),

    # Data archives & DOIs
    (r'zenodo\.org/', 'Zenodo archive'),
    (r'doi\.org/', 'DOI link'),
    (r'datadryad\.org/', 'Dryad archive'),
    (r'10\.5061/dryad', 'Dryad DOI'),
    (r'figshare\.com/', 'Figshare'),
    (r'm9\.figshare', 'Figshare'),
    (r'osf\.io/', 'Open Science Framework'),

    # File servers
    (r'^ftp://', 'FTP server'),
    (r'ftp\.[a-z]', 'FTP server'),

    # Package repositories
    (r'cran\.r-project\.org/', 'CRAN package'),
    (r'bioconductor\.org/packages/', 'Bioconductor package'),
    (r'pypi\.org/', 'PyPI package'),
]


def is_excluded_url(url: str) -> Tuple[bool, str]:
    """
    Check if a URL matches any exclusion pattern.

    Args:
        url: URL string to check

    Returns:
        Tuple of (is_excluded, reason)
    """
    if not url or url == 'NOT_FOUND':
        return False, ''

    url_lower = url.lower()

    for pattern, reason in EXCLUDE_PATTERNS:
        if re.search(pattern, url_lower):
            return True, reason

    return False, ''


def filter_urls(urls_str: str) -> Tuple[str, List[str]]:
    """
    Filter a pipe-separated URL string, removing excluded URLs.

    Args:
        urls_str: Pipe-separated URL string

    Returns:
        Tuple of (filtered_urls_str, list of excluded reasons)
    """
    if not urls_str or urls_str == 'NOT_FOUND':
        return urls_str, []

    urls = urls_str.split('|')
    kept_urls = []
    excluded_reasons = []

    for url in urls:
        url = url.strip()
        if not url:
            continue

        is_excluded, reason = is_excluded_url(url)
        if is_excluded:
            excluded_reasons.append(f"{url} ({reason})")
        else:
            kept_urls.append(url)

    if not kept_urls:
        return 'NOT_FOUND', excluded_reasons

    return '|'.join(kept_urls), excluded_reasons


def filter_dataframe_urls(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    Filter URLs in a DataFrame, tracking exclusions.

    Args:
        df: DataFrame with 'found_urls' column

    Returns:
        Tuple of (filtered_df, excluded_df, stats)
    """
    if df.empty or 'found_urls' not in df.columns:
        return df, pd.DataFrame(), {}

    filtered_records = []
    excluded_records = []
    stats = {
        'total_checked': 0,
        'urls_filtered': 0,
        'records_converted_to_not_found': 0,
        'exclusion_reasons': {}
    }

    for idx, row in df.iterrows():
        record = row.to_dict()
        original_urls = str(record.get('found_urls', ''))
        stats['total_checked'] += 1

        if original_urls == 'NOT_FOUND' or pd.isna(record.get('found_urls')):
            filtered_records.append(record)
            continue

        filtered_urls, excluded_reasons = filter_urls(original_urls)

        if excluded_reasons:
            stats['urls_filtered'] += len(excluded_reasons)
            for reason in excluded_reasons:
                # Extract just the reason type
                reason_type = reason.split('(')[-1].rstrip(')')
                stats['exclusion_reasons'][reason_type] = \
                    stats['exclusion_reasons'].get(reason_type, 0) + 1

        if filtered_urls == 'NOT_FOUND':
            # All URLs were excluded
            stats['records_converted_to_not_found'] += 1
            excluded_record = record.copy()
            excluded_record['exclusion_reasons'] = '; '.join(excluded_reasons)
            excluded_records.append(excluded_record)

            # Convert to NOT_FOUND
            record['found_urls'] = 'NOT_FOUND'
            record['url_source'] = 'filtered_out'
            existing_notes = record.get('notes', '')
            existing_notes = '' if pd.isna(existing_notes) else str(existing_notes)
            record['notes'] = existing_notes + f' [Filtered: {"; ".join(excluded_reasons)}]'
        else:
            record['found_urls'] = filtered_urls
            if excluded_reasons:
                existing_notes = record.get('notes', '')
                existing_notes = '' if pd.isna(existing_notes) else str(existing_notes)
                record['notes'] = existing_notes + f' [Some URLs filtered: {len(excluded_reasons)}]'

        filtered_records.append(record)

    return (
        pd.DataFrame(filtered_records),
        pd.DataFrame(excluded_records),
        stats
    )


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Filter and finalize URL recovery results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Process merged results from session directory
  python 34_merge_websearch_results.py --session-dir results/2025-12-04-111420-z381s
        '''
    )

    # Session directory (required)
    parser.add_argument(
        '--session-dir',
        required=True,
        help='Session directory containing pipeline data'
    )

    # Optional filtering options
    parser.add_argument(
        '--validate',
        action='store_true',
        default=True,
        help='Validate input format (default: True)'
    )
    parser.add_argument(
        '--strict',
        action='store_true',
        help='Fail on validation errors (default: warn only)'
    )
    parser.add_argument(
        '--no-filter',
        action='store_true',
        help='Skip URL filtering (keep GitHub, Zenodo, DOI links)'
    )
    parser.add_argument(
        '--filter-sources',
        nargs='+',
        default=['web_search'],
        help='Which url_source values to apply filtering to (default: web_search only)'
    )

    return parser.parse_args()


def validate_agent_output(df: pd.DataFrame, filename: str) -> List[str]:
    """
    Validate that agent output matches expected format.

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    # Check required columns
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")

    # Check url_source values
    if 'url_source' in df.columns:
        invalid_sources = df[~df['url_source'].isin(VALID_SOURCES)]['url_source'].unique()
        if len(invalid_sources) > 0:
            errors.append(f"Invalid url_source values: {list(invalid_sources)}")

    # Check match_quality values
    if 'match_quality' in df.columns:
        # Allow NaN/empty for NOT_FOUND records
        valid_mask = df['match_quality'].isna() | df['match_quality'].isin(VALID_QUALITIES)
        invalid_quality = df[~valid_mask]['match_quality'].unique()
        if len(invalid_quality) > 0:
            errors.append(f"Invalid match_quality values: {list(invalid_quality)}")

    # Check found_urls format
    if 'found_urls' in df.columns:
        # URLs should not contain spaces (except NOT_FOUND)
        url_issues = []
        for idx, url in df['found_urls'].items():
            if pd.notna(url) and url != 'NOT_FOUND':
                if ' ' in str(url) and '|' not in str(url):
                    url_issues.append(idx)
        if url_issues:
            errors.append(f"URLs with spaces at rows: {url_issues[:5]}...")

    return errors


def load_websearch_results(websearch_dir: Path, validate: bool = True, strict: bool = False) -> pd.DataFrame:
    """Load and combine all websearch result files."""
    result_files = sorted(websearch_dir.glob('websearch_results_*.csv'))

    if not result_files:
        # Also try alternative naming patterns
        result_files = sorted(websearch_dir.glob('results_chunk_*.csv'))

    if not result_files:
        print(f"  WARNING: No websearch result files found in {websearch_dir}")
        return pd.DataFrame()

    all_results = []
    validation_errors = []

    for filepath in result_files:
        print(f"  Loading: {filepath.name}")
        try:
            df = pd.read_csv(filepath)

            if validate:
                errors = validate_agent_output(df, filepath.name)
                if errors:
                    validation_errors.extend([f"{filepath.name}: {e}" for e in errors])
                    if strict:
                        continue  # Skip invalid files in strict mode

            # Ensure url_source is set to web_search for found URLs
            if 'url_source' in df.columns:
                found_mask = (df['found_urls'] != 'NOT_FOUND') & (df['found_urls'].notna())
                df.loc[found_mask, 'url_source'] = 'web_search'

            all_results.append(df)
            print(f"    Records: {len(df)}, Found: {(df['found_urls'] != 'NOT_FOUND').sum()}")

        except Exception as e:
            print(f"    ERROR: {e}")
            validation_errors.append(f"{filepath.name}: Load error - {e}")

    if validation_errors:
        print(f"\n  Validation issues:")
        for err in validation_errors[:10]:
            print(f"    - {err}")
        if len(validation_errors) > 10:
            print(f"    ... and {len(validation_errors) - 10} more")

        if strict and validation_errors:
            raise ValueError("Validation failed in strict mode")

    if not all_results:
        return pd.DataFrame()

    return pd.concat(all_results, ignore_index=True)


def main():
    args = parse_args()

    print(f"=" * 60)
    print(f"Script 34: Filter and Finalize URL Recovery Results")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"=" * 60)

    # Validate session directory and setup paths
    validate_session_dir(args.session_dir)
    session_dir = args.session_dir
    url_recovery_dir = get_session_path(session_dir, '08_url_recovery')

    # Define input and output paths
    recovered_path = url_recovery_dir / 'recovered_urls.csv'
    websearch_dir = url_recovery_dir / 'websearch_chunks'
    output_file = url_recovery_dir / 'final_url_recovery.csv'
    excluded_file = url_recovery_dir / 'excluded_urls.csv'
    summary_file = url_recovery_dir / 'url_recovery_summary.json'

    # Create output directory if needed
    url_recovery_dir.mkdir(parents=True, exist_ok=True)

    filter_stats = {}
    all_excluded = pd.DataFrame()
    websearch_found = 0
    websearch_found_after_filter = 0

    # ==========================================================================
    # Load recovered URLs (if exists) and websearch results
    # ==========================================================================
    # Check what files exist to determine mode
    has_recovered = recovered_path.exists()
    has_websearch = websearch_dir.exists()

    print(f"\nSession: {session_dir.name}")
    print(f"URL Recovery Dir: {url_recovery_dir}")
    print(f"  Recovered URLs: {'Found' if has_recovered else 'Not found'}")
    print(f"  Websearch Dir: {'Found' if has_websearch else 'Not found'}")

    if not has_recovered and not has_websearch:
        print("\nERROR: No input files found. Need either:")
        print(f"  - {recovered_path}")
        print(f"  - {websearch_dir}/")
        sys.exit(1)

    # Load recovered URLs (abstract + fulltext sources)
    if has_recovered:
        print(f"\nLoading recovered URLs: {recovered_path}")
        recovered_df = pd.read_csv(recovered_path)
        print(f"  Records with URLs (abstract+fulltext): {len(recovered_df)}")
    else:
        recovered_df = pd.DataFrame()

    # Load web search results
    if has_websearch:
        print(f"\nLoading web search results from: {websearch_dir}")
        websearch_df = load_websearch_results(
            websearch_dir,
            validate=args.validate,
            strict=args.strict
        )
    else:
        websearch_df = pd.DataFrame()

    # ==========================================================================
    # Process and merge results
    # ==========================================================================
    if websearch_df.empty:
        print("\nNo web search results to process.")
        if recovered_df.empty:
            print("ERROR: No data to process!")
            sys.exit(1)
        final_df = recovered_df.copy()
        websearch_found = 0
        websearch_found_after_filter = 0
    else:
        # Filter to found URLs only
        websearch_found_df = websearch_df[websearch_df['found_urls'] != 'NOT_FOUND'].copy()
        websearch_found = len(websearch_found_df)
        print(f"  Web search URLs found (before filtering): {websearch_found}")

        # Apply URL filtering unless --no-filter is set
        if not args.no_filter:
            print(f"\n  Applying URL exclusion filters...")
            websearch_found_df, excluded_df, filter_stats = filter_dataframe_urls(websearch_found_df)
            all_excluded = excluded_df

            websearch_found_after_filter = len(
                websearch_found_df[websearch_found_df['found_urls'] != 'NOT_FOUND']
            )

            print(f"  URLs filtered out: {filter_stats.get('urls_filtered', 0)}")
            print(f"  Records converted to NOT_FOUND: {filter_stats.get('records_converted_to_not_found', 0)}")
            print(f"  Web search URLs found (after filtering): {websearch_found_after_filter}")

            if filter_stats.get('exclusion_reasons'):
                print(f"\n  Exclusion breakdown:")
                for reason, count in sorted(filter_stats['exclusion_reasons'].items()):
                    print(f"    {reason}: {count}")
        else:
            print(f"\n  URL filtering SKIPPED (--no-filter)")
            websearch_found_after_filter = websearch_found

        # Only keep found URLs (after filtering)
        websearch_found_df = websearch_found_df[
            websearch_found_df['found_urls'] != 'NOT_FOUND'
        ].copy()

        # Combine all found URLs
        if not recovered_df.empty:
            final_df = pd.concat([recovered_df, websearch_found_df], ignore_index=True)
        else:
            final_df = websearch_found_df

    # Deduplicate by original_record_num (keep first = highest priority source)
    # Priority: abstract > fulltext > web_search
    source_priority = {'abstract': 0, 'fulltext': 1, 'web_search': 2, 'filtered_out': 98, 'not_available': 99}
    if 'url_source' in final_df.columns:
        final_df['_priority'] = final_df['url_source'].map(source_priority).fillna(99)
        final_df = final_df.sort_values('_priority')
        final_df = final_df.drop_duplicates(subset=['original_record_num'], keep='first')
        final_df = final_df.drop(columns=['_priority'])

    # Save final output
    final_df.to_csv(output_file, index=False)
    print(f"\nSaved: {output_file}")
    print(f"  Total records with URLs: {len(final_df)}")

    # Save excluded URLs if any
    if not all_excluded.empty:
        all_excluded.to_csv(excluded_file, index=False)
        print(f"Saved: {excluded_file}")
        print(f"  Excluded records: {len(all_excluded)}")

    # Calculate statistics - convert numpy types to Python native for JSON serialization
    source_counts = {k: int(v) for k, v in final_df['url_source'].value_counts().items()} if 'url_source' in final_df.columns else {}
    quality_counts = {k: int(v) for k, v in final_df['match_quality'].value_counts().items()} if 'match_quality' in final_df.columns else {}

    # Build summary - convert all numpy types to Python native
    exclusion_reasons = {}
    if filter_stats and filter_stats.get('exclusion_reasons'):
        exclusion_reasons = {k: int(v) for k, v in filter_stats['exclusion_reasons'].items()}

    summary = {
        'timestamp': datetime.now().isoformat(),
        'session_dir': str(session_dir),
        'total_urls_recovered': int(len(final_df)),
        'by_source': source_counts,
        'by_quality': quality_counts,
        'websearch_files_processed': int(len(list(websearch_dir.glob('websearch_results_*.csv')))) if has_websearch else 0,
        'websearch_urls_found_before_filter': int(websearch_found),
        'websearch_urls_found_after_filter': int(websearch_found_after_filter),
        'filtering': {
            'enabled': not args.no_filter,
            'urls_filtered': int(filter_stats.get('urls_filtered', 0)) if filter_stats else 0,
            'records_excluded': int(filter_stats.get('records_converted_to_not_found', 0)) if filter_stats else 0,
            'exclusion_reasons': exclusion_reasons,
        }
    }

    # Save summary
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"Saved: {summary_file}")

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Summary")
    print(f"{'=' * 60}")
    print(f"  Total URLs recovered: {len(final_df)}")
    print(f"\n  By source:")
    for src, count in source_counts.items():
        print(f"    {src}: {count}")
    print(f"\n  By quality:")
    for qual, count in sorted(quality_counts.items()):
        print(f"    {qual}: {count}")

    if filter_stats:
        print(f"\n  URL Filtering:")
        print(f"    URLs removed: {filter_stats.get('urls_filtered', 0)}")
        print(f"    Records excluded: {filter_stats.get('records_converted_to_not_found', 0)}")

    print(f"\n{'=' * 60}")
    print(f"Output Files")
    print(f"{'=' * 60}")
    print(f"  {output_file}")
    if not all_excluded.empty:
        print(f"  {excluded_file}")
    print(f"  {summary_file}")

    print(f"\nMerge complete. Final file ready for Phase 9 (Finalization).")
    print(f"Completed: {datetime.now().isoformat()}")


if __name__ == '__main__':
    main()
