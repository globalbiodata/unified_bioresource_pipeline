#!/usr/bin/env python3
"""
Script 32: Search Fulltext for URLs

Purpose: Search fulltext for URLs for records still missing after abstract search.
         Uses same enhanced patterns and exclusion rules.

Input:  - abstract_url_results.csv (from script 30)
        - abstracts_cache.json (for PMCID lookup)
        - fulltext_cache.json (from script 31)
Output: fulltext_url_results.csv

Author: Pipeline Team
Date: 2025-11-28
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from url_patterns import extract_and_filter_urls
# Add unified_bioresource_pipeline to path for session utils
pipeline_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(pipeline_root))
from lib.session_utils import validate_session_dir, get_session_path


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Search fulltext for URLs'
    )
    parser.add_argument(
        '--session-dir',
        required=True,
        help='Session directory containing pipeline data'
    )

    return parser.parse_args()


def search_record_fulltext(
    record: Dict,
    abstracts_cache: Dict,
    fulltext_cache: Dict
) -> Tuple[str, str, str, str]:
    """
    Search fulltext for URLs for a record.

    Args:
        record: Record dict with pmids, database_name, long_database_name
        abstracts_cache: Cache for PMCID lookup
        fulltext_cache: Cache of fetched fulltext

    Returns:
        Tuple of (found_urls, url_source, match_quality, notes)
    """
    pmids_val = record.get('pmids', '')
    if pd.isna(pmids_val) or pmids_val is None:
        pmids_val = str(record.get('pmid', ''))  # Fallback to single pmid column
    pmids = str(pmids_val).split('|')
    pmids = [p.strip() for p in pmids if p.strip()]
    # Clean float-like PMIDs (e.g., "39593035.0" -> "39593035")
    cleaned_pmids = []
    for p in pmids:
        if '.' in p:
            try:
                p = str(int(float(p)))
            except ValueError:
                pass
        cleaned_pmids.append(p)
    pmids = cleaned_pmids

    db_name = record.get('database_name', '')
    if pd.isna(db_name) or db_name is None:
        db_name = ''
    else:
        db_name = str(db_name)
    long_name = record.get('long_database_name', '')
    if pd.isna(long_name) or long_name is None:
        long_name = ''
    else:
        long_name = str(long_name)

    for pmid in pmids:
        # Get PMCID from abstracts cache
        abstract_data = abstracts_cache.get(pmid, {})
        pmcid = abstract_data.get('pmcid')

        if not pmcid:
            continue

        # Get fulltext
        fulltext_data = fulltext_cache.get(pmcid, {})
        fulltext = fulltext_data.get('fulltext', '')

        if not fulltext:
            continue

        # Extract and filter URLs
        url_results = extract_and_filter_urls(fulltext, db_name, long_name)

        if url_results:
            urls = [u for u, q, n in url_results]
            quality = url_results[0][1]

            return (
                '|'.join(urls),
                'fulltext',
                quality,
                f'Found in PMCID {pmcid}'
            )

    # No URLs found
    return ('NOT_FOUND', 'not_available', '', 'No URLs found in fulltext')


def main():
    args = parse_args()

    print(f"=" * 60)
    print(f"Script 32: Search Fulltext for URLs")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"=" * 60)

    # Validate session directory
    validate_session_dir(args.session_dir)
    session_dir = args.session_dir

    # Input files from scripts 29, 30, 31
    input_path = get_session_path(session_dir, '08_url_recovery', 'abstract_url_results.csv')
    abstracts_path = get_session_path(session_dir, '08_url_recovery', 'abstracts_cache.json')
    fulltext_path = get_session_path(session_dir, '08_url_recovery', 'fulltext_cache.json')

    # Output to URL recovery directory
    output_dir = get_session_path(session_dir, '08_url_recovery')
    output_file = output_dir / 'fulltext_url_results.csv'

    # Load abstract results
    print(f"\nLoading abstract results: {input_path}")
    df = pd.read_csv(input_path)
    print(f"  Total records: {len(df)}")

    # Filter to NOT_FOUND records only
    not_found_df = df[df['found_urls'] == 'NOT_FOUND'].copy()
    print(f"  Still missing URLs: {len(not_found_df)}")

    # Load caches
    print(f"\nLoading abstracts cache: {abstracts_path}")
    with open(abstracts_path, 'r') as f:
        abstracts_cache = json.load(f)

    print(f"Loading fulltext cache: {fulltext_path}")
    with open(fulltext_path, 'r') as f:
        fulltext_cache = json.load(f)
    print(f"  Cached fulltext entries: {len(fulltext_cache)}")

    # Process NOT_FOUND records
    print(f"\nSearching fulltext for URLs...")
    results = []

    for idx, row in not_found_df.iterrows():
        record = row.to_dict()
        found_urls, url_source, quality, notes = search_record_fulltext(
            record, abstracts_cache, fulltext_cache
        )

        results.append({
            'original_record_num': record.get('original_record_num'),
            'record_index': record.get('record_index'),
            'database_name': record.get('database_name', ''),
            'long_database_name': record.get('long_database_name', ''),
            'pmids': record.get('pmids', ''),
            'found_urls': found_urls,
            'url_source': url_source,
            'match_quality': quality,
            'notes': notes,
        })

    # Create results dataframe
    results_df = pd.DataFrame(results)

    # Save results
    output_file = output_dir / 'fulltext_url_results.csv'
    results_df.to_csv(output_file, index=False)
    print(f"\nSaved: {output_file}")

    # Statistics
    found_mask = results_df['found_urls'] != 'NOT_FOUND'
    found_count = found_mask.sum()
    total_not_found = len(not_found_df)

    print(f"\n{'=' * 60}")
    print(f"Summary")
    print(f"{'=' * 60}")
    print(f"  Records searched: {len(results_df)}")
    print(f"  URLs found: {found_count} ({found_count/max(total_not_found,1)*100:.1f}%)")
    print(f"  Still missing: {len(results_df) - found_count}")

    if found_count > 0:
        quality_counts = results_df[found_mask]['match_quality'].value_counts()
        print(f"\n  Quality breakdown:")
        for q, c in quality_counts.items():
            print(f"    {q}: {c} ({c/found_count*100:.1f}%)")

    print(f"\nNext step: Run 33_consolidate_recovery.py")
    print(f"Completed: {datetime.now().isoformat()}")


if __name__ == '__main__':
    main()
