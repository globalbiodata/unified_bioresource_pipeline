#!/usr/bin/env python3
"""
Script 30: Search Abstracts for URLs

Purpose: Search abstracts for URLs using enhanced regex patterns.
         Applies exclusion rules and quality assessment.

Input:  - missing_urls_prepared.csv (from script 28)
        - abstracts_cache.json (from script 29)
Output: abstract_url_results.csv

Author: Pipeline Team
Date: 2025-11-28
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
# Add unified_bioresource_pipeline to path for session utils
pipeline_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(pipeline_root))
from lib.session_utils import validate_session_dir, get_session_path

# Add phase8_url_recovery to path for url_patterns import
sys.path.insert(0, str(Path(__file__).parent))
from url_patterns import extract_and_filter_urls, extract_pmids


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Search abstracts for URLs'
    )
    parser.add_argument(
        '--session-dir',
        required=True,
        help='Session directory containing pipeline data'
    )

    return parser.parse_args()


def search_record_abstracts(
    record: Dict,
    abstracts_cache: Dict
) -> Tuple[str, str, str, str]:
    """
    Search all abstracts for a record for URLs.

    Args:
        record: Record dict with pmids, database_name, long_database_name
        abstracts_cache: Cache of fetched abstracts

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
    long_name = record.get('long_database_name', '')

    all_found_urls = []

    for pmid in pmids:
        abstract_data = abstracts_cache.get(pmid, {})
        title = abstract_data.get('title', '')
        abstract = abstract_data.get('abstract', '')

        # Combine title and abstract for search
        text = f"{title} {abstract}"

        if not text.strip():
            continue

        # Extract and filter URLs
        url_results = extract_and_filter_urls(text, db_name, long_name)

        if url_results:
            # Found URLs in this abstract
            urls = [u for u, q, n in url_results]
            quality = url_results[0][1]  # Best quality (already sorted)

            return (
                '|'.join(urls),
                'abstract',
                quality,
                f'Found in PMID {pmid}'
            )

    # No URLs found in any abstract
    return ('NOT_FOUND', 'not_available', '', 'No URLs found in abstracts')


def main():
    args = parse_args()

    print(f"=" * 60)
    print(f"Script 30: Search Abstracts for URLs")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"=" * 60)

    # Validate session directory
    validate_session_dir(args.session_dir)
    session_dir = args.session_dir
    print(f"\nSession directory: {session_dir}")

    # Input files from script 28 and 29
    input_path = get_session_path(session_dir, '08_url_recovery', 'missing_urls_prepared.csv')
    abstracts_path = get_session_path(session_dir, '08_url_recovery', 'abstracts_cache.json')

    # Output to URL recovery directory
    output_dir = get_session_path(session_dir, '08_url_recovery')
    output_file = output_dir / 'abstract_url_results.csv'

    # Load inputs
    print(f"\nLoading input: {input_path}")
    df = pd.read_csv(input_path)
    print(f"  Records: {len(df)}")

    print(f"\nLoading abstracts cache: {abstracts_path}")
    with open(abstracts_path, 'r') as f:
        abstracts_cache = json.load(f)
    print(f"  Cached abstracts: {len(abstracts_cache)}")

    # Process each record
    print(f"\nSearching abstracts for URLs...")
    results = []

    for idx, row in df.iterrows():
        if idx % 100 == 0:
            print(f"  Progress: {idx}/{len(df)} ({idx/len(df)*100:.0f}%)")

        record = row.to_dict()
        found_urls, url_source, quality, notes = search_record_abstracts(
            record, abstracts_cache
        )

        results.append({
            'original_record_num': record.get('original_record_num', idx),
            'record_index': record.get('record_index', idx + 1),
            'database_name': record.get('database_name', ''),
            'long_database_name': record.get('long_database_name', ''),
            'pmids': record.get('pmids', ''),
            'found_urls': found_urls,
            'url_source': url_source,
            'match_quality': quality,
            'notes': notes,
        })

    print(f"  Progress: {len(df)}/{len(df)} (100%)")

    # Create results dataframe
    results_df = pd.DataFrame(results)

    # Save results
    results_df.to_csv(output_file, index=False)
    print(f"\nSaved: {output_file}")

    # Statistics
    found_mask = results_df['found_urls'] != 'NOT_FOUND'
    found_count = found_mask.sum()

    print(f"\n{'=' * 60}")
    print(f"Summary")
    print(f"{'=' * 60}")
    print(f"  Total records: {len(results_df)}")
    print(f"  URLs found: {found_count} ({found_count/len(results_df)*100:.1f}%)")
    print(f"  Still missing: {len(results_df) - found_count}")

    if found_count > 0:
        quality_counts = results_df[found_mask]['match_quality'].value_counts()
        print(f"\n  Quality breakdown:")
        for q, c in quality_counts.items():
            print(f"    {q}: {c} ({c/found_count*100:.1f}%)")

    print(f"\nNext step: Run 31_fetch_fulltext.py")
    print(f"Completed: {datetime.now().isoformat()}")


if __name__ == '__main__':
    main()
