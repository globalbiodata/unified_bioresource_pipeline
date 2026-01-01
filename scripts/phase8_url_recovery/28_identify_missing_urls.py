#!/usr/bin/env python3
"""
Script 28: Identify Records Missing URLs

Purpose: Find bioresource records that are missing URLs and prepare them
         for URL recovery processing.

Input:  Deduplicated bioresource inventory (from phase7)
Output: - missing_urls_prepared.csv (records needing URL recovery)
        - pmids_list.txt (unique PMIDs to fetch)

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-11-28
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Set

import pandas as pd

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from url_patterns import extract_pmids

# Add unified_bioresource_pipeline to path for session utils
pipeline_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(pipeline_root))
from lib.session_utils import validate_session_dir, get_session_path


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Identify bioresource records missing URLs'
    )
    parser.add_argument(
        '--session-dir',
        required=True,
        help='Session directory containing pipeline data'
    )
    parser.add_argument(
        '--profile',
        default='aggressive',
        choices=['conservative', 'balanced', 'aggressive'],
        help='Deduplication profile to use (default: aggressive)'
    )
    parser.add_argument(
        '--url-column',
        default='all_urls',
        help='Name of URL column (default: all_urls)'
    )
    parser.add_argument(
        '--pmid-column',
        default='pmid',
        help='Name of PMID column (default: pmid)'
    )
    parser.add_argument(
        '--name-column',
        default='primary_entity_short',
        help='Name of database name column (default: primary_entity_short)'
    )
    parser.add_argument(
        '--long-name-column',
        default='primary_entity_long',
        help='Name of long database name column (default: primary_entity_long)'
    )

    return parser.parse_args()


def is_missing_url(url_value) -> bool:
    """Check if a URL value is missing or invalid."""
    if pd.isna(url_value):
        return True
    url_str = str(url_value).strip().lower()
    return url_str in ('', 'nan', 'none', 'not_found', 'n/a', '-')


def main():
    args = parse_args()

    print(f"=" * 60)
    print(f"Script 28: Identify Records Missing URLs")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"=" * 60)

    # Validate session directory
    validate_session_dir(args.session_dir)
    session_dir = args.session_dir
    print(f"\nSession: {session_dir}")
    print(f"Profile: {args.profile}")

    # Input from deduplication output
    input_path = get_session_path(session_dir, f'07_deduplication/{args.profile}', 'set_c_final.csv')

    # Output to URL recovery directory
    output_dir = get_session_path(session_dir, '08_url_recovery')
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load input data
    print(f"\nLoading input: {input_path}")
    df = pd.read_csv(input_path)
    print(f"  Total records: {len(df)}")

    # Identify column names (handle variations)
    url_col = args.url_column
    pmid_col = args.pmid_column
    name_col = args.name_column
    long_name_col = args.long_name_column

    # Check columns exist
    available_cols = df.columns.tolist()
    print(f"\nAvailable columns: {available_cols[:10]}...")

    # Find actual column names (case-insensitive matching)
    def find_column(target: str, available: List[str]) -> str:
        target_lower = target.lower()
        for col in available:
            if col.lower() == target_lower:
                return col
        # Try partial match
        for col in available:
            if target_lower in col.lower():
                return col
        return target

    url_col = find_column(url_col, available_cols)
    pmid_col = find_column(pmid_col, available_cols)
    name_col = find_column(name_col, available_cols)
    long_name_col = find_column(long_name_col, available_cols)

    print(f"\nUsing columns:")
    print(f"  URL: {url_col}")
    print(f"  PMID: {pmid_col}")
    print(f"  Name: {name_col}")
    print(f"  Long name: {long_name_col}")

    # Find records with missing URLs
    if url_col not in df.columns:
        print(f"\nWARNING: URL column '{url_col}' not found. Treating all as missing.")
        missing_mask = pd.Series([True] * len(df))
    else:
        missing_mask = df[url_col].apply(is_missing_url)

    missing_df = df[missing_mask].copy()
    print(f"\nRecords with missing URLs: {len(missing_df)} ({len(missing_df)/len(df)*100:.1f}%)")

    if len(missing_df) == 0:
        print("\nNo records need URL recovery. Exiting.")
        # Create empty output files
        pd.DataFrame().to_csv(output_dir / 'missing_urls_prepared.csv', index=False)
        with open(output_dir / 'pmids_list.txt', 'w') as f:
            f.write('')
        return

    # Extract all PMIDs
    all_pmids: Set[str] = set()
    pmid_counts = []

    for idx, row in missing_df.iterrows():
        pmid_value = row.get(pmid_col, '')
        pmids = extract_pmids(str(pmid_value))
        pmid_counts.append(len(pmids))
        all_pmids.update(pmids)

    print(f"\nPMID statistics:")
    print(f"  Total unique PMIDs: {len(all_pmids)}")
    print(f"  Records with 1 PMID: {sum(1 for c in pmid_counts if c == 1)}")
    print(f"  Records with 2+ PMIDs: {sum(1 for c in pmid_counts if c > 1)}")
    print(f"  Records with 0 PMIDs: {sum(1 for c in pmid_counts if c == 0)}")

    # Prepare output dataframe
    output_records = []
    for idx, (orig_idx, row) in enumerate(missing_df.iterrows()):
        pmid_value = row.get(pmid_col, '')
        pmids = extract_pmids(str(pmid_value))

        output_records.append({
            'original_record_num': orig_idx,
            'record_index': idx + 1,
            'database_name': row.get(name_col, ''),
            'long_database_name': row.get(long_name_col, ''),
            'pmids': '|'.join(pmids) if pmids else '',
            'pmid_count': len(pmids),
            'original_url': row.get(url_col, ''),
        })

    output_df = pd.DataFrame(output_records)

    # Save outputs
    output_csv = output_dir / 'missing_urls_prepared.csv'
    output_df.to_csv(output_csv, index=False)
    print(f"\nSaved: {output_csv}")
    print(f"  Records: {len(output_df)}")

    pmids_file = output_dir / 'pmids_list.txt'
    with open(pmids_file, 'w') as f:
        for pmid in sorted(all_pmids):
            f.write(f"{pmid}\n")
    print(f"Saved: {pmids_file}")
    print(f"  PMIDs: {len(all_pmids)}")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Summary")
    print(f"{'=' * 60}")
    print(f"  Input records: {len(df)}")
    print(f"  Missing URLs: {len(missing_df)} ({len(missing_df)/len(df)*100:.1f}%)")
    print(f"  Unique PMIDs to fetch: {len(all_pmids)}")
    print(f"\nNext step: Run 29_fetch_abstracts.py")
    print(f"Completed: {datetime.now().isoformat()}")


if __name__ == '__main__':
    main()
