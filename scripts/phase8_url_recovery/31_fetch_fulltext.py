#!/usr/bin/env python3
"""
Script 31: Fetch Fulltext from Europe PMC

Purpose: Fetch fulltext XML for records still missing URLs after abstract search.
         Only fetches for PMIDs that have PMCID (open access fulltext available).

Input:  - abstract_url_results.csv (from script 30)
        - abstracts_cache.json (for PMCID lookup)
Output: fulltext_cache.json

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-11-28
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Set

import pandas as pd
import requests

# Add unified_bioresource_pipeline to path for session utils
pipeline_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(pipeline_root))
from lib.session_utils import validate_session_dir, get_session_path


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Fetch fulltext XML from Europe PMC'
    )
    parser.add_argument(
        '--session-dir',
        required=True,
        help='Session directory containing pipeline data'
    )
    parser.add_argument(
        '--cache-file',
        default='fulltext_cache.json',
        help='Cache filename (default: fulltext_cache.json)'
    )
    parser.add_argument(
        '--rate-limit',
        type=float,
        default=0.15,
        help='Seconds between API requests (default: 0.15)'
    )
    parser.add_argument(
        '--max-size',
        type=int,
        default=100000,
        help='Max characters to store per fulltext (default: 100000)'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from existing cache file'
    )

    return parser.parse_args()


def fetch_fulltext(pmcid: str, rate_limit: float = 0.15, max_size: int = 100000) -> Dict:
    """
    Fetch fulltext XML from Europe PMC and extract text.

    Args:
        pmcid: PubMed Central ID (e.g., 'PMC1234567')
        rate_limit: Seconds to wait after request
        max_size: Maximum characters to store

    Returns:
        Dict with fulltext, pmcid, error status
    """
    try:
        url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
        resp = requests.get(url, timeout=60)

        if resp.status_code == 200:
            # Strip XML tags to get plain text
            text = re.sub(r'<[^>]+>', ' ', resp.text)
            text = ' '.join(text.split())  # Normalize whitespace

            return {
                'pmcid': pmcid,
                'fulltext': text[:max_size],
                'fulltext_length': len(text),
                'fetched': True,
                'error': None
            }
        else:
            return {
                'pmcid': pmcid,
                'fulltext': '',
                'fulltext_length': 0,
                'fetched': False,
                'error': f'status_{resp.status_code}'
            }

    except requests.exceptions.Timeout:
        return {
            'pmcid': pmcid,
            'fulltext': '',
            'fulltext_length': 0,
            'fetched': False,
            'error': 'timeout'
        }
    except Exception as e:
        return {
            'pmcid': pmcid,
            'fulltext': '',
            'fulltext_length': 0,
            'fetched': False,
            'error': str(e)[:100]
        }
    finally:
        time.sleep(rate_limit)


def main():
    args = parse_args()

    print(f"=" * 60)
    print(f"Script 31: Fetch Fulltext from Europe PMC")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"=" * 60)

    # Validate session directory
    validate_session_dir(args.session_dir)
    session_dir = args.session_dir

    # Input files from script 30
    input_path = get_session_path(session_dir, '08_url_recovery', 'abstract_url_results.csv')
    abstracts_path = get_session_path(session_dir, '08_url_recovery', 'abstracts_cache.json')

    # Output to URL recovery directory
    output_dir = get_session_path(session_dir, '08_url_recovery')
    cache_path = output_dir / args.cache_file

    # Load abstract results to find NOT_FOUND records
    print(f"\nLoading abstract results: {input_path}")
    df = pd.read_csv(input_path)
    not_found_df = df[df['found_urls'] == 'NOT_FOUND']
    print(f"  Total records: {len(df)}")
    print(f"  Still missing URLs: {len(not_found_df)}")

    # Load abstracts cache for PMCID lookup
    print(f"\nLoading abstracts cache: {abstracts_path}")
    with open(abstracts_path, 'r') as f:
        abstracts_cache = json.load(f)

    # Collect PMIDs that need fulltext (have PMCID)
    pmids_with_pmcid: Set[str] = set()
    pmcid_map: Dict[str, str] = {}  # PMID -> PMCID

    for _, row in not_found_df.iterrows():
        pmids_val = row.get('pmids', '')
        if pd.isna(pmids_val) or pmids_val is None:
            pmids_val = str(row.get('pmid', ''))  # Fallback to single pmid column
        pmids = str(pmids_val).split('|')
        for pmid in pmids:
            pmid = pmid.strip()
            # Clean float-like PMIDs (e.g., "39593035.0" -> "39593035")
            if '.' in pmid:
                try:
                    pmid = str(int(float(pmid)))
                except ValueError:
                    pass
            if not pmid:
                continue

            abstract_data = abstracts_cache.get(pmid, {})
            pmcid = abstract_data.get('pmcid')

            if pmcid:
                pmids_with_pmcid.add(pmid)
                pmcid_map[pmid] = pmcid

    print(f"\n  PMIDs with PMCID available: {len(pmids_with_pmcid)}")
    print(f"  Unique PMCIDs to fetch: {len(set(pmcid_map.values()))}")

    # Load existing cache if resuming
    cache: Dict[str, Dict] = {}
    if args.resume and cache_path.exists():
        print(f"\nResuming from existing cache: {cache_path}")
        with open(cache_path, 'r') as f:
            cache = json.load(f)
        print(f"  Cached entries: {len(cache)}")

    # Identify PMCIDs to fetch
    pmcids_to_fetch = [pmcid for pmid, pmcid in pmcid_map.items()
                       if pmcid not in cache]
    pmcids_to_fetch = list(set(pmcids_to_fetch))  # Deduplicate

    print(f"  PMCIDs to fetch: {len(pmcids_to_fetch)}")

    if not pmcids_to_fetch:
        print("\nAll PMCIDs already cached. Nothing to fetch.")
    else:
        # Estimate time
        est_time = len(pmcids_to_fetch) * args.rate_limit
        print(f"\nEstimated time: {est_time/60:.1f} minutes")
        print(f"Rate limit: {args.rate_limit}s between requests")

        # Fetch fulltext
        print(f"\nFetching fulltext...")
        for i, pmcid in enumerate(pmcids_to_fetch):
            if i % 30 == 0:
                print(f"  Progress: {i}/{len(pmcids_to_fetch)} "
                      f"({i/len(pmcids_to_fetch)*100:.0f}%)")

            result = fetch_fulltext(pmcid, args.rate_limit, args.max_size)
            cache[pmcid] = result

            # Save checkpoint every 50 PMCIDs
            if (i + 1) % 50 == 0:
                with open(cache_path, 'w') as f:
                    json.dump(cache, f)

        print(f"  Progress: {len(pmcids_to_fetch)}/{len(pmcids_to_fetch)} (100%)")

    # Save final cache
    with open(cache_path, 'w') as f:
        json.dump(cache, f)
    print(f"\nSaved: {cache_path}")

    # Statistics
    with_fulltext = sum(1 for v in cache.values() if v.get('fulltext'))
    fetch_errors = sum(1 for v in cache.values() if v.get('error'))

    print(f"\n{'=' * 60}")
    print(f"Summary")
    print(f"{'=' * 60}")
    print(f"  Total PMCIDs: {len(cache)}")
    print(f"  With fulltext: {with_fulltext} ({with_fulltext/max(len(cache),1)*100:.1f}%)")
    print(f"  Fetch errors: {fetch_errors}")
    print(f"\nNext step: Run 32_search_fulltext_urls.py")
    print(f"Completed: {datetime.now().isoformat()}")


if __name__ == '__main__':
    main()
