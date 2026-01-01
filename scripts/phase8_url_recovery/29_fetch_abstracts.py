#!/usr/bin/env python3
"""
Script 29: Fetch Abstracts from Europe PMC

Purpose: Fetch abstracts and metadata for all PMIDs identified in script 28.
         Results are cached to avoid redundant API calls.

Input:  pmids_list.txt (from script 28)
Output: abstracts_cache.json

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-11-28
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import requests

# Add unified_bioresource_pipeline to path for session utils
pipeline_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(pipeline_root))
from lib.session_utils import validate_session_dir, get_session_path


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Fetch abstracts from Europe PMC API'
    )
    parser.add_argument(
        '--session-dir',
        required=True,
        help='Session directory containing pipeline data'
    )
    parser.add_argument(
        '--cache-file',
        default='abstracts_cache.json',
        help='Cache filename (default: abstracts_cache.json)'
    )
    parser.add_argument(
        '--rate-limit',
        type=float,
        default=0.1,
        help='Seconds between API requests (default: 0.1)'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from existing cache file'
    )

    return parser.parse_args()


def fetch_abstract(pmid: str, rate_limit: float = 0.1) -> Dict:
    """
    Fetch abstract and metadata from Europe PMC API.

    Args:
        pmid: PubMed ID
        rate_limit: Seconds to wait after request

    Returns:
        Dict with title, abstract, pmcid, etc.
    """
    try:
        url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        params = {
            'query': f'ext_id:{pmid}',
            'format': 'json',
            'resultType': 'core'
        }

        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get('resultList', {}).get('result'):
            result = data['resultList']['result'][0]
            return {
                'pmid': pmid,
                'title': result.get('title', ''),
                'abstract': result.get('abstractText', ''),
                'pmcid': result.get('pmcid'),
                'doi': result.get('doi'),
                'pubYear': result.get('pubYear'),
                'journalTitle': result.get('journalTitle'),
                'fetched': True,
                'error': None
            }
        else:
            return {
                'pmid': pmid,
                'title': '',
                'abstract': '',
                'pmcid': None,
                'fetched': True,
                'error': 'not_found'
            }

    except requests.exceptions.Timeout:
        return {
            'pmid': pmid,
            'title': '',
            'abstract': '',
            'pmcid': None,
            'fetched': False,
            'error': 'timeout'
        }
    except Exception as e:
        return {
            'pmid': pmid,
            'title': '',
            'abstract': '',
            'pmcid': None,
            'fetched': False,
            'error': str(e)[:100]
        }
    finally:
        time.sleep(rate_limit)


def main():
    args = parse_args()

    print(f"=" * 60)
    print(f"Script 29: Fetch Abstracts from Europe PMC")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"=" * 60)

    # Validate session directory
    validate_session_dir(args.session_dir)
    session_dir = args.session_dir

    # Input from script 28 output
    input_path = get_session_path(session_dir, '08_url_recovery', 'pmids_list.txt')

    # Output to URL recovery directory
    output_dir = get_session_path(session_dir, '08_url_recovery')
    cache_path = output_dir / args.cache_file

    # Load PMIDs
    print(f"\nLoading PMIDs from: {input_path}")
    with open(input_path, 'r') as f:
        pmids = [line.strip() for line in f if line.strip()]
    print(f"  PMIDs to fetch: {len(pmids)}")

    # Load existing cache if resuming
    cache: Dict[str, Dict] = {}
    if args.resume and cache_path.exists():
        print(f"\nResuming from existing cache: {cache_path}")
        with open(cache_path, 'r') as f:
            cache = json.load(f)
        print(f"  Cached PMIDs: {len(cache)}")

    # Identify PMIDs to fetch
    pmids_to_fetch = [p for p in pmids if p not in cache]
    print(f"  PMIDs to fetch: {len(pmids_to_fetch)}")

    if not pmids_to_fetch:
        print("\nAll PMIDs already cached. Nothing to fetch.")
    else:
        # Estimate time
        est_time = len(pmids_to_fetch) * args.rate_limit
        print(f"\nEstimated time: {est_time/60:.1f} minutes")
        print(f"Rate limit: {args.rate_limit}s between requests")

        # Fetch abstracts
        print(f"\nFetching abstracts...")
        errors = 0
        for i, pmid in enumerate(pmids_to_fetch):
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(pmids_to_fetch)} "
                      f"({i/len(pmids_to_fetch)*100:.0f}%)")

            result = fetch_abstract(pmid, args.rate_limit)
            cache[pmid] = result

            if result.get('error'):
                errors += 1

            # Save checkpoint every 100 PMIDs
            if (i + 1) % 100 == 0:
                with open(cache_path, 'w') as f:
                    json.dump(cache, f, indent=2)

        print(f"  Progress: {len(pmids_to_fetch)}/{len(pmids_to_fetch)} (100%)")

    # Save final cache
    with open(cache_path, 'w') as f:
        json.dump(cache, f, indent=2)
    print(f"\nSaved: {cache_path}")

    # Statistics
    with_abstract = sum(1 for v in cache.values() if v.get('abstract'))
    with_pmcid = sum(1 for v in cache.values() if v.get('pmcid'))
    fetch_errors = sum(1 for v in cache.values() if v.get('error'))

    print(f"\n{'=' * 60}")
    print(f"Summary")
    print(f"{'=' * 60}")
    print(f"  Total PMIDs: {len(cache)}")
    print(f"  With abstract: {with_abstract} ({with_abstract/len(cache)*100:.1f}%)")
    print(f"  With PMCID (fulltext available): {with_pmcid} ({with_pmcid/len(cache)*100:.1f}%)")
    print(f"  Fetch errors: {fetch_errors}")
    print(f"\nNext step: Run 30_search_abstracts_urls.py")
    print(f"Completed: {datetime.now().isoformat()}")


if __name__ == '__main__':
    main()
