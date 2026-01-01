#!/usr/bin/env python3
"""
Script 28b: Merge Resources with URLs and Identify Missing

Purpose: Merge deduplicated resources with extracted URLs, then identify
         which resources are still missing URLs for recovery.

Input:  - Deduplicated resources (from phase7)
        - Extracted URLs (from phase6)
Output: - resources_with_urls.csv (merged data)
        - missing_urls_for_recovery.csv (resources needing URL recovery)
        - pmids_for_url_recovery.txt (PMIDs to fetch abstracts for)

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-12-03
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Set

import pandas as pd


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Merge resources with URLs and identify missing'
    )
    parser.add_argument(
        '--resources', '-r',
        required=True,
        help='Input: deduplicated resources CSV (from phase7)'
    )
    parser.add_argument(
        '--urls', '-u',
        required=True,
        help='Input: extracted URLs CSV (from phase6)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        required=True,
        help='Output directory'
    )
    parser.add_argument(
        '--min-papers',
        type=int,
        default=2,
        help='Minimum papers for resource to be included (default: 2)'
    )
    return parser.parse_args()


def parse_pmids(pmid_str: str) -> Set[str]:
    """Parse comma-separated PMIDs into a set."""
    if pd.isna(pmid_str) or str(pmid_str).strip() == '':
        return set()

    pmids = set()
    for pmid in str(pmid_str).split(','):
        pmid = pmid.strip()
        if pmid and pmid.isdigit():
            pmids.add(pmid)
    return pmids


def main():
    args = parse_args()

    print("=" * 70)
    print("Script 28b: Merge Resources with URLs and Identify Missing")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 70)

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load deduplicated resources
    print(f"\nLoading resources: {args.resources}")
    resources_df = pd.read_csv(args.resources)
    print(f"  Total resources: {len(resources_df):,}")

    # Filter by minimum papers if specified
    if 'unique_paper_count' in resources_df.columns:
        resources_df = resources_df[resources_df['unique_paper_count'] >= args.min_papers]
        print(f"  After min papers filter (>={args.min_papers}): {len(resources_df):,}")
    elif 'total_paper_count' in resources_df.columns:
        resources_df = resources_df[resources_df['total_paper_count'] >= args.min_papers]
        print(f"  After min papers filter (>={args.min_papers}): {len(resources_df):,}")

    # Load extracted URLs
    print(f"\nLoading URLs: {args.urls}")
    urls_df = pd.read_csv(args.urls)
    print(f"  Total URL entries: {len(urls_df):,}")
    print(f"  Unique PMIDs with URLs: {urls_df['pmid'].nunique():,}")

    # Create PMID to URLs mapping
    print("\nBuilding PMID -> URLs mapping...")
    pmid_to_urls = {}
    for _, row in urls_df.iterrows():
        pmid = str(row['pmid'])
        url = row.get('url', '')
        if pmid not in pmid_to_urls:
            pmid_to_urls[pmid] = set()
        if pd.notna(url) and str(url).strip():
            pmid_to_urls[pmid].add(str(url).strip())

    print(f"  PMIDs with at least one URL: {len(pmid_to_urls):,}")

    # For each resource, find associated URLs via PMIDs
    print("\nMatching resources to URLs via PMIDs...")
    results = []

    for idx, row in resources_df.iterrows():
        resource_name = row.get('resource_name', row.get('normalized', ''))
        pmids_str = row.get('pmids', '')
        resource_pmids = parse_pmids(pmids_str)

        # Find all URLs associated with this resource's papers
        resource_urls = set()
        for pmid in resource_pmids:
            if pmid in pmid_to_urls:
                resource_urls.update(pmid_to_urls[pmid])

        results.append({
            'resource_name': resource_name,
            'normalized': row.get('normalized', ''),
            'entity_type': row.get('entity_type', ''),
            'paper_count': row.get('unique_paper_count', row.get('total_paper_count', 0)),
            'pmids': pmids_str,
            'pmid_count': len(resource_pmids),
            'urls': ';'.join(sorted(resource_urls)) if resource_urls else '',
            'url_count': len(resource_urls),
            'has_url': len(resource_urls) > 0
        })

    results_df = pd.DataFrame(results)

    # Statistics
    with_urls = results_df['has_url'].sum()
    without_urls = len(results_df) - with_urls

    print(f"\n" + "-" * 70)
    print("URL Coverage Summary")
    print("-" * 70)
    print(f"  Resources with URLs:    {with_urls:,} ({with_urls/len(results_df)*100:.1f}%)")
    print(f"  Resources WITHOUT URLs: {without_urls:,} ({without_urls/len(results_df)*100:.1f}%)")

    # Save merged results
    merged_file = output_dir / 'resources_merged_with_urls.csv'
    results_df.to_csv(merged_file, index=False)
    print(f"\nSaved: {merged_file}")

    # Extract resources missing URLs
    missing_df = results_df[~results_df['has_url']].copy()
    missing_df = missing_df.sort_values('paper_count', ascending=False)

    missing_file = output_dir / 'missing_urls_for_recovery.csv'
    missing_df.to_csv(missing_file, index=False)
    print(f"Saved: {missing_file} ({len(missing_df):,} resources)")

    # Extract unique PMIDs for abstract fetching
    all_missing_pmids: Set[str] = set()
    for pmids_str in missing_df['pmids']:
        all_missing_pmids.update(parse_pmids(pmids_str))

    pmids_file = output_dir / 'pmids_for_url_recovery.txt'
    with open(pmids_file, 'w') as f:
        for pmid in sorted(all_missing_pmids, key=int):
            f.write(f"{pmid}\n")
    print(f"Saved: {pmids_file} ({len(all_missing_pmids):,} PMIDs)")

    # Top 20 missing resources by paper count
    print(f"\n" + "-" * 70)
    print("Top 20 Resources Missing URLs (by paper count)")
    print("-" * 70)
    for _, row in missing_df.head(20).iterrows():
        print(f"  {row['paper_count']:3d} papers | {row['resource_name']}")

    # Summary
    print(f"\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"  Total resources (>={args.min_papers} papers): {len(results_df):,}")
    print(f"  With URLs:    {with_urls:,} ({with_urls/len(results_df)*100:.1f}%)")
    print(f"  Missing URLs: {without_urls:,} ({without_urls/len(results_df)*100:.1f}%)")
    print(f"  PMIDs to fetch: {len(all_missing_pmids):,}")
    print(f"\nNext step: Run 29_fetch_abstracts.py with pmids_for_url_recovery.txt")
    print(f"Completed: {datetime.now().isoformat()}")


if __name__ == '__main__':
    main()
