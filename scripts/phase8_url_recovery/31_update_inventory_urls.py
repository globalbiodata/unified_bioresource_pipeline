#!/usr/bin/env python3
"""
Script 31: Update Inventory with Recovered URLs

Purpose: Merge recovered URLs into the final inventory.

Input:  - Final inventory (from phase9)
        - Recovered URLs (from script 30b)
        - Merged resources with original URLs (from script 28b)
Output: Updated inventory with URL coverage stats

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-12-03
"""

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Update inventory with recovered URLs'
    )
    parser.add_argument(
        '--inventory', '-i',
        required=True,
        help='Final inventory CSV'
    )
    parser.add_argument(
        '--merged', '-m',
        required=True,
        help='Merged resources with original URLs (from 28b)'
    )
    parser.add_argument(
        '--recovered', '-r',
        required=True,
        help='Recovered URLs CSV (from 30b)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        required=True,
        help='Output directory'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 70)
    print("Script 31: Update Inventory with Recovered URLs")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 70)

    # Setup paths
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load final inventory
    print(f"\nLoading inventory: {args.inventory}")
    inventory_df = pd.read_csv(args.inventory)
    print(f"  Resources: {len(inventory_df):,}")

    # Load merged resources with original URLs
    print(f"\nLoading merged resources: {args.merged}")
    merged_df = pd.read_csv(args.merged)
    print(f"  Resources: {len(merged_df):,}")

    # Load recovered URLs
    print(f"\nLoading recovered URLs: {args.recovered}")
    recovered_df = pd.read_csv(args.recovered)
    print(f"  Recovered: {len(recovered_df):,}")

    # Create lookup for original URLs (from merged)
    original_urls = {}
    for _, row in merged_df.iterrows():
        name = row['resource_name']
        if row.get('has_url') and pd.notna(row.get('urls')) and str(row['urls']).strip():
            original_urls[name] = row['urls']

    print(f"\nResources with original URLs: {len(original_urls):,}")

    # Create lookup for recovered URLs
    recovered_urls = {}
    for _, row in recovered_df.iterrows():
        name = row['resource_name']
        if pd.notna(row.get('recovered_urls')) and str(row['recovered_urls']).strip():
            recovered_urls[name] = row['recovered_urls']

    print(f"Resources with recovered URLs: {len(recovered_urls):,}")

    # Update inventory with URLs
    print("\nUpdating inventory with URLs...")
    urls_added = 0
    urls_recovered = 0

    url_data = []
    for _, row in inventory_df.iterrows():
        name = row.get('resource_name', row.get('normalized', ''))

        # Check for original URLs
        if name in original_urls:
            url = original_urls[name]
            source = 'original'
            urls_added += 1
        # Check for recovered URLs
        elif name in recovered_urls:
            url = recovered_urls[name]
            source = 'recovered'
            urls_recovered += 1
        else:
            url = ''
            source = 'none'

        url_data.append({
            'urls': url,
            'url_source': source
        })

    # Add URL columns to inventory
    url_df = pd.DataFrame(url_data)
    inventory_df['urls'] = url_df['urls']
    inventory_df['url_source'] = url_df['url_source']

    # Calculate stats
    with_urls = inventory_df['urls'].apply(lambda x: bool(x) if pd.notna(x) else False).sum()
    without_urls = len(inventory_df) - with_urls

    # Save updated inventory
    output_file = output_dir / 'bioresource_inventory_2022_mid2025_with_urls.csv'
    inventory_df.to_csv(output_file, index=False)
    print(f"\nSaved: {output_file}")

    # Summary
    print(f"\n" + "=" * 70)
    print("URL Update Summary")
    print("=" * 70)
    print(f"  Total resources: {len(inventory_df):,}")
    print(f"  With URLs (original): {urls_added:,}")
    print(f"  With URLs (recovered): {urls_recovered:,}")
    print(f"  With URLs (total): {with_urls:,} ({with_urls/len(inventory_df)*100:.1f}%)")
    print(f"  Without URLs: {without_urls:,} ({without_urls/len(inventory_df)*100:.1f}%)")
    print(f"\nCompleted: {datetime.now().isoformat()}")


if __name__ == '__main__':
    main()
