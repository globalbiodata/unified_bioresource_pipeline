#!/usr/bin/env python3
"""
Script 27: Generate Final Inventory

Purpose: Produce final output matching data/final_inventory_2022.csv format
         - Select and order columns
         - Validate all required columns present
         - Generate statistics summary

Authors: AI Assistant
Date: 2025-11-27
Updated: 2025-12-05 (Session-based refactor)
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add lib imports
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from lib.session_utils import get_session_path, validate_session_dir


# Target column order from final_inventory_2022.csv (base 20 columns)
BASE_COLUMNS = [
    'ID',
    'best_name',
    'best_name_prob',
    'best_common',
    'best_common_prob',
    'best_full',
    'best_full_prob',
    'article_count',
    'extracted_url',
    'extracted_url_status',
    'extracted_url_country',
    'extracted_url_coordinates',
    'wayback_url',
    'publication_date',
    'affiliation',
    'authors',
    'grant_ids',
    'grant_agencies',
    'num_citations',
    'affiliation_countries'
]

# Additional columns for data quality tracking
# name_modification_flags moved to FIRST position for easier QC
EXTRA_COLUMNS = [
    'best_name_original',
    'url_validation',
    'paper_titles'  # NEW: for QC - can be deleted after review
]

# QC columns that go at the START of the file (for easy filtering)
QC_FIRST_COLUMNS = [
    'name_modification_flags'
]

# Full target columns: QC first, then base, then extra
TARGET_COLUMNS = QC_FIRST_COLUMNS + BASE_COLUMNS + EXTRA_COLUMNS


def get_args() -> argparse.Namespace:
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Generate final inventory CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 27_generate_final_inventory.py --session-dir results/2025-12-04-143052-a3f9b
        """
    )

    parser.add_argument(
        '--session-dir',
        type=str,
        required=True,
        help='Session directory path'
    )
    parser.add_argument(
        '--reference',
        type=str,
        default='data/final_inventory_2022.csv',
        help='Reference file for column order validation (default: data/final_inventory_2022.csv)'
    )

    return parser.parse_args()


def validate_columns(df: pd.DataFrame) -> tuple:
    """
    Validate that all required columns are present.

    Returns (is_valid, missing_columns, extra_columns)
    """
    current_cols = set(df.columns)
    # Only BASE_COLUMNS are strictly required
    required_cols = set(BASE_COLUMNS)

    missing = required_cols - current_cols
    # Extra = columns not in QC_FIRST, BASE, or EXTRA
    known_cols = set(QC_FIRST_COLUMNS + BASE_COLUMNS + EXTRA_COLUMNS)
    extra = current_cols - known_cols

    return (len(missing) == 0, list(missing), list(extra))


def format_final_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format dataframe to match final inventory format.

    - Ensure all required columns exist
    - Order columns correctly (QC columns first for easy filtering)
    - Clean up data types
    - Include extra columns if present
    """
    # Ensure all base columns exist
    for col in BASE_COLUMNS:
        if col not in df.columns:
            df[col] = ''

    # Build output column list: QC first + base + any extra columns that exist
    output_columns = []

    # QC columns first (for easy filtering in spreadsheet)
    for col in QC_FIRST_COLUMNS:
        if col in df.columns:
            output_columns.append(col)

    # Then base columns
    output_columns.extend(BASE_COLUMNS)

    # Then extra columns
    for col in EXTRA_COLUMNS:
        if col in df.columns:
            output_columns.append(col)

    # Select and order columns
    out_df = df[output_columns].copy()

    # Clean up data types
    # Numeric columns
    numeric_cols = ['best_name_prob', 'best_common_prob', 'best_full_prob',
                   'article_count', 'num_citations']
    for col in numeric_cols:
        if col in out_df.columns:
            out_df[col] = pd.to_numeric(out_df[col], errors='coerce')

    # String columns - fill NaN with empty string
    string_cols = [c for c in output_columns if c not in numeric_cols]
    for col in string_cols:
        if col in out_df.columns:
            out_df[col] = out_df[col].fillna('').astype(str)

    return out_df


def generate_statistics(df: pd.DataFrame, excluded_count: int) -> dict:
    """Generate comprehensive statistics for the final inventory"""

    stats = {
        'total_resources': len(df),
        'excluded_resources': excluded_count,

        'coverage': {
            'has_best_name': int((df['best_name'] != '').sum()),
            'has_best_common': int((df['best_common'] != '').sum()),
            'has_best_full': int((df['best_full'] != '').sum()),
            'has_url': int((df['extracted_url'] != '').sum()),
            'has_wayback': int((df['wayback_url'] != '').sum()),
            'has_publication_date': int((df['publication_date'] != '').sum()),
            'has_affiliation': int((df['affiliation'] != '').sum()),
            'has_authors': int((df['authors'] != '').sum()),
            'has_grants': int((df['grant_ids'] != '').sum()),
            'has_citations': int(df['num_citations'].notna().sum()),
            'has_affiliation_countries': int((df['affiliation_countries'] != '').sum()),
            'has_url_country': int((df['extracted_url_country'] != '').sum()),
        },

        'url_status': {},
        'top_countries': {},
    }

    # URL status breakdown
    url_statuses = df['extracted_url_status'].value_counts().head(10)
    stats['url_status'] = url_statuses.to_dict()

    # Top affiliation countries
    all_countries = []
    for countries in df['affiliation_countries'].dropna():
        if countries:
            all_countries.extend([c.strip() for c in str(countries).split(',')])

    if all_countries:
        country_counts = pd.Series(all_countries).value_counts().head(10)
        stats['top_countries'] = country_counts.to_dict()

    # Citation statistics
    citations = df['num_citations'].dropna()
    if len(citations) > 0:
        stats['citation_stats'] = {
            'total': int(citations.sum()),
            'mean': round(float(citations.mean()), 1),
            'median': round(float(citations.median()), 1),
            'max': int(citations.max())
        }

    # Name modification statistics (if column exists)
    if 'name_modification_flags' in df.columns:
        flag_counts = {}
        for flags_str in df['name_modification_flags'].dropna():
            if flags_str:
                for flag in str(flags_str).split(','):
                    flag = flag.strip()
                    if flag:
                        flag_counts[flag] = flag_counts.get(flag, 0) + 1
        if flag_counts:
            stats['name_modifications'] = flag_counts

    # URL validation statistics (if column exists)
    if 'url_validation' in df.columns:
        url_val_counts = df['url_validation'].value_counts().to_dict()
        if url_val_counts:
            stats['url_validation'] = url_val_counts

    return stats


def main() -> None:
    """Main function"""
    args = get_args()

    # Validate session directory
    SESSION_DIR = Path(args.session_dir).resolve()

    if not SESSION_DIR.exists():
        print(f"ERROR: Session directory not found: {SESSION_DIR}")
        sys.exit(1)

    try:
        validate_session_dir(SESSION_DIR, required_phases=['09_finalization'])
    except ValueError as e:
        print(f"ERROR: Invalid session directory: {e}")
        sys.exit(1)

    # Input/output paths
    input_file = get_session_path(SESSION_DIR, '09_finalization', 'countries_processed_resources.csv')
    output_dir = get_session_path(SESSION_DIR, '09_finalization')

    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)

    print(f"Phase 9 - Script 27: Generate Final Inventory")
    print(f"=" * 80)
    print(f"Session: {SESSION_DIR.name}")
    print(f"Input: {input_file.relative_to(SESSION_DIR)}")
    print(f"Output directory: {output_dir.relative_to(SESSION_DIR)}")
    print()

    # Load input
    print("Loading processed resources...")
    df = pd.read_csv(input_file)
    print(f"  Loaded {len(df)} rows")

    # Check for excluded file to get count
    excluded_file = output_dir / 'excluded_no_url.csv'
    excluded_count = 0
    if excluded_file.exists():
        excluded_df = pd.read_csv(excluded_file)
        excluded_count = len(excluded_df)
        print(f"  Found {excluded_count} excluded resources")
    print()

    # Validate columns
    print("Validating columns...")
    is_valid, missing, extra = validate_columns(df)

    if missing:
        print(f"  Warning: Missing columns: {missing}")
    if extra:
        print(f"  Note: Extra columns (will be dropped): {extra}")

    if is_valid:
        print(f"  All {len(TARGET_COLUMNS)} required columns present")
    print()

    # Format final output
    print("Formatting final output...")
    final_df = format_final_output(df)
    print(f"  Output has {len(final_df)} rows x {len(final_df.columns)} columns")
    print()

    # Generate statistics
    print("Generating statistics...")
    stats = generate_statistics(final_df, excluded_count)
    stats['script'] = '27_generate_final_inventory'
    stats['timestamp'] = datetime.now().isoformat()
    stats['session'] = SESSION_DIR.name

    print(f"  Coverage summary:")
    for key, value in stats['coverage'].items():
        pct = value / len(final_df) * 100 if len(final_df) > 0 else 0
        print(f"    - {key}: {value} ({pct:.1f}%)")
    print()

    # Compare with reference if exists
    reference_file = PROJECT_ROOT / args.reference
    if reference_file.exists():
        print(f"Comparing with reference file: {args.reference}")
        ref_df = pd.read_csv(reference_file, nrows=1)
        ref_cols = list(ref_df.columns)
        if ref_cols == TARGET_COLUMNS:
            print("  Column order matches reference file")
        else:
            print("  Warning: Column order differs from reference")
            print(f"    Reference: {ref_cols[:5]}...")
            print(f"    Output: {TARGET_COLUMNS[:5]}...")
        print()

    # Save outputs
    output_file = output_dir / 'final_inventory.csv'
    final_df.to_csv(output_file, index=False)
    print(f"Saved final inventory to: {output_file.relative_to(SESSION_DIR)}")

    # Save full statistics
    stats_file = output_dir / 'statistics.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved statistics to: {stats_file.relative_to(SESSION_DIR)}")

    # Create summary log
    log_file = output_dir / 'finalization.log'
    with open(log_file, 'w') as f:
        f.write(f"Phase 9 Finalization Summary\n")
        f.write(f"=" * 80 + "\n\n")
        f.write(f"Session: {SESSION_DIR.name}\n")
        f.write(f"Timestamp: {datetime.now().isoformat()}\n")
        f.write(f"Final inventory: {len(final_df)} resources\n")
        f.write(f"Excluded (no URL): {excluded_count} resources\n")
        f.write(f"Total processed: {len(final_df) + excluded_count} resources\n\n")
        f.write(f"Output file: {output_file.relative_to(SESSION_DIR)}\n")
        f.write(f"Statistics: {stats_file.relative_to(SESSION_DIR)}\n")
    print(f"Saved log to: {log_file.relative_to(SESSION_DIR)}")

    print()
    print("=" * 80)
    print(f"FINAL INVENTORY COMPLETE")
    print(f"  Resources: {len(final_df)}")
    print(f"  Excluded: {excluded_count}")
    print(f"  Output: {output_file.relative_to(SESSION_DIR)}")
    print("=" * 80)


if __name__ == '__main__':
    main()
