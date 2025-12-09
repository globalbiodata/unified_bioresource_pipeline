#!/usr/bin/env python3
"""
Phase 5: Add Quality Indicators to Union Papers with Primary Resources

This script enriches the dataset with quality indicators based on title analysis:
- entity_from_title: Extract resource name from title (text before colon)
- db_keyword_found: Database-related keywords in title
- title_entity_in_ner: Whether title entity matches NER-detected primary entity
- very_high_conf: Both db_keyword AND title_entity match (highest quality)

Usage:
    # Session-based (PREFERRED):
    python 12_add_quality_indicators.py --session-dir results/2025-12-04-143052-abc12

    # Legacy mode (auto-detect):
    python 12_add_quality_indicators.py --auto

    # Custom paths (legacy):
    python 12_add_quality_indicators.py \
        --input-file data/union_papers_with_primary_resources.csv \
        --output-file data/union_papers_with_quality_indicators.csv

Session Mode:
    When --session-dir is provided:
    - Reads from: {session_dir}/05_mapping/union_papers_with_primary_resources.csv
    - Outputs to: {session_dir}/05_mapping/union_papers_with_quality_indicators.csv
    - Statistics: {session_dir}/05_mapping/quality_indicators_statistics.txt

Author: Pipeline Automation
Date: 2025-11-18
Updated: 2025-12-04 (added session-dir support, argparse)
"""

import argparse
import pandas as pd
import numpy as np
import re
import sys
from pathlib import Path
from datetime import datetime

# Requires: lib/session_utils.py (run from unified_bioresource_pipeline directory)
# Add lib to path for session utilities
SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

# Import from lib - will fail loudly if lib not found
from lib.session_utils import get_session_path, validate_session_dir

# Legacy paths
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent  # inventory_2022
LEGACY_DATA_DIR = PROJECT_ROOT / 'advanced_filtering_pipeline/data'
LEGACY_INPUT_FILE = LEGACY_DATA_DIR / 'union_papers_with_primary_resources.csv'
LEGACY_OUTPUT_FILE = LEGACY_DATA_DIR / 'union_papers_with_quality_indicators.csv'
LEGACY_STATS_FILE = LEGACY_DATA_DIR / 'quality_indicators_statistics.txt'

# Alternative legacy path (pipeline_synthesis)
ALT_LEGACY_DATA_DIR = PROJECT_ROOT / 'pipeline_synthesis_2025-11-18/data'
ALT_LEGACY_INPUT = ALT_LEGACY_DATA_DIR / 'union_papers_with_primary_resources.csv'


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Phase 5: Add quality indicators to union papers with primary resources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Session-based mode (PREFERRED):
  python 12_add_quality_indicators.py --session-dir results/2025-12-04-143052-abc12

  # Legacy mode (auto-detect):
  python 12_add_quality_indicators.py --auto

  # Custom paths (legacy):
  python 12_add_quality_indicators.py \\
      --input-file data/union_papers_with_primary_resources.csv \\
      --output-file data/union_papers_with_quality_indicators.csv
        """
    )

    # Session mode arguments
    parser.add_argument("--session-dir", type=Path,
                        help="Session directory path (e.g., results/2025-12-04-143052-abc12)")

    # Legacy mode arguments
    parser.add_argument("--input-file", type=Path,
                        help="Path to union papers with primary resources CSV")
    parser.add_argument("--output-file", type=Path,
                        help="Output path for papers with quality indicators CSV")
    parser.add_argument("--stats-file", type=Path,
                        help="Output path for statistics file")
    parser.add_argument("--auto", action="store_true",
                        help="Auto-detect files in legacy paths")

    return parser.parse_args()


def find_legacy_files():
    """Auto-detect legacy file paths."""
    print("=" * 80)
    print("AUTO-DETECTING LEGACY FILES")
    print("=" * 80)

    input_file = None
    output_file = None
    stats_file = None

    # Try standard legacy path first
    if LEGACY_INPUT_FILE.exists():
        print(f"\nFound input file: {LEGACY_INPUT_FILE.name}")
        input_file = LEGACY_INPUT_FILE
        output_file = LEGACY_OUTPUT_FILE
        stats_file = LEGACY_STATS_FILE
        return input_file, output_file, stats_file

    # Try alternative legacy path
    if ALT_LEGACY_INPUT.exists():
        print(f"\nFound input file: {ALT_LEGACY_INPUT.name}")
        input_file = ALT_LEGACY_INPUT
        output_file = ALT_LEGACY_DATA_DIR / 'union_papers_with_quality_indicators.csv'
        stats_file = ALT_LEGACY_DATA_DIR / 'quality_indicators_statistics.txt'
        return input_file, output_file, stats_file

    # Report what's missing
    print("\nERROR: Could not find input file!")
    print(f"\nSearched for:")
    print(f"  {LEGACY_INPUT_FILE}")
    print(f"  {ALT_LEGACY_INPUT}")
    sys.exit(1)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def normalize_text(text):
    """Normalize text for matching"""
    if pd.isna(text):
        return ''
    return str(text).lower().strip()

def extract_entity_from_title(title):
    """Extract potential resource name from title (text before first colon)"""
    if pd.isna(title):
        return ''

    # Find first colon
    if ':' not in title:
        return ''

    # Extract text before first colon
    entity = title.split(':')[0].strip()

    # Strip common articles
    for article in ['The ', 'A ', 'An ']:
        if entity.startswith(article):
            entity = entity[len(article):].strip()

    # Remove version numbers
    # Pattern: v2.0, version 2.0, 2025, etc.
    entity = re.sub(r'\bv?\d+\.?\d*\b', '', entity, flags=re.IGNORECASE)
    entity = re.sub(r'\bversion\s+\d+\.?\d*\b', '', entity, flags=re.IGNORECASE)

    # Handle parenthetical acronyms: "Name (ACRONYM)" -> "Name"
    entity = re.sub(r'\s*\([^)]+\)\s*$', '', entity)

    # Clean up extra whitespace
    entity = ' '.join(entity.split()).strip()

    return entity

def detect_db_keywords(title):
    """Detect database-related keywords in title"""
    if pd.isna(title):
        return False

    title_lower = title.lower()
    keywords = [
        'database', 'db',
        'server',
        'portal',
        'resource',
        'repository',
        'archive',
        'registry',
        'catalog',
        'collection'
    ]

    return any(keyword in title_lower for keyword in keywords)

def calculate_db_keyword_score(title):
    """Calculate strength of database indicators in title"""
    if pd.isna(title):
        return 0

    title_lower = title.lower()
    score = 0

    # High-value keywords (5 points each)
    high_keywords = ['database', 'server', 'portal', 'repository']
    for kw in high_keywords:
        if kw in title_lower:
            score += 5

    # Medium-value keywords (3 points each)
    med_keywords = ['resource', 'archive', 'registry', 'catalog']
    for kw in med_keywords:
        if kw in title_lower:
            score += 3

    # Low-value keywords (1 point each)
    low_keywords = ['collection', 'db']
    for kw in low_keywords:
        if kw in title_lower:
            score += 1

    return score

def process_quality_indicators(df):
    """Add quality indicators to dataframe."""
    print("\n2. Adding quality indicators...")

    # Extract entity from title
    df['entity_from_title'] = df['title'].apply(extract_entity_from_title)
    entities_found = (df['entity_from_title'] != '').sum()
    print(f"   ✓ Extracted entities from {entities_found:,} titles ({entities_found/len(df)*100:.1f}%)")

    # Detect database keywords
    df['db_keyword_found'] = df['title'].apply(detect_db_keywords)
    db_keywords = df['db_keyword_found'].sum()
    print(f"   ✓ Found DB keywords in {db_keywords:,} titles ({db_keywords/len(df)*100:.1f}%)")

    # Calculate keyword score
    df['db_keyword_score'] = df['title'].apply(calculate_db_keyword_score)
    scored_titles = (df['db_keyword_score'] > 0).sum()
    print(f"   ✓ Calculated keyword scores for {scored_titles:,} titles (mean: {df['db_keyword_score'].mean():.2f})")

    # Check if title entity matches primary
    def title_matches_primary(row):
        """Check if extracted title entity matches primary entity"""
        title_entity = normalize_text(row['entity_from_title'])
        primary_long = normalize_text(row['primary_entity_long'])
        primary_short = normalize_text(row['primary_entity_short'])

        if not title_entity:
            return False

        return title_entity == primary_long or title_entity == primary_short

    df['title_entity_in_ner'] = df.apply(title_matches_primary, axis=1)
    matches = df['title_entity_in_ner'].sum()
    print(f"   ✓ Title matches primary in {matches:,} papers ({matches/len(df)*100:.1f}%)")

    # Calculate very high confidence indicator
    df['very_high_conf'] = (
        df['db_keyword_found'] & df['title_entity_in_ner']
    )
    high_conf = df['very_high_conf'].sum()
    print(f"   ✓ Very high confidence: {high_conf:,} papers ({high_conf/len(df)*100:.1f}%)")

    return df


def generate_statistics(df, stats_file):
    """Generate and save statistics report."""
    print("\n4. Generating statistics...")

    # Overall stats
    stats = {
        'total': len(df),
        'entity_from_title': (df['entity_from_title'] != '').sum(),
        'db_keyword_found': df['db_keyword_found'].sum(),
        'title_entity_in_ner': df['title_entity_in_ner'].sum(),
        'very_high_conf': df['very_high_conf'].sum(),
        'status_ok': (df['status'] == 'ok').sum(),
        'status_conflict': (df['status'] == 'conflict').sum(),
        'status_low_score': (df['status'] == 'low_score').sum(),
        'status_no_entities': (df['status'] == 'no_entities').sum(),
    }

    # Calculate percentages
    for key in ['entity_from_title', 'db_keyword_found', 'title_entity_in_ner', 'very_high_conf', 'status_ok']:
        stats[f'{key}_pct'] = (stats[key] / stats['total']) * 100 if stats['total'] > 0 else 0

    # Breakdown by source (linguistic vs setfit)
    ling_stats = {
        'total': df['in_linguistic'].sum(),
        'very_high_conf': df[df['in_linguistic']]['very_high_conf'].sum(),
    }
    setfit_stats = {
        'total': df['in_setfit'].sum(),
        'very_high_conf': df[df['in_setfit']]['very_high_conf'].sum(),
    }

    ling_stats['very_high_conf_pct'] = (ling_stats['very_high_conf'] / ling_stats['total']) * 100 if ling_stats['total'] > 0 else 0
    setfit_stats['very_high_conf_pct'] = (setfit_stats['very_high_conf'] / setfit_stats['total']) * 100 if setfit_stats['total'] > 0 else 0

    # Write statistics report
    with open(stats_file, 'w') as f:
        f.write("="*80 + "\n")
        f.write("QUALITY INDICATORS STATISTICS\n")
        f.write("="*80 + "\n\n")

        f.write(f"Total papers: {stats['total']:,}\n\n")

        f.write("Quality Indicators:\n")
        f.write(f"  Entity from title:      {stats['entity_from_title']:>6,} ({stats['entity_from_title_pct']:>5.1f}%)\n")
        f.write(f"  DB keyword found:       {stats['db_keyword_found']:>6,} ({stats['db_keyword_found_pct']:>5.1f}%)\n")
        f.write(f"  Title matches primary:  {stats['title_entity_in_ner']:>6,} ({stats['title_entity_in_ner_pct']:>5.1f}%)\n")
        f.write(f"  Very high confidence:   {stats['very_high_conf']:>6,} ({stats['very_high_conf_pct']:>5.1f}%)\n\n")

        f.write("Status Breakdown:\n")
        f.write(f"  OK (clear primary):     {stats['status_ok']:>6,} ({stats['status_ok_pct']:>5.1f}%)\n")
        f.write(f"  Conflict (tie):         {stats['status_conflict']:>6,}\n")
        f.write(f"  Low score:              {stats['status_low_score']:>6,}\n")
        f.write(f"  No entities:            {stats['status_no_entities']:>6,}\n\n")

        f.write("="*80 + "\n")
        f.write("BREAKDOWN BY SOURCE\n")
        f.write("="*80 + "\n\n")

        f.write("Linguistic Papers:\n")
        f.write(f"  Total:                  {ling_stats['total']:>6,}\n")
        f.write(f"  Very high confidence:   {ling_stats['very_high_conf']:>6,} ({ling_stats['very_high_conf_pct']:>5.1f}%)\n\n")

        f.write("SetFit Papers:\n")
        f.write(f"  Total:                  {setfit_stats['total']:>6,}\n")
        f.write(f"  Very high confidence:   {setfit_stats['very_high_conf']:>6,} ({setfit_stats['very_high_conf_pct']:>5.1f}%)\n\n")

        f.write("="*80 + "\n")
        f.write("KEYWORD SCORE DISTRIBUTION\n")
        f.write("="*80 + "\n\n")

        f.write(f"  Mean score:             {df['db_keyword_score'].mean():>6.2f}\n")
        f.write(f"  Median score:           {df['db_keyword_score'].median():>6.0f}\n")
        f.write(f"  Max score:              {df['db_keyword_score'].max():>6.0f}\n")
        f.write(f"  Papers with score > 0:  {(df['db_keyword_score'] > 0).sum():>6,}\n")
        f.write(f"  Papers with score >= 5: {(df['db_keyword_score'] >= 5).sum():>6,}\n")
        f.write(f"  Papers with score >= 10:{(df['db_keyword_score'] >= 10).sum():>6,}\n")

    print(f"   ✓ Saved statistics to: {stats_file}")
    return stats


def main():
    """Main execution."""
    args = parse_args()
    start_time = datetime.now()

    print("=" * 80)
    print("PHASE 5: ADD QUALITY INDICATORS TO UNION PAPERS")
    print("=" * 80)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Determine input/output paths
    if args.session_dir:
        # SESSION MODE (PREFERRED)
        print("MODE: Session-based")
        print(f"Session: {args.session_dir}\n")

        # Validate session directory
        session_path = Path(args.session_dir).resolve()
        if not session_path.exists():
            print(f"ERROR: Session directory not found: {session_path}")
            sys.exit(1)

        # Validate session structure
        try:
            validate_session_dir(session_path, required_phases=['05_mapping'])
        except ValueError as e:
            print(f"ERROR: Invalid session directory: {e}")
            sys.exit(1)

        input_file = get_session_path(args.session_dir, '05_mapping', 'union_papers_with_primary_resources.csv')
        output_file = get_session_path(args.session_dir, '05_mapping', 'union_papers_with_quality_indicators.csv')
        stats_file = get_session_path(args.session_dir, '05_mapping', 'quality_indicators_statistics.txt')

    elif args.input_file:
        # LEGACY: Explicit paths
        print("MODE: Legacy (explicit paths)")
        input_file = args.input_file
        output_file = args.output_file if args.output_file else LEGACY_OUTPUT_FILE
        stats_file = args.stats_file if args.stats_file else LEGACY_STATS_FILE

    elif args.auto or not args.input_file:
        # LEGACY: Auto-detect
        print("MODE: Legacy (auto-detect)")
        input_file, output_file, stats_file = find_legacy_files()

    else:
        print("ERROR: Must provide --session-dir, --input-file, or use --auto")
        sys.exit(1)

    # Create output directory
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Verify input file exists
    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)

    print(f"\nInput file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Statistics: {stats_file}")

    # ========================================================================
    # LOAD DATA
    # ========================================================================
    print("\n" + "=" * 80)
    print("1. Loading union dataset...")
    df = pd.read_csv(input_file)
    print(f"   Loaded: {len(df):,} papers")
    print(f"   Columns: {list(df.columns)}")

    # ========================================================================
    # PROCESS
    # ========================================================================
    print("\n" + "=" * 80)
    df = process_quality_indicators(df)

    # ========================================================================
    # SAVE OUTPUT
    # ========================================================================
    print("\n" + "=" * 80)
    print("3. Saving enriched dataset...")
    df.to_csv(output_file, index=False)
    print(f"   ✓ Saved to: {output_file}")
    print(f"   ✓ Shape: {df.shape}")

    # ========================================================================
    # GENERATE STATISTICS
    # ========================================================================
    print("\n" + "=" * 80)
    stats = generate_statistics(df, stats_file)

    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    print(f"\nQuality Indicators Added:")
    print(f"  Entity from title:      {stats['entity_from_title']:>6,} ({stats['entity_from_title_pct']:>5.1f}%)")
    print(f"  DB keywords found:      {stats['db_keyword_found']:>6,} ({stats['db_keyword_found_pct']:>5.1f}%)")
    print(f"  Title matches primary:  {stats['title_entity_in_ner']:>6,} ({stats['title_entity_in_ner_pct']:>5.1f}%)")
    print(f"  Very high confidence:   {stats['very_high_conf']:>6,} ({stats['very_high_conf_pct']:>5.1f}%)")

    print(f"\nOutput file: {output_file}")
    print(f"Statistics:  {stats_file}")

    # Runtime
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 80)
    print("SUCCESS")
    print("=" * 80)
    print(f"\nCompleted: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")

    print("\n" + "=" * 80)
    print("NEXT STEP")
    print("=" * 80)
    if args.session_dir:
        print(f"\nRun Phase 6 - URL Extraction:")
        print(f"  python scripts/phase6_url_extraction/11_extract_urls.py --session-dir {args.session_dir}")
        print(f"  Input: {output_file}")
        print(f"  Papers with quality indicators: {len(df):,}")
    else:
        print("\nNext: Extract URLs from papers with quality indicators")
        print(f"  Input: {output_file}")
        print(f"  Papers to process: {len(df):,}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
