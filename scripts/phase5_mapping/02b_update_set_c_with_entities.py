#!/usr/bin/env python3
"""
Update set_c_union.csv with Entity and URL Data

PROBLEM: Script 02 creates set_c_union.csv with only basic fields (pmid, source, scores).
Scripts 09-11 add entity and URL data to union_papers_with_primary_resources.csv
and filtered datasets, but set_c_union.csv never gets updated.

SOLUTION: This script merges entity and URL data back into set_c_union.csv.

EXECUTION ORDER:
1. Run Script 02 (create_paper_sets.py) - creates set_c_union.csv with basic fields
2. Run Script 09 (create_primary_resource_csv.py) - adds entity data
3. Run Script 10 (create_filtered_datasets.py) - creates filtered datasets with entities
4. Run Script 11 (extract_urls.py) - adds URL data to filtered datasets
5. Run THIS SCRIPT - merges entity/URL data back into set_c_union.csv

Created: 2025-11-24
Purpose: Fix data loss bug causing 1,247 baseline papers to be incorrectly excluded
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

# Paths
BASE_DIR = Path("/Users/warren/development/GBC/inventory_2022")
SYNTHESIS_DIR = BASE_DIR / "pipeline_synthesis_2025-11-18"

# Input files
SET_C_UNION = SYNTHESIS_DIR / "data/paper_sets/set_c_union.csv"
UNION_PRIMARY = SYNTHESIS_DIR / "data/union_papers_with_primary_resources.csv"
LINGUISTIC_FILE = SYNTHESIS_DIR / "data/filtered/linguistic_all_papers.csv"
SETFIT_FILE = SYNTHESIS_DIR / "data/filtered/setfit_all_papers.csv"

# Output (overwrites original)
OUTPUT_FILE = SET_C_UNION

def main():
    print("=" * 80)
    print("UPDATING SET_C_UNION WITH ENTITY AND URL DATA")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # ========================================================================
    # STEP 1: Load set_c_union.csv (basic fields only)
    # ========================================================================
    print("1. Loading set_c_union.csv...")
    df_set_c = pd.read_csv(SET_C_UNION)
    print(f"   Loaded: {len(df_set_c):,} papers")
    print(f"   Current columns: {list(df_set_c.columns)}")

    original_count = len(df_set_c)

    # ========================================================================
    # STEP 2: Load entity data from union_papers_with_primary_resources.csv
    # ========================================================================
    print("\n2. Loading entity data from union_papers_with_primary_resources.csv...")
    df_union_primary = pd.read_csv(UNION_PRIMARY)
    print(f"   Loaded: {len(df_union_primary):,} papers")

    # Select entity-related columns
    entity_cols = [
        'pmid', 'title', 'abstract',
        'primary_entity_long', 'primary_entity_short',
        'primary_score', 'status',
        'matched_long_short', 'all_long', 'all_short',
        'ner_source', 'ner_confidence'
    ]
    df_entities = df_union_primary[entity_cols].copy()

    # Merge entity data
    print("   Merging entity data...")
    df_set_c = df_set_c.merge(df_entities, on='pmid', how='left')
    print(f"   After merge: {len(df_set_c):,} papers")

    # Check for papers with entities
    has_entity = (df_set_c['primary_entity_long'].notna() &
                  (df_set_c['primary_entity_long'] != ''))
    print(f"   Papers with primary_entity_long: {has_entity.sum():,}")

    # ========================================================================
    # STEP 3: Load URL data from filtered datasets
    # ========================================================================
    print("\n3. Loading URL data from filtered datasets...")

    # Load both filtered files
    df_linguistic = pd.read_csv(LINGUISTIC_FILE)
    df_setfit = pd.read_csv(SETFIT_FILE)

    print(f"   Linguistic papers: {len(df_linguistic):,}")
    print(f"   SetFit papers: {len(df_setfit):,}")

    # Select URL-related columns
    url_cols = [
        'pmid', 'entity_from_title', 'db_keyword_found',
        'title_entity_in_ner', 'very_high_conf',
        'all_urls', 'resource_url', 'has_resource_url', 'url_context'
    ]

    # Combine URL data from both sources (union, preferring non-null values)
    df_linguistic_urls = df_linguistic[url_cols].copy()
    df_setfit_urls = df_setfit[url_cols].copy()

    # Concatenate and remove duplicates (keep first occurrence)
    df_urls = pd.concat([df_linguistic_urls, df_setfit_urls], ignore_index=True)
    df_urls = df_urls.drop_duplicates(subset=['pmid'], keep='first')

    print(f"   Combined URL data: {len(df_urls):,} unique papers")

    # Merge URL data
    print("   Merging URL data...")
    df_set_c = df_set_c.merge(df_urls, on='pmid', how='left')
    print(f"   After merge: {len(df_set_c):,} papers")

    # Check for papers with URLs
    has_url = (df_set_c['has_resource_url'] == True)
    print(f"   Papers with resource URLs: {has_url.sum():,}")

    # Check for papers with db keywords
    has_db_keyword = (df_set_c['db_keyword_found'] == True)
    print(f"   Papers with DB keywords: {has_db_keyword.sum():,}")

    # ========================================================================
    # STEP 4: Validate data integrity
    # ========================================================================
    print("\n4. Validating data integrity...")

    # Check 1: No papers lost
    if len(df_set_c) != original_count:
        print(f"   ⚠️  WARNING: Paper count changed! Original: {original_count}, Current: {len(df_set_c)}")
    else:
        print(f"   ✅ Paper count preserved: {len(df_set_c):,}")

    # Check 2: All papers should have entities (except those with status='no_entities')
    no_entity_status = (df_set_c['status'] == 'no_entities')
    should_have_entity = ~no_entity_status
    missing_entity = should_have_entity & ~has_entity

    if missing_entity.sum() > 0:
        print(f"   ⚠️  WARNING: {missing_entity.sum()} papers missing entities (status != 'no_entities')")
    else:
        print(f"   ✅ All papers have entities (or status='no_entities')")

    # Check 3: Papers by source
    print("\n   Papers by source:")
    for source in ['linguistic_only', 'setfit_only', 'both']:
        source_count = (df_set_c['source'] == source).sum()
        source_with_url = ((df_set_c['source'] == source) & has_url).sum()
        source_with_entity = ((df_set_c['source'] == source) & has_entity).sum()
        print(f"     {source}:")
        print(f"       Total: {source_count:,}")
        print(f"       With entity: {source_with_entity:,} ({100*source_with_entity/source_count:.1f}%)")
        print(f"       With URL: {source_with_url:,} ({100*source_with_url/source_count:.1f}%)")

    # ========================================================================
    # STEP 5: Save updated set_c_union.csv
    # ========================================================================
    print("\n5. Saving updated set_c_union.csv...")

    # Create backup of original
    backup_path = SET_C_UNION.parent / f"{SET_C_UNION.stem}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    print(f"   Creating backup: {backup_path.name}")
    df_original = pd.read_csv(SET_C_UNION)
    df_original.to_csv(backup_path, index=False)

    # Save updated version
    df_set_c.to_csv(OUTPUT_FILE, index=False)
    print(f"   Saved: {OUTPUT_FILE}")
    print(f"   Total columns: {len(df_set_c.columns)}")
    print(f"   New columns: {list(df_set_c.columns)}")

    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\nOriginal set_c_union.csv:")
    print(f"  Papers: {original_count:,}")
    print(f"  Columns: {len(df_original.columns)} (pmid, source, ling_score, setfit_confidence)")

    print(f"\nUpdated set_c_union.csv:")
    print(f"  Papers: {len(df_set_c):,}")
    print(f"  Columns: {len(df_set_c.columns)}")
    print(f"  Papers with entities: {has_entity.sum():,} ({100*has_entity.sum()/len(df_set_c):.1f}%)")
    print(f"  Papers with URLs: {has_url.sum():,} ({100*has_url.sum()/len(df_set_c):.1f}%)")
    print(f"  Papers with DB keywords: {has_db_keyword.sum():,} ({100*has_db_keyword.sum()/len(df_set_c):.1f}%)")

    print(f"\nExpected Impact:")
    print(f"  Before fix: 1,247 baseline papers incorrectly excluded (blank entity/URL)")
    print(f"  After fix: These papers should now pass deduplication filters")

    print(f"\nBackup saved to: {backup_path.name}")
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

if __name__ == "__main__":
    main()
