#!/usr/bin/env python3
"""
Map Papers to Entities from NER Results

For each paper set (A, B, C), extract all entities from both spaCy and V2 NER results.
Generate entity inventories with paper counts, PMID lists, and source tracking.
"""

import pandas as pd
import json
import ast
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Paths
BASE_DIR = Path("/Users/warren/development/GBC/inventory_2022")
PAPER_SETS_DIR = BASE_DIR / "pipeline_synthesis_2025-11-18/data/paper_sets"
NER_RESULTS_DIR = BASE_DIR / "validation_spacy_v_BERT/results/phase2/ner"

# NER result files
SPACY_NER_FILE = NER_RESULTS_DIR / "spacy_ner_full_hybrid_results_2025-11-16-q9tnzj.csv"
V2_NER_FILE = NER_RESULTS_DIR / "v2_ner_results_2025-11-15-icond7.csv"

# Output directories
ENTITY_MAPPINGS_DIR = BASE_DIR / "pipeline_synthesis_2025-11-18/data/entity_mappings"
ENTITY_INVENTORIES_DIR = BASE_DIR / "pipeline_synthesis_2025-11-18/data/entity_inventories"

def safe_parse_entities(entity_str):
    """Safely parse entity string (might be list or string)"""
    if pd.isna(entity_str) or entity_str == '[]' or entity_str == '':
        return []

    try:
        # Try to parse as literal
        entities = ast.literal_eval(entity_str)
        if isinstance(entities, list):
            return entities
        else:
            return []
    except:
        # If parsing fails, return empty list
        return []

def normalize_entity_name(entity_name):
    """Normalize entity name for matching"""
    if pd.isna(entity_name):
        return None
    return str(entity_name).strip().lower()

def load_ner_results():
    """Load both NER result files"""
    print("Loading NER results...")

    # Load spaCy NER
    print(f"  Loading spaCy NER: {SPACY_NER_FILE.name}")
    df_spacy = pd.read_csv(SPACY_NER_FILE)
    print(f"    Loaded: {len(df_spacy):,} papers")

    # Load V2 NER
    print(f"  Loading V2 NER: {V2_NER_FILE.name}")
    df_v2 = pd.read_csv(V2_NER_FILE)
    print(f"    Loaded: {len(df_v2):,} papers")

    return df_spacy, df_v2

def create_pmid_to_entities_map(df_spacy, df_v2):
    """Create mapping from PMID to all entities (union of spaCy + V2)

    NER files have one row per entity mention:
    - spaCy: ID, text, mention, label, canonical_id, source, start_char, end_char
    - V2: ID, text, publication_date, mention, label, prob
    """
    print("\nCreating PMID → entities mapping...")

    pmid_entities = defaultdict(lambda: {'spacy': set(), 'v2': set(), 'union': set()})

    # Process spaCy results (ID is the PMID, mention is the entity text)
    print("  Processing spaCy entities...")
    spacy_count = 0
    for _, row in df_spacy.iterrows():
        pmid = str(row['ID'])  # PMID is in 'ID' column
        mention = row.get('mention', '')

        if pd.notna(mention) and mention:
            normalized = normalize_entity_name(mention)
            if normalized:
                pmid_entities[pmid]['spacy'].add(normalized)
                pmid_entities[pmid]['union'].add(normalized)
                spacy_count += 1

    print(f"    Processed: {spacy_count:,} spaCy entities from {len(set(df_spacy['ID'])):,} papers")

    # Process V2 results (ID is the PMID, mention is the entity text)
    print("  Processing V2 entities...")
    v2_count = 0
    for _, row in df_v2.iterrows():
        pmid = str(row['ID'])  # PMID is in 'ID' column
        mention = row.get('mention', '')

        if pd.notna(mention) and mention:
            normalized = normalize_entity_name(mention)
            if normalized:
                pmid_entities[pmid]['v2'].add(normalized)
                pmid_entities[pmid]['union'].add(normalized)
                v2_count += 1

    print(f"    Processed: {v2_count:,} V2 entities from {len(set(df_v2['ID'])):,} papers")
    print(f"  Total papers with entities: {len(pmid_entities):,}")

    return pmid_entities

def map_paper_set_to_entities(set_name, paper_set_file, pmid_entities):
    """Map a paper set to its entities"""
    print(f"\n{'='*80}")
    print(f"Processing {set_name}")
    print(f"{'='*80}")

    # Load paper set
    df_papers = pd.read_csv(paper_set_file)
    print(f"Papers in set: {len(df_papers):,}")

    # Extract PMIDs
    pmids = set(df_papers['pmid'])

    # Map papers to entities
    paper_to_entities = []
    entity_inventory = defaultdict(lambda: {'spacy_papers': set(), 'v2_papers': set(),
                                            'union_papers': set(), 'total_papers': set()})

    papers_with_entities = 0
    total_entities = 0

    for pmid in pmids:
        pmid_str = str(pmid)  # Ensure PMID is string for matching
        if pmid_str in pmid_entities:
            entities_data = pmid_entities[pmid_str]

            # Get union of entities for this paper
            all_entities = entities_data['union']

            if all_entities:
                papers_with_entities += 1
                total_entities += len(all_entities)

                # Record paper → entities mapping
                paper_to_entities.append({
                    'pmid': pmid,
                    'entity_count': len(all_entities),
                    'entities': list(all_entities),
                    'spacy_count': len(entities_data['spacy']),
                    'v2_count': len(entities_data['v2'])
                })

                # Build entity inventory
                for entity in all_entities:
                    entity_inventory[entity]['total_papers'].add(pmid)

                    if entity in entities_data['spacy']:
                        entity_inventory[entity]['spacy_papers'].add(pmid)
                    if entity in entities_data['v2']:
                        entity_inventory[entity]['v2_papers'].add(pmid)
                    if entity in entities_data['spacy'] and entity in entities_data['v2']:
                        entity_inventory[entity]['union_papers'].add(pmid)

    print(f"Papers with entities: {papers_with_entities:,} ({papers_with_entities/len(df_papers)*100:.1f}%)")
    print(f"Total entity mentions: {total_entities:,}")
    print(f"Unique entities: {len(entity_inventory):,}")

    # Save paper → entities mapping
    df_mapping = pd.DataFrame(paper_to_entities)
    if len(df_mapping) > 0:
        df_mapping = df_mapping.sort_values('entity_count', ascending=False)

    mapping_file = ENTITY_MAPPINGS_DIR / f"{set_name}_paper_to_entities.csv"
    df_mapping.to_csv(mapping_file, index=False)
    print(f"  Saved mapping: {mapping_file}")

    # Create entity inventory DataFrame
    inventory_data = []
    for entity, data in entity_inventory.items():
        inventory_data.append({
            'entity_name': entity,
            'total_papers': len(data['total_papers']),
            'spacy_papers': len(data['spacy_papers']),
            'v2_papers': len(data['v2_papers']),
            'source': 'both' if (len(data['spacy_papers']) > 0 and len(data['v2_papers']) > 0)
                     else 'spacy' if len(data['spacy_papers']) > 0
                     else 'v2',
            'pmids': ','.join(map(str, sorted(data['total_papers'])))
        })

    df_inventory = pd.DataFrame(inventory_data)
    if len(df_inventory) > 0:
        df_inventory = df_inventory.sort_values('total_papers', ascending=False)

    inventory_file = ENTITY_INVENTORIES_DIR / f"{set_name}_entity_inventory.csv"
    df_inventory.to_csv(inventory_file, index=False)
    print(f"  Saved inventory: {inventory_file}")

    # Save statistics
    stats = {
        'set_name': set_name,
        'timestamp': datetime.now().isoformat(),
        'total_papers': len(df_papers),
        'papers_with_entities': papers_with_entities,
        'coverage_rate': round(papers_with_entities / len(df_papers) * 100, 2) if len(df_papers) > 0 else 0,
        'total_entity_mentions': total_entities,
        'unique_entities': len(entity_inventory),
        'avg_entities_per_paper': round(total_entities / papers_with_entities, 2) if papers_with_entities > 0 else 0,
        'source_breakdown': {
            'spacy_only': len([e for e in entity_inventory.values() if len(e['spacy_papers']) > 0 and len(e['v2_papers']) == 0]),
            'v2_only': len([e for e in entity_inventory.values() if len(e['v2_papers']) > 0 and len(e['spacy_papers']) == 0]),
            'both': len([e for e in entity_inventory.values() if len(e['spacy_papers']) > 0 and len(e['v2_papers']) > 0])
        }
    }

    stats_file = ENTITY_INVENTORIES_DIR / f"{set_name}_entity_stats.json"
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"  Saved stats: {stats_file}")

    return stats

def main():
    print("=" * 80)
    print("MAPPING PAPERS TO ENTITIES FROM NER RESULTS")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Create output directories
    ENTITY_MAPPINGS_DIR.mkdir(parents=True, exist_ok=True)
    ENTITY_INVENTORIES_DIR.mkdir(parents=True, exist_ok=True)

    # Load NER results
    df_spacy, df_v2 = load_ner_results()

    # Create PMID → entities mapping
    pmid_entities = create_pmid_to_entities_map(df_spacy, df_v2)

    # Process each paper set
    paper_sets = [
        ('set_a', PAPER_SETS_DIR / "set_a_linguistic.csv"),
        ('set_b', PAPER_SETS_DIR / "set_b_setfit.csv"),
        ('set_c', PAPER_SETS_DIR / "set_c_union.csv")
    ]

    all_stats = {}
    for set_name, paper_file in paper_sets:
        stats = map_paper_set_to_entities(set_name, paper_file, pmid_entities)
        all_stats[set_name] = stats

    # Summary report
    print("\n" + "=" * 80)
    print("ENTITY MAPPING SUMMARY")
    print("=" * 80)
    for set_name, stats in all_stats.items():
        print(f"\n{set_name.upper()}:")
        print(f"  Total papers:         {stats['total_papers']:,}")
        print(f"  Papers with entities: {stats['papers_with_entities']:,} ({stats['coverage_rate']}%)")
        print(f"  Unique entities:      {stats['unique_entities']:,}")
        print(f"  Avg entities/paper:   {stats['avg_entities_per_paper']:.2f}")
        print(f"  Source breakdown:")
        print(f"    spaCy only:         {stats['source_breakdown']['spacy_only']:,}")
        print(f"    V2 only:            {stats['source_breakdown']['v2_only']:,}")
        print(f"    Both:               {stats['source_breakdown']['both']:,}")

    print("\n" + "=" * 80)
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

if __name__ == "__main__":
    main()
