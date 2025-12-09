#!/usr/bin/env python3
"""
Phase 5: Create Primary Resource Mapping from Union Papers

Create consolidated CSV with Union papers and primary resource identification.

Scores entities based on:
1. Title mention (+10)
2. NER confidence/probability (+0-1)
3. Abstract mention (+5)
4. Consensus (both spaCy + V2) (+3)
5. Mention frequency (+count)

Output: One row per paper with primary resource and all metadata.

Usage:
    # Session-based (PREFERRED):
    python 11_create_primary_resources.py --session-dir results/2025-12-04-143052-abc12

    # Legacy mode (auto-detect):
    python 11_create_primary_resources.py --auto

    # Custom paths (legacy):
    python 11_create_primary_resources.py \
        --union-papers data/set_c_union.csv \
        --ner-file data/ner_union.csv \
        --metadata-file data/papers_metadata.csv \
        --output-file data/primary_resources.csv

Session Mode:
    When --session-dir is provided:
    - Reads from: {session_dir}/05_mapping/set_c_union.csv (union papers)
    - Reads from: {session_dir}/02_ner/ner_union.csv (NER entities - SINGLE SOURCE OF TRUTH)
    - Reads from: {session_dir}/input/papers_metadata.csv (title, abstract)
    - Outputs to: {session_dir}/05_mapping/union_papers_with_primary_resources.csv

Author: Pipeline Automation
Date: 2025-11-18
Updated: 2025-12-04 (added session-dir support, argparse, ner_union.csv integration)
"""

import argparse
import pandas as pd
import numpy as np
import json
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import re

# Requires: lib/session_utils.py (run from unified_bioresource_pipeline directory)
# Add lib to path for session utilities
SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

# Import from lib - will fail loudly if lib not found
from lib.session_utils import get_session_path, validate_session_dir

# Legacy paths
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent  # inventory_2022
LEGACY_SYNTHESIS_DIR = PROJECT_ROOT / 'pipeline_synthesis_2025-11-18'
LEGACY_UNION_PAPERS = LEGACY_SYNTHESIS_DIR / 'data/paper_sets/set_c_union.csv'
LEGACY_FULL_PAPERS = PROJECT_ROOT / 'validation_spacy_v_BERT/data/v5.1_cleaned.csv'
LEGACY_SPACY_NER = PROJECT_ROOT / 'validation_spacy_v_BERT/results/phase2/ner/spacy_ner_results_2025-11-15-h728fg.csv'
LEGACY_V2_NER = PROJECT_ROOT / 'validation_spacy_v_BERT/results/phase2/ner/v2_ner_results_2025-11-15-icond7.csv'
LEGACY_OUTPUT = LEGACY_SYNTHESIS_DIR / 'data/union_papers_with_primary_resources.csv'

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Phase 5: Create primary resource mapping from union papers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Session-based mode (PREFERRED):
  python 11_create_primary_resources.py --session-dir results/2025-12-04-143052-abc12

  # Legacy mode (auto-detect):
  python 11_create_primary_resources.py --auto

  # Custom paths (legacy):
  python 11_create_primary_resources.py \\
      --union-papers data/set_c_union.csv \\
      --ner-file data/ner_union.csv \\
      --metadata-file data/papers_metadata.csv \\
      --output-file data/primary_resources.csv
        """
    )

    # Session mode arguments
    parser.add_argument("--session-dir", type=Path,
                        help="Session directory path (e.g., results/2025-12-04-143052-abc12)")

    # Legacy mode arguments
    parser.add_argument("--union-papers", type=Path,
                        help="Path to union papers CSV (Set C)")
    parser.add_argument("--ner-file", type=Path,
                        help="Path to NER union CSV (single source of truth for entities)")
    parser.add_argument("--spacy-ner", type=Path,
                        help="[DEPRECATED] Path to spaCy NER CSV (use --ner-file instead)")
    parser.add_argument("--v2-ner", type=Path,
                        help="[DEPRECATED] Path to V2 NER CSV (use --ner-file instead)")
    parser.add_argument("--metadata-file", type=Path,
                        help="Path to papers metadata CSV (with title, abstract)")
    parser.add_argument("--output-file", type=Path,
                        help="Output path for primary resources CSV")
    parser.add_argument("--auto", action="store_true",
                        help="Auto-detect files in legacy paths")

    return parser.parse_args()


def find_legacy_files():
    """Auto-detect legacy file paths."""
    print("=" * 80)
    print("AUTO-DETECTING LEGACY FILES")
    print("=" * 80)

    union_papers = None
    metadata_file = None
    spacy_ner = None
    v2_ner = None
    output_file = None

    # Check legacy paths
    if LEGACY_UNION_PAPERS.exists():
        print(f"\nFound union papers: {LEGACY_UNION_PAPERS.name}")
        union_papers = LEGACY_UNION_PAPERS

    if LEGACY_FULL_PAPERS.exists():
        print(f"Found metadata: {LEGACY_FULL_PAPERS.name}")
        metadata_file = LEGACY_FULL_PAPERS

    if LEGACY_SPACY_NER.exists():
        print(f"Found spaCy NER: {LEGACY_SPACY_NER.name}")
        spacy_ner = LEGACY_SPACY_NER

    if LEGACY_V2_NER.exists():
        print(f"Found V2 NER: {LEGACY_V2_NER.name}")
        v2_ner = LEGACY_V2_NER

    output_file = LEGACY_OUTPUT

    # Check if we found everything
    if not union_papers or not metadata_file or not (spacy_ner and v2_ner):
        print("\nERROR: Could not find all required files!")
        if not union_papers:
            print(f"  Missing: {LEGACY_UNION_PAPERS}")
        if not metadata_file:
            print(f"  Missing: {LEGACY_FULL_PAPERS}")
        if not spacy_ner:
            print(f"  Missing: {LEGACY_SPACY_NER}")
        if not v2_ner:
            print(f"  Missing: {LEGACY_V2_NER}")
        sys.exit(1)

    return union_papers, metadata_file, spacy_ner, v2_ner, output_file


def load_ner_from_union_csv(ner_union_file):
    """
    Load NER entities from ner_union.csv (session mode).
    This is the SINGLE SOURCE OF TRUTH for entity extraction.

    Returns two DataFrames compatible with legacy code:
    - df_spacy: spaCy entities
    - df_v2: V2 entities
    """
    print("\n3. Loading NER results from UNION CSV...")
    print(f"   Source: {ner_union_file}")
    print("   NOTE: This is the single source of truth for entity extraction")

    df_ner = pd.read_csv(ner_union_file)
    print(f"   Loaded: {len(df_ner):,} entity mentions")
    print(f"   Columns: {list(df_ner.columns)}")

    # Split by source
    # SpaCy hybrid NER uses sources: 'statistical', 'ruler', 'spacy_hybrid'
    # V2 BERT uses source: 'v2_bert'
    spacy_sources = ['statistical', 'ruler', 'spacy_hybrid']
    df_spacy = df_ner[df_ner['source'].isin(spacy_sources)].copy()
    df_v2 = df_ner[df_ner['source'] == 'v2_bert'].copy()

    # Normalize column names to match legacy code expectations
    # ner_union.csv uses: publication_id, entity_text, entity_type, confidence, source
    # Legacy code expects: ID, mention, label, prob

    if 'publication_id' in df_spacy.columns:
        df_spacy['ID'] = df_spacy['publication_id']
    if 'entity_text' in df_spacy.columns:
        df_spacy['mention'] = df_spacy['entity_text']
    if 'entity_type' in df_spacy.columns:
        df_spacy['label'] = df_spacy['entity_type']
    # spaCy doesn't have prob in ner_union, confidence is always 1.0

    if 'publication_id' in df_v2.columns:
        df_v2['ID'] = df_v2['publication_id']
    if 'entity_text' in df_v2.columns:
        df_v2['mention'] = df_v2['entity_text']
    if 'entity_type' in df_v2.columns:
        df_v2['label'] = df_v2['entity_type']
    if 'confidence' in df_v2.columns:
        df_v2['prob'] = df_v2['confidence']

    print(f"   spaCy entities: {len(df_spacy):,}")
    print(f"   V2 entities: {len(df_v2):,}")

    return df_spacy, df_v2


def load_ner_from_separate_files(spacy_file, v2_file):
    """Load NER entities from separate spaCy and V2 files (legacy mode)."""
    print("\n3. Loading NER results from separate files...")
    print("   NOTE: In session mode, use ner_union.csv instead")

    df_spacy = pd.read_csv(spacy_file)
    print(f"   spaCy NER: {len(df_spacy):,} entity mentions")
    print(f"   Columns: {list(df_spacy.columns)}")

    df_v2 = pd.read_csv(v2_file)
    print(f"   V2 NER: {len(df_v2):,} entity mentions")
    print(f"   Columns: {list(df_v2.columns)}")

    return df_spacy, df_v2


def normalize_entity(name):
    """Normalize entity name for matching"""
    if pd.isna(name):
        return ''
    return str(name).lower().strip()

def is_short_form(entity):
    """Heuristic: short forms are usually uppercase acronyms or short words"""
    if not entity:
        return False
    # Short if: all uppercase, or length <= 6 chars
    return entity.isupper() or len(entity) <= 6


def find_long_short_matches(entities):
    """Find potential long/short form pairs"""
    matches = []
    entity_list = list(entities)

    for i, e1 in enumerate(entity_list):
        for e2 in entity_list[i+1:]:
            # Check if one is acronym of the other
            # Simple heuristic: short form chars appear in long form
            if len(e1) < len(e2):
                short, long = e1, e2
            else:
                short, long = e2, e1

            # Check if short could be acronym of long
            if len(short) <= 6 and len(long) > 6:
                # Check if first letters match
                long_words = long.split()
                if len(long_words) >= len(short):
                    acronym = ''.join([w[0] for w in long_words if w])
                    if acronym == short:
                        matches.append(f"{short} | {long}")

    return matches


def score_entity(entity, entity_data, title, abstract):
    """Score entity based on multiple signals"""
    score = 0.0

    # Title mention (+10)
    if title and pd.notna(title):
        if entity in normalize_entity(title):
            score += 10.0

    # Abstract mention (+5)
    if abstract and pd.notna(abstract):
        if entity in normalize_entity(abstract):
            score += 5.0

    # Consensus (both spaCy and V2) (+3)
    if entity_data['spacy_count'] > 0 and entity_data['v2_count'] > 0:
        score += 3.0

    # V2 probability (0-1)
    score += entity_data['v2_max_prob']

    # Mention frequency
    score += entity_data['total_mentions']

    return score


def safe_pmid_to_string(pmid_value):
    """Safely convert PMID to string, handling NaN and type variations."""
    if pd.isna(pmid_value):
        return None
    try:
        return str(int(float(pmid_value)))
    except (ValueError, TypeError):
        return None


def build_entity_index(df_spacy, df_v2):
    """Build entity index per paper."""
    print("\n4. Building entity index per paper...")

    # Index: pmid -> {entity_name: {spacy: count, v2: {count, max_prob}, total_mentions}}
    entity_index = defaultdict(lambda: defaultdict(lambda: {
        'spacy_count': 0,
        'v2_count': 0,
        'v2_max_prob': 0.0,
        'total_mentions': 0,
        'is_short': False
    }))

    # Process spaCy NER
    for _, row in df_spacy.iterrows():
        pmid = safe_pmid_to_string(row['ID'])
        if pmid is None:
            continue
        mention = row.get('mention', '')
        if pd.notna(mention) and mention:
            entity = normalize_entity(mention)
            entity_index[pmid][entity]['spacy_count'] += 1
            entity_index[pmid][entity]['total_mentions'] += 1
            entity_index[pmid][entity]['is_short'] = is_short_form(mention)

    # Process V2 NER
    for _, row in df_v2.iterrows():
        pmid = safe_pmid_to_string(row['ID'])
        if pmid is None:
            continue
        mention = row.get('mention', '')
        prob = row.get('prob', 0.0)
        if pd.notna(mention) and mention:
            entity = normalize_entity(mention)
            entity_index[pmid][entity]['v2_count'] += 1
            entity_index[pmid][entity]['total_mentions'] += 1
            # Track max probability
            if pd.notna(prob):
                entity_index[pmid][entity]['v2_max_prob'] = max(
                    entity_index[pmid][entity]['v2_max_prob'],
                    float(prob)
                )
            entity_index[pmid][entity]['is_short'] = is_short_form(mention)

    print(f"   Indexed entities for {len(entity_index):,} papers")
    return entity_index


def process_primary_resources(df_merged, entity_index):
    """Score entities for primary resource identification."""
    print("\n5. Scoring entities for primary resource identification...")

    results = []

    for idx, row in df_merged.iterrows():
        if idx % 1000 == 0:
            print(f"   Processing paper {idx+1}/{len(df_merged)}...")

        pmid = safe_pmid_to_string(row['pmid'])
        if pmid is None:
            continue
        title = row.get('title', '')
        abstract = row.get('abstract', '')

        # Get entities for this paper
        entities = entity_index.get(pmid, {})

        if not entities:
            # No entities found
            results.append({
                'pmid': pmid,
                'title': title,
                'abstract': abstract,
                'in_linguistic': row['in_linguistic'],
                'in_setfit': row['in_setfit'],
                'ling_score': row.get('ling_score', ''),
                'setfit_confidence': row.get('setfit_confidence', ''),
                'primary_entity_long': '',
                'primary_entity_short': '',
                'primary_score': 0.0,
                'status': 'no_entities',
                'matched_long_short': '',
                'all_long': '',
                'all_short': '',
                'ner_source': '',
                'ner_confidence': ''
            })
            continue

        # Score all entities
        entity_scores = {}
        for entity, data in entities.items():
            entity_scores[entity] = score_entity(entity, data, title, abstract)

        # Find top scoring entity/entities
        if entity_scores:
            max_score = max(entity_scores.values())
            top_entities = [e for e, s in entity_scores.items() if s == max_score]

            # Categorize entities as long/short
            long_entities = [e for e in entities.keys() if not entities[e]['is_short']]
            short_entities = [e for e in entities.keys() if entities[e]['is_short']]

            # Find matched pairs
            matched_pairs = find_long_short_matches(entities.keys())

            # Determine primary long/short
            primary_long = ''
            primary_short = ''
            for e in top_entities:
                if entities[e]['is_short']:
                    primary_short = e if not primary_short else primary_short + ' | ' + e
                else:
                    primary_long = e if not primary_long else primary_long + ' | ' + e

            # Determine status
            status = 'ok'
            if len(top_entities) > 1:
                status = 'conflict'
            elif max_score < 5.0:
                status = 'low_score'

            # Determine NER source
            primary_entity = top_entities[0]
            ner_source = []
            if entities[primary_entity]['spacy_count'] > 0:
                ner_source.append('spacy')
            if entities[primary_entity]['v2_count'] > 0:
                ner_source.append('v2')

            ner_confidence = entities[primary_entity]['v2_max_prob']

            # Remove primary from all_long/all_short
            other_long = [e for e in long_entities if e not in top_entities]
            other_short = [e for e in short_entities if e not in top_entities]

            results.append({
                'pmid': pmid,
                'title': title,
                'abstract': abstract,
                'in_linguistic': row['in_linguistic'],
                'in_setfit': row['in_setfit'],
                'ling_score': row.get('ling_score', ''),
                'setfit_confidence': row.get('setfit_confidence', ''),
                'primary_entity_long': primary_long,
                'primary_entity_short': primary_short,
                'primary_score': max_score,
                'status': status,
                'matched_long_short': ' ; '.join(matched_pairs),
                'all_long': ' | '.join(other_long) if other_long else '',
                'all_short': ' | '.join(other_short) if other_short else '',
                'ner_source': '+'.join(ner_source),
                'ner_confidence': ner_confidence
            })

    return results


def main():
    """Main execution."""
    args = parse_args()
    start_time = datetime.now()

    print("=" * 80)
    print("PHASE 5: CREATE PRIMARY RESOURCE MAPPING")
    print("=" * 80)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Determine input/output paths
    use_ner_union = False  # Flag to track if using session's ner_union.csv

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
            validate_session_dir(session_path, required_phases=['02_ner', '05_mapping'])
        except ValueError as e:
            print(f"ERROR: Invalid session directory: {e}")
            sys.exit(1)

        union_papers_file = get_session_path(args.session_dir, '05_mapping', 'set_c_union.csv')
        ner_union_file = get_session_path(args.session_dir, '02_ner', 'ner_union.csv')
        metadata_file = get_session_path(args.session_dir, 'input', 'papers_metadata.csv')
        output_file = get_session_path(args.session_dir, '05_mapping', 'union_papers_with_primary_resources.csv')
        use_ner_union = True

    elif args.auto or not (args.union_papers or args.ner_file):
        # LEGACY: Auto-detect
        print("MODE: Legacy (auto-detect)")
        union_papers_file, metadata_file, spacy_ner_file, v2_ner_file, output_file = find_legacy_files()

    elif args.union_papers and args.ner_file:
        # LEGACY: Explicit paths with ner_union.csv
        print("MODE: Legacy (explicit paths with ner_union.csv)")
        union_papers_file = args.union_papers
        ner_union_file = args.ner_file
        metadata_file = args.metadata_file if args.metadata_file else LEGACY_FULL_PAPERS
        output_file = args.output_file if args.output_file else LEGACY_OUTPUT
        use_ner_union = True

    elif args.union_papers and args.spacy_ner and args.v2_ner:
        # LEGACY: Explicit paths with separate NER files
        print("MODE: Legacy (explicit paths with separate NER files)")
        union_papers_file = args.union_papers
        spacy_ner_file = args.spacy_ner
        v2_ner_file = args.v2_ner
        metadata_file = args.metadata_file if args.metadata_file else LEGACY_FULL_PAPERS
        output_file = args.output_file if args.output_file else LEGACY_OUTPUT

    else:
        print("ERROR: Must provide --session-dir, or use --auto, or provide all required files")
        sys.exit(1)

    # Create output directory
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Verify input files exist
    if not union_papers_file.exists():
        print(f"ERROR: Union papers file not found: {union_papers_file}")
        sys.exit(1)
    if not metadata_file.exists():
        print(f"ERROR: Metadata file not found: {metadata_file}")
        sys.exit(1)

    print(f"\nInput files:")
    print(f"  Union papers: {union_papers_file}")
    print(f"  Metadata: {metadata_file}")
    if use_ner_union:
        print(f"  NER entities: {ner_union_file} (SINGLE SOURCE OF TRUTH)")
    else:
        print(f"  spaCy NER: {spacy_ner_file}")
        print(f"  V2 NER: {v2_ner_file}")
    print(f"\nOutput file: {output_file}")

    # ========================================================================
    # LOAD DATA
    # ========================================================================

    # Load Union papers
    print("\n" + "=" * 80)
    print("1. Loading Union paper set...")
    df_union = pd.read_csv(union_papers_file)
    print(f"   Loaded {len(df_union):,} papers")
    print(f"   Columns: {list(df_union.columns)}")

    # Create flags
    df_union['in_linguistic'] = df_union['source'].isin(['linguistic', 'linguistic_only', 'both'])
    df_union['in_setfit'] = df_union['source'].isin(['setfit', 'setfit_only', 'both'])

    # Load paper metadata (title, abstract)
    print("\n2. Loading paper metadata (title, abstract)...")
    df_papers = pd.read_csv(metadata_file)
    print(f"   Loaded {len(df_papers):,} papers from full dataset")

    # Rename id/publication_id to pmid if needed
    if 'pmid' not in df_papers.columns:
        if 'publication_id' in df_papers.columns:
            df_papers = df_papers.rename(columns={'publication_id': 'pmid'})
        elif 'id' in df_papers.columns:
            df_papers = df_papers.rename(columns={'id': 'pmid'})

    # Merge metadata
    df_merged = df_union.merge(
        df_papers[['pmid', 'title', 'abstract']],
        on='pmid',
        how='left'
    )
    print(f"   Merged: {len(df_merged):,} papers with metadata")

    # Load NER results
    if use_ner_union:
        if not ner_union_file.exists():
            print(f"ERROR: NER union file not found: {ner_union_file}")
            sys.exit(1)
        df_spacy, df_v2 = load_ner_from_union_csv(ner_union_file)
    else:
        if not spacy_ner_file.exists():
            print(f"ERROR: spaCy NER file not found: {spacy_ner_file}")
            sys.exit(1)
        if not v2_ner_file.exists():
            print(f"ERROR: V2 NER file not found: {v2_ner_file}")
            sys.exit(1)
        df_spacy, df_v2 = load_ner_from_separate_files(spacy_ner_file, v2_ner_file)

    # ========================================================================
    # PROCESS
    # ========================================================================

    # Build entity index
    entity_index = build_entity_index(df_spacy, df_v2)

    # Process primary resources
    results = process_primary_resources(df_merged, entity_index)

    # Create output dataframe
    print("\n6. Creating output CSV...")
    df_output = pd.DataFrame(results)

    # Save
    df_output.to_csv(output_file, index=False)
    print(f"   Saved to: {output_file}")
    print(f"   Total rows: {len(df_output):,}")

    # ========================================================================
    # SUMMARY STATISTICS
    # ========================================================================
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)

    print(f"\nTotal papers: {len(df_output):,}")
    print(f"  In Linguistic: {df_output['in_linguistic'].sum():,}")
    print(f"  In SetFit: {df_output['in_setfit'].sum():,}")

    print(f"\nStatus breakdown:")
    for status in df_output['status'].value_counts().items():
        print(f"  {status[0]}: {status[1]:,} ({status[1]/len(df_output)*100:.1f}%)")

    print(f"\nPrimary entity assignment:")
    print(f"  Has primary_long: {(df_output['primary_entity_long'] != '').sum():,}")
    print(f"  Has primary_short: {(df_output['primary_entity_short'] != '').sum():,}")
    print(f"  Has both: {((df_output['primary_entity_long'] != '') & (df_output['primary_entity_short'] != '')).sum():,}")
    print(f"  Has neither: {((df_output['primary_entity_long'] == '') & (df_output['primary_entity_short'] == '')).sum():,}")

    print(f"\nScore distribution:")
    print(df_output['primary_score'].describe())

    # Runtime
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 80)
    print("SUCCESS")
    print("=" * 80)
    print(f"\nCompleted: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print(f"\nOutput file: {output_file}")
    print(f"Columns: {', '.join(df_output.columns)}")

    print("\n" + "=" * 80)
    print("NEXT STEP")
    print("=" * 80)
    if args.session_dir:
        print(f"\nRun Phase 6 - URL Extraction:")
        print(f"  python scripts/phase6_url_extraction/11_extract_urls.py --session-dir {args.session_dir}")
        print(f"  Input: {output_file}")
        print(f"  Papers with primary resources: {len(df_output):,}")
    else:
        print("\nNext: Extract URLs from primary resources")
        print(f"  Input: {output_file}")
        print(f"  Papers to process: {len(df_output):,}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
