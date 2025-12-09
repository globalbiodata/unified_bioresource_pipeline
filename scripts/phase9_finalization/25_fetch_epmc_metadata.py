#!/usr/bin/env python3
"""
Script 25: Fetch EuropePMC Metadata

Purpose: Get metadata from EuropePMC API for all PMIDs
         - publication_date
         - affiliation
         - authors
         - grant_ids
         - grant_agencies
         - num_citations
         - paper_titles (NEW: for QC and capitalization)

Also applies title-aware capitalization to best_name:
- Searches paper titles for resource name
- Uses capitalization from title if found
- Falls back to smart title case otherwise

Authors: AI Assistant
Date: 2025-11-27
Updated: 2025-12-05 (Session-based refactor)
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

# Add lib imports
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from lib.session_utils import get_session_path, validate_session_dir


def get_args() -> argparse.Namespace:
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Fetch metadata from EuropePMC API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 25_fetch_epmc_metadata.py --session-dir results/2025-12-04-143052-a3f9b
  python 25_fetch_epmc_metadata.py --session-dir results/2025-12-04-143052-a3f9b --chunk-size 50
        """
    )

    parser.add_argument(
        '--session-dir',
        type=str,
        required=True,
        help='Session directory path'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=20,
        help='Number of PMIDs per API request (default: 20)'
    )

    return parser.parse_args()


def extract_all_pmids(pmid_str) -> List[str]:
    """Extract all PMIDs from a comma-separated string"""
    if pd.isna(pmid_str):
        return []
    pmid_str = str(pmid_str).strip('"').strip()
    pmids = [p.strip().strip('"') for p in pmid_str.split(',')]
    return [p for p in pmids if p]


def query_epmc(pmids: List[str]) -> Dict[str, dict]:
    """
    Query EuropePMC API for metadata.

    Returns dict mapping PMID -> metadata
    """
    if not pmids:
        return {}

    # Build query
    query = ' OR '.join(set(pmids))
    url = (
        'https://www.ebi.ac.uk/europepmc/webservices/rest/search'
        f'?query={query}&resultType=core&format=json&pageSize=1000'
    )

    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            print(f"    Warning: API returned status {r.status_code}")
            return {}

        data = r.json()
        results = data.get('resultList', {}).get('result', [])

        metadata = {}
        for paper in results:
            pmid = paper.get('id', '')
            if not pmid:
                continue

            # Extract authors
            authors = []
            for author in paper.get('authorList', {}).get('author', []):
                if author:
                    authors.append(author.get('fullName', ''))

            # Extract grants
            grant_ids = []
            agencies = []
            for grant in paper.get('grantsList', {}).get('grant', []):
                if grant:
                    grant_id = grant.get('grantId', '')
                    agency = grant.get('agency', '')
                    if grant_id:
                        grant_ids.append(grant_id)
                    if agency:
                        agencies.append(agency)

            metadata[pmid] = {
                'publication_date': paper.get('firstPublicationDate', ''),
                'affiliation': paper.get('affiliation', ''),
                'authors': ', '.join([a for a in authors if a]),
                'grant_ids': ', '.join([g for g in grant_ids if g]),
                'grant_agencies': ', '.join([a for a in agencies if a]),
                'num_citations': paper.get('citedByCount', 0),
                'title': paper.get('title', '')
            }

        return metadata

    except Exception as e:
        print(f"    Warning: API error - {str(e)[:50]}")
        return {}


def fetch_all_metadata(all_pmids: List[str], chunk_size: int) -> Dict[str, dict]:
    """Fetch metadata for all PMIDs in batches"""
    unique_pmids = list(set(all_pmids))
    print(f"  Fetching metadata for {len(unique_pmids)} unique PMIDs...")

    all_metadata = {}

    # Process in chunks
    for i in range(0, len(unique_pmids), chunk_size):
        chunk = unique_pmids[i:i + chunk_size]
        chunk_metadata = query_epmc(chunk)
        all_metadata.update(chunk_metadata)

        if (i + chunk_size) % 100 == 0 or i + chunk_size >= len(unique_pmids):
            print(f"    Processed {min(i + chunk_size, len(unique_pmids))}/{len(unique_pmids)} PMIDs...")

        # Rate limiting
        time.sleep(0.2)

    return all_metadata


def find_name_in_title(name: str, title: str) -> Optional[str]:
    """
    Search title for resource name and return with original capitalization.

    Returns the matched text from the title if found, None otherwise.
    """
    if not name or not title:
        return None

    name_lower = name.lower().strip()

    # Skip very short names (prone to false matches)
    if len(name_lower) < 3:
        return None

    # Try to find the name in the title (case-insensitive)
    try:
        pattern = re.compile(re.escape(name_lower), re.IGNORECASE)
        match = pattern.search(title)
        if match:
            return match.group(0)  # Returns matched text with original case
    except re.error:
        pass

    return None


def smart_title_case(name: str) -> str:
    """
    Apply smart title case: capitalize first letter of each word,
    but preserve existing uppercase letters (likely acronyms).

    Examples:
    - "mouse genome database" → "Mouse Genome Database"
    - "HMDB" → "HMDB" (preserved)
    - "genbank" → "Genbank"
    - "UniProt" → "UniProt" (preserved)
    """
    if not name:
        return name

    words = name.split()
    result = []

    for word in words:
        if not word:
            continue
        if word.isupper():
            # Preserve all-caps (likely acronym like "HMDB", "DNA")
            result.append(word)
        elif word.islower():
            # All lowercase - capitalize first letter
            result.append(word.capitalize())
        else:
            # Mixed case (like "GenBank", "UniProt") - preserve
            result.append(word)

    return ' '.join(result)


def get_best_capitalization(name: str, titles: List[str]) -> Tuple[str, bool]:
    """
    Get best capitalization for a resource name.

    1. Try to find the name in paper titles and use that capitalization
       (but only if it's not all lowercase - titles sometimes have bad caps)
    2. Fall back to smart title case

    Returns (capitalized_name, was_found_in_title)
    """
    if not name:
        return name, False

    # Try each title
    for title in titles:
        if not title:
            continue
        found = find_name_in_title(name, title)
        if found:
            # Only use title capitalization if it's not all lowercase
            # (titles sometimes have incorrect lowercase for resource names)
            if not found.islower():
                return found, True
            # If title has it lowercase, continue searching other titles
            # or fall through to smart_title_case

    # Fallback to smart title case
    return smart_title_case(name), False


def aggregate_metadata(pmid_str, metadata_dict: Dict[str, dict]) -> dict:
    """
    Aggregate metadata for a resource with multiple PMIDs.

    For resources with multiple papers:
    - publication_date: earliest date
    - affiliation: join with semicolons
    - authors: join unique authors
    - grant_ids: join unique
    - grant_agencies: join unique
    - num_citations: sum
    - titles: list of titles (for capitalization lookup)
    - paper_titles: joined string for QC column
    """
    pmids = extract_all_pmids(pmid_str)

    if not pmids:
        return {
            'publication_date': '',
            'affiliation': '',
            'authors': '',
            'grant_ids': '',
            'grant_agencies': '',
            'num_citations': '',
            'titles': [],
            'paper_titles': ''
        }

    # Collect metadata for all PMIDs
    dates = []
    affiliations = []
    all_authors = set()
    all_grants = set()
    all_agencies = set()
    total_citations = 0
    all_titles = []

    for pmid in pmids:
        meta = metadata_dict.get(pmid, {})

        date = meta.get('publication_date', '')
        if date:
            dates.append(date)

        affil = meta.get('affiliation', '')
        if affil:
            affiliations.append(affil)

        authors = meta.get('authors', '')
        if authors:
            for a in authors.split(', '):
                if a.strip():
                    all_authors.add(a.strip())

        grants = meta.get('grant_ids', '')
        if grants:
            for g in grants.split(', '):
                if g.strip():
                    all_grants.add(g.strip())

        agencies = meta.get('grant_agencies', '')
        if agencies:
            for a in agencies.split(', '):
                if a.strip():
                    all_agencies.add(a.strip())

        citations = meta.get('num_citations', 0)
        if citations:
            try:
                total_citations += int(citations)
            except (ValueError, TypeError):
                pass

        title = meta.get('title', '')
        if title:
            all_titles.append(title)

    # Aggregate
    earliest_date = min(dates) if dates else ''

    return {
        'publication_date': earliest_date,
        'affiliation': '; '.join(affiliations),
        'authors': ', '.join(sorted(all_authors)),
        'grant_ids': ', '.join(sorted(all_grants)),
        'grant_agencies': ', '.join(sorted(all_agencies)),
        'num_citations': total_citations if total_citations > 0 else '',
        'titles': all_titles,  # List for capitalization lookup
        'paper_titles': ' | '.join(all_titles)  # String for QC column
    }


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
    input_file = get_session_path(SESSION_DIR, '09_finalization', 'url_checked_resources.csv')
    output_dir = get_session_path(SESSION_DIR, '09_finalization')

    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)

    print(f"Phase 9 - Script 25: Fetch EuropePMC Metadata")
    print(f"=" * 80)
    print(f"Session: {SESSION_DIR.name}")
    print(f"Input: {input_file.relative_to(SESSION_DIR)}")
    print(f"Output directory: {output_dir.relative_to(SESSION_DIR)}")
    print(f"Chunk size: {args.chunk_size}")
    print()

    # Load input
    print("Loading URL-checked resources...")
    df = pd.read_csv(input_file)
    print(f"  Loaded {len(df)} rows")
    print()

    # Extract all PMIDs
    print("Extracting PMIDs...")
    all_pmids = []
    for pmid_str in df['ID']:
        all_pmids.extend(extract_all_pmids(pmid_str))
    print(f"  Found {len(all_pmids)} total PMIDs ({len(set(all_pmids))} unique)")
    print()

    # Fetch metadata from API
    print("Querying EuropePMC API...")
    start_time = time.time()
    metadata_dict = fetch_all_metadata(all_pmids, args.chunk_size)
    elapsed = time.time() - start_time
    print(f"  Fetched metadata for {len(metadata_dict)} PMIDs in {elapsed:.1f} seconds")
    print()

    # Apply aggregated metadata to each row
    print("Applying metadata to resources...")

    def apply_metadata(row):
        return aggregate_metadata(row['ID'], metadata_dict)

    metadata_results = df.apply(apply_metadata, axis=1)

    df['publication_date'] = metadata_results.apply(lambda x: x['publication_date'])
    df['affiliation'] = metadata_results.apply(lambda x: x['affiliation'])
    df['authors'] = metadata_results.apply(lambda x: x['authors'])
    df['grant_ids'] = metadata_results.apply(lambda x: x['grant_ids'])
    df['grant_agencies'] = metadata_results.apply(lambda x: x['grant_agencies'])
    df['num_citations'] = metadata_results.apply(lambda x: x['num_citations'])
    df['paper_titles'] = metadata_results.apply(lambda x: x['paper_titles'])

    # Apply title-aware capitalization to best_name
    print("Applying title-aware capitalization to best_name...")
    title_cap_count = 0
    smart_cap_count = 0

    def apply_capitalization(row):
        nonlocal title_cap_count, smart_cap_count

        name = row.get('best_name', '')
        if not name or pd.isna(name):
            return row

        # Get titles for this resource
        idx = row.name
        meta_result = metadata_results.iloc[idx]
        titles = meta_result.get('titles', [])

        # Apply capitalization
        new_name, from_title = get_best_capitalization(str(name), titles)

        # Only update if changed
        if new_name != name:
            row['best_name'] = new_name

            # Update modification flags
            existing_flags = row.get('name_modification_flags', '')
            if pd.isna(existing_flags):
                existing_flags = ''

            if from_title:
                new_flag = 'TITLE_CAPITALIZED'
                title_cap_count += 1
            else:
                new_flag = 'SMART_CAPITALIZED'
                smart_cap_count += 1

            if existing_flags:
                row['name_modification_flags'] = f"{existing_flags},{new_flag}"
            else:
                row['name_modification_flags'] = new_flag

        return row

    df = df.apply(apply_capitalization, axis=1)
    print(f"  Title-based capitalization: {title_cap_count}")
    print(f"  Smart title case: {smart_cap_count}")
    print()

    # Statistics
    stats = {
        'script': '25_fetch_epmc_metadata',
        'timestamp': datetime.now().isoformat(),
        'session': SESSION_DIR.name,
        'runtime_seconds': round(elapsed, 1),
        'input_rows': len(df),
        'total_pmids': len(all_pmids),
        'unique_pmids': len(set(all_pmids)),
        'metadata_fetched': len(metadata_dict),
        'fields_populated': {
            'publication_date': int((df['publication_date'] != '').sum()),
            'affiliation': int((df['affiliation'] != '').sum()),
            'authors': int((df['authors'] != '').sum()),
            'grant_ids': int((df['grant_ids'] != '').sum()),
            'grant_agencies': int((df['grant_agencies'] != '').sum()),
            'num_citations': int((df['num_citations'] != '').sum()),
            'paper_titles': int((df['paper_titles'] != '').sum())
        },
        'capitalization': {
            'title_based': title_cap_count,
            'smart_title_case': smart_cap_count,
            'total': title_cap_count + smart_cap_count
        }
    }

    print(f"  Fields populated:")
    for field, count in stats['fields_populated'].items():
        pct = count / len(df) * 100 if len(df) > 0 else 0
        print(f"    - {field}: {count} ({pct:.1f}%)")
    print()

    # Save outputs
    output_file = output_dir / 'metadata_enriched_resources.csv'
    df.to_csv(output_file, index=False)
    print(f"Saved metadata-enriched resources to: {output_file.relative_to(SESSION_DIR)}")

    # Save raw metadata for reference
    metadata_file = output_dir / 'epmc_metadata.csv'
    metadata_list = [
        {'pmid': pmid, **meta}
        for pmid, meta in metadata_dict.items()
    ]
    pd.DataFrame(metadata_list).to_csv(metadata_file, index=False)
    print(f"Saved raw EPMC metadata to: {metadata_file.relative_to(SESSION_DIR)}")

    stats_file = output_dir / 'script_25_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved statistics to: {stats_file.relative_to(SESSION_DIR)}")

    print()
    print(f"Done! Enriched {len(df)} resources with metadata from {len(metadata_dict)} PMIDs")


if __name__ == '__main__':
    main()
