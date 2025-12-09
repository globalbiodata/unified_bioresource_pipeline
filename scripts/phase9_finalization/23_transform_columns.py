#!/usr/bin/env python3
"""
Script 23: Transform Columns

Purpose: Map columns from pipeline format to target inventory format
         matching data/final_inventory_2022.csv structure

         Also performs data quality sanitization:
         - Recalculates article_count from PMID count
         - Cleans pipe-separated names
         - Removes HTML tags
         - Sanitizes non-ASCII characters
         - Validates minimum name length
         - Populates empty names from URL domain
         - Auto-capitalizes short acronyms
         - Flags all modifications for review

Authors: AI Assistant
Date: 2025-11-27
Updated: 2025-11-28 (added data quality sanitization)
"""

import argparse
import json
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd

# Add lib imports
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from lib.session_utils import get_session_path, validate_session_dir


class Args(NamedTuple):
    """Command-line arguments"""
    session_dir: Path
    profile: str


def get_args() -> Args:
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Transform columns to target inventory format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 23_transform_columns.py --session-dir 2025-12-04-111420-z381s
  python 23_transform_columns.py --session-dir 2025-12-04-111420-z381s --profile balanced
        """
    )

    parser.add_argument(
        '--session-dir',
        type=str,
        required=True,
        help='Session directory path'
    )
    parser.add_argument(
        '--profile',
        type=str,
        default='aggressive',
        choices=['conservative', 'balanced', 'aggressive'],
        help='Deduplication profile to use (default: aggressive)'
    )

    args = parser.parse_args()

    return Args(
        session_dir=Path(args.session_dir).resolve(),
        profile=args.profile
    )


# ============================================================================
# Data Quality Sanitization Functions
# ============================================================================

def calculate_article_count(id_str) -> int:
    """Count PMIDs in comma-separated ID string."""
    if pd.isna(id_str) or str(id_str).strip() == '':
        return 0
    pmids = [p.strip() for p in str(id_str).split(',') if p.strip()]
    return len(pmids)


def clean_pipe_names(name: str) -> Tuple[str, bool]:
    """
    Take first name when pipe-separated.
    Returns: (cleaned_name, was_modified)
    """
    if pd.isna(name) or not name:
        return name, False
    name_str = str(name)
    if '|' in name_str:
        return name_str.split('|')[0].strip(), True
    return name_str, False


def remove_html_tags(name: str) -> Tuple[str, bool]:
    """
    Remove HTML tags from name.
    Returns: (cleaned_name, was_modified)
    """
    if pd.isna(name) or not name:
        return name, False
    name_str = str(name)
    original = name_str

    # Remove complete HTML tags
    clean = re.sub(r'<[^>]+>', '', name_str)

    # Remove partial/broken tags (handles cases like "i>econtour</i" or "plasmir</i")
    # Remove opening partial tags like "i>" at start
    clean = re.sub(r'^[a-zA-Z]+>', '', clean)
    # Remove closing partial tags like "</i" or "/i" at end
    clean = re.sub(r'</[a-zA-Z]+$', '', clean)
    clean = re.sub(r'/[a-zA-Z]+$', '', clean)

    # Remove any remaining angle brackets
    clean = re.sub(r'[<>]', '', clean)
    clean = clean.strip()

    modified = clean != original
    return clean, modified


def sanitize_characters(name: str) -> Tuple[str, bool]:
    """
    Replace non-ASCII with ASCII equivalents.
    Returns: (sanitized_name, was_modified)
    """
    if pd.isna(name) or not name:
        return name, False

    name_str = str(name)
    original = name_str

    # Specific character replacements
    replacements = {
        '\u03bc': 'mu',     # Greek mu (μ)
        '\u00ae': '(R)',    # Registered (®)
        '\u2122': '(TM)',   # Trademark (™)
        '\u2018': "'",      # Left single quote
        '\u2019': "'",      # Right single quote
        '\u201c': '"',      # Left double quote
        '\u201d': '"',      # Right double quote
        '\u2013': '-',      # En dash
        '\u2014': '-',      # Em dash
        '\u00b5': 'mu',     # Micro sign (µ)
        '\u00f8': 'o',      # Latin small letter o with stroke (ø)
        '\u00d8': 'O',      # Latin capital letter O with stroke (Ø)
        '\u00e9': 'e',      # Latin small letter e with acute (é)
        '\u00e8': 'e',      # Latin small letter e with grave (è)
        '\u00ea': 'e',      # Latin small letter e with circumflex (ê)
        '\u00eb': 'e',      # Latin small letter e with diaeresis (ë)
        '\u00e0': 'a',      # Latin small letter a with grave (à)
        '\u00e1': 'a',      # Latin small letter a with acute (á)
        '\u00e2': 'a',      # Latin small letter a with circumflex (â)
        '\u00e4': 'a',      # Latin small letter a with diaeresis (ä)
        '\u00fc': 'u',      # Latin small letter u with diaeresis (ü)
        '\u00f1': 'n',      # Latin small letter n with tilde (ñ)
    }

    for char, replacement in replacements.items():
        name_str = name_str.replace(char, replacement)

    # Normalize remaining unicode to ASCII
    name_str = unicodedata.normalize('NFKD', name_str)
    name_str = name_str.encode('ascii', 'ignore').decode('ascii')
    name_str = name_str.strip()

    modified = name_str != original
    return name_str, modified


def validate_name_length(name: str, min_length: int = 3) -> Tuple[str, bool]:
    """
    Flag names below minimum length.
    Returns: (name, needs_review)
    """
    if pd.isna(name) or not name:
        return name, True  # Empty names need review
    if len(str(name).strip()) < min_length:
        return name, True  # Short names need review
    return name, False


def extract_name_from_url(url: str) -> str:
    """
    Smart extraction of resource name from URL.

    Priority:
    1. CamelCase segment in path (e.g., CoralTBase)
    2. ALL_CAPS segment in path (e.g., START, NPASS)
    3. Last meaningful path segment
    4. Subdomain if specific (e.g., sorghum.riken.jp -> sorghum)
    5. First domain part as fallback
    """
    if pd.isna(url) or not url:
        return ''

    # Noise segments to skip
    SKIP_SEGMENTS = {
        'www', 'db', 'data', 'download', 'downloads', 'index', 'home',
        'browse', 'search', 'view', 'dataset', 'datasets', 'ontologies',
        'packages', 'fsl', 'fslwiki', 'php', 'html', 'aspx', 'jsp',
        'species', 'gene', 'genes', 'protein', 'proteins', 'genome',
        'tool', 'tools', 'resource', 'resources', 'about', 'help',
        'api', 'docs', 'documentation', 'wiki', 'portal'
    }

    try:
        parsed = urlparse(str(url))

        # Extract path segments (filter out empty and file extensions)
        path = parsed.path.rstrip('/')
        segments = [s for s in path.split('/') if s and not s.startswith('~')]

        # Filter out query strings and file extensions
        clean_segments = []
        for seg in segments:
            # Remove query params
            seg = seg.split('?')[0].split('=')[0]
            # Remove file extensions
            seg = re.sub(r'\.(php|html|aspx|jsp|htm)$', '', seg, flags=re.IGNORECASE)
            # Skip noise words
            if seg.lower() not in SKIP_SEGMENTS and len(seg) > 1:
                clean_segments.append(seg)

        # Strategy 1: Look for CamelCase in path (e.g., CoralTBase)
        for seg in clean_segments:
            # Check if has mixed case (not all upper, not all lower)
            if (re.search(r'[a-z]', seg) and re.search(r'[A-Z]', seg)):
                return seg

        # Strategy 2: Look for ALL_CAPS in path (e.g., START, NPASS)
        for seg in clean_segments:
            if seg.isupper() and len(seg) >= 3:
                return seg

        # Strategy 3: Use last meaningful path segment
        if clean_segments:
            last_seg = clean_segments[-1]
            if len(last_seg) >= 3:
                return last_seg

        # Strategy 4: Check subdomain (e.g., sorghum.riken.jp)
        domain = parsed.netloc.replace('www.', '')
        parts = domain.split('.')

        # If first part looks like a resource name (not generic)
        if len(parts) >= 2:
            subdomain = parts[0]
            # Skip generic subdomains
            if subdomain.lower() not in SKIP_SEGMENTS and len(subdomain) >= 3:
                return subdomain

        # Strategy 5: Fallback to first domain part
        if parts:
            name = parts[0]
            name = re.sub(r'[^a-zA-Z0-9]', '', name)
            return name if name else ''

    except Exception:
        pass

    return ''


def auto_capitalize(name: str) -> Tuple[str, bool]:
    """
    Auto-capitalize short (<=6 char) all-lowercase alphabetic names.
    These are likely acronyms.
    Returns: (capitalized_name, was_modified)
    """
    if pd.isna(name) or not name:
        return name, False

    name_str = str(name).strip()

    # Only process if: all lowercase, all alphabetic, 6 chars or less
    if (name_str.islower() and
        name_str.isalpha() and
        len(name_str) <= 6):
        return name_str.upper(), True

    return name_str, False


def name_appears_in_url(name: str, url: str, min_match_length: int = 5) -> bool:
    """
    Check if name (or significant part) appears in URL.
    Used to detect ASCII conversion mismatches.

    More strict matching - requires the core name (first meaningful word) to appear.
    Returns True if name appears in URL (case-insensitive).
    """
    if not name or not url:
        return False

    name_lower = str(name).lower()
    url_lower = str(url).lower()

    # Remove common suffixes to get core name
    core_name = re.sub(r'[-_]?(db|base|database|server|portal|tool|hub)$', '', name_lower, flags=re.IGNORECASE)
    core_name = core_name.strip('-_')

    # Direct match of full name
    if name_lower in url_lower:
        return True

    # Direct match of core name (at least 3 chars)
    if len(core_name) >= 3 and core_name in url_lower:
        return True

    # Check if a significant portion of the name appears (first 5+ chars)
    if len(name_lower) >= min_match_length:
        prefix = name_lower[:min_match_length]
        if prefix.isalnum() and prefix in url_lower:
            return True

    return False


def extract_subdomain(url: str) -> str:
    """
    Extract meaningful subdomain from URL for disambiguation.
    E.g., 'breastcancer.gxb.io' -> 'breastcancer'

    Returns subdomain if specific, empty string otherwise.
    """
    if pd.isna(url) or not url:
        return ''

    SKIP_SUBDOMAINS = {'www', 'api', 'data', 'db', 'portal', 'web', 'app', 'dev'}

    try:
        parsed = urlparse(str(url))
        domain = parsed.netloc
        parts = domain.split('.')

        # Need at least subdomain.domain.tld
        if len(parts) >= 3:
            subdomain = parts[0]
            if subdomain.lower() not in SKIP_SUBDOMAINS and len(subdomain) >= 3:
                return subdomain
    except Exception:
        pass

    return ''


def sanitize_name(name: str, url: str) -> Tuple[str, str, List[str]]:
    """
    Apply all sanitization steps to a name.

    Returns:
        Tuple of (final_name, original_name, list_of_flags)
    """
    original = name if pd.notna(name) else ''
    flags = []
    current = original

    # Step 1: Clean pipe-separated names
    current, modified = clean_pipe_names(current)
    if modified:
        flags.append('PIPE_CLEANED')

    # Step 2: Remove HTML tags
    current, modified = remove_html_tags(current)
    if modified:
        flags.append('HTML_REMOVED')

    # Step 3: Sanitize non-ASCII characters
    current, modified = sanitize_characters(current)
    if modified:
        flags.append('CHARS_SANITIZED')
        # Step 3b: Check for name-URL mismatch after sanitization
        # If sanitized name doesn't appear in URL (4+ char match), recover from URL
        if not name_appears_in_url(current, url, min_match_length=4):
            url_name = extract_name_from_url(url)
            if url_name and len(url_name) >= 3:
                # Replace sanitized name with URL-extracted name
                current = url_name
                flags.append('NAME_RECOVERED_FROM_URL')

    # Step 4: Check if empty, try to populate from URL
    if pd.isna(current) or not current or str(current).strip() == '':
        url_name = extract_name_from_url(url)
        if url_name:
            current = url_name
            flags.append('POPULATED_FROM_URL')
        else:
            current = '[No Name Extracted]'
            flags.append('POPULATED_FROM_URL')

    # Step 5: Handle single-letter names (replace with URL name)
    # Only for single character names (Option B: keep 2-letter names)
    current_stripped = str(current).strip() if current else ''
    if len(current_stripped) == 1:
        url_name = extract_name_from_url(url)
        if url_name and len(url_name) > 1:
            current = url_name
            flags.append('SHORT_NAME_REPLACED')
        else:
            # No URL or couldn't extract - keep original but flag
            flags.append('SHORT_NAME_NO_URL')

    # Step 6: Validate length (flag 2-char names for review, but don't replace)
    current, needs_review = validate_name_length(current)
    if needs_review and 'SHORT_NAME_REPLACED' not in flags and 'SHORT_NAME_NO_URL' not in flags:
        flags.append('SHORT_NAME_FLAGGED')

    # Step 7: Auto-capitalize acronyms (<=6 char lowercase alphabetic)
    current, modified = auto_capitalize(current)
    if modified:
        flags.append('CAPITALIZED')

    return current, original, flags


def disambiguate_duplicate_names(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Disambiguate resources with the same best_name by adding subdomain qualifiers.

    E.g., Multiple resources named 'GXB' with different URLs:
        - breastcancer.gxb.io -> 'GXB (breastcancer)'
        - copd.gxb.io -> 'GXB (copd)'

    Returns:
        Tuple of (modified_df, count_disambiguated)
    """
    df = df.copy()
    disambiguated_count = 0

    # Find duplicate names
    name_counts = df['best_name'].value_counts()
    duplicate_names = name_counts[name_counts > 1].index.tolist()

    if not duplicate_names:
        return df, 0

    print(f"  Disambiguating {len(duplicate_names)} duplicate names...")

    for dup_name in duplicate_names:
        # Get rows with this name
        mask = df['best_name'] == dup_name
        dup_rows = df[mask]

        if len(dup_rows) <= 1:
            continue

        # Try to disambiguate using subdomain
        for idx, row in dup_rows.iterrows():
            url = row.get('extracted_url', '')
            subdomain = extract_subdomain(url)

            if subdomain and subdomain.lower() != dup_name.lower():
                # Add subdomain qualifier
                new_name = f"{dup_name} ({subdomain})"
                df.at[idx, 'best_name'] = new_name

                # Update flags
                existing_flags = row.get('name_modification_flags', '')
                new_flags = existing_flags + ',DISAMBIGUATED' if existing_flags else 'DISAMBIGUATED'
                df.at[idx, 'name_modification_flags'] = new_flags

                disambiguated_count += 1

    return df, disambiguated_count


def transform_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transform columns from pipeline format to inventory format.

    Source columns (from set_c_final.csv):
        - pmid -> ID
        - primary_entity_short -> best_common
        - primary_entity_long -> best_full
        - ner_confidence -> best_name_prob, best_common_prob, best_full_prob
        - resource_url -> extracted_url
        - article_count -> recalculated from pmid count

    Target columns (from final_inventory_2022.csv):
        ID, best_name, best_name_prob, best_common, best_common_prob,
        best_full, best_full_prob, article_count, extracted_url,
        extracted_url_status, extracted_url_country, extracted_url_coordinates,
        wayback_url, publication_date, affiliation, authors, grant_ids,
        grant_agencies, num_citations, affiliation_countries,
        best_name_original, name_modification_flags

    Returns:
        Tuple of (transformed_df, sanitization_stats)
    """
    out_df = pd.DataFrame()

    # Direct mappings
    out_df['ID'] = df['pmid']
    out_df['extracted_url'] = df['resource_url']

    # Recalculate article_count from PMID count (FIX)
    out_df['article_count'] = df['pmid'].apply(calculate_article_count)

    # Entity name mappings (raw, before sanitization)
    out_df['best_common'] = df['primary_entity_short']
    out_df['best_full'] = df['primary_entity_long']

    # best_name: prefer short if available, else use long
    raw_best_name = df['primary_entity_short'].fillna(df['primary_entity_long'])

    # Apply sanitization to best_name
    print("  Applying name sanitization...")
    sanitization_results = []
    for idx, row in df.iterrows():
        name = raw_best_name.loc[idx] if idx in raw_best_name.index else ''
        url = row.get('resource_url', '')
        final_name, original, flags = sanitize_name(name, url)
        sanitization_results.append({
            'final_name': final_name,
            'original': original,
            'flags': ','.join(flags) if flags else ''
        })

    # Apply sanitized names
    out_df['best_name'] = [r['final_name'] for r in sanitization_results]
    out_df['best_name_original'] = [r['original'] for r in sanitization_results]
    out_df['name_modification_flags'] = [r['flags'] for r in sanitization_results]

    # Disambiguate duplicate names using subdomain
    print("  Applying duplicate name disambiguation...")
    out_df, disambiguated_count = disambiguate_duplicate_names(out_df)

    # Confidence scores - use ner_confidence for all
    confidence = df['ner_confidence'].fillna(0.0)
    out_df['best_name_prob'] = confidence
    out_df['best_common_prob'] = confidence
    out_df['best_full_prob'] = confidence

    # Placeholder columns (to be filled by later scripts)
    out_df['extracted_url_status'] = ''
    out_df['extracted_url_country'] = ''
    out_df['extracted_url_coordinates'] = ''
    out_df['wayback_url'] = ''
    out_df['publication_date'] = ''
    out_df['affiliation'] = ''
    out_df['authors'] = ''
    out_df['grant_ids'] = ''
    out_df['grant_agencies'] = ''
    out_df['num_citations'] = ''
    out_df['affiliation_countries'] = ''

    # Reorder columns to match target format (with new columns at end)
    target_columns = [
        'ID', 'best_name', 'best_name_prob', 'best_common', 'best_common_prob',
        'best_full', 'best_full_prob', 'article_count', 'extracted_url',
        'extracted_url_status', 'extracted_url_country', 'extracted_url_coordinates',
        'wayback_url', 'publication_date', 'affiliation', 'authors', 'grant_ids',
        'grant_agencies', 'num_citations', 'affiliation_countries',
        'best_name_original', 'name_modification_flags'
    ]

    out_df = out_df[target_columns]

    # Calculate sanitization statistics
    # Re-count flags from out_df (includes DISAMBIGUATED added later)
    all_flags = out_df['name_modification_flags'].dropna().tolist()
    flag_counts = {}
    for flags_str in all_flags:
        if flags_str:
            for flag in str(flags_str).split(','):
                flag = flag.strip()
                if flag:
                    flag_counts[flag] = flag_counts.get(flag, 0) + 1

    sanitization_stats = {
        'total_rows': len(out_df),
        'rows_modified': len([f for f in all_flags if f]),
        'flag_counts': flag_counts,
        'disambiguated_count': disambiguated_count
    }

    return out_df, sanitization_stats


def main() -> None:
    """Main function"""
    args = get_args()

    # Validate session directory
    SESSION_DIR = args.session_dir

    if not SESSION_DIR.exists():
        print(f"ERROR: Session directory not found: {SESSION_DIR}")
        sys.exit(1)

    try:
        validate_session_dir(SESSION_DIR, required_phases=['07_deduplication'])
    except ValueError as e:
        print(f"ERROR: Invalid session directory: {e}")
        sys.exit(1)

    # Input/output paths
    input_file = get_session_path(SESSION_DIR, f'07_deduplication/{args.profile}', 'set_c_final.csv')
    output_dir = get_session_path(SESSION_DIR, '09_finalization')
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)

    print(f"Phase 9 - Script 23: Transform Columns")
    print(f"=" * 80)
    print(f"Session: {SESSION_DIR.name}")
    print(f"Profile: {args.profile}")
    print(f"Input: {input_file.relative_to(SESSION_DIR)}")
    print(f"Output directory: {output_dir.relative_to(SESSION_DIR)}")
    print()

    # Load input
    print("Loading filtered resources...")
    df = pd.read_csv(input_file)
    print(f"  Loaded {len(df)} rows")
    print()

    # Transform columns with sanitization
    print("Transforming columns to target format...")
    transformed_df, sanitization_stats = transform_columns(df)

    # Report column mapping results
    print(f"  Column mapping complete:")
    print(f"    - ID: {transformed_df['ID'].notna().sum()} values")
    print(f"    - best_name: {transformed_df['best_name'].notna().sum()} values")
    print(f"    - best_common: {transformed_df['best_common'].notna().sum()} values")
    print(f"    - best_full: {transformed_df['best_full'].notna().sum()} values")
    print(f"    - extracted_url: {(transformed_df['extracted_url'].notna() & (transformed_df['extracted_url'] != '')).sum()} values")
    print()

    # Report sanitization results
    print(f"  Name sanitization results:")
    print(f"    - Rows modified: {sanitization_stats['rows_modified']}")
    for flag, count in sorted(sanitization_stats['flag_counts'].items()):
        print(f"    - {flag}: {count}")
    print()

    # Statistics
    stats = {
        'script': '23_transform_columns',
        'timestamp': datetime.now().isoformat(),
        'session': SESSION_DIR.name,
        'profile': args.profile,
        'input_rows': len(df),
        'output_rows': len(transformed_df),
        'columns_mapped': {
            'ID': int(transformed_df['ID'].notna().sum()),
            'best_name': int(transformed_df['best_name'].notna().sum()),
            'best_common': int(transformed_df['best_common'].notna().sum()),
            'best_full': int(transformed_df['best_full'].notna().sum()),
            'extracted_url': int((transformed_df['extracted_url'].notna() & (transformed_df['extracted_url'] != '')).sum()),
            'article_count': int(transformed_df['article_count'].notna().sum())
        },
        'sanitization': sanitization_stats,
        'placeholder_columns': [
            'extracted_url_status', 'extracted_url_country', 'extracted_url_coordinates',
            'wayback_url', 'publication_date', 'affiliation', 'authors', 'grant_ids',
            'grant_agencies', 'num_citations', 'affiliation_countries'
        ]
    }

    # Save outputs
    output_file = output_dir / 'transformed_resources.csv'
    transformed_df.to_csv(output_file, index=False)
    print(f"Saved transformed resources to: {output_file.relative_to(SESSION_DIR)}")

    stats_file = output_dir / 'script_23_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved statistics to: {stats_file.relative_to(SESSION_DIR)}")

    # Save a separate file with only modified rows for review
    modified_df = transformed_df[transformed_df['name_modification_flags'] != ''].copy()
    if len(modified_df) > 0:
        review_file = output_dir / 'names_for_review.csv'
        review_cols = ['ID', 'best_name', 'best_name_original', 'name_modification_flags', 'extracted_url']
        modified_df[review_cols].to_csv(review_file, index=False)
        print(f"Saved modified names for review to: {review_file.relative_to(SESSION_DIR)}")

    print()
    print(f"Done! Transformed {len(df)} rows with {len(transformed_df.columns)} columns")
    print(f"  ({sanitization_stats['rows_modified']} rows with name modifications flagged for review)")


if __name__ == '__main__':
    main()
