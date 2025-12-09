#!/usr/bin/env python3
"""
Script 26: Process Countries

Purpose: Extract country codes from affiliation text and standardize
         URL country codes to ISO 3166-1 format

Authors: AI Assistant
Date: 2025-11-27
Updated: 2025-12-05 (Session-based refactor)
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, NamedTuple, Set

import pandas as pd

try:
    import pycountry
except ImportError:
    print("Error: pycountry package required. Install with: pip install pycountry")
    sys.exit(1)

# Add lib imports
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from lib.session_utils import get_session_path, validate_session_dir


class Args(NamedTuple):
    """Command-line arguments"""
    session_dir: Path
    country_format: str


def get_args() -> Args:
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Extract country codes from affiliations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 26_process_countries.py --session-dir 2025-12-04-111420-z381s
  python 26_process_countries.py --session-dir 2025-12-04-111420-z381s --format alpha-2
        """
    )

    parser.add_argument(
        '--session-dir',
        type=str,
        required=True,
        help='Session directory path'
    )
    parser.add_argument(
        '--format',
        choices=['alpha-2', 'alpha-3', 'full', 'numeric'],
        default='full',
        help='Country code output format (default: full name)'
    )

    args = parser.parse_args()

    return Args(
        session_dir=Path(args.session_dir).resolve(),
        country_format=args.format
    )


def extract_countries(text: str, country_format: str) -> str:
    """
    Extract country names/codes from text.

    Searches for country names and alpha-3 codes in the text,
    returns formatted list of unique countries found.
    """
    if not text or pd.isna(text) or str(text).strip() == '':
        return ''

    text = str(text)
    found_countries: Set[str] = set()

    for country in pycountry.countries:
        # Search for full name and alpha-3 code
        for search_term in [country.name, country.alpha_3]:
            # Word boundary matching to avoid partial matches
            pattern = fr'\b{re.escape(search_term)}\b'
            if re.search(pattern, text, re.IGNORECASE):
                # Format output based on preference
                if country_format == 'alpha-2':
                    found_countries.add(country.alpha_2)
                elif country_format == 'alpha-3':
                    found_countries.add(country.alpha_3)
                elif country_format == 'numeric':
                    found_countries.add(country.numeric)
                else:  # 'full'
                    found_countries.add(country.name)
                break  # Found this country, move to next

    # Handle common variations
    common_mappings = {
        'USA': 'United States',
        'UK': 'United Kingdom',
        'PRC': 'China',
        'ROC': 'Taiwan',
        'ROK': 'Korea, Republic of',
        'DPRK': "Korea, Democratic People's Republic of",
    }

    for abbrev, full_name in common_mappings.items():
        if re.search(fr'\b{abbrev}\b', text):
            try:
                country = pycountry.countries.search_fuzzy(full_name)[0]
                if country_format == 'alpha-2':
                    found_countries.add(country.alpha_2)
                elif country_format == 'alpha-3':
                    found_countries.add(country.alpha_3)
                elif country_format == 'numeric':
                    found_countries.add(country.numeric)
                else:
                    found_countries.add(country.name)
            except LookupError:
                pass

    return ', '.join(sorted(found_countries))


def standardize_url_country(country_code: str, country_format: str) -> str:
    """
    Standardize URL country code (usually alpha-2) to desired format.
    """
    if not country_code or pd.isna(country_code) or str(country_code).strip() == '':
        return ''

    country_code = str(country_code).strip().upper()

    try:
        # Try alpha-2 lookup first (most common from IP geolocation)
        if len(country_code) == 2:
            country = pycountry.countries.get(alpha_2=country_code)
        elif len(country_code) == 3:
            country = pycountry.countries.get(alpha_3=country_code)
        else:
            # Try fuzzy search
            results = pycountry.countries.search_fuzzy(country_code)
            country = results[0] if results else None

        if country:
            if country_format == 'alpha-2':
                return country.alpha_2
            elif country_format == 'alpha-3':
                return country.alpha_3
            elif country_format == 'numeric':
                return country.numeric
            else:
                return country.name

    except (LookupError, AttributeError):
        pass

    return country_code  # Return original if can't parse


def main() -> None:
    """Main function"""
    args = get_args()

    # Validate session directory
    SESSION_DIR = args.session_dir

    if not SESSION_DIR.exists():
        print(f"ERROR: Session directory not found: {SESSION_DIR}")
        sys.exit(1)

    try:
        validate_session_dir(SESSION_DIR, required_phases=['09_finalization'])
    except ValueError as e:
        print(f"ERROR: Invalid session directory: {e}")
        sys.exit(1)

    # Input/output paths
    input_file = get_session_path(SESSION_DIR, '09_finalization', 'metadata_enriched_resources.csv')
    output_dir = get_session_path(SESSION_DIR, '09_finalization')

    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        print(f"  This script requires Script 25 (fetch_epmc_metadata) to run first.")
        sys.exit(1)

    print(f"Phase 9 - Script 26: Process Countries")
    print(f"=" * 80)
    print(f"Session: {SESSION_DIR.name}")
    print(f"Input: {input_file.relative_to(SESSION_DIR)}")
    print(f"Output directory: {output_dir.relative_to(SESSION_DIR)}")
    print(f"Country format: {args.country_format}")
    print()

    # Load input
    print("Loading metadata-enriched resources...")
    df = pd.read_csv(input_file)
    print(f"  Loaded {len(df)} rows")
    print()

    # Process affiliation countries
    print("Extracting countries from affiliations...")
    df['affiliation_countries'] = df['affiliation'].apply(
        lambda x: extract_countries(x, args.country_format)
    )

    affil_countries_count = (df['affiliation_countries'] != '').sum()
    print(f"  Extracted countries for {affil_countries_count} rows")

    # Standardize URL country format
    print("Standardizing URL country codes...")
    df['extracted_url_country'] = df['extracted_url_country'].apply(
        lambda x: standardize_url_country(x, args.country_format)
    )

    url_countries_count = (df['extracted_url_country'] != '').sum()
    print(f"  Standardized {url_countries_count} URL countries")
    print()

    # Count unique countries found
    all_affil_countries = set()
    for countries in df['affiliation_countries'].dropna():
        if countries:
            for c in countries.split(', '):
                if c.strip():
                    all_affil_countries.add(c.strip())

    all_url_countries = set()
    for country in df['extracted_url_country'].dropna():
        if country:
            all_url_countries.add(country.strip())

    # Statistics
    stats = {
        'script': '26_process_countries',
        'timestamp': datetime.now().isoformat(),
        'session': SESSION_DIR.name,
        'input_rows': len(df),
        'country_format': args.country_format,
        'affiliation_countries': {
            'rows_with_countries': int(affil_countries_count),
            'unique_countries': len(all_affil_countries),
            'country_list': sorted(list(all_affil_countries))[:20]  # Top 20
        },
        'url_countries': {
            'rows_with_countries': int(url_countries_count),
            'unique_countries': len(all_url_countries),
            'country_list': sorted(list(all_url_countries))
        }
    }

    print(f"  Summary:")
    print(f"    - Affiliation countries: {len(all_affil_countries)} unique")
    print(f"    - URL countries: {len(all_url_countries)} unique")
    print()

    # Save outputs
    output_file = output_dir / 'countries_processed_resources.csv'
    df.to_csv(output_file, index=False)
    print(f"Saved countries-processed resources to: {output_file.relative_to(SESSION_DIR)}")

    stats_file = output_dir / 'script_26_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved statistics to: {stats_file.relative_to(SESSION_DIR)}")

    print()
    print(f"Done! Processed countries for {len(df)} resources")


if __name__ == '__main__':
    main()
