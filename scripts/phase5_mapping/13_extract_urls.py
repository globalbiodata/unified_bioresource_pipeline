#!/usr/bin/env python3
"""
Phase 5: Extract URLs from Abstracts and Identify Bioresource Websites

Extracts URLs from paper abstracts and scores them to identify primary bioresource URLs.

Adds 4 columns to the dataset:
1. all_urls - All URLs detected (pipe-separated)
2. resource_url - Primary bioresource URL (filtered, scored)
3. has_resource_url - Boolean (True if resource_url exists)
4. url_context - Text surrounding primary URL (for validation)

Usage:
    # Session-based (PREFERRED):
    python 13_extract_urls.py --session-dir results/2025-12-04-143052-abc12

    # Legacy mode (auto-detect):
    python 13_extract_urls.py --auto

    # Custom paths (legacy):
    python 13_extract_urls.py \
        --input-file data/union_papers_with_primary_resources.csv \
        --output-file data/union_papers_with_urls.csv

Session Mode:
    When --session-dir is provided:
    - Reads from: {session_dir}/05_mapping/union_papers_with_primary_resources.csv
    - Outputs to: {session_dir}/05_mapping/union_papers_with_urls.csv (NEW FILE)

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-11-18
Updated: 2025-12-04 (added session-dir support, argparse, NEW file output)
"""

import argparse
import pandas as pd
import re
import sys
import json
from pathlib import Path
from urllib.parse import urlparse
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
LEGACY_INPUT_FILE = PROJECT_ROOT / 'pipeline_synthesis_2025-11-18/data/union_papers_with_primary_resources.csv'
LEGACY_OUTPUT_FILE = PROJECT_ROOT / 'pipeline_synthesis_2025-11-18/data/union_papers_with_urls.csv'

# ============================================================================
# CONFIGURATION
# ============================================================================


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Phase 5: Extract URLs from abstracts and identify bioresource websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Session-based mode (PREFERRED):
  python 13_extract_urls.py --session-dir results/2025-12-04-143052-abc12

  # Legacy mode (auto-detect):
  python 13_extract_urls.py --auto

  # Custom paths (legacy):
  python 13_extract_urls.py \\
      --input-file data/union_papers_with_primary_resources.csv \\
      --output-file data/union_papers_with_urls.csv
        """
    )

    # Session mode arguments
    parser.add_argument("--session-dir", type=Path,
                        help="Session directory path (e.g., results/2025-12-04-143052-abc12)")

    # Legacy mode arguments
    parser.add_argument("--input-file", type=Path,
                        help="Path to input CSV with primary resources")
    parser.add_argument("--output-file", type=Path,
                        help="Path to output CSV with URLs (NEW FILE, not in-place)")
    parser.add_argument("--auto", action="store_true",
                        help="Auto-detect files in legacy paths")

    return parser.parse_args()


# Domains to exclude (not bioresources)
EXCLUDE_DOMAINS = [
    # Code repositories
    'github.com', 'gitlab.com', 'bitbucket.org', 'sourceforge.net',

    # DOI and reference systems
    'doi.org', 'dx.doi.org', 'pubmed', 'europepmc.org', 'ncbi.nlm.nih.gov/pubmed',
    'sciencedirect.com', 'springer.com', 'nature.com', 'cell.com', 'wiley.com',
    'plos.org', 'frontiersin.org', 'mdpi.com', 'biorxiv.org', 'arxiv.org',

    # Social media and professional networks
    'twitter.com', 'facebook.com', 'linkedin.com', 'instagram.com',
    'researchgate.net', 'academia.edu', 'orcid.org',

    # File sharing and cloud storage
    'dropbox.com', 'google.com/drive', 'drive.google.com', 'onedrive.com',
    'figshare.com', 'zenodo.org', 'dryad.org', 'mendeley.com',

    # Video/media platforms
    'youtube.com', 'vimeo.com',

    # Generic
    'wikipedia.org', 'google.com', 'yahoo.com', 'bing.com',

    # Email domains (common false positives)
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
]

# Keywords that suggest bioresource in domain name
RESOURCE_KEYWORDS = [
    'database', 'db', 'bio', 'genom', 'protein', 'gene',
    'tool', 'portal', 'server', 'resource', 'data',
    'omics', 'seq', 'transcriptom', 'proteom', 'metabolom',
    'repository', 'archive', 'catalog', 'registry',
]

# Academic/government TLDs (boost confidence)
ACADEMIC_TLDS = ['.edu', '.gov', '.ac.uk', '.ac.jp', '.ac.cn', '.org']

# Context phrases that suggest resource URL nearby
CONTEXT_PHRASES = [
    'available at', 'accessible at', 'can be accessed',
    'hosted at', 'found at', 'visit', 'web server',
    'online at', 'website:', 'freely available',
    'public resource', 'can be downloaded', 'deposited at',
    'available from', 'accessible through', 'available via',
]

# ============================================================================
# URL EXTRACTION FUNCTIONS
# ============================================================================

def extract_urls(text):
    """Extract all URLs from text using comprehensive patterns"""
    if pd.isna(text):
        return []

    text = str(text)
    urls = []

    # Pattern 1: Full URLs with protocol
    pattern1 = r'https?://[^\s<>"\',)]+(?:[^\s<>"\',.]|(?<=/))'
    urls.extend(re.findall(pattern1, text, re.IGNORECASE))

    # Pattern 2: URLs starting with www
    pattern2 = r'www\.[a-zA-Z0-9][-a-zA-Z0-9.]*\.[a-zA-Z]{2,}(?:/[^\s<>"\',)]*)?'
    urls.extend(re.findall(pattern2, text, re.IGNORECASE))

    # Pattern 3: FTP URLs
    pattern3 = r'ftp://[^\s<>"\',)]+(?:[^\s<>"\',.]|(?<=/))'
    urls.extend(re.findall(pattern3, text, re.IGNORECASE))

    # Pattern 4: Domain-like patterns (more aggressive)
    # Only if they appear in context of "available at", "visit", etc.
    for phrase in CONTEXT_PHRASES:
        if phrase.lower() in text.lower():
            # Look for domain patterns after these phrases
            phrase_pattern = re.escape(phrase) + r'\s+([a-zA-Z0-9][-a-zA-Z0-9.]*\.[a-zA-Z]{2,}(?:/[^\s<>"\',)]*)?)'
            urls.extend(re.findall(phrase_pattern, text, re.IGNORECASE))

    # Clean URLs
    cleaned_urls = []
    for url in urls:
        # Remove trailing punctuation
        url = re.sub(r'[.,;:)\]]+$', '', url)

        # Remove markdown artifacts
        url = url.strip('[](){}')

        # Skip if it's likely an email address
        if '@' in url and not url.startswith('http'):
            continue

        # Skip very short URLs (likely false positives)
        if len(url) < 8:
            continue

        # Add http:// if missing and starts with www
        if url.startswith('www.') and not url.startswith('http'):
            url = 'http://' + url

        cleaned_urls.append(url)

    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in cleaned_urls:
        url_lower = url.lower()
        if url_lower not in seen:
            seen.add(url_lower)
            unique_urls.append(url)

    return unique_urls

def is_excluded_domain(url):
    """Check if URL is in exclusion list"""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower() if parsed.netloc else url.lower()

        # Remove www. prefix for matching
        domain = re.sub(r'^www\.', '', domain)

        for excluded in EXCLUDE_DOMAINS:
            if excluded in domain:
                return True

        return False
    except:
        return False

def has_resource_keywords(url):
    """Check if URL contains bioresource keywords"""
    url_lower = url.lower()

    for keyword in RESOURCE_KEYWORDS:
        if keyword in url_lower:
            return True

    return False

def has_academic_tld(url):
    """Check if URL has academic/government TLD"""
    url_lower = url.lower()

    for tld in ACADEMIC_TLDS:
        if tld in url_lower:
            return True

    return False

def get_url_context(text, url, window=50):
    """Extract text surrounding URL for context"""
    if pd.isna(text):
        return ''

    text = str(text)
    url_pos = text.lower().find(url.lower())

    if url_pos == -1:
        return ''

    # Get text before and after URL
    start = max(0, url_pos - window)
    end = min(len(text), url_pos + len(url) + window)

    context = text[start:end].strip()

    # Add ellipsis if truncated
    if start > 0:
        context = '...' + context
    if end < len(text):
        context = context + '...'

    return context

def score_url(url, context, abstract):
    """Score URL to determine if it's likely a bioresource"""
    score = 0.0

    # Check for exclusions first
    if is_excluded_domain(url):
        return -1000  # Exclude completely

    # +10: Near context phrases ("available at", etc.)
    context_lower = context.lower()
    for phrase in CONTEXT_PHRASES:
        if phrase in context_lower:
            score += 10
            break

    # +8: Contains resource keywords in domain
    if has_resource_keywords(url):
        score += 8

    # +5: Academic/government domain
    if has_academic_tld(url):
        score += 5

    # +3: Has subdomain suggesting resource (db., data., tools., portal.)
    parsed = urlparse(url)
    domain = parsed.netloc.lower() if parsed.netloc else url.lower()
    if any(sub in domain for sub in ['db.', 'data.', 'tools.', 'portal.', 'www.']):
        score += 3

    # +2: URL appears in first half of abstract (more prominent)
    if pd.notna(abstract):
        abstract_str = str(abstract)
        url_pos = abstract_str.lower().find(url.lower())
        if url_pos != -1 and url_pos < len(abstract_str) / 2:
            score += 2

    return score

def process_urls(row):
    """Extract and score URLs for a single paper"""
    abstract = row.get('abstract', '')

    # Extract all URLs
    all_urls = extract_urls(abstract)

    if not all_urls:
        return {
            'all_urls': '',
            'resource_url': '',
            'has_resource_url': False,
            'url_context': ''
        }

    # Score each URL
    scored_urls = []
    for url in all_urls:
        context = get_url_context(abstract, url)
        score = score_url(url, context, abstract)
        scored_urls.append({
            'url': url,
            'score': score,
            'context': context
        })

    # Filter out excluded URLs (score < 0)
    resource_urls = [u for u in scored_urls if u['score'] >= 0]

    # Sort by score descending
    resource_urls.sort(key=lambda x: x['score'], reverse=True)

    # Get primary resource URL (highest scoring)
    if resource_urls:
        primary = resource_urls[0]
        return {
            'all_urls': ' | '.join([u['url'] for u in scored_urls if u['score'] >= 0]),
            'resource_url': primary['url'],
            'has_resource_url': True,
            'url_context': primary['context']
        }
    else:
        # No resource URLs (all were excluded) - just show all URLs found
        return {
            'all_urls': ' | '.join(all_urls),
            'resource_url': '',
            'has_resource_url': False,
            'url_context': ''
        }

# ============================================================================
# MAIN FUNCTION
# ============================================================================


def main():
    """Main function to extract URLs and identify bioresource websites."""
    args = parse_args()

    print("=" * 80)
    print("Phase 5: URL Extraction and Bioresource Website Identification")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # ============================================================================
    # DETERMINE INPUT/OUTPUT PATHS
    # ============================================================================

    if args.session_dir:
        print("Mode: Session-based")
        print(f"Session directory: {args.session_dir}")
        print()

        # Validate session directory
        try:
            validate_session_dir(args.session_dir, required_phases=['05_mapping'])
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)

        # Session-based paths - read from Script 12 output (quality indicators)
        input_file = get_session_path(args.session_dir, '05_mapping', 'union_papers_with_quality_indicators.csv')
        output_file = get_session_path(args.session_dir, '05_mapping', 'union_papers_with_urls.csv')

    elif args.auto:
        print("Mode: Legacy (auto-detect)")
        print()

        # Use legacy paths with auto-detection
        input_file = LEGACY_INPUT_FILE
        output_file = LEGACY_OUTPUT_FILE

        if not input_file.exists():
            print(f"ERROR: Legacy input file not found: {input_file}")
            sys.exit(1)

    elif args.input_file and args.output_file:
        print("Mode: Custom paths")
        print()

        input_file = args.input_file
        output_file = args.output_file

    else:
        print("ERROR: Must provide either --session-dir, --auto, or both --input-file and --output-file")
        sys.exit(1)

    # ============================================================================
    # VALIDATE INPUT FILE
    # ============================================================================

    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)

    print(f"Input file:  {input_file}")
    print(f"Output file: {output_file}")
    print()

    # ============================================================================
    # LOAD INPUT DATA
    # ============================================================================

    print("Loading input data...")
    try:
        df = pd.read_csv(input_file)
        print(f"  Loaded: {len(df):,} papers")
    except Exception as e:
        print(f"ERROR: Failed to load input file: {e}")
        sys.exit(1)

    # Verify required columns
    required_cols = ['abstract']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"ERROR: Input file missing required columns: {missing_cols}")
        sys.exit(1)

    print()

    # ============================================================================
    # EXTRACT URLs
    # ============================================================================

    print("Extracting URLs from abstracts...")
    print("  This may take a few minutes...")
    print()

    # Process URLs for each row
    url_results = df.apply(process_urls, axis=1)

    # Convert to DataFrame
    url_df = pd.DataFrame(url_results.tolist())

    # Add new columns to original DataFrame
    df['all_urls'] = url_df['all_urls']
    df['resource_url'] = url_df['resource_url']
    df['has_resource_url'] = url_df['has_resource_url']
    df['url_context'] = url_df['url_context']

    # ============================================================================
    # STATISTICS
    # ============================================================================

    total_with_urls = (df['all_urls'] != '').sum()
    total_with_resource = df['has_resource_url'].sum()

    print("URL Extraction Statistics:")
    print(f"  Total papers:           {len(df):,}")
    print(f"  Papers with URLs:       {total_with_urls:,} ({total_with_urls/len(df)*100:.1f}%)")
    print(f"  Papers with resource:   {total_with_resource:,} ({total_with_resource/len(df)*100:.1f}%)")
    print()

    # ============================================================================
    # SAVE OUTPUT (NEW FILE)
    # ============================================================================

    print(f"Saving output to NEW file: {output_file}")

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        df.to_csv(output_file, index=False)
        print(f"  Successfully saved: {len(df):,} rows")
        print(f"  Added 4 new columns:")
        print(f"    - all_urls")
        print(f"    - resource_url")
        print(f"    - has_resource_url")
        print(f"    - url_context")
    except Exception as e:
        print(f"ERROR: Failed to save output file: {e}")
        sys.exit(1)

    print()

    # ============================================================================
    # SAVE STATISTICS
    # ============================================================================

    stats_file = output_file.parent / 'url_extraction_statistics.json'
    stats = {
        'timestamp': datetime.now().isoformat(),
        'input_file': str(input_file),
        'output_file': str(output_file),
        'total_papers': len(df),
        'papers_with_urls': int(total_with_urls),
        'papers_with_resource_urls': int(total_with_resource),
        'resource_url_percentage': round(total_with_resource/len(df)*100, 2),
    }

    try:
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        print(f"Statistics saved: {stats_file}")
    except Exception as e:
        print(f"WARNING: Failed to save statistics: {e}")

    print()
    print("=" * 80)
    print("COMPLETE!")
    print("=" * 80)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print(f"Output file: {output_file}")
    print(f"  - Original input file preserved (NOT modified in-place)")
    print(f"  - New file created with URL columns added")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()
