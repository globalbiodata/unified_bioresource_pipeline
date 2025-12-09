#!/usr/bin/env python3
"""
Unified Deduplication for Sets A, B, and C with Multi-Profile Support

Set A: Linguistic papers (all, including baseline)
Set B: SetFit papers (all, including baseline)
Set C: Union of deduplicated A + B

Profiles:
  - conservative: High precision, strict filtering
  - balanced: Recommended default, good precision/recall
  - aggressive: Maximum recall, accepts more false positives

Usage:
  python 17_deduplicate_all_sets.py --session-dir 2025-12-04-111420-z381s
  python 17_deduplicate_all_sets.py --session-dir 2025-12-04-111420-z381s --profiles balanced
  python 17_deduplicate_all_sets.py --session-dir 2025-12-04-111420-z381s --profiles conservative,balanced

Inputs (from session):
  - 05_mapping/union_papers_with_urls.csv (Output of Script 13 - contains all papers with URLs)

Outputs (to session):
  - 07_deduplication/{profile}/set_a_linguistic.csv
  - 07_deduplication/{profile}/set_b_setfit.csv
  - 07_deduplication/{profile}/set_c_final.csv
  - 07_deduplication/{profile}/deduplication_stats.txt
  - 07_deduplication/profile_comparison_summary.md

Created: 2025-11-20
Updated: 2025-11-21 (Added session support)
Updated: 2025-11-25 (Added multi-profile filtering support)
Updated: 2025-12-04 (Refactored to require session-dir, removed legacy paths)
Purpose: Complete three-strategy comparison with configurable filtering profiles
"""

import argparse
import sys
import pandas as pd
import re
import yaml
from pathlib import Path
from collections import defaultdict
from urllib.parse import urlparse
from difflib import SequenceMatcher
from datetime import datetime

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Deduplicate Sets A, B, and C with multi-profile support')
parser.add_argument('--session-dir', type=str, required=True,
                    help='Session directory for outputs (required)')
parser.add_argument('--profiles', type=str, default='all',
                    help='Filtering profiles to run: conservative, balanced, aggressive, or all (default: all)')
parser.add_argument('--config', type=str, required=False,
                    help='Path to config file (default: unified_bioresource_pipeline/config/pipeline_config.yaml)')
args = parser.parse_args()

# Determine project root
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
BASE_DIR = PROJECT_ROOT.parent if PROJECT_ROOT.name == 'unified_bioresource_pipeline' else PROJECT_ROOT

# Add project root to path for lib imports
sys.path.insert(0, str(PROJECT_ROOT))
from lib.session_utils import validate_session_dir, get_session_path

# Validate and configure session directory
SESSION_DIR = Path(args.session_dir).resolve()

# Validate session directory exists
if not SESSION_DIR.exists():
    print(f"ERROR: Session directory not found: {SESSION_DIR}")
    sys.exit(1)

try:
    # Validate session structure - dedup scripts need prior phases' outputs
    validate_session_dir(SESSION_DIR, required_phases=['05_mapping'])
except ValueError as e:
    print(f"ERROR: Invalid session directory: {e}")
    sys.exit(1)

# Load filtering profiles from config
CONFIG_PATH = Path(args.config) if args.config else PROJECT_ROOT / 'config' / 'pipeline_config.yaml'

if CONFIG_PATH.exists():
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
    FILTERING_PROFILES = config.get('filtering_profiles', {})
    DEFAULT_PROFILE = config.get('default_profile', 'balanced')
else:
    print(f"Warning: Config file not found at {CONFIG_PATH}, using default profiles")
    FILTERING_PROFILES = {
        'balanced': {
            'description': 'Default profile',
            'db_keywords': ['database', 'server', 'portal', 'repository', 'archive'],
            'linguistic_bypass_threshold': 5,
            'setfit_threshold': 0.58,
            'require_url': True
        }
    }
    DEFAULT_PROFILE = 'balanced'

# Parse which profiles to run
if args.profiles == 'all':
    PROFILES_TO_RUN = list(FILTERING_PROFILES.keys())
else:
    PROFILES_TO_RUN = [p.strip() for p in args.profiles.split(',')]

# Input file from session directory - output of Script 13 (extract URLs)
INPUT_FILE = get_session_path(SESSION_DIR, '05_mapping', 'union_papers_with_urls.csv')

# Validate input file exists
if not INPUT_FILE.exists():
    print(f"ERROR: Input file not found: {INPUT_FILE}")
    print(f"Required: Run Script 13 (extract URLs) first to create this file")
    sys.exit(1)

# Output base directory
BASE_RESULTS_DIR = get_session_path(SESSION_DIR, '07_deduplication')

print("="*80)
print("UNIFIED DEDUPLICATION FOR SETS A, B, AND C")
print("="*80)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Session: {SESSION_DIR.name}")
print(f"Profiles to run: {', '.join(PROFILES_TO_RUN)}")
print(f"Config file: {CONFIG_PATH}")
print()
print("INPUT FILE:")
print(f"  Union papers with URLs: {INPUT_FILE.name}")
print()

# ============================================================================
# GENERIC DOMAIN LISTS (from code review 2025-11-27)
# ============================================================================

# Institutional domains that host MANY unrelated databases
# URLs from these domains should NOT be clustered together just by domain
GENERIC_INSTITUTIONAL_DOMAINS = [
    'ac.uk',    # UK universities (100+ institutions)
    'edu.cn',   # Chinese universities (2000+ institutions)
    'ac.cn',    # Chinese academic
    'edu.tw',   # Taiwan universities
    'ac.jp',    # Japanese universities
    'ac.kr',    # Korean universities
    'ac.in',    # Indian universities
    'res.in',   # Indian research institutes
    'nih.gov',  # US NIH (multiple institutes: NCI, NCBI, NIAID, etc.)
    'edu.au',   # Australian universities
    'ac.at',    # Austrian academic
    'edu',      # Generic .edu (US universities)
]

# Multi-database platforms where subdomain matters for identity
MULTI_DB_PLATFORMS = [
    'github.io',       # GitHub Pages (many users)
    'shinyapps.io',    # Shiny apps (many users)
    'gbif.org',        # GBIF (many datasets)
    'gxbsidra.org',    # Multiple databases (breastcancer, sepsis, etc.)
    'herokuapp.com',   # Heroku apps
    'netlify.app',     # Netlify apps
]


def is_generic_domain(base_domain):
    """Check if domain is a generic institutional domain."""
    if not base_domain:
        return False
    for generic in GENERIC_INSTITUTIONAL_DOMAINS:
        if base_domain.endswith(generic):
            return True
    return False


def is_multi_db_platform(hostname):
    """Check if hostname is a multi-database platform where subdomain matters."""
    if not hostname:
        return False
    for platform in MULTI_DB_PLATFORMS:
        if hostname.endswith(platform):
            return True
    return False


# ============================================================================
# URL SIMILARITY FUNCTIONS
# ============================================================================

def parse_url_components(url):
    """Parse URL into normalized components for comparison."""
    if pd.isna(url) or url == '':
        return None

    url = str(url).strip()

    # Add http:// if missing
    if not url.startswith(('http://', 'https://', 'ftp://')):
        url = 'http://' + url

    parsed = urlparse(url)

    # Extract components
    protocol = parsed.scheme
    full_domain = parsed.netloc.lower()
    path = parsed.path.rstrip('/').lower()

    # Split domain into parts
    domain_parts = full_domain.split('.')

    # Extract TLD (last part)
    tld = '.' + domain_parts[-1] if len(domain_parts) > 0 else ''

    # Extract main domain (second-to-last part)
    if len(domain_parts) >= 2:
        # Handle compound TLDs like .ac.uk, .co.uk, .edu.cn
        if len(domain_parts) >= 3 and domain_parts[-2] in ['ac', 'co', 'edu', 'gov']:
            main_domain = domain_parts[-3]
            tld = '.' + '.'.join(domain_parts[-2:])
            subdomain = '.'.join(domain_parts[:-3]) if len(domain_parts) > 3 else ''
        else:
            main_domain = domain_parts[-2]
            subdomain = '.'.join(domain_parts[:-2]) if len(domain_parts) > 2 else ''
    else:
        main_domain = full_domain
        subdomain = ''

    return {
        'protocol': protocol,
        'subdomain': subdomain,
        'domain': main_domain,
        'tld': tld,
        'path': path,
        'full_domain': full_domain,
        'original_url': url
    }

def normalize_url_aggressive(url):
    """Aggressively normalize URL for matching."""
    components = parse_url_components(url)
    if not components:
        return ''

    # Build normalized form: domain + tld + path
    subdomain = components['subdomain']

    # Remove www from subdomain
    if subdomain == 'www':
        subdomain = ''
    elif subdomain.endswith('.www'):
        subdomain = subdomain[:-4]
    elif subdomain.startswith('www.'):
        subdomain = subdomain[4:]

    # Build normalized URL
    if subdomain:
        normalized = f"{subdomain}.{components['domain']}{components['tld']}{components['path']}"
    else:
        normalized = f"{components['domain']}{components['tld']}{components['path']}"

    return normalized


def normalize_url_strict(url):
    """
    Strictly normalize URL for exact matching (added 2025-11-27).
    Handles: trailing slash, www prefix, http/https protocol.

    Examples:
    - http://www.db.com/ -> db.com
    - https://db.com -> db.com
    - http://db.com/path/ -> db.com/path
    """
    if pd.isna(url) or url == '':
        return ''

    url = str(url).strip().lower()

    # Remove protocol
    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'^ftp://', '', url)

    # Remove www. prefix
    if url.startswith('www.'):
        url = url[4:]

    # Remove trailing slash
    url = url.rstrip('/')

    # Remove common index files
    url = re.sub(r'/index\.(html?|php|asp)$', '', url)

    # Remove port 80 (default http)
    url = re.sub(r':80($|/)', r'\1', url)

    return url


def urls_match_exactly(url1, url2):
    """
    Check if two URLs are identical after strict normalization.
    Returns True if URLs point to the same resource.
    """
    n1 = normalize_url_strict(url1)
    n2 = normalize_url_strict(url2)

    if not n1 or not n2:
        return False

    return n1 == n2

def compute_url_similarity(url1, url2):
    """Compute similarity score between two URLs (0.0 to 1.0)."""
    # First check strict exact match (handles www, trailing slash, protocol)
    if urls_match_exactly(url1, url2):
        return 1.0

    c1 = parse_url_components(url1)
    c2 = parse_url_components(url2)

    if not c1 or not c2:
        return 0.0

    # Exact match after aggressive normalization
    norm1 = normalize_url_aggressive(url1)
    norm2 = normalize_url_aggressive(url2)

    if norm1 == norm2:
        return 1.0

    # Domain similarity
    domain_sim = SequenceMatcher(None, c1['domain'], c2['domain']).ratio()

    # Path similarity
    path_sim = SequenceMatcher(None, c1['path'], c2['path']).ratio() if c1['path'] or c2['path'] else 1.0

    # Subdomain similarity
    subdomain_sim = SequenceMatcher(None, c1['subdomain'], c2['subdomain']).ratio()

    # Scoring logic
    score = 0.0

    # Case 1: Same main domain and TLD
    if c1['domain'] == c2['domain'] and c1['tld'] == c2['tld']:
        score = 0.8

        # Same path
        if c1['path'] == c2['path']:
            score += 0.2
        # Similar path
        elif path_sim > 0.8:
            score += 0.15
        # Different path but subdomain similar
        elif subdomain_sim > 0.8:
            score += 0.1

    # Case 2: Very similar domain (likely typo or variation)
    elif domain_sim > 0.85:
        score = 0.5

        # Same TLD
        if c1['tld'] == c2['tld']:
            score += 0.2

        # Similar path
        if path_sim > 0.8:
            score += 0.2

    # Case 3: Different domain but very similar subdomains and paths
    else:
        # Check if one is subdomain of the other
        if c1['full_domain'] in c2['full_domain'] or c2['full_domain'] in c1['full_domain']:
            score = 0.6 + (path_sim * 0.3)
        else:
            # Different domains
            score = domain_sim * 0.4

    return score

def urls_are_similar(url1, url2, threshold=0.85):
    """Check if two URLs are similar above threshold."""
    return compute_url_similarity(url1, url2) >= threshold

# ============================================================================
# ENTITY NORMALIZATION
# ============================================================================

def normalize_entity(name):
    """Normalize entity name for matching."""
    if pd.isna(name) or name == '':
        return ''

    name = str(name).lower().strip()

    # Remove common punctuation and extra spaces
    name = re.sub(r'[_\-\.]', ' ', name)
    name = ' '.join(name.split())

    return name

def get_primary_entity(row):
    """Get primary entity (prefer long, fallback to short)."""
    if pd.notna(row['primary_entity_long']) and row['primary_entity_long'] != '':
        return str(row['primary_entity_long'])
    elif pd.notna(row['primary_entity_short']) and row['primary_entity_short'] != '':
        return str(row['primary_entity_short'])
    return ''

# ============================================================================
# URL CLUSTERING
# ============================================================================

def cluster_similar_urls(urls, threshold=0.85):
    """
    Cluster URLs by similarity using union-find with domain blocking optimization.

    Domain blocking: Only compare URLs with the same domain, reducing complexity
    from O(n²) to O(d * k²) where d = number of domains, k = avg URLs per domain.

    Returns dict mapping each URL to its canonical URL (first in cluster).
    """
    if len(urls) == 0:
        return {}

    urls_list = list(urls)
    n = len(urls_list)

    # Union-find data structure
    parent = list(range(n))

    def find(x):
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            # Always make lower index the parent (to preserve first URL as canonical)
            if px < py:
                parent[py] = px
            else:
                parent[px] = py

    # === IMPROVED DOMAIN BLOCKING (2025-11-27) ===
    # Key insight from code review: different databases from same institutional
    # domain (ac.uk, edu.cn) should NOT be clustered together.
    # Solution: Use blocking key that preserves subdomain for:
    #   1. Generic institutional domains (ac.uk, nih.gov, etc.)
    #   2. Multi-database platforms (github.io, shinyapps.io, etc.)
    #
    # This prevents false merges like:
    #   - phasingserver.ox.ac.uk ≠ memprotmd.ox.ac.uk (different .ac.uk DBs)
    #   - breastcancer.gxbsidra.org ≠ sepsis.gxbsidra.org (same platform, different DBs)
    UNKNOWN_DOMAIN = '__unknown__'
    domain_groups = defaultdict(list)
    generic_domain_count = 0

    for i, url in enumerate(urls_list):
        components = parse_url_components(url)
        if components:
            full_domain = components['full_domain']  # e.g., "sub.example.com"
            base_domain = f"{components['domain']}{components['tld']}"  # e.g., "example.com"

            # Determine blocking key based on domain type
            if is_multi_db_platform(full_domain):
                # Multi-DB platform: use full domain (keeps subdomain)
                blocking_key = full_domain
            elif is_generic_domain(base_domain):
                # Generic institutional domain: use full domain (keeps institution)
                blocking_key = full_domain
                generic_domain_count += 1
            else:
                # Specific domain: can use base domain for efficiency
                blocking_key = full_domain  # Still use full for safety
        else:
            blocking_key = UNKNOWN_DOMAIN
        domain_groups[blocking_key].append(i)

    if generic_domain_count > 0:
        print(f"   Note: {generic_domain_count} URLs from generic institutional domains (blocking by full domain)")

    # Find similar pairs within each domain group only
    total_comparisons = 0
    for domain, indices in domain_groups.items():
        group_size = len(indices)
        if group_size > 1:
            for i_pos in range(group_size):
                for j_pos in range(i_pos + 1, group_size):
                    i, j = indices[i_pos], indices[j_pos]
                    total_comparisons += 1
                    if urls_are_similar(urls_list[i], urls_list[j], threshold):
                        union(i, j)

    naive_comparisons = n * (n - 1) // 2
    reduction_pct = 100 * (1 - total_comparisons / naive_comparisons) if naive_comparisons > 0 else 0
    print(f"   Domain blocking: {len(domain_groups):,} domains, {total_comparisons:,} comparisons "
          f"(vs {naive_comparisons:,} without blocking) [{reduction_pct:.1f}% reduction]")

    # Build mapping from URL to canonical URL
    clusters = defaultdict(list)
    for i in range(n):
        root = find(i)
        clusters[root].append(urls_list[i])

    # Create mapping: url -> canonical_url (first in cluster)
    url_to_canonical = {}
    for cluster in clusters.values():
        canonical = cluster[0]  # First URL in cluster
        for url in cluster:
            url_to_canonical[url] = canonical

    return url_to_canonical

# ============================================================================
# PROFILE-BASED FILTERING FUNCTIONS
# ============================================================================

def check_keyword_match(title, keywords):
    """Check if any keyword appears in the title (case-insensitive)."""
    if pd.isna(title) or not title:
        return False
    title_lower = str(title).lower()
    return any(kw.lower() in title_lower for kw in keywords)


# =============================================================================
# TITLE-BASED SCORE MODIFIERS (False Positive Reduction)
# =============================================================================
# Based on analysis of 200 manually reviewed papers:
# - Papers with "database", "archive", etc. in title are 6x more likely to be legitimate
# - Papers with "tool for", "method for", etc. WITHOUT data words are likely methodology FPs

# Data resource words - presence indicates legitimate bioresource (+1 boost)
DATA_RESOURCE_WORDS = [
    'database', 'archive', 'repository', 'atlas', 'resource', 'commons',
    'data management', 'data integration', 'data platform'
]

# Methodology patterns - presence WITHOUT data words indicates pure methodology paper (-1 penalty)
METHODOLOGY_PATTERNS = [
    r'\btool for\b',
    r'\bmethod for\b',
    r'\bapproach for\b',
    r'\bframework for\b',
    r'\bpipeline for\b',
    r'\bworkflow for\b',
    r'\balgorithm for\b',
    r'\bprediction of\b',
    r'\bpredicting\b',
    r'\bidentifying\b',
    r'\bdetection of\b',
]

# Pre-compile regex patterns for performance
METHODOLOGY_PATTERNS_COMPILED = [re.compile(pattern) for pattern in METHODOLOGY_PATTERNS]


def compute_title_score_modifier(title):
    """
    Compute a score modifier based on title patterns.

    Returns:
        int: Score modifier (+1 for data resource words, -1 for methodology without data words)
        str: Reason for modifier (for logging)
    """
    if pd.isna(title) or not title:
        return 0, "no_title"

    title_lower = str(title).lower()

    # Check for data resource words (boost)
    has_data_word = any(word in title_lower for word in DATA_RESOURCE_WORDS)

    if has_data_word:
        return 1, "data_resource_boost"

    # Check for methodology patterns (penalty only if NO data words)
    has_methodology = any(pattern.search(title_lower) for pattern in METHODOLOGY_PATTERNS_COMPILED)

    if has_methodology:
        # Methodology word without data word = likely pure methodology paper
        return -1, "methodology_penalty"

    return 0, "neutral"


def passes_profile_filter(row, profile):
    """
    Check if a paper passes the profile filter.

    Papers can pass either via:
    1. Linguistic bypass: effective_ling_score >= linguistic_bypass_threshold (bypasses keyword check)
       - effective_ling_score = ling_score + title_modifier
       - title_modifier: +1 for data resource words, -1 for methodology patterns
    2. Keyword match: title contains one of the db_keywords AND (has URL if required)

    Args:
        row: DataFrame row with paper data
        profile: Profile dict with filtering parameters

    Returns:
        bool: True if paper passes filter
    """
    # Get profile parameters with defaults
    bypass_threshold = profile.get('linguistic_bypass_threshold', 6)
    keywords = profile.get('db_keywords', ['database'])
    require_url = profile.get('require_url', True)
    setfit_threshold = profile.get('setfit_threshold', 0.58)

    # Helper to check URL requirement
    def has_url():
        url_val = row.get('has_resource_url', False)
        # Handle string representations of boolean
        if isinstance(url_val, str):
            return url_val.lower() in ('true', '1', 'yes')
        return bool(url_val)

    # Get title for modifier and keyword check
    title = row.get('title', '')

    # Compute title-based score modifier
    title_modifier, modifier_reason = compute_title_score_modifier(title)

    # Check linguistic bypass first (using effective score with title modifier)
    ling_score = row.get('ling_score')
    if pd.notna(ling_score):
        try:
            ling_score = float(ling_score)
            effective_score = ling_score + title_modifier

            if effective_score >= bypass_threshold:
                # High effective score bypasses keyword filter
                # Still need URL if required
                if require_url:
                    return has_url()
                return True
        except (ValueError, TypeError):
            pass  # Invalid score, continue to keyword check

    # Check keyword match FIRST (before SetFit threshold)
    has_keyword = check_keyword_match(title, keywords)

    # If no keyword match, filter out (no need to check other criteria)
    if not has_keyword:
        return False

    # Has keyword - now check SetFit confidence threshold
    # Papers without SetFit scores (NaN) pass this check (they're linguistic papers)
    setfit_conf = row.get('setfit_confidence')
    if pd.notna(setfit_conf):
        try:
            setfit_conf = float(setfit_conf)
            if setfit_conf < setfit_threshold:
                # Below confidence threshold - filter out
                return False
        except (ValueError, TypeError):
            pass  # Invalid confidence - treat as passing (benefit of doubt)

    # Check URL requirement
    if require_url:
        return has_url()

    return True

# ============================================================================
# CORE DEDUPLICATION FUNCTION
# ============================================================================

def deduplicate_dataset(df, dataset_name, profile=None, filter_criteria=True):
    """
    Deduplicate a dataset using URL clustering and entity matching.

    Args:
        df: DataFrame to deduplicate
        dataset_name: Name for logging (e.g., "Set A")
        profile: Profile dict with filtering parameters (optional)
        filter_criteria: If True, filter for resources (legacy mode if no profile)

    Returns:
        Deduplicated DataFrame
    """
    print(f"\n{'='*80}")
    print(f"DEDUPLICATING {dataset_name}")
    print(f"{'='*80}")

    print(f"\n1. Input: {len(df)} papers")

    # Filter for resources based on profile or legacy criteria
    if filter_criteria:
        if profile:
            # Use profile-based filtering
            print(f"\n2. Filtering with profile: {profile.get('description', 'custom')}")
            print(f"   Keywords: {profile.get('db_keywords', [])[:5]}...")
            print(f"   Linguistic bypass: ling_score >= {profile.get('linguistic_bypass_threshold', 6)}")
            print(f"   SetFit threshold: >= {profile.get('setfit_threshold', 0.58)}")
            print(f"   Require URL: {profile.get('require_url', True)}")

            # Apply profile filter
            mask = df.apply(lambda row: passes_profile_filter(row, profile), axis=1)
            filtered = df[mask].copy()

            # Count how many passed via bypass vs keywords (in the FILTERED set)
            bypass_threshold = profile.get('linguistic_bypass_threshold', 6)
            print(f"\n   Papers passing filter: {len(filtered)}")
            if 'ling_score' in filtered.columns and len(filtered) > 0:
                # Calculate effective scores with title modifiers (compute ONCE)
                def get_effective_score_and_modifier(row):
                    base = row.get('ling_score', 0)
                    if pd.isna(base):
                        return pd.Series({'_effective_score': 0, '_modifier': 0})
                    modifier, _ = compute_title_score_modifier(row.get('title', ''))
                    return pd.Series({
                        '_effective_score': float(base) + modifier,
                        '_modifier': modifier
                    })

                # Apply once and extract both columns
                temp_df = filtered.apply(get_effective_score_and_modifier, axis=1)
                filtered['_effective_score'] = temp_df['_effective_score']
                filtered['_modifier'] = temp_df['_modifier']

                bypassed_mask = filtered['_effective_score'] >= bypass_threshold
                bypassed_count = bypassed_mask.sum()
                keyword_count = (~bypassed_mask).sum()
                print(f"   - Via linguistic bypass (effective_score >= {bypass_threshold}): {bypassed_count}")
                print(f"   - Via keyword match: {keyword_count}")

                # Show title modifier impact (using pre-computed modifiers)
                boost_count = (filtered['_modifier'] == 1).sum()
                penalty_count = (filtered['_modifier'] == -1).sum()
                print(f"   Title modifiers applied:")
                print(f"   - Data resource boost (+1): {boost_count} papers")
                print(f"   - Methodology penalty (-1): {penalty_count} papers")

                # Clean up temp columns
                filtered = filtered.drop(columns=['_effective_score', '_modifier'])
            else:
                print(f"   - Breakdown not available (ling_score column missing or no data)")

            # Handle empty filtered results
            if len(filtered) == 0:
                print(f"\n   WARNING: No papers passed filter for {dataset_name}")
                print(f"   Returning empty DataFrame")
                return pd.DataFrame()
        else:
            # No profile provided but filtering requested - use URL-only filter
            print("\n2. Filtering for papers with URLs (no profile specified)...")
            filtered = df[df['has_resource_url'] == True].copy()
            print(f"   Filtered: {len(filtered)} papers")
    else:
        filtered = df.copy()
        print("\n2. No filtering applied (using all papers)")

    # Prepare for deduplication
    print("\n3. Preparing data for deduplication...")

    # Get primary entity
    filtered['primary_entity'] = filtered.apply(get_primary_entity, axis=1)
    filtered['norm_entity'] = filtered['primary_entity'].apply(normalize_entity)

    print(f"   Unique URLs: {filtered['resource_url'].nunique()}")
    print(f"   Unique entities: {filtered['norm_entity'].nunique()}")

    # Cluster similar URLs
    print("\n4. Clustering similar URLs (threshold=0.85)...")

    all_urls = filtered['resource_url'].unique()
    url_to_canonical = cluster_similar_urls(all_urls, threshold=0.85)
    filtered['canonical_url'] = filtered['resource_url'].map(url_to_canonical)

    num_merged = len(all_urls) - len(set(url_to_canonical.values()))
    print(f"   URLs before clustering: {len(all_urls)}")
    print(f"   URLs after clustering: {len(set(url_to_canonical.values()))}")
    print(f"   URLs merged: {num_merged}")

    # Find duplicates
    print("\n5. Finding duplicates by (canonical_url, norm_entity)...")

    filtered['dedup_key'] = filtered['canonical_url'] + '||' + filtered['norm_entity']

    duplicates = filtered.duplicated(['canonical_url', 'norm_entity'], keep=False)
    dup_df = filtered[duplicates]
    unique_df = filtered[~duplicates]

    print(f"   Duplicates: {len(dup_df)} papers in {dup_df.groupby(['canonical_url', 'norm_entity']).ngroups if len(dup_df) > 0 else 0} groups")

    # Deduplicate
    print("\n6. Deduplicating...")

    if len(dup_df) > 0:
        # For duplicates, keep earliest paper and join PMIDs
        # Convert pmid to string to handle mixed types
        dup_df['pmid'] = dup_df['pmid'].astype(str)
        duplicate_merged = dup_df.sort_values('pmid').groupby(
            ['canonical_url', 'norm_entity']
        ).agg({
            'pmid': lambda x: ', '.join(map(str, x)),
            'title': 'first',
            'abstract': 'first',
            'in_linguistic': 'first',
            'in_setfit': 'first',
            'ling_score': 'first',
            'setfit_confidence': 'first',
            'primary_entity_long': 'first',
            'primary_entity_short': 'first',
            'primary_score': 'max',
            'status': 'first',
            'matched_long_short': 'first',
            'all_long': lambda x: ' | '.join(set(' | '.join(str(v) for v in x if pd.notna(v)).split(' | ')) - {'', 'nan'}),
            'all_short': lambda x: ' | '.join(set(' | '.join(str(v) for v in x if pd.notna(v)).split(' | ')) - {'', 'nan'}),
            'ner_source': 'first',
            'ner_confidence': 'max',
            'all_urls': 'first',
            'resource_url': 'first',  # Keep first (canonical)
            'has_resource_url': 'first',
            'url_context': 'first',
        }).reset_index()

        # Add article count
        duplicate_merged['article_count'] = dup_df.groupby(
            ['canonical_url', 'norm_entity']
        ).size().values

        # Drop temporary columns
        duplicate_merged = duplicate_merged.drop(['canonical_url', 'norm_entity'], axis=1)
    else:
        duplicate_merged = pd.DataFrame()

    # Add article_count to unique papers
    unique_df['article_count'] = 1

    # Drop temporary columns from unique
    unique_df = unique_df.drop(['primary_entity', 'norm_entity', 'canonical_url', 'dedup_key'], axis=1)

    # Combine
    if len(duplicate_merged) > 0:
        dedup_df = pd.concat([unique_df, duplicate_merged], ignore_index=True)
    else:
        dedup_df = unique_df

    # Sort by article count (descending) then primary_score
    dedup_df = dedup_df.sort_values(['article_count', 'primary_score'], ascending=[False, False])

    print(f"\n   Final deduplicated count: {len(dedup_df)}")
    print(f"   Papers removed: {len(filtered) - len(dedup_df)}")
    print(f"   Reduction: {((len(filtered) - len(dedup_df)) / len(filtered) * 100):.1f}%")

    return dedup_df

# ============================================================================
# MAIN EXECUTION
# ============================================================================

# Load data - single unified input file from Script 13
print("\nLoading datasets...")
df_all = pd.read_csv(INPUT_FILE)
print(f"  Total papers loaded: {len(df_all)}")

# Split into Set A (linguistic) and Set B (SetFit) based on source columns
# in_linguistic=True means paper came from linguistic strategy (Set A)
# in_setfit=True means paper came from SetFit strategy (Set B)
# Papers can be in both sets (overlap)
df_a = df_all[df_all['in_linguistic'] == True].copy()
df_b = df_all[df_all['in_setfit'] == True].copy()
print(f"  Set A (Linguistic): {len(df_a)} papers")
print(f"  Set B (SetFit): {len(df_b)} papers")

# Store results for each profile for final comparison
all_profile_results = {}

# Process each profile
for profile_name in PROFILES_TO_RUN:
    print(f"\n{'#'*80}")
    print(f"# PROCESSING PROFILE: {profile_name.upper()}")
    print(f"{'#'*80}")

    profile = FILTERING_PROFILES.get(profile_name)
    if not profile:
        print(f"Warning: Profile '{profile_name}' not found in config, skipping...")
        continue

    # Validate required profile keys
    required_keys = ['db_keywords', 'linguistic_bypass_threshold', 'setfit_threshold', 'require_url']
    missing_keys = [k for k in required_keys if k not in profile]
    if missing_keys:
        print(f"Warning: Profile '{profile_name}' missing keys: {missing_keys}")
        print("Using defaults for missing keys...")

    # Validate db_keywords is not empty
    if not profile.get('db_keywords'):
        print(f"Warning: Profile '{profile_name}' has empty db_keywords, using default")
        profile['db_keywords'] = ['database']

    print(f"Description: {profile.get('description', 'N/A')}")

    # Create output directory for this profile
    PROFILE_OUTPUT_DIR = BASE_RESULTS_DIR / profile_name
    PROFILE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    OUTPUT_SET_A = PROFILE_OUTPUT_DIR / 'set_a_linguistic.csv'
    OUTPUT_SET_B = PROFILE_OUTPUT_DIR / 'set_b_setfit.csv'
    OUTPUT_SET_C = PROFILE_OUTPUT_DIR / 'set_c_final.csv'
    STATS_FILE = PROFILE_OUTPUT_DIR / 'deduplication_stats.txt'

    # Deduplicate Set A with profile
    dedup_a = deduplicate_dataset(df_a.copy(), f"SET A (LINGUISTIC) - {profile_name}", profile=profile, filter_criteria=True)
    dedup_a.to_csv(OUTPUT_SET_A, index=False)
    print(f"\n✓ Saved Set A: {OUTPUT_SET_A}")

    # Deduplicate Set B with profile
    dedup_b = deduplicate_dataset(df_b.copy(), f"SET B (SETFIT) - {profile_name}", profile=profile, filter_criteria=True)
    dedup_b.to_csv(OUTPUT_SET_B, index=False)
    print(f"\n✓ Saved Set B: {OUTPUT_SET_B}")

    # Create Set C (Union of deduplicated A + B)
    print(f"\n{'='*80}")
    print(f"CREATING SET C (UNION OF DEDUPLICATED A + B) - {profile_name}")
    print(f"{'='*80}")

    # Combine dedup_a and dedup_b
    df_c = pd.concat([dedup_a, dedup_b], ignore_index=True)
    print(f"\n1. Combined A + B: {len(df_c)} total rows")

    # Deduplicate the union (no profile filter, already filtered)
    dedup_c = deduplicate_dataset(df_c, f"SET C (UNION) - {profile_name}", profile=None, filter_criteria=False)
    dedup_c.to_csv(OUTPUT_SET_C, index=False)
    print(f"\n✓ Saved Set C: {OUTPUT_SET_C}")

    # Store results for comparison
    all_profile_results[profile_name] = {
        'set_a_count': len(dedup_a),
        'set_b_count': len(dedup_b),
        'set_c_count': len(dedup_c),
        'output_dir': PROFILE_OUTPUT_DIR,
        'dedup_a': dedup_a,
        'dedup_b': dedup_b,
        'dedup_c': dedup_c
    }

# ============================================================================
# GENERATE STATISTICS FOR EACH PROFILE
# ============================================================================

for profile_name, results in all_profile_results.items():
    print(f"\n{'='*80}")
    print(f"GENERATING STATISTICS - {profile_name.upper()}")
    print(f"{'='*80}")

    dedup_a = results['dedup_a']
    dedup_b = results['dedup_b']
    dedup_c = results['dedup_c']
    PROFILE_OUTPUT_DIR = results['output_dir']
    STATS_FILE = PROFILE_OUTPUT_DIR / 'deduplication_stats.txt'

    stats = []
    stats.append("="*80)
    stats.append(f"UNIFIED DEDUPLICATION STATISTICS - {profile_name.upper()}")
    stats.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    stats.append("="*80)
    stats.append("")

    profile = FILTERING_PROFILES.get(profile_name, {})
    stats.append("PROFILE CONFIGURATION:")
    stats.append(f"  Description: {profile.get('description', 'N/A')}")
    stats.append(f"  Keywords: {len(profile.get('db_keywords', []))} terms")
    stats.append(f"  Linguistic bypass threshold: >= {profile.get('linguistic_bypass_threshold', 6)}")
    stats.append(f"  SetFit threshold: >= {profile.get('setfit_threshold', 0.58)}")
    stats.append(f"  Require URL: {profile.get('require_url', True)}")
    stats.append("")

    stats.append("INPUT DATA:")
    stats.append(f"  Set A (Linguistic): {len(df_a):>6} papers")
    stats.append(f"  Set B (SetFit):     {len(df_b):>6} papers")
    stats.append("")

    stats.append("DEDUPLICATED OUTPUTS:")
    stats.append(f"  Set A: {len(dedup_a):>6} unique resources")
    stats.append(f"  Set B: {len(dedup_b):>6} unique resources")
    stats.append(f"  Set C: {len(dedup_c):>6} unique resources (union)")
    stats.append("")

    stats.append("OVERLAP ANALYSIS:")
    a_pmids = set()
    for pmids in dedup_a['pmid'].astype(str):
        a_pmids.update(pmids.split(', '))

    b_pmids = set()
    for pmids in dedup_b['pmid'].astype(str):
        b_pmids.update(pmids.split(', '))

    overlap = a_pmids & b_pmids
    only_a = a_pmids - b_pmids
    only_b = b_pmids - a_pmids

    stats.append(f"  Papers only in A:   {len(only_a):>6}")
    stats.append(f"  Papers only in B:   {len(only_b):>6}")
    stats.append(f"  Papers in both:     {len(overlap):>6}")
    if len(a_pmids | b_pmids) > 0:
        stats.append(f"  Overlap rate:       {(len(overlap)/(len(a_pmids | b_pmids))*100):>5.1f}%")
    stats.append("")

    stats.append("TOP RESOURCES BY ARTICLE COUNT:")
    stats.append("\n  Set A (Linguistic):")
    for _, row in dedup_a.nlargest(5, 'article_count').iterrows():
        entity = row['primary_entity_long'] if pd.notna(row['primary_entity_long']) else row['primary_entity_short']
        entity_str = str(entity)[:50] if entity else 'N/A'
        stats.append(f"    {entity_str:50s} : {row['article_count']} papers")

    stats.append("\n  Set B (SetFit):")
    for _, row in dedup_b.nlargest(5, 'article_count').iterrows():
        entity = row['primary_entity_long'] if pd.notna(row['primary_entity_long']) else row['primary_entity_short']
        entity_str = str(entity)[:50] if entity else 'N/A'
        stats.append(f"    {entity_str:50s} : {row['article_count']} papers")

    stats.append("\n  Set C (Union):")
    for _, row in dedup_c.nlargest(5, 'article_count').iterrows():
        entity = row['primary_entity_long'] if pd.notna(row['primary_entity_long']) else row['primary_entity_short']
        entity_str = str(entity)[:50] if entity else 'N/A'
        stats.append(f"    {entity_str:50s} : {row['article_count']} papers")

    stats.append("")

    stats_text = '\n'.join(stats)
    with open(STATS_FILE, 'w') as f:
        f.write(stats_text)

    print(stats_text)

# ============================================================================
# GENERATE PROFILE COMPARISON SUMMARY
# ============================================================================

print(f"\n{'='*80}")
print("PROFILE COMPARISON SUMMARY")
print(f"{'='*80}")

comparison_lines = []
comparison_lines.append("# Profile Comparison Summary")
comparison_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
comparison_lines.append("")
comparison_lines.append("## Results by Profile")
comparison_lines.append("")
comparison_lines.append("| Profile | Set A | Set B | Set C (Final) | Keywords | Bypass Threshold |")
comparison_lines.append("|---------|-------|-------|---------------|----------|------------------|")

for profile_name, results in all_profile_results.items():
    profile = FILTERING_PROFILES.get(profile_name, {})
    comparison_lines.append(
        f"| {profile_name} | {results['set_a_count']} | {results['set_b_count']} | "
        f"{results['set_c_count']} | {len(profile.get('db_keywords', []))} | "
        f">= {profile.get('linguistic_bypass_threshold', 6)} |"
    )

comparison_lines.append("")
comparison_lines.append("## Output Directories")
comparison_lines.append("")
for profile_name, results in all_profile_results.items():
    comparison_lines.append(f"- **{profile_name}**: `{results['output_dir']}`")

comparison_lines.append("")
comparison_lines.append("## Profile Descriptions")
comparison_lines.append("")
for profile_name in all_profile_results.keys():
    profile = FILTERING_PROFILES.get(profile_name, {})
    comparison_lines.append(f"- **{profile_name}**: {profile.get('description', 'N/A')}")

comparison_text = '\n'.join(comparison_lines)

# Save comparison summary
COMPARISON_FILE = BASE_RESULTS_DIR / 'profile_comparison_summary.md'
with open(COMPARISON_FILE, 'w') as f:
    f.write(comparison_text)

print(comparison_text)

print("\n" + "="*80)
print("COMPLETE!")
print("="*80)
print(f"\nOutput directories:")
for profile_name, results in all_profile_results.items():
    print(f"  {profile_name}: {results['output_dir']}")
print(f"\nProfile comparison: {COMPARISON_FILE}")
print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
