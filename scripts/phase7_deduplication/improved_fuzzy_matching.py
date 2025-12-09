#!/usr/bin/env python3
"""
Improved Fuzzy Matching Algorithm v3

Fixes based on code review (2025-11-27):
1. Reduce SAME_DOMAIN weight for generic institutional domains
2. Require multiple signals for HIGH confidence
3. Add subdomain distinction for multi-database platforms
4. Enforce one-to-one mapping (baseline -> extracted)
5. Increase edit distance weights for close matches
6. Context-aware contains matching
7. Add manual review flags

This module can be imported into other scripts or run standalone.
"""

import pandas as pd
import re
from difflib import SequenceMatcher
from urllib.parse import urlparse
from collections import defaultdict

# =============================================================================
# GENERIC DOMAIN LISTS
# =============================================================================

# Institutional domains that host MANY unrelated databases
# These get reduced SAME_DOMAIN score
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

# Multi-database platforms where subdomain matters
MULTI_DB_PLATFORMS = [
    'github.io',       # GitHub Pages (many users)
    'shinyapps.io',    # Shiny apps (many users)
    'gbif.org',        # GBIF (many datasets)
    'gxbsidra.org',    # Multiple databases (breastcancer, sepsis, etc.)
    'herokuapp.com',   # Heroku apps
    'netlify.app',     # Netlify apps
]


# =============================================================================
# URL PARSING AND NORMALIZATION
# =============================================================================

def parse_url_components(url):
    """Parse URL into normalized components."""
    if pd.isna(url) or url == '':
        return None
    try:
        url = str(url).strip()
        if not url.startswith(('http://', 'https://', 'ftp://')):
            url = 'http://' + url
        parsed = urlparse(url)
        hostname = parsed.netloc.lower()

        # Strip www prefix
        if hostname.startswith('www.'):
            hostname = hostname[4:]

        domain_parts = hostname.split('.')
        path = parsed.path.lower().rstrip('/').replace('/index.html', '').replace('/index.php', '')

        # Handle compound TLDs (.ac.uk, .edu.cn, etc.)
        if len(domain_parts) >= 3 and domain_parts[-2] in ['ac', 'co', 'edu', 'gov', 'org', 'res']:
            tld = '.'.join(domain_parts[-2:])
            main_domain = domain_parts[-3]
            subdomain = '.'.join(domain_parts[:-3]) if len(domain_parts) > 3 else ''
        elif len(domain_parts) >= 2:
            tld = domain_parts[-1]
            main_domain = domain_parts[-2]
            subdomain = '.'.join(domain_parts[:-2]) if len(domain_parts) > 2 else ''
        else:
            return None

        return {
            'subdomain': subdomain,
            'domain': main_domain,
            'tld': tld,
            'path': path,
            'full_domain': hostname,
            'base_domain': f"{main_domain}.{tld}"
        }
    except:
        return None


def get_effective_domain(url):
    """
    Get effective domain for comparison, preserving subdomain for multi-DB platforms.

    Examples:
    - breastcancer.gxbsidra.org → breastcancer.gxbsidra.org (keep subdomain)
    - www.ebi.ac.uk → ebi.ac.uk (strip www, use base)
    - http://2d.bjmu.edu.cn → bjmu.edu.cn (institutional, use base)
    """
    c = parse_url_components(url)
    if not c:
        return None

    # Check if it's a multi-database platform
    for platform in MULTI_DB_PLATFORMS:
        if c['full_domain'].endswith(platform):
            return c['full_domain']  # Keep full domain including subdomain

    # For other domains, use base domain
    return c['base_domain']


def is_generic_domain(base_domain):
    """Check if domain is a generic institutional domain."""
    if not base_domain:
        return False
    for generic in GENERIC_INSTITUTIONAL_DOMAINS:
        if base_domain.endswith(generic):
            return True
    return False


def normalize_url(url):
    """Normalize URL for similarity comparison."""
    c = parse_url_components(url)
    if not c:
        return ''
    return f"{c['full_domain']}{c['path']}"


def normalize_url_strict(url):
    """
    Strictly normalize URL for exact matching.
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


def url_similarity(url1, url2):
    """Compute URL similarity score (0.0 to 1.0)."""
    # First check for exact match after strict normalization
    if urls_match_exactly(url1, url2):
        return 1.0

    n1 = normalize_url(url1)
    n2 = normalize_url(url2)
    if not n1 or not n2:
        return 0.0
    if n1 == n2:
        return 1.0
    return SequenceMatcher(None, n1, n2).ratio()


# =============================================================================
# NAME MATCHING RULES
# =============================================================================

def is_digit_only_diff(name1, name2):
    """
    Check if difference is only digits (version numbers).
    Examples: SUBA vs SUBA3, HomeoDB vs homeodb2
    """
    n1_no_digits = re.sub(r'\d+', '', name1.lower())
    n2_no_digits = re.sub(r'\d+', '', name2.lower())
    return n1_no_digits == n2_no_digits and n1_no_digits != '' and len(n1_no_digits) >= 2


def is_db_suffix_diff(name1, name2):
    """
    Check if difference is DB/database suffix.
    Examples: Gene vs GeneDB, HOCTAR vs hoctardb
    """
    suffixes = ['db', 'database', 'base']
    n1, n2 = name1.lower(), name2.lower()
    for suffix in suffixes:
        n1_stripped = re.sub(f'{suffix}$', '', n1)
        n2_stripped = re.sub(f'{suffix}$', '', n2)
        if n1_stripped and n2_stripped:
            if n1_stripped == n2_stripped or n1_stripped == n2 or n2_stripped == n1:
                return True
    return False


def edit_distance(s1, s2):
    """Compute Levenshtein edit distance."""
    if len(s1) < len(s2):
        return edit_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    prev = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        for j, c2 in enumerate(s2):
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (c1 != c2)))
        prev = curr
    return prev[-1]


def calculate_contains_score(shorter, longer):
    """
    Context-aware contains score.
    Returns higher score if shorter is a complete token or significant portion.
    """
    if len(shorter) < 3 or len(longer) < 3:
        return 0.0

    shorter_lower = shorter.lower()
    longer_lower = longer.lower()

    if shorter_lower not in longer_lower:
        return 0.0

    # Check if shorter is a complete token (word boundary)
    tokens = re.findall(r'\b\w+\b', longer_lower)
    if shorter_lower in tokens:
        return 0.8  # Complete token match

    # Check if significant portion (>60%)
    if len(shorter) / len(longer) >= 0.6:
        return 0.6

    # Check if likely acronym
    if is_likely_acronym(shorter, longer):
        return 0.7

    # Weak substring match
    return 0.3


def is_likely_acronym(short, long):
    """Check if short could be acronym of long."""
    words = re.findall(r'\b[A-Za-z]\w*', long)
    if len(short) < 2 or len(words) < 2:
        return False

    # Check if first letters match
    initials = ''.join(w[0] for w in words).lower()
    if short.lower() == initials:
        return True

    # Check if uppercase letters match
    caps = ''.join(c for c in long if c.isupper())
    if short.upper() == caps and len(caps) >= 2:
        return True

    return False


# =============================================================================
# IMPROVED MATCHING ALGORITHM
# =============================================================================

def compute_match_score(extracted_name, baseline_name, extracted_url=None, baseline_url=None,
                        extracted_long=None, baseline_long=None):
    """
    Compute match score with all improvements.

    Returns:
        dict with score, signals, confidence, and review flags
    """
    extracted = extracted_name.lower().strip()
    baseline = baseline_name.lower().strip()

    score = 0.0
    signals = []
    review_flags = []

    # Parse URLs
    extracted_domain = get_effective_domain(extracted_url)
    baseline_domain = get_effective_domain(baseline_url)
    extracted_base = parse_url_components(extracted_url)
    baseline_base = parse_url_components(baseline_url)

    # ==========================================================================
    # RULE 1: Same effective domain (with reduced weight for generic domains)
    # ==========================================================================
    same_domain = False
    if extracted_domain and baseline_domain and extracted_domain == baseline_domain:
        same_domain = True
        base_domain = extracted_base['base_domain'] if extracted_base else ''

        if is_generic_domain(base_domain):
            # Generic institutional domain - weak signal
            score += 0.25
            signals.append("SAME_DOMAIN_GENERIC")
            review_flags.append("GENERIC_DOMAIN")
        else:
            # Specific domain - moderate signal
            score += 0.75
            signals.append("SAME_DOMAIN_SPECIFIC")

    # ==========================================================================
    # RULE 2: Digit-only difference (version numbers)
    # ==========================================================================
    if is_digit_only_diff(extracted, baseline):
        score += 1.5
        signals.append("DIGIT_VERSION_DIFF")

    # ==========================================================================
    # RULE 3: DB suffix difference
    # ==========================================================================
    if is_db_suffix_diff(extracted, baseline):
        score += 1.0
        signals.append("DB_SUFFIX_DIFF")

    # ==========================================================================
    # RULE 4: Edit distance (with improved weights)
    # ==========================================================================
    ed = edit_distance(extracted, baseline)
    if ed == 0:
        score += 3.0  # Exact match
        signals.append("EXACT_NAME")
    elif ed == 1 and len(extracted) >= 3:
        score += 1.0  # Very close (typo, case)
        signals.append(f"EDIT_DIST_1")
    elif ed == 2 and len(extracted) >= 4:
        score += 0.6  # Close
        signals.append(f"EDIT_DIST_2")
    elif ed == 3 and len(extracted) >= 5:
        score += 0.3
        signals.append(f"EDIT_DIST_3")

    # ==========================================================================
    # RULE 5: Contains (context-aware)
    # ==========================================================================
    if len(extracted) >= 4 and len(baseline) >= 4:
        contains_score = 0.0
        if extracted in baseline:
            contains_score = calculate_contains_score(extracted, baseline)
        elif baseline in extracted:
            contains_score = calculate_contains_score(baseline, extracted)

        if contains_score > 0:
            score += contains_score
            signals.append(f"CONTAINS_{contains_score:.1f}")

    # ==========================================================================
    # RULE 6: URL matching (improved 2025-11-27)
    # ==========================================================================
    if extracted_url and baseline_url:
        # Check for exact URL match first (handles www, trailing slash, protocol)
        if urls_match_exactly(extracted_url, baseline_url):
            score += 2.0  # Strong signal - URLs are identical
            signals.append("URL_EXACT_MATCH")
        else:
            url_sim = url_similarity(extracted_url, baseline_url)
            if url_sim >= 0.95:
                score += 1.0
                signals.append(f"URL_SIM_{url_sim:.2f}")
            elif url_sim >= 0.8:
                score += 0.6
                signals.append(f"URL_SIM_{url_sim:.2f}")
            elif url_sim >= 0.6:
                score += 0.3
                signals.append(f"URL_SIM_{url_sim:.2f}")

    # ==========================================================================
    # RULE 7: Long name similarity
    # ==========================================================================
    if extracted_long and baseline_long:
        long_sim = SequenceMatcher(None, extracted_long.lower(), baseline_long.lower()).ratio()
        if long_sim >= 0.9:
            score += 0.8
            signals.append(f"LONG_SIM_{long_sim:.2f}")
        elif long_sim >= 0.7:
            score += 0.4
            signals.append(f"LONG_SIM_{long_sim:.2f}")

    # ==========================================================================
    # DETERMINE CONFIDENCE (require multiple signals for HIGH)
    # ==========================================================================
    signal_count = len([s for s in signals if not s.startswith('SAME_DOMAIN')])

    # Special handling: SAME_DOMAIN alone is NOT enough for HIGH
    if score >= 2.0 and signal_count >= 1:
        confidence = 'HIGH'
        is_match = 'Y'
    elif score >= 1.0:
        confidence = 'MEDIUM'
        is_match = 'MAYBE'
    elif score >= 0.5:
        confidence = 'LOW'
        is_match = 'N'
    else:
        confidence = 'NONE'
        is_match = 'N'

    # ==========================================================================
    # ADD REVIEW FLAGS
    # ==========================================================================
    # High edit distance but marked as match
    if ed > 10 and is_match == 'Y':
        review_flags.append('HIGH_ED_MATCH')

    # SAME_DOMAIN only (no other meaningful signals)
    if same_domain and signal_count == 0:
        review_flags.append('SAME_DOMAIN_ONLY')
        # Downgrade to MAYBE if only signal is generic domain
        if is_match == 'Y':
            is_match = 'MAYBE'
            confidence = 'MEDIUM'

    # Score near threshold
    if 1.9 <= score < 2.1:
        review_flags.append('THRESHOLD_EDGE')

    return {
        'score': round(score, 2),
        'signals': signals,
        'signal_count': signal_count + (1 if same_domain else 0),
        'edit_distance': ed,
        'same_domain': same_domain,
        'confidence': confidence,
        'is_match': is_match,
        'review_flags': review_flags
    }


def resolve_one_to_one_conflicts(matches_df):
    """
    Enforce one-to-one mapping: each baseline database matches at most one extracted.

    For conflicts, keep the best match based on:
    1. EXACT matches first
    2. Highest score
    3. Lowest edit distance
    4. Shortest name difference
    """
    if len(matches_df) == 0:
        return matches_df, pd.DataFrame()

    # Only process Y matches for conflict resolution
    y_matches = matches_df[matches_df['is_match'] == 'Y'].copy()
    other_matches = matches_df[matches_df['is_match'] != 'Y'].copy()

    if len(y_matches) == 0:
        return matches_df, pd.DataFrame()

    # Group by baseline_name
    resolved = []
    conflicts = []

    for baseline, group in y_matches.groupby('baseline_name'):
        if len(group) == 1:
            resolved.append(group.iloc[0])
        else:
            # Multiple matches - resolve conflict
            # Sort by: score desc, edit_distance asc, name length similarity
            sorted_group = group.sort_values(
                by=['score', 'edit_distance'],
                ascending=[False, True]
            )

            # Keep best match
            best = sorted_group.iloc[0].copy()
            best['conflict_resolved'] = True
            resolved.append(best)

            # Mark others as conflicts
            for idx in range(1, len(sorted_group)):
                conflict = sorted_group.iloc[idx].copy()
                conflict['conflict_winner'] = sorted_group.iloc[0]['extracted_name']
                conflict['is_match'] = 'CONFLICT'
                conflict['confidence'] = 'REJECTED'
                conflicts.append(conflict)

    resolved_df = pd.DataFrame(resolved)
    conflicts_df = pd.DataFrame(conflicts) if conflicts else pd.DataFrame()

    # Combine resolved Y matches with other matches
    final_df = pd.concat([resolved_df, other_matches], ignore_index=True)

    return final_df, conflicts_df


# =============================================================================
# MAIN MATCHING FUNCTION
# =============================================================================

def run_fuzzy_matching(extracted_df, baseline_df,
                       extracted_name_col='database_name',
                       extracted_long_col='long_database_name',
                       extracted_url_col='resource_url',
                       baseline_name_col='best_name',
                       baseline_long_col='best_full',
                       baseline_url_col='extracted_url'):
    """
    Run improved fuzzy matching between extracted and baseline datasets.

    Returns:
        matches_df: All potential matches with scores
        conflicts_df: Resolved conflicts (rejected matches)
        filter_list: List of extracted names that should be filtered (match baseline)
    """
    print("=== IMPROVED FUZZY MATCHING v3 ===\n")

    # Build baseline lookup
    print("Building baseline index...")
    baseline_lookup = {}
    domain_index = defaultdict(set)
    prefix_index = defaultdict(set)

    for _, row in baseline_df.iterrows():
        name = str(row[baseline_name_col]).lower().strip()
        url = str(row.get(baseline_url_col, ''))
        long_name = str(row.get(baseline_long_col, ''))

        baseline_lookup[name] = {
            'original_name': row[baseline_name_col],
            'long_name': long_name,
            'url': url
        }

        # Index by domain
        domain = get_effective_domain(url)
        if domain:
            domain_index[domain].add(name)

        # Index by prefix
        if len(name) >= 2:
            prefix_index[name[:2]].add(name)
        if len(name) >= 3:
            prefix_index[name[:3]].add(name)

        # Index by digit-stripped version
        stripped = re.sub(r'\d+', '', name)
        if len(stripped) >= 2:
            prefix_index[stripped[:2]].add(name)

    print(f"  Baseline entries: {len(baseline_lookup)}")
    print(f"  Unique domains: {len(domain_index)}")

    # Get unique extracted names
    extracted_unique = extracted_df[[extracted_name_col, extracted_long_col]].dropna(subset=[extracted_name_col])
    extracted_unique = extracted_unique.drop_duplicates(subset=[extracted_name_col])
    print(f"  Unique extracted names: {len(extracted_unique)}")

    # Build URL lookup for extracted
    extracted_urls = {}
    if extracted_url_col in extracted_df.columns:
        for _, row in extracted_df.iterrows():
            name = str(row[extracted_name_col]).lower().strip()
            if pd.notna(row.get(extracted_url_col)):
                extracted_urls[name] = str(row[extracted_url_col])

    # Run matching
    print("\nRunning matching...")
    results = []

    for _, row in extracted_unique.iterrows():
        extracted = str(row[extracted_name_col]).lower().strip()
        extracted_long = str(row[extracted_long_col]) if pd.notna(row[extracted_long_col]) else ''
        extracted_url = extracted_urls.get(extracted, '')
        extracted_domain = get_effective_domain(extracted_url)

        # EXACT MATCH
        if extracted in baseline_lookup:
            bdata = baseline_lookup[extracted]
            results.append({
                'extracted_name': row[extracted_name_col],
                'extracted_long': row[extracted_long_col],
                'extracted_url': extracted_url,
                'baseline_name': bdata['original_name'],
                'baseline_long': bdata['long_name'],
                'baseline_url': bdata['url'],
                'score': 10.0,
                'signals': 'EXACT',
                'signal_count': 1,
                'edit_distance': 0,
                'same_domain': True,
                'confidence': 'HIGH',
                'is_match': 'Y',
                'review_flags': ''
            })
            continue

        # Build candidate set
        candidates = set()
        if len(extracted) >= 2:
            candidates.update(prefix_index.get(extracted[:2], set()))
        if len(extracted) >= 3:
            candidates.update(prefix_index.get(extracted[:3], set()))
        stripped = re.sub(r'\d+', '', extracted)
        if len(stripped) >= 2:
            candidates.update(prefix_index.get(stripped[:2], set()))

        # Add candidates from same domain
        if extracted_domain:
            candidates.update(domain_index.get(extracted_domain, set()))

        # Add substring candidates
        for bname in baseline_lookup.keys():
            if len(extracted) >= 4 and len(bname) >= 4:
                if extracted in bname or bname in extracted:
                    candidates.add(bname)

        # Score each candidate
        best_match = None
        best_result = None

        for bname in candidates:
            bdata = baseline_lookup[bname]

            result = compute_match_score(
                extracted_name=extracted,
                baseline_name=bname,
                extracted_url=extracted_url,
                baseline_url=bdata['url'],
                extracted_long=extracted_long,
                baseline_long=bdata['long_name']
            )

            if result['score'] >= 0.5:
                if best_result is None or result['score'] > best_result['score']:
                    best_match = bname
                    best_result = result
                    best_result['baseline_data'] = bdata

        if best_result:
            results.append({
                'extracted_name': row[extracted_name_col],
                'extracted_long': row[extracted_long_col],
                'extracted_url': extracted_url,
                'baseline_name': best_result['baseline_data']['original_name'],
                'baseline_long': best_result['baseline_data']['long_name'],
                'baseline_url': best_result['baseline_data']['url'],
                'score': best_result['score'],
                'signals': ', '.join(best_result['signals']),
                'signal_count': best_result['signal_count'],
                'edit_distance': best_result['edit_distance'],
                'same_domain': best_result['same_domain'],
                'confidence': best_result['confidence'],
                'is_match': best_result['is_match'],
                'review_flags': ', '.join(best_result['review_flags'])
            })

    # Create DataFrame
    matches_df = pd.DataFrame(results)
    matches_df = matches_df.sort_values('score', ascending=False)

    # Resolve one-to-one conflicts
    print("\nResolving one-to-one conflicts...")
    matches_df, conflicts_df = resolve_one_to_one_conflicts(matches_df)

    # Generate filter list
    filter_list = matches_df[matches_df['is_match'] == 'Y']['extracted_name'].tolist()

    # Print summary
    print(f"\n{'='*60}")
    print("MATCHING SUMMARY")
    print(f"{'='*60}")
    print(f"Total potential matches: {len(matches_df)}")
    if len(matches_df) > 0:
        print(f"\nis_match breakdown:")
        print(matches_df['is_match'].value_counts().to_string())

        y_matches = matches_df[matches_df['is_match'] == 'Y']
        print(f"\nHIGH confidence matches: {len(y_matches)}")
        print(f"  - EXACT: {len(y_matches[y_matches['signals'].str.contains('EXACT', na=False)])}")
        print(f"  - DIGIT_VERSION: {len(y_matches[y_matches['signals'].str.contains('DIGIT_VERSION', na=False)])}")
        print(f"  - DB_SUFFIX: {len(y_matches[y_matches['signals'].str.contains('DB_SUFFIX', na=False)])}")

        if len(conflicts_df) > 0:
            print(f"\nConflicts resolved: {len(conflicts_df)}")

        flagged = matches_df[matches_df['review_flags'] != '']
        if len(flagged) > 0:
            print(f"\nMatches with review flags: {len(flagged)}")

    print(f"\nFilter list: {len(filter_list)} names")

    return matches_df, conflicts_df, filter_list


# =============================================================================
# STANDALONE EXECUTION
# =============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Run improved fuzzy matching')
    parser.add_argument('--extracted', required=True, help='Path to extracted CSV')
    parser.add_argument('--baseline', required=True, help='Path to baseline CSV')
    parser.add_argument('--output', required=True, help='Output path for matches CSV')
    args = parser.parse_args()

    # Load data
    extracted_df = pd.read_csv(args.extracted)
    baseline_df = pd.read_csv(args.baseline)

    # Run matching
    matches_df, conflicts_df, filter_list = run_fuzzy_matching(extracted_df, baseline_df)

    # Save results
    matches_df.to_csv(args.output, index=False, quoting=1)
    print(f"\nSaved matches to: {args.output}")

    if len(conflicts_df) > 0:
        conflicts_path = args.output.replace('.csv', '_conflicts.csv')
        conflicts_df.to_csv(conflicts_path, index=False, quoting=1)
        print(f"Saved conflicts to: {conflicts_path}")

    filter_path = args.output.replace('.csv', '_filter_list.txt')
    with open(filter_path, 'w') as f:
        for name in sorted(set(filter_list)):
            f.write(f"{name}\n")
    print(f"Saved filter list to: {filter_path}")
