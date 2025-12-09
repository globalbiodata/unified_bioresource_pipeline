#!/usr/bin/env python3
"""
URL Patterns and Exclusion Rules for URL Recovery

Shared constants used across all URL recovery scripts.
Based on lessons learned from url_recovery_v2 analysis.

Author: Pipeline Team
Date: 2025-11-28
"""

import re
from typing import List, Tuple

# =============================================================================
# URL EXTRACTION PATTERNS
# =============================================================================

# Patterns to find URLs (ordered by specificity)
URL_PATTERNS = [
    # Standard HTTP/HTTPS URLs
    r'https?://[^\s<>"\'\)\]\}]+',

    # Bare domains with path (common TLDs)
    r'\b(?:www\.)?[a-zA-Z0-9][-a-zA-Z0-9]*\.'
    r'(org|edu|gov|com|net|io|eu|uk|de|fr|jp|cn|au|ca|br|ch|nl|se|no|dk|fi)'
    r'/[^\s<>"\'\)\]\}]+',

    # Bare domains without path (restricted to academic TLDs)
    r'\b(?:www\.)?[a-zA-Z0-9][-a-zA-Z0-9]*\.(org|edu|gov)\b',
]

# Compile patterns for efficiency
COMPILED_URL_PATTERNS = [re.compile(p, re.IGNORECASE) for p in URL_PATTERNS]


# =============================================================================
# EXCLUSION RULES
# =============================================================================

# Domains to always exclude (code repos, archives, etc.)
EXCLUDE_DOMAINS = [
    # Code repositories
    'github.com',
    'gitlab.com',
    'bitbucket.org',
    'sourceforge.net',

    # File archives (not web interfaces)
    'zenodo.org',
    'figshare.com',
    'mendeley.com',
    'osf.io',
    'dryad',

    # Package repositories
    'cran.r-project.org',
    'bioconductor.org',
    'pypi.org',
    'npmjs.com',

    # DOI/Identifier services
    'doi.org',
    'orcid.org',
    'creativecommons.org',

    # Generic infrastructure
    'apache.org',
    'mysql.com',
    'jquery.com',
    'amazonaws.com',
    'cloudfront.net',

    # Social/communication
    'twitter.com',
    'linkedin.com',
    'facebook.com',
    'youtube.com',
]

# Reference databases - exclude UNLESS the database name matches
# (e.g., exclude ncbi.nlm.nih.gov unless the resource IS an NCBI database)
REFERENCE_DBS = [
    'ncbi.nlm.nih.gov',
    'ebi.ac.uk',
    'uniprot.org',
    'genome.ucsc.edu',
    'kegg.jp',
    'ensembl.org',
    'string-db.org',
    'interpro',
    'pubmed',
    'pmc',
]

# Generic institutional domains (need specific path to be valid)
GENERIC_INSTITUTIONAL = [
    '.edu',
    '.ac.uk',
    '.edu.au',
    '.edu.cn',
]


# =============================================================================
# QUALITY ASSESSMENT
# =============================================================================

def assess_url_quality(url: str, database_name: str, long_name: str = '') -> str:
    """
    Assess the quality/confidence of a URL match.

    Args:
        url: The URL found
        database_name: Short name of the database
        long_name: Full name of the database

    Returns:
        'HIGH', 'MEDIUM', or 'LOW'
    """
    url_lower = url.lower()
    db_lower = (database_name or '').lower()
    long_lower = (long_name or '').lower()

    # HIGH: URL contains database name or abbreviation
    if db_lower and len(db_lower) > 2 and db_lower in url_lower:
        return 'HIGH'

    # HIGH: URL contains significant words from long name
    if long_lower:
        significant_words = [w for w in long_lower.split() if len(w) > 4]
        if any(w in url_lower for w in significant_words[:3]):
            return 'HIGH'

    # MEDIUM: Context suggests this is the database
    # (This is typically determined by the surrounding text, not just URL)
    # Default to MEDIUM if we have some name match
    if db_lower and any(c in url_lower for c in db_lower if c.isalnum()):
        return 'MEDIUM'

    # LOW: URL found but connection uncertain
    return 'LOW'


def filter_url(url: str, database_name: str = '') -> Tuple[bool, str]:
    """
    Check if a URL should be included or excluded.

    Args:
        url: The URL to check
        database_name: Name of the database (for reference DB check)

    Returns:
        Tuple of (should_include, reason_if_excluded)
    """
    url_lower = url.lower()
    db_lower = (database_name or '').lower()

    # Check explicit exclusions
    for domain in EXCLUDE_DOMAINS:
        if domain in url_lower:
            return False, f'excluded_domain:{domain}'

    # Check FTP
    if url_lower.startswith('ftp://'):
        return False, 'ftp_protocol'

    # Check reference databases
    for ref_db in REFERENCE_DBS:
        if ref_db in url_lower:
            # Only include if database name suggests this IS the resource
            if db_lower and db_lower in url_lower:
                return True, ''
            return False, f'reference_db:{ref_db}'

    # Check generic institutional (need path to be valid)
    for inst in GENERIC_INSTITUTIONAL:
        if url_lower.endswith(inst) or f'{inst}/' not in url_lower:
            # Bare institutional domain without specific path
            if inst in url_lower and '/' not in url_lower.split(inst)[-1]:
                return False, f'generic_institutional:{inst}'

    return True, ''


def extract_urls(text: str) -> List[str]:
    """
    Extract all potential URLs from text using enhanced patterns.

    Args:
        text: Text to search

    Returns:
        List of unique URLs found
    """
    urls = []

    for pattern in COMPILED_URL_PATTERNS:
        matches = pattern.findall(text)
        # findall returns tuples for groups, flatten if needed
        for match in matches:
            if isinstance(match, tuple):
                # Reconstruct URL from capture groups
                continue  # Skip partial matches from grouped patterns
            urls.append(match)

    # Also try simple pattern that catches more
    simple_pattern = r'https?://[^\s<>"\'\)\]\}]+'
    urls.extend(re.findall(simple_pattern, text, re.IGNORECASE))

    # Clean URLs
    cleaned = []
    for url in urls:
        # Remove trailing punctuation
        url = re.sub(r'[.,;:!?\)\]\}]+$', '', url)
        # Normalize
        url = url.strip()
        if url and len(url) > 8:  # Minimum viable URL length
            cleaned.append(url)

    return list(set(cleaned))


def extract_and_filter_urls(
    text: str,
    database_name: str,
    long_name: str = ''
) -> List[Tuple[str, str, str]]:
    """
    Extract URLs from text, filter, and assess quality.

    Args:
        text: Text to search
        database_name: Short name of the database
        long_name: Full name of the database

    Returns:
        List of (url, quality, notes) tuples for valid URLs
    """
    urls = extract_urls(text)
    results = []

    for url in urls:
        should_include, reason = filter_url(url, database_name)

        if should_include:
            quality = assess_url_quality(url, database_name, long_name)
            results.append((url, quality, ''))
        # Optionally track exclusions for debugging
        # else:
        #     results.append((url, 'EXCLUDED', reason))

    # Sort by quality (HIGH first)
    quality_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
    results.sort(key=lambda x: quality_order.get(x[1], 99))

    return results


# =============================================================================
# PMID EXTRACTION
# =============================================================================

def extract_pmids(pmid_str: str) -> List[str]:
    """
    Extract all PMIDs from a string (handles multiple PMIDs).

    Args:
        pmid_str: String potentially containing PMIDs

    Returns:
        List of PMID strings
    """
    if not pmid_str or str(pmid_str).lower() in ('nan', 'none', ''):
        return []

    # PMIDs are typically 7-8 digits
    return re.findall(r'\b\d{7,8}\b', str(pmid_str))
