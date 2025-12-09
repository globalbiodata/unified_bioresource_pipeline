#!/usr/bin/env python3
"""
Script 24: Check URLs with Geolocation

Purpose: Check URL status, add geolocation data, and handle Wayback fallback
         - Live URLs: get status code + geo (country, coordinates)
         - Dead URLs: check Wayback Machine for archive
         - No URL available: exclude to separate file
         - Validate URLs against blocked/review patterns

Authors: AI Assistant
Date: 2025-11-27
Updated: 2025-11-28 (added URL pattern validation)
"""

import argparse
import json
import re
import socket
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Tuple, Union

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add lib imports
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from lib.session_utils import get_session_path, validate_session_dir


# ============================================================================
# URL Pattern Validation
# ============================================================================

# URLs matching these patterns are BLOCKED (not bioresources)
BLOCKED_URL_PATTERNS = [
    r'oxfordjournals\.org',           # NAR database list pages
    r'academic\.oup\.com/nar',        # NAR articles (same as above, different domain)
    r'mozilla\.org',                  # Browser website
    r'mozilla\.com',                  # Browser website
    # Repository hosting sites
    r'bitbucket\.org',                # Code repository
    r'gitlab\.com',                   # Code repository
    r'sourceforge\.net',              # Code repository
    # File download patterns (not resource landing pages)
    r'\.pdf($|\?)',                   # PDF files
    r'\.xlsx?($|\?)',                 # Excel files
    r'\.docx?($|\?)',                 # Word files
    r'\.zip($|\?)',                   # ZIP archives
    r'\.tar\.gz($|\?)',               # Tarball archives
    r'\.tar($|\?)',                   # Tar archives
    r'\.gz($|\?)',                    # Gzip files
    r'\.rar($|\?)',                   # RAR archives
    r'\.7z($|\?)',                    # 7zip archives
]

# URLs matching these patterns need REVIEW (may or may not be valid)
REVIEW_URL_PATTERNS = [
    r'github\.io',                    # GitHub pages - could be valid resource sites
    r'github\.com',                   # GitHub repos - could be valid
    r'clinicaltrials\.gov',           # Trial registry - usually not bioresources
]


def validate_url_pattern(url: str) -> str:
    """
    Check URL against known problematic patterns.

    Returns:
        'blocked' - URL matches blocked pattern, should be excluded
        'review' - URL matches review pattern, needs human review
        'ok' - URL is fine
    """
    if not url or pd.isna(url):
        return 'ok'

    url_str = str(url).lower()

    for pattern in BLOCKED_URL_PATTERNS:
        if re.search(pattern, url_str):
            return 'blocked'

    for pattern in REVIEW_URL_PATTERNS:
        if re.search(pattern, url_str):
            return 'review'

    return 'ok'


class Args(NamedTuple):
    """Command-line arguments"""
    session_dir: Path
    workers: int
    timeout: int
    skip_geo: bool
    skip_wayback: bool


class URLResult(NamedTuple):
    """Result of URL check"""
    url: str
    status: Union[int, str]
    country: str
    coordinates: str
    wayback_url: str
    is_live: bool
    url_validation: str  # 'ok', 'blocked', or 'review'


def get_args() -> Args:
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Check URL status with geolocation and Wayback fallback',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 24_check_urls_with_geo.py --session-dir 2025-12-04-111420-z381s
  python 24_check_urls_with_geo.py --session-dir 2025-12-04-111420-z381s --workers 20 --skip-wayback
        """
    )

    parser.add_argument(
        '--session-dir',
        type=str,
        required=True,
        help='Session directory path'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='Number of concurrent workers (default: 10)'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=10,
        help='Request timeout in seconds (default: 10)'
    )
    parser.add_argument(
        '--skip-geo',
        action='store_true',
        help='Skip geolocation lookups'
    )
    parser.add_argument(
        '--skip-wayback',
        action='store_true',
        help='Skip Wayback Machine lookups'
    )

    args = parser.parse_args()

    return Args(
        session_dir=Path(args.session_dir).resolve(),
        workers=args.workers,
        timeout=args.timeout,
        skip_geo=args.skip_geo,
        skip_wayback=args.skip_wayback
    )


def get_session(timeout: int) -> requests.Session:
    """Create a requests session with retry logic"""
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def extract_domain(url: str) -> str:
    """Extract domain from URL"""
    domain = re.sub(r'https?://', '', url)
    domain = re.sub(r'/.*$', '', domain)
    return domain


def get_ip_location(url: str) -> Tuple[str, str]:
    """Get country and coordinates from URL's IP address"""
    try:
        domain = extract_domain(url)
        ip = socket.gethostbyname(domain)

        # Try ipinfo.io first
        try:
            r = requests.get(f'https://ipinfo.io/{ip}/json', timeout=5)
            if r.status_code == 200:
                data = r.json()
                country = data.get('country', '')
                loc = data.get('loc', '')
                if loc:
                    lat, lon = loc.split(',')
                    coordinates = f"({lat},{lon})"
                else:
                    coordinates = ''
                return country, coordinates
        except Exception:
            pass

        # Fallback to ip-api.com
        try:
            r = requests.get(f'http://ip-api.com/json/{ip}', timeout=5)
            if r.status_code == 200:
                data = r.json()
                country = data.get('country', '')
                lat = data.get('lat', '')
                lon = data.get('lon', '')
                coordinates = f"({lat},{lon})" if lat and lon else ''
                return country, coordinates
        except Exception:
            pass

    except Exception:
        pass

    return '', ''


def check_wayback(url: str) -> str:
    """Check Wayback Machine for archived version"""
    try:
        r = requests.get(
            f'http://archive.org/wayback/available?url={url}',
            headers={'User-agent': 'biodata_resource_inventory'},
            timeout=10
        )

        if r.status_code in [503, 504]:
            return 'wayback_unavailable'

        if r.status_code == 200:
            data = r.json()
            snapshots = data.get('archived_snapshots', {})
            closest = snapshots.get('closest', {})
            return closest.get('url', 'no_wayback')

    except Exception as e:
        return f'wayback_error: {str(e)[:50]}'

    return 'no_wayback'


def check_url(url: str, session: requests.Session, timeout: int,
              skip_geo: bool = False, skip_wayback: bool = False) -> URLResult:
    """
    Check a single URL and return result with geo/wayback info.

    Logic:
    - First validate URL pattern (blocked/review/ok)
    - If URL is live (status < 400): get geo info, no wayback needed
    - If URL is dead (status >= 400 or error): check wayback
    """
    status: Union[int, str] = ''
    country = ''
    coordinates = ''
    wayback_url = ''
    is_live = False
    url_validation = 'ok'

    if not url or pd.isna(url) or url.strip() == '':
        return URLResult(url='', status='no_url', country='', coordinates='',
                        wayback_url='', is_live=False, url_validation='ok')

    url = url.strip()

    # Validate URL pattern first
    url_validation = validate_url_pattern(url)

    # Check URL status
    try:
        r = session.head(url, timeout=timeout, allow_redirects=True)
        status = r.status_code

        if status < 400:
            is_live = True
            # Get geolocation for live URLs
            if not skip_geo:
                country, coordinates = get_ip_location(url)
        else:
            # Dead URL - check wayback
            if not skip_wayback:
                wayback_url = check_wayback(url)

    except requests.exceptions.RequestException as e:
        status = str(e)[:100]
        # Error = dead URL, check wayback
        if not skip_wayback:
            wayback_url = check_wayback(url)

    return URLResult(
        url=url,
        status=status,
        country=country,
        coordinates=coordinates,
        wayback_url=wayback_url,
        is_live=is_live,
        url_validation=url_validation
    )


def process_urls(urls: List[str], workers: int, timeout: int,
                 skip_geo: bool, skip_wayback: bool) -> Dict[str, URLResult]:
    """Process multiple URLs concurrently with rate limiting"""

    # Get unique URLs to avoid duplicate requests
    unique_urls = list(set([u for u in urls if u and pd.notna(u) and u.strip()]))
    results = {}

    print(f"  Processing {len(unique_urls)} unique URLs with {workers} workers...")

    session = get_session(timeout)

    # Track domain request times for rate limiting
    domain_last_request = defaultdict(float)
    rate_limit_delay = 1.0  # 1 second between requests to same domain

    def check_with_rate_limit(url: str) -> URLResult:
        """Check URL with domain-based rate limiting"""
        domain = extract_domain(url)

        # Rate limiting
        elapsed = time.time() - domain_last_request[domain]
        if elapsed < rate_limit_delay:
            time.sleep(rate_limit_delay - elapsed)

        domain_last_request[domain] = time.time()

        return check_url(url, session, timeout, skip_geo, skip_wayback)

    # Process with thread pool
    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(check_with_rate_limit, url): url for url in unique_urls}

        for future in as_completed(futures):
            url = futures[future]
            try:
                result = future.result()
                results[url] = result
            except Exception as e:
                results[url] = URLResult(
                    url=url, status=f'error: {str(e)[:50]}',
                    country='', coordinates='', wayback_url='', is_live=False,
                    url_validation=validate_url_pattern(url)
                )

            completed += 1
            if completed % 100 == 0:
                print(f"    Processed {completed}/{len(unique_urls)} URLs...")

    session.close()
    return results


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
    input_file = get_session_path(SESSION_DIR, '09_finalization', 'transformed_resources.csv')
    output_dir = get_session_path(SESSION_DIR, '09_finalization')

    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)

    print(f"Phase 9 - Script 24: Check URLs with Geolocation")
    print(f"=" * 80)
    print(f"Session: {SESSION_DIR.name}")
    print(f"Input: {input_file.relative_to(SESSION_DIR)}")
    print(f"Output directory: {output_dir.relative_to(SESSION_DIR)}")
    print(f"Workers: {args.workers}")
    print(f"Timeout: {args.timeout}s")
    print(f"Skip geo: {args.skip_geo}")
    print(f"Skip wayback: {args.skip_wayback}")
    print()

    # Load input
    print("Loading transformed resources...")
    df = pd.read_csv(input_file)
    print(f"  Loaded {len(df)} rows")
    print()

    # Process URLs
    print("Checking URLs...")
    start_time = time.time()

    urls = df['extracted_url'].tolist()
    url_results = process_urls(urls, args.workers, args.timeout,
                               args.skip_geo, args.skip_wayback)

    elapsed = time.time() - start_time
    print(f"  URL checking completed in {elapsed:.1f} seconds")
    print()

    # Apply results to dataframe
    print("Applying results to dataframe...")

    def apply_result(url):
        if not url or pd.isna(url) or url.strip() == '':
            return URLResult(url='', status='no_url', country='', coordinates='',
                           wayback_url='', is_live=False, url_validation='ok')
        return url_results.get(url.strip(), URLResult(
            url=url, status='not_checked', country='', coordinates='',
            wayback_url='', is_live=False, url_validation=validate_url_pattern(url)
        ))

    results_series = df['extracted_url'].apply(apply_result)

    df['extracted_url_status'] = results_series.apply(lambda r: r.status)
    df['extracted_url_country'] = results_series.apply(lambda r: r.country)
    df['extracted_url_coordinates'] = results_series.apply(lambda r: r.coordinates)
    df['wayback_url'] = results_series.apply(lambda r: r.wayback_url)
    df['url_validation'] = results_series.apply(lambda r: r.url_validation)

    # Determine which rows to exclude
    # Exclude if: blocked URL pattern OR (no live URL AND no wayback)
    def should_exclude(row):
        url = row['extracted_url']
        status = row['extracted_url_status']
        wayback = row['wayback_url']
        url_val = row.get('url_validation', 'ok')

        # Blocked URL pattern - always exclude
        if url_val == 'blocked':
            return True

        # No URL at all
        if not url or pd.isna(url) or str(url).strip() == '':
            return True

        # Check if live
        try:
            if isinstance(status, int) and status < 400:
                return False  # Live URL, keep
        except:
            pass

        # Dead URL - check wayback
        if wayback and wayback not in ['', 'no_wayback', 'wayback_unavailable'] and not wayback.startswith('wayback_error'):
            return False  # Has wayback, keep

        return True  # No live URL and no wayback, exclude

    df['_exclude'] = df.apply(should_exclude, axis=1)

    # Split into included and excluded
    included_df = df[~df['_exclude']].drop('_exclude', axis=1).copy()
    excluded_df = df[df['_exclude']].drop('_exclude', axis=1).copy()

    # Statistics
    live_count = sum(1 for r in results_series if r.is_live)
    wayback_count = sum(1 for r in results_series
                       if not r.is_live and r.wayback_url
                       and r.wayback_url not in ['', 'no_wayback', 'wayback_unavailable']
                       and not r.wayback_url.startswith('wayback_error'))
    blocked_count = sum(1 for r in results_series if r.url_validation == 'blocked')
    review_count = sum(1 for r in results_series if r.url_validation == 'review')

    stats = {
        'script': '24_check_urls_with_geo',
        'timestamp': datetime.now().isoformat(),
        'session': SESSION_DIR.name,
        'runtime_seconds': round(elapsed, 1),
        'input_rows': len(df),
        'unique_urls_checked': len(url_results),
        'live_urls': live_count,
        'wayback_rescued': wayback_count,
        'url_validation': {
            'blocked': blocked_count,
            'review_needed': review_count,
            'ok': len(df) - blocked_count - review_count
        },
        'included_rows': len(included_df),
        'excluded_rows': len(excluded_df),
        'exclusion_rate': round(len(excluded_df) / len(df) * 100, 2) if len(df) > 0 else 0
    }

    print(f"  Results summary:")
    print(f"    - Live URLs: {live_count}")
    print(f"    - Wayback rescued: {wayback_count}")
    print(f"    - Blocked URL patterns: {blocked_count}")
    print(f"    - URLs needing review: {review_count}")
    print(f"    - Included: {len(included_df)} rows")
    print(f"    - Excluded (no URL/blocked): {len(excluded_df)} rows")
    print()

    # Save outputs
    output_file = output_dir / 'url_checked_resources.csv'
    included_df.to_csv(output_file, index=False)
    print(f"Saved URL-checked resources to: {output_file.relative_to(SESSION_DIR)}")

    excluded_file = output_dir / 'excluded_no_url.csv'
    excluded_df.to_csv(excluded_file, index=False)
    print(f"Saved excluded resources to: {excluded_file.relative_to(SESSION_DIR)}")

    # Save detailed URL check results
    url_results_list = [
        {
            'url': r.url,
            'status': r.status,
            'country': r.country,
            'coordinates': r.coordinates,
            'wayback_url': r.wayback_url,
            'is_live': r.is_live,
            'url_validation': r.url_validation
        }
        for r in url_results.values()
    ]
    url_results_file = output_dir / 'url_check_results.csv'
    pd.DataFrame(url_results_list).to_csv(url_results_file, index=False)
    print(f"Saved URL check details to: {url_results_file.relative_to(SESSION_DIR)}")

    # Save URLs needing review in a separate file
    review_df = included_df[included_df['url_validation'] == 'review'].copy()
    if len(review_df) > 0:
        review_file = output_dir / 'urls_for_review.csv'
        review_cols = ['ID', 'best_name', 'extracted_url', 'url_validation', 'extracted_url_status']
        review_df[review_cols].to_csv(review_file, index=False)
        print(f"Saved URLs needing review to: {review_file.relative_to(SESSION_DIR)}")

    stats_file = output_dir / 'script_24_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved statistics to: {stats_file.relative_to(SESSION_DIR)}")

    print()
    print(f"Done! {len(included_df)} resources with valid URLs, {len(excluded_df)} excluded")


if __name__ == '__main__':
    main()
