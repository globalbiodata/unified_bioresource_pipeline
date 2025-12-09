#!/usr/bin/env python3
"""
BioresourceScanner - URL validation and scoring module
V4 - With Wayback Machine fallback support

Originally from: bioresource_url_scanner/scripts/scan_gbc_full.py
Integrated: 2025-12-05
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from urllib.parse import urlparse, urljoin, quote
import threading
import time
import re
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from tqdm import tqdm
from datetime import datetime
from pathlib import Path
import signal
import sys

# Suppress XML parsing warnings (some sites return XML, but HTML parser works fine)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# CONFIGURATION DEFAULTS
DEFAULT_MAX_WORKERS = 10
DEFAULT_DOMAIN_DELAY = 1.0
DEFAULT_TIMEOUT = 20
DEFAULT_MAX_CONTENT_SIZE = 512000
DEFAULT_MAX_META_REDIRECTS = 3
DEFAULT_WAYBACK_TIMEOUT = 15

# INDICATOR SCORES (V3 - proven effective on 964 URLs: 47.2% HIGH+CRITICAL)
INDICATOR_SCORES = {
    'NCBI': 5, 'EBI': 5, 'NIH': 5, 'Ensembl': 5, 'UniProt': 5,
    'search database': 4, 'search our database': 4, 'query database': 4,
    'browse database': 4, 'download data': 4, 'bulk download': 4,
    'data access': 4, 'programmatic access': 4,
    'genomics': 3, 'proteomics': 3, 'bioinformatics': 3, 'genome': 3,
    'gene': 3, 'protein': 3, 'sequence': 3, 'molecular': 3,
    'biological': 3, 'variant': 3, 'mutation': 3, 'expression': 3,
    'repository': 2, 'archive': 2, 'collection': 2, 'resource': 2,
    'tool': 2, 'platform': 2, 'search': 2, 'query': 2, 'browse': 2,
    'download': 2, 'submit': 2, 'curated': 2, 'annotation': 2, 'data': 2,
    'database': 1, 'server': 1, 'portal': 1, 'web service': 1,
}

TITLE_KEYWORDS = ['database', 'server', 'portal', 'resource', 'tool', 'repository', 'archive', 'collection']
TITLE_BONUS = 5

# Global state for signal handler (Ctrl+C)
_current_results = []
_checkpoint_file = None
_interrupt_flag = False


def _save_checkpoint(results, checkpoint_file):
    """Save partial results to checkpoint file"""
    if checkpoint_file and results:
        try:
            df = pd.DataFrame(results)
            df['indicators_found'] = df['indicators_found'].apply(
                lambda x: '; '.join(x) if isinstance(x, list) else ''
            )
            df.to_csv(checkpoint_file, index=False)
        except Exception as e:
            print(f"\nWarning: Failed to save checkpoint: {e}", file=sys.stderr)


def _signal_handler(signum, frame):
    """Handle Ctrl+C interrupt - save completed results before exiting"""
    global _interrupt_flag, _current_results, _checkpoint_file
    _interrupt_flag = True
    completed_count = len(_current_results)
    print(f"\n\nInterrupted! Saving {completed_count} completed results...")
    _save_checkpoint(_current_results, _checkpoint_file)
    if _checkpoint_file:
        print(f"Partial results saved to: {_checkpoint_file}")
    # Use os._exit() to force immediate termination - sys.exit() doesn't work
    # inside ThreadPoolExecutor context as it waits for all threads to complete
    import os
    os._exit(0)


class DomainRateLimiter:
    """Thread-safe rate limiter that limits requests per domain"""
    def __init__(self, delay_seconds=1.0):
        self.delay = delay_seconds
        self.last_request = {}
        self.lock = threading.Lock()

    def wait_if_needed(self, url):
        try:
            domain = urlparse(url).netloc
        except:
            domain = "unknown"
        with self.lock:
            if domain in self.last_request:
                elapsed = time.time() - self.last_request[domain]
                if elapsed < self.delay:
                    time.sleep(self.delay - elapsed)
            self.last_request[domain] = time.time()
        return domain


class BioresourceScanner:
    """V4 Scanner with meta refresh redirect and Wayback Machine support"""
    def __init__(self, max_workers=DEFAULT_MAX_WORKERS, domain_delay=DEFAULT_DOMAIN_DELAY,
                 timeout=DEFAULT_TIMEOUT, max_content_size=DEFAULT_MAX_CONTENT_SIZE,
                 max_meta_redirects=DEFAULT_MAX_META_REDIRECTS, wayback_timeout=DEFAULT_WAYBACK_TIMEOUT):
        self.max_workers = max_workers
        self.timeout = timeout
        self.max_content_size = max_content_size
        self.max_meta_redirects = max_meta_redirects
        self.wayback_timeout = wayback_timeout
        self.rate_limiter = DomainRateLimiter(domain_delay)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'BioresourceScanner/4.0 (GBC Publication Analysis + Wayback - Biodata Inventory Project)'
        })

    def get_wayback_snapshot(self, url):
        """Query Wayback Machine API for latest snapshot"""
        try:
            api_url = f"https://archive.org/wayback/available?url={quote(url)}"
            response = self.session.get(api_url, timeout=self.wayback_timeout)

            if response.status_code != 200:
                return None, None

            data = response.json()

            if 'archived_snapshots' not in data:
                return None, None

            closest = data['archived_snapshots'].get('closest', {})

            if not closest.get('available'):
                return None, None

            snapshot_url = closest.get('url')
            timestamp = closest.get('timestamp')

            if snapshot_url and timestamp:
                # Convert timestamp to readable date (YYYYMMDDHHMMSS -> YYYY-MM-DD)
                snapshot_date = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}"
                return snapshot_url, snapshot_date

            return None, None
        except Exception as e:
            return None, None

    def extract_meta_refresh_url(self, soup, current_url):
        """Extract redirect URL from meta refresh tag"""
        meta_refresh = soup.find('meta', attrs={'http-equiv': re.compile('refresh', re.I)})
        if not meta_refresh:
            return None
        content = meta_refresh.get('content', '')
        if not content:
            return None
        match = re.search(r'url\s*=\s*["\']?([^"\'>]+)', content, re.I)
        if match:
            redirect_url = match.group(1).strip()
            return urljoin(current_url, redirect_url)
        return None

    def follow_meta_redirects(self, response, original_url):
        """Follow meta refresh redirects, return final response and redirect chain"""
        redirect_chain = []
        current_response = response
        current_url = original_url

        for hop in range(self.max_meta_redirects):
            soup = BeautifulSoup(current_response.content[:self.max_content_size], 'lxml')
            redirect_url = self.extract_meta_refresh_url(soup, current_url)

            if not redirect_url:
                return current_response, redirect_chain

            redirect_chain.append(redirect_url)

            try:
                self.rate_limiter.wait_if_needed(redirect_url)
                current_response = self.session.get(redirect_url, timeout=self.timeout, allow_redirects=True)
                current_url = redirect_url

                if current_response.status_code not in range(200, 400):
                    return current_response, redirect_chain
            except Exception as e:
                return current_response, redirect_chain

        return current_response, redirect_chain

    def score_content(self, content, page_title):
        """Score content for bioresource indicators"""
        page_title_lower = page_title.lower()
        content_lower = content[:self.max_content_size].lower()

        base_score = 0
        indicators = []

        for term, term_score in INDICATOR_SCORES.items():
            term_lower = term.lower()
            if term_lower in page_title_lower:
                base_score += term_score
                indicators.append(f"Title: {term}")
            elif term_lower in content_lower:
                base_score += term_score
                indicators.append(f"Content: {term}")

        title_bonus = 0
        for keyword in TITLE_KEYWORDS:
            if keyword in page_title_lower:
                title_bonus = TITLE_BONUS
                indicators.append(f"★ TITLE BONUS: '{keyword}' in page title")
                break

        return base_score, title_bonus, indicators

    def scan_url(self, url_data):
        """Scan a single URL and return detailed results (with Wayback fallback)"""
        url = url_data['url']
        result = {
            **url_data,
            'is_live': False,
            'status_code': None,
            'total_score': 0,
            'base_score': 0,
            'title_bonus': 0,
            'likelihood': 'VERY LOW',
            'indicators_found': [],
            'response_time_ms': None,
            'error_message': None,
            'meta_redirects': 0,
            'final_url': url,
            'wayback_used': False,
            'wayback_url': None,
            'wayback_snapshot_date': None
        }

        domain = self.rate_limiter.wait_if_needed(url)

        # Try original URL first
        try:
            start_time = time.time()
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            final_response, redirect_chain = self.follow_meta_redirects(response, response.url)
            response_time = (time.time() - start_time) * 1000

            result['status_code'] = final_response.status_code
            result['response_time_ms'] = round(response_time, 2)
            result['is_live'] = 200 <= final_response.status_code < 400
            result['meta_redirects'] = len(redirect_chain)
            result['final_url'] = redirect_chain[-1] if redirect_chain else response.url

            if result['is_live']:
                # Original URL works - score it
                content = final_response.content[:self.max_content_size].decode('utf-8', errors='ignore')
                soup = BeautifulSoup(content, 'lxml')
                page_title = soup.title.string if soup.title and soup.title.string else ""

                base_score, title_bonus, indicators = self.score_content(content, page_title)

                result['base_score'] = base_score
                result['title_bonus'] = title_bonus
                result['total_score'] = base_score + title_bonus
                result['indicators_found'] = indicators

                score = result['total_score']
                if score >= 15:
                    result['likelihood'] = 'CRITICAL'
                elif score >= 10:
                    result['likelihood'] = 'HIGH'
                elif score >= 5:
                    result['likelihood'] = 'MEDIUM'
                elif score >= 1:
                    result['likelihood'] = 'LOW'

                return result
            else:
                # Original URL failed - try Wayback
                result['error_message'] = f"HTTP {final_response.status_code}"

        except requests.exceptions.Timeout:
            result['error_message'] = f'Timeout ({self.timeout}s)'
        except Exception as e:
            result['error_message'] = f'Error: {str(e)[:100]}'

        # Original URL failed - try Wayback Machine
        wayback_url, snapshot_date = self.get_wayback_snapshot(url)

        if wayback_url:
            try:
                self.rate_limiter.wait_if_needed(wayback_url)
                wayback_response = self.session.get(wayback_url, timeout=self.timeout, allow_redirects=True)

                if 200 <= wayback_response.status_code < 400:
                    # Wayback worked - score it
                    content = wayback_response.content[:self.max_content_size].decode('utf-8', errors='ignore')
                    soup = BeautifulSoup(content, 'lxml')
                    page_title = soup.title.string if soup.title and soup.title.string else ""

                    base_score, title_bonus, indicators = self.score_content(content, page_title)

                    result['is_live'] = True  # Mark as live (via Wayback)
                    result['wayback_used'] = True
                    result['wayback_url'] = wayback_url
                    result['wayback_snapshot_date'] = snapshot_date
                    result['base_score'] = base_score
                    result['title_bonus'] = title_bonus
                    result['total_score'] = base_score + title_bonus
                    result['indicators_found'] = indicators

                    score = result['total_score']
                    if score >= 15:
                        result['likelihood'] = 'CRITICAL'
                    elif score >= 10:
                        result['likelihood'] = 'HIGH'
                    elif score >= 5:
                        result['likelihood'] = 'MEDIUM'
                    elif score >= 1:
                        result['likelihood'] = 'LOW'

            except Exception as e:
                # Wayback also failed
                result['error_message'] = f"{result.get('error_message', 'Original failed')} | Wayback: {str(e)[:50]}"

        return result

    def scan_batch(self, urls_data, show_progress=True, checkpoint_file=None):
        """
        Scan URLs in parallel with optional progress bar

        Args:
            urls_data: List of URL data dictionaries to scan
            show_progress: Show progress bar
            checkpoint_file: Path to save incremental checkpoints (every 100 URLs)

        Returns:
            List of result dictionaries
        """
        global _current_results, _checkpoint_file, _interrupt_flag

        # Register signal handler for Ctrl+C
        _current_results = []
        _checkpoint_file = checkpoint_file
        _interrupt_flag = False
        signal.signal(signal.SIGINT, _signal_handler)

        results = []
        completed_count = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.scan_url, url_data): url_data for url_data in urls_data}

            if show_progress:
                pbar = tqdm(total=len(urls_data), desc="Scanning URLs", unit="url")

                for future in as_completed(futures):
                    try:
                        # Hard timeout: 2 minutes per URL
                        result = future.result(timeout=120)
                        results.append(result)
                    except TimeoutError:
                        # URL took too long - create timeout result
                        url_data = futures[future]
                        timeout_result = {
                            **url_data,
                            'is_live': False,
                            'status_code': None,
                            'total_score': 0,
                            'base_score': 0,
                            'title_bonus': 0,
                            'likelihood': 'VERY LOW',
                            'indicators_found': [],
                            'response_time_ms': None,
                            'error_message': 'HARD_TIMEOUT',
                            'meta_redirects': 0,
                            'final_url': url_data.get('url', ''),
                            'wayback_used': False,
                            'wayback_url': None,
                            'wayback_snapshot_date': None
                        }
                        results.append(timeout_result)
                    except Exception as e:
                        # Unexpected error - create error result
                        url_data = futures[future]
                        error_result = {
                            **url_data,
                            'is_live': False,
                            'status_code': None,
                            'total_score': 0,
                            'base_score': 0,
                            'title_bonus': 0,
                            'likelihood': 'VERY LOW',
                            'indicators_found': [],
                            'response_time_ms': None,
                            'error_message': f'FUTURE_ERROR: {str(e)[:100]}',
                            'meta_redirects': 0,
                            'final_url': url_data.get('url', ''),
                            'wayback_used': False,
                            'wayback_url': None,
                            'wayback_snapshot_date': None
                        }
                        results.append(error_result)

                    completed_count += 1
                    pbar.update(1)

                    # Update global state for signal handler
                    _current_results = results.copy()

                    # Incremental save every 100 URLs
                    if checkpoint_file and completed_count % 100 == 0:
                        _save_checkpoint(results, checkpoint_file)
                        pbar.set_postfix({"checkpoint": f"{completed_count} saved"})

                pbar.close()
            else:
                for future in as_completed(futures):
                    try:
                        # Hard timeout: 2 minutes per URL
                        result = future.result(timeout=120)
                        results.append(result)
                    except TimeoutError:
                        # URL took too long - create timeout result
                        url_data = futures[future]
                        timeout_result = {
                            **url_data,
                            'is_live': False,
                            'status_code': None,
                            'total_score': 0,
                            'base_score': 0,
                            'title_bonus': 0,
                            'likelihood': 'VERY LOW',
                            'indicators_found': [],
                            'response_time_ms': None,
                            'error_message': 'HARD_TIMEOUT',
                            'meta_redirects': 0,
                            'final_url': url_data.get('url', ''),
                            'wayback_used': False,
                            'wayback_url': None,
                            'wayback_snapshot_date': None
                        }
                        results.append(timeout_result)
                    except Exception as e:
                        # Unexpected error - create error result
                        url_data = futures[future]
                        error_result = {
                            **url_data,
                            'is_live': False,
                            'status_code': None,
                            'total_score': 0,
                            'base_score': 0,
                            'title_bonus': 0,
                            'likelihood': 'VERY LOW',
                            'indicators_found': [],
                            'response_time_ms': None,
                            'error_message': f'FUTURE_ERROR: {str(e)[:100]}',
                            'meta_redirects': 0,
                            'final_url': url_data.get('url', ''),
                            'wayback_used': False,
                            'wayback_url': None,
                            'wayback_snapshot_date': None
                        }
                        results.append(error_result)

                    completed_count += 1

                    # Update global state for signal handler
                    _current_results = results.copy()

                    # Incremental save every 100 URLs
                    if checkpoint_file and completed_count % 100 == 0:
                        _save_checkpoint(results, checkpoint_file)

        # Final cleanup: reset signal handler to default
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        return results


def scan_urls_from_file(input_file, output_file=None, max_workers=DEFAULT_MAX_WORKERS,
                        domain_delay=DEFAULT_DOMAIN_DELAY, timeout=DEFAULT_TIMEOUT,
                        session_dir=None, show_progress=True):
    """
    Scan URLs from a CSV file and save results.

    Args:
        input_file: Path to CSV file with 'url' column (and optionally 'id', 'domain', etc.)
        output_file: Path to save results (defaults to input_file with _scan_results suffix)
        max_workers: Number of concurrent workers
        domain_delay: Delay between requests to same domain (seconds)
        timeout: Request timeout in seconds
        session_dir: Session directory for organizing outputs (optional)
        show_progress: Show progress bar

    Returns:
        DataFrame with scan results
    """
    # Load URLs
    input_path = Path(input_file)
    df = pd.read_csv(input_path)

    print(f"{'=' * 80}")
    print(f"BIORESOURCE URL SCANNER V4")
    print(f"{'=' * 80}")
    print(f"\nDataset: {len(df)} URLs")
    if 'domain' in df.columns:
        print(f"Unique domains: {df['domain'].nunique()}")
    print(f"\nConfiguration:")
    print(f"  Workers: {max_workers}")
    print(f"  Domain delay: {domain_delay}s")
    print(f"  Timeout: {timeout}s")
    print(f"  Wayback fallback: Enabled")
    if session_dir:
        print(f"  Session: {session_dir}")
    print(f"\n{'=' * 80}\n")

    # Create scanner and run
    print(f"Starting scan at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    urls_data = df.to_dict('records')
    scanner = BioresourceScanner(
        max_workers=max_workers,
        domain_delay=domain_delay,
        timeout=timeout
    )

    # Determine checkpoint file path
    if session_dir:
        checkpoint_path = Path(session_dir) / '06_scanning' / 'scan_checkpoint.csv'
    else:
        checkpoint_path = input_path.parent / 'scan_checkpoint.csv'

    start_time = time.time()
    results = scanner.scan_batch(urls_data, show_progress=show_progress, checkpoint_file=str(checkpoint_path))
    total_time = time.time() - start_time

    # Convert to DataFrame
    results_df = pd.DataFrame(results)
    results_df['indicators_found'] = results_df['indicators_found'].apply(
        lambda x: '; '.join(x) if isinstance(x, list) else ''
    )

    # Determine output path
    if output_file is None:
        if session_dir:
            session_path = Path(session_dir)
            scan_dir = session_path / '06_scanning'
            scan_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = scan_dir / f"scan_results_{timestamp}.csv"
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = input_path.parent / f"{input_path.stem}_scan_results_{timestamp}.csv"
    else:
        output_path = Path(output_file)

    # Save results
    results_df.to_csv(output_path, index=False)

    # Clean up checkpoint file after successful completion
    try:
        if checkpoint_path.exists():
            checkpoint_path.unlink()
    except Exception:
        pass  # Ignore cleanup errors

    # Print summary
    print(f"\n{'=' * 80}")
    print(f"SCAN COMPLETE - {len(results)} URLs")
    print(f"{'=' * 80}")

    print(f"\nPerformance:")
    print(f"  Total time: {total_time / 60:.1f} minutes")
    print(f"  Average: {total_time / len(results):.2f}s per URL")
    print(f"  Throughput: {len(results) / total_time:.2f} URLs/sec")

    print(f"\nConnectivity:")
    live_count = results_df['is_live'].sum()
    failed_count = (~results_df['is_live']).sum()
    print(f"  Live URLs: {live_count} ({live_count / len(results_df) * 100:.1f}%)")
    print(f"  Failed: {failed_count} ({failed_count / len(results_df) * 100:.1f}%)")

    # Wayback stats
    wayback_count = results_df['wayback_used'].sum()
    if wayback_count > 0:
        print(f"\nWayback Machine:")
        print(f"  Rescued via Wayback: {wayback_count} ({wayback_count / len(results_df) * 100:.1f}%)")
        wayback_live = results_df[results_df['wayback_used'] == True]
        print(f"  Mean score (Wayback): {wayback_live['total_score'].mean():.1f}")

    print(f"\nLikelihood Distribution:")
    for likelihood in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'VERY LOW']:
        count = (results_df['likelihood'] == likelihood).sum()
        pct = count / len(results_df) * 100
        print(f"  {likelihood:12s}: {count:4d} ({pct:5.1f}%)")

    high_quality = ((results_df['likelihood'] == 'CRITICAL') | (results_df['likelihood'] == 'HIGH')).sum()
    print(f"\nHIGH QUALITY DETECTION (CRITICAL + HIGH):")
    print(f"  Count: {high_quality}/{len(results_df)} ({high_quality/len(results_df)*100:.1f}%)")

    live_df = results_df[results_df['is_live'] == True]
    if len(live_df) > 0:
        print(f"\nScore Statistics (Live URLs only, n={len(live_df)}):")
        print(f"  Mean: {live_df['total_score'].mean():.1f}")
        print(f"  Median: {live_df['total_score'].median():.1f}")
        print(f"  Std Dev: {live_df['total_score'].std():.1f}")

    meta_redirected = results_df[results_df['meta_redirects'] > 0]
    if len(meta_redirected) > 0:
        print(f"\nMeta refresh redirects detected: {len(meta_redirected)} sites")

    print(f"\nResults saved to:")
    print(f"  {output_path}")
    print(f"\n{'=' * 80}")

    return results_df
