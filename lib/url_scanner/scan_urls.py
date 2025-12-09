#!/usr/bin/env python3
"""
Standalone CLI script for URL scanning.

This script provides a command-line interface to the BioresourceScanner,
compatible with the original bioresource_url_scanner interface while
supporting session-based workflows.

Usage:
    # Basic scan (looks for data/gbc_urls.csv)
    python scan_urls.py

    # Scan with custom input
    python scan_urls.py --input-file /path/to/urls.csv

    # Scan with session directory
    python scan_urls.py --input-file urls.csv --session-dir /path/to/session

    # Scan with custom parameters
    python scan_urls.py --input-file urls.csv --workers 20 --timeout 30
"""

import argparse
import sys
from pathlib import Path

from scanner import scan_urls_from_file


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Scan URLs for bioresource indicators',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--input-file',
        type=str,
        default='data/gbc_urls.csv',
        help='Input CSV file with URLs (default: data/gbc_urls.csv)'
    )
    parser.add_argument(
        '--output-file',
        type=str,
        help='Output CSV file path (default: auto-generated)'
    )
    parser.add_argument(
        '--session-dir',
        type=str,
        help='Session directory for organizing outputs'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='Number of concurrent workers (default: 10)'
    )
    parser.add_argument(
        '--domain-delay',
        type=float,
        default=1.0,
        help='Delay between requests to same domain in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=20,
        help='Request timeout in seconds (default: 20)'
    )
    parser.add_argument(
        '--no-progress',
        action='store_true',
        help='Disable progress bar'
    )

    return parser.parse_args()


def main():
    """Main execution function."""
    args = parse_args()

    # Validate input file exists
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        print(f"Current directory: {Path.cwd()}")
        sys.exit(1)

    # Run scanner
    try:
        results_df = scan_urls_from_file(
            input_file=args.input_file,
            output_file=args.output_file,
            max_workers=args.workers,
            domain_delay=args.domain_delay,
            timeout=args.timeout,
            session_dir=args.session_dir,
            show_progress=not args.no_progress
        )
        print(f"\nScan completed successfully!")
        sys.exit(0)

    except KeyboardInterrupt:
        print("\n\nScan interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: Scan failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
