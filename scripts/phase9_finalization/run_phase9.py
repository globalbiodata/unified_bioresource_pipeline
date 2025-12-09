#!/usr/bin/env python3
"""
Run Phase 9: Finalization Pipeline

Convenience script to run all Phase 9 scripts in sequence.
This transforms novel bioresources into final inventory format.

Usage:
    python run_phase9.py --set-c PATH --final PATH [--session-id ID]

    # Example with default paths:
    python run_phase9.py \
        --set-c pipeline_synthesis_2025-11-18/results/deduplicated/aggressive/set_c_final.csv \
        --final false_positive_analysis/FINAL_novel_bioresources_with_urls.csv

Authors: AI Assistant
Date: 2025-11-27
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
import random
import string


def generate_session_id() -> str:
    """Generate a session ID in format: YYYY-MM-DD-HHMMSS-xxxxx"""
    timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
    return f"{timestamp}-{suffix}"


def run_script(script_path: str, args: list) -> bool:
    """Run a Python script and return success status"""
    cmd = [sys.executable, script_path] + args
    print(f"\n{'='*60}")
    print(f"Running: {os.path.basename(script_path)}")
    print(f"{'='*60}\n")

    result = subprocess.run(cmd, cwd=os.getcwd())
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(
        description='Run Phase 9 Finalization Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default paths (from project root):
  python unified_bioresource_pipeline/scripts/phase9_finalization/run_phase9.py \\
      --set-c pipeline_synthesis_2025-11-18/results/deduplicated/aggressive/set_c_final.csv \\
      --final false_positive_analysis/FINAL_novel_bioresources_with_urls.csv

  # With custom session ID:
  python run_phase9.py --set-c PATH --final PATH --session-id my-session-001

  # Skip URL checking (faster for testing):
  python run_phase9.py --set-c PATH --final PATH --skip-urls
        """
    )

    parser.add_argument(
        '--set-c',
        required=True,
        help='Path to set_c_final.csv (full pipeline output)'
    )
    parser.add_argument(
        '--final',
        required=True,
        help='Path to FINAL_novel_bioresources_with_urls.csv (filtered list)'
    )
    parser.add_argument(
        '--session-id',
        help='Session ID for output directory (default: auto-generated)'
    )
    parser.add_argument(
        '--output-base',
        default='unified_bioresource_pipeline/results',
        help='Base output directory'
    )
    parser.add_argument(
        '--skip-urls',
        action='store_true',
        help='Skip URL checking (use for testing)'
    )
    parser.add_argument(
        '--skip-epmc',
        action='store_true',
        help='Skip EPMC metadata fetching (use for testing)'
    )

    args = parser.parse_args()

    # Generate or use provided session ID
    session_id = args.session_id or generate_session_id()
    output_dir = os.path.join(args.output_base, session_id, 'finalization')

    print("=" * 60)
    print("PHASE 9: FINALIZATION PIPELINE")
    print("=" * 60)
    print(f"Session ID: {session_id}")
    print(f"Output directory: {output_dir}")
    print(f"Input files:")
    print(f"  - set_c: {args.set_c}")
    print(f"  - final: {args.final}")
    print()

    # Verify input files exist
    if not os.path.exists(args.set_c):
        print(f"Error: set_c file not found: {args.set_c}")
        sys.exit(1)
    if not os.path.exists(args.final):
        print(f"Error: final file not found: {args.final}")
        sys.exit(1)

    # Get script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Run scripts in sequence
    scripts = [
        ('22_filter_novel_resources.py', [
            '--set-c', args.set_c,
            '--final', args.final,
            '--output-dir', args.output_base,
            '--session-id', session_id
        ]),
        ('23_transform_columns.py', [
            '--input', os.path.join(output_dir, 'filtered_novel_resources.csv'),
            '--output-dir', output_dir
        ]),
    ]

    # Add URL checking (can be skipped for testing)
    if not args.skip_urls:
        scripts.append(('24_check_urls_with_geo.py', [
            '--input', os.path.join(output_dir, 'transformed_resources.csv'),
            '--output-dir', output_dir
        ]))
        next_input = 'url_checked_resources.csv'
    else:
        print("\n[SKIPPING URL CHECKING]")
        next_input = 'transformed_resources.csv'

    # Add EPMC metadata fetching (can be skipped for testing)
    if not args.skip_epmc:
        scripts.append(('25_fetch_epmc_metadata.py', [
            '--input', os.path.join(output_dir, next_input),
            '--output-dir', output_dir
        ]))
        next_input = 'metadata_enriched_resources.csv'
    else:
        print("\n[SKIPPING EPMC METADATA FETCHING]")

    # Add remaining scripts
    scripts.extend([
        ('26_process_countries.py', [
            '--input', os.path.join(output_dir, next_input),
            '--output-dir', output_dir
        ]),
        ('27_generate_final_inventory.py', [
            '--input', os.path.join(output_dir, 'countries_processed_resources.csv'),
            '--output-dir', output_dir
        ])
    ])

    # Execute each script
    for script_name, script_args in scripts:
        script_path = os.path.join(script_dir, script_name)
        if not run_script(script_path, script_args):
            print(f"\nError: {script_name} failed!")
            sys.exit(1)

    # Print final summary
    print("\n" + "=" * 60)
    print("PHASE 9 COMPLETE!")
    print("=" * 60)
    print(f"\nOutput files in: {output_dir}")
    print(f"  - final_inventory.csv: Main output")
    print(f"  - excluded_no_url.csv: Excluded resources")
    print(f"  - statistics.json: Summary statistics")
    print(f"  - finalization.log: Processing log")


if __name__ == '__main__':
    main()
