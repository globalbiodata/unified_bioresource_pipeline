#!/usr/bin/env python3
"""
Phase 8: URL Recovery - Orchestrator Script

Runs all URL recovery scripts in sequence to recover URLs for bioresources
that are missing them after initial pipeline processing.

Usage:
    python run_phase8.py --input deduped_bioresources.csv --output-dir results/url_recovery/
    python run_phase8.py --input deduped_bioresources.csv --output-dir results/url_recovery/ --skip-fetch
    python run_phase8.py --status results/url_recovery/

Stages:
    1. Identify missing URLs (28_identify_missing_urls.py)
    2. Fetch abstracts from EPMC (29_fetch_abstracts.py)
    3. Search abstracts for URLs (30_search_abstracts_urls.py)
    4. Fetch fulltext from EPMC (31_fetch_fulltext.py)
    5. Search fulltext for URLs (32_search_fulltext_urls.py)
    6. Consolidate and prepare web search (33_consolidate_recovery.py)

Output:
    - recovered_urls.csv: Records where URLs were found
    - still_missing.csv: Records that need web search
    - websearch_chunks/: Prepared files for agent web search

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-11-28
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def run_script(script_path: Path, args: list, dry_run: bool = False) -> bool:
    """Run a Python script with arguments."""
    cmd = [sys.executable, str(script_path)] + args
    print(f"\n{'='*60}")
    print(f"Running: {script_path.name}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}\n")

    if dry_run:
        print("[DRY RUN] Would execute above command")
        return True

    try:
        result = subprocess.run(cmd, check=True)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Script failed with return code {e.returncode}")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False


def check_status(output_dir: Path):
    """Check the status of URL recovery in output directory."""
    print(f"\n{'='*60}")
    print(f"URL Recovery Status: {output_dir}")
    print(f"{'='*60}\n")

    files_to_check = [
        ('missing_urls_prepared.csv', 'Records identified for recovery'),
        ('pmids_list.txt', 'PMIDs to fetch'),
        ('abstracts_cache.json', 'Abstracts cache'),
        ('abstract_url_results.csv', 'Abstract search results'),
        ('fulltext_cache.json', 'Fulltext cache'),
        ('fulltext_url_results.csv', 'Fulltext search results'),
        ('recovered_urls.csv', 'URLs recovered'),
        ('still_missing.csv', 'Records needing web search'),
    ]

    for filename, description in files_to_check:
        filepath = output_dir / filename
        if filepath.exists():
            size = filepath.stat().st_size
            if filename.endswith('.csv'):
                import pandas as pd
                df = pd.read_csv(filepath)
                print(f"  [OK] {filename}: {len(df)} records")
            elif filename.endswith('.json'):
                import json
                with open(filepath) as f:
                    data = json.load(f)
                print(f"  [OK] {filename}: {len(data)} entries")
            else:
                print(f"  [OK] {filename}: {size} bytes")
        else:
            print(f"  [--] {filename}: Not created yet")

    # Check websearch chunks
    websearch_dir = output_dir / 'websearch_chunks'
    if websearch_dir.exists():
        chunks = list(websearch_dir.glob('chunk_*.csv'))
        print(f"\n  Web search chunks: {len(chunks)} files")
        for chunk in sorted(chunks):
            import pandas as pd
            df = pd.read_csv(chunk)
            print(f"    {chunk.name}: {len(df)} records")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='URL Recovery Pipeline Orchestrator'
    )
    parser.add_argument(
        '--input', '-i',
        help='Input CSV (deduplicated bioresources)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        help='Output directory for all results'
    )
    parser.add_argument(
        '--status',
        help='Check status of URL recovery in directory'
    )
    parser.add_argument(
        '--skip-fetch',
        action='store_true',
        help='Skip fetching (use existing cache files)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print commands without executing'
    )
    parser.add_argument(
        '--rate-limit',
        type=float,
        default=0.1,
        help='API rate limit in seconds (default: 0.1)'
    )

    return parser.parse_args()


def main():
    args = parse_args()
    script_dir = Path(__file__).parent

    # Status check mode
    if args.status:
        check_status(Path(args.status))
        return

    # Validation
    if not args.input or not args.output_dir:
        print("ERROR: --input and --output-dir are required")
        print("       Use --status <dir> to check existing progress")
        sys.exit(1)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'#'*60}")
    print(f"# Phase 8: URL Recovery Pipeline")
    print(f"# Started: {datetime.now().isoformat()}")
    print(f"# Input: {input_path}")
    print(f"# Output: {output_dir}")
    print(f"{'#'*60}")

    # Script 28: Identify missing URLs
    success = run_script(
        script_dir / '28_identify_missing_urls.py',
        ['--input', str(input_path), '--output-dir', str(output_dir)],
        args.dry_run
    )
    if not success:
        print("Pipeline stopped at script 28")
        sys.exit(1)

    # Script 29: Fetch abstracts
    if not args.skip_fetch:
        success = run_script(
            script_dir / '29_fetch_abstracts.py',
            [
                '--input', str(output_dir / 'pmids_list.txt'),
                '--output-dir', str(output_dir),
                '--rate-limit', str(args.rate_limit),
                '--resume'
            ],
            args.dry_run
        )
        if not success:
            print("Pipeline stopped at script 29")
            sys.exit(1)

    # Script 30: Search abstracts for URLs
    success = run_script(
        script_dir / '30_search_abstracts_urls.py',
        [
            '--input', str(output_dir / 'missing_urls_prepared.csv'),
            '--abstracts', str(output_dir / 'abstracts_cache.json'),
            '--output-dir', str(output_dir)
        ],
        args.dry_run
    )
    if not success:
        print("Pipeline stopped at script 30")
        sys.exit(1)

    # Script 31: Fetch fulltext
    if not args.skip_fetch:
        success = run_script(
            script_dir / '31_fetch_fulltext.py',
            [
                '--input', str(output_dir / 'abstract_url_results.csv'),
                '--abstracts', str(output_dir / 'abstracts_cache.json'),
                '--output-dir', str(output_dir),
                '--rate-limit', str(args.rate_limit * 1.5),  # Slightly slower for fulltext
                '--resume'
            ],
            args.dry_run
        )
        if not success:
            print("Pipeline stopped at script 31")
            sys.exit(1)

    # Script 32: Search fulltext for URLs
    success = run_script(
        script_dir / '32_search_fulltext_urls.py',
        [
            '--input', str(output_dir / 'abstract_url_results.csv'),
            '--abstracts', str(output_dir / 'abstracts_cache.json'),
            '--fulltext', str(output_dir / 'fulltext_cache.json'),
            '--output-dir', str(output_dir)
        ],
        args.dry_run
    )
    if not success:
        print("Pipeline stopped at script 32")
        sys.exit(1)

    # Script 33: Consolidate results
    success = run_script(
        script_dir / '33_consolidate_recovery.py',
        [
            '--abstract-results', str(output_dir / 'abstract_url_results.csv'),
            '--fulltext-results', str(output_dir / 'fulltext_url_results.csv'),
            '--output-dir', str(output_dir)
        ],
        args.dry_run
    )
    if not success:
        print("Pipeline stopped at script 33")
        sys.exit(1)

    # Final status
    print(f"\n{'#'*60}")
    print(f"# Phase 8 Complete")
    print(f"# Finished: {datetime.now().isoformat()}")
    print(f"{'#'*60}")

    check_status(output_dir)

    print(f"\n{'='*60}")
    print("Next Steps:")
    print("='*60")
    print("1. Review recovered_urls.csv - URLs found automatically")
    print("2. Review websearch_chunks/ - Records needing web search")
    print("3. (Optional) Run web search agents on chunk files")
    print("4. Merge recovered URLs back into main inventory")


if __name__ == '__main__':
    main()
