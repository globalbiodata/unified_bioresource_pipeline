#!/usr/bin/env python3
"""
Unified Bioresource Pipeline Orchestrator

Coordinates all refactored scripts to run the complete bioresource discovery pipeline
from NER union extraction through final inventory generation.

Architecture:
    The pipeline consists of 7 main phases (Phases 2-9), executed in order:

    Phase 2: NER Union          - Extract unique PMIDs from spaCy + V2 BERT NER
    Phase 3: Linguistic Scoring - Apply linguistic patterns to score papers
    Phase 4: SetFit Inference   - Classify medium-score papers (requires Colab)
    Phase 5: Mapping            - Create paper sets, primary resources, extract URLs
    Phase 7: Deduplication      - Deduplicate resources across profiles
    Phase 6: URL Scanning       - Validate URLs with web scanner (after dedup)
    Phase 8: URL Recovery       - Recover missing URLs (2-part with manual breakpoint)
    Phase 9: Finalization       - Transform and finalize inventory

    Note: Phase 6 (URL Scanning) runs AFTER Phase 7 (Deduplication) to avoid
    wasting time scanning URLs that will be merged away.

Session Management:
    - Each run creates a unique session ID: YYYY-MM-DD-HHMMSS-xxxxx
    - All outputs stored in: results/{session_id}/
    - Session directories follow standardized structure (see lib/session_utils.py)
    - Can resume from existing session with --session-id

Features:
    - Run full pipeline or specific phase ranges
    - Multi-profile support (conservative/balanced/aggressive)
    - Manual breakpoint support for web search
    - Progress reporting with timing
    - Error handling with detailed logging
    - Dry-run mode for planning

Usage:
    # Full pipeline with new session
    python run_pipeline.py --input-dir data/2022_mid2025/

    # Resume from phase 5
    python run_pipeline.py --session-id 2025-12-04-143052-abc12 --start-phase 5

    # Run just phases 2-4
    python run_pipeline.py --input-dir data/ --start-phase 2 --end-phase 4

    # Run with specific dedup profile
    python run_pipeline.py --input-dir data/ --profiles balanced

    # Dry run to see what would execute
    python run_pipeline.py --input-dir data/ --dry-run

    # Skip web search breakpoint
    python run_pipeline.py --session-id SESSION --skip-web-search

Author: Pipeline Consolidation
Date: 2025-12-04
Version: 2.0.0 (Refactored for session-based execution)
"""

import argparse
import subprocess
import sys
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add lib to path for session utilities
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from lib.session_utils import (
    generate_session_id,
    create_session_dirs,
    get_session_path,
    validate_session_dir,
    get_latest_session
)


# =============================================================================
# PIPELINE CONFIGURATION
# =============================================================================

# Phase definitions with script paths and descriptions
PIPELINE_PHASES = {
    2: {
        'name': 'NER Union',
        'description': 'Extract unique PMIDs from spaCy + V2 BERT NER results',
        'scripts': [
            ('scripts/phase2_ner/06_extract_pmid_union.py', 'Extract PMID union')
        ],
        'required_inputs': ['input/spacy_ner_results.csv', 'input/v2_ner_results.csv'],
        'outputs': ['02_ner/ner_union.csv', '02_ner/ner_union_pmids.txt']
    },
    3: {
        'name': 'Linguistic Scoring',
        'description': 'Apply linguistic patterns to score papers',
        'scripts': [
            ('scripts/phase3_linguistic/run_linguistic_scoring.py', 'Run linguistic scoring')
        ],
        'required_inputs': ['02_ner/ner_union_pmids.txt', 'input/classification_union.csv'],
        'outputs': ['03_linguistic/high_score_papers.csv', '03_linguistic/medium_score_papers.csv']
    },
    4: {
        'name': 'SetFit Inference',
        'description': 'Classify medium-score papers (requires Colab)',
        'scripts': [
            ('scripts/phase4_setfit/08_setfit_inference.py', 'Run SetFit inference')
        ],
        'required_inputs': ['03_linguistic/medium_score_papers.csv'],
        'outputs': ['04_setfit/setfit_introductions.csv'],
        'notes': 'Requires GPU and SetFit model. Consider using Google Colab.'
    },
    5: {
        'name': 'Mapping & Resources',
        'description': 'Create paper sets, primary resources, and extract URLs',
        'scripts': [
            ('scripts/phase5_mapping/09_create_paper_sets.py', 'Create paper sets'),
            ('scripts/phase5_mapping/11_create_primary_resources.py', 'Create primary resources'),
            ('scripts/phase5_mapping/12_add_quality_indicators.py', 'Add quality indicators'),
            ('scripts/phase5_mapping/13_extract_urls.py', 'Extract URLs')
        ],
        'required_inputs': ['03_linguistic/high_score_papers.csv', '04_setfit/setfit_introductions.csv'],
        'outputs': ['05_mapping/set_a_linguistic.csv', '05_mapping/set_b_setfit.csv',
                   '05_mapping/set_c_union.csv', '05_mapping/papers_with_urls.csv']
    },
    7: {
        'name': 'Deduplication',
        'description': 'Deduplicate resources across profiles',
        'scripts': [
            ('scripts/phase7_deduplication/17_deduplicate_all_sets.py', 'Deduplicate all sets')
        ],
        'required_inputs': ['05_mapping/papers_with_urls.csv'],
        'outputs': ['07_deduplication/{profile}/set_c_dedup.csv',
                   '07_deduplication/{profile}/unclear_cases.csv',
                   '07_deduplication/{profile}/set_c_final.csv'],
        'notes': 'Creates conservative, balanced, and aggressive profiles'
    },
    6: {
        'name': 'URL Scanning',
        'description': 'Validate URLs with web scanner',
        'scripts': [
            ('scripts/phase6_scanning/14_prepare_urls.py', 'Prepare URLs for scanning'),
            ('scripts/phase6_scanning/15_scan_urls.py', 'Scan URLs'),
            ('scripts/phase6_scanning/16_merge_scan_scores.py', 'Merge scan scores'),
            ('scripts/phase6_scanning/18_scan_urls_set_c.py', 'Scan Set C URLs'),
            ('scripts/phase6_scanning/19_backfill_url_data.py', 'Backfill URL data')
        ],
        'required_inputs': ['07_deduplication/{profile}/set_c_final.csv'],
        'outputs': ['06_scanning/scanned_urls.csv'],
        'notes': 'Runs AFTER deduplication to avoid scanning URLs that get merged'
    },
    8: {
        'name': 'URL Recovery',
        'description': 'Recover missing URLs (2-part with manual breakpoint)',
        'scripts': [
            ('scripts/phase8_url_recovery/run_phase8.py', 'Run URL recovery pipeline')
        ],
        'required_inputs': ['07_deduplication/{profile}/set_c_final.csv'],
        'outputs': ['08_url_recovery/missing_urls_prepared.csv',
                   '08_url_recovery/websearch_chunks/',
                   '08_url_recovery/final_url_recovery.csv'],
        'notes': 'Includes manual web search breakpoint. Use --skip-web-search to bypass.',
        'manual_breakpoint': True
    },
    9: {
        'name': 'Finalization',
        'description': 'Transform and finalize inventory',
        'scripts': [
            ('scripts/phase9_finalization/run_phase9.py', 'Run finalization pipeline')
        ],
        'required_inputs': ['07_deduplication/{profile}/set_c_final.csv'],
        'outputs': ['09_finalization/final_inventory.csv',
                   '09_finalization/statistics.json'],
        'notes': 'Creates final inventory with EPMC metadata and statistics'
    }
}

# Profiles for deduplication (Phase 7)
DEDUP_PROFILES = ['conservative', 'balanced', 'aggressive']


# =============================================================================
# ORCHESTRATOR FUNCTIONS
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Unified Bioresource Pipeline Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline with new session
  python run_pipeline.py --input-dir data/2022_mid2025/

  # Resume from phase 5
  python run_pipeline.py --session-id 2025-12-04-143052-abc12 --start-phase 5

  # Run just phases 2-4
  python run_pipeline.py --input-dir data/ --start-phase 2 --end-phase 4

  # Run with specific dedup profile
  python run_pipeline.py --input-dir data/ --profiles balanced

  # Dry run to see what would execute
  python run_pipeline.py --input-dir data/ --dry-run

  # Skip web search breakpoint
  python run_pipeline.py --session-id SESSION --skip-web-search

Phase Numbers:
  2: NER Union
  3: Linguistic Scoring
  4: SetFit Inference
  5: Mapping & Resources
  6: URL Scanning
  7: Deduplication
  8: URL Recovery
  9: Finalization
        """
    )

    # Session management
    parser.add_argument('--session-id', help='Resume existing session ID')
    parser.add_argument('--input-dir', type=Path, help='Input data directory (for new session)')
    parser.add_argument('--results-dir', type=Path, default=Path('results'),
                       help='Base results directory (default: results/)')

    # Phase control
    parser.add_argument('--start-phase', type=int, default=2,
                       help='Start from phase N (default: 2)')
    parser.add_argument('--end-phase', type=int, default=9,
                       help='End at phase N (default: 9)')
    parser.add_argument('--only-phase', type=int,
                       help='Run only this phase (overrides start/end)')

    # Deduplication profiles
    parser.add_argument('--profiles', default='all',
                       help='Dedup profiles to run: all, conservative, balanced, aggressive, '
                            'or comma-separated list (default: all)')

    # Execution control
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would run without executing')
    parser.add_argument('--continue-on-error', action='store_true',
                       help='Continue to next phase even if a script fails')
    parser.add_argument('--skip-web-search', action='store_true',
                       help='Skip manual web search breakpoint in Phase 8')

    # Config
    parser.add_argument('--config', type=Path,
                       default=Path('config/pipeline_config.yaml'),
                       help='Pipeline configuration file')

    # Logging
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level (default: INFO)')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed output from scripts')

    return parser.parse_args()


def setup_session(args) -> Tuple[str, Path]:
    """
    Setup or resume session.

    Returns:
        Tuple of (session_id, session_dir)
    """
    if args.session_id:
        # Resume existing session
        session_id = args.session_id
        session_dir = args.results_dir / session_id

        if not session_dir.exists():
            print(f"ERROR: Session directory not found: {session_dir}")
            sys.exit(1)

        print(f"Resuming session: {session_id}")
        print(f"Session directory: {session_dir}")

    else:
        # Create new session
        if not args.input_dir:
            print("ERROR: --input-dir required for new session")
            sys.exit(1)

        if not args.input_dir.exists():
            print(f"ERROR: Input directory not found: {args.input_dir}")
            sys.exit(1)

        session_id = generate_session_id()
        session_dir = args.results_dir / session_id

        print(f"\nCreating new session: {session_id}")
        print(f"Session directory: {session_dir}")

        # Create directory structure
        create_session_dirs(session_dir)

        # Copy input files to session/input/
        print("\nCopying input files...")
        input_files = list(args.input_dir.glob('*.csv'))
        if not input_files:
            print(f"WARNING: No CSV files found in {args.input_dir}")

        import shutil
        for file in input_files:
            dest = get_session_path(session_dir, 'input', file.name)
            shutil.copy2(file, dest)
            print(f"  - {file.name}")

    return session_id, session_dir


def get_profiles_to_run(profiles_arg: str) -> List[str]:
    """Parse --profiles argument and return list of profiles."""
    if profiles_arg == 'all':
        return DEDUP_PROFILES
    else:
        profiles = [p.strip() for p in profiles_arg.split(',')]
        invalid = [p for p in profiles if p not in DEDUP_PROFILES]
        if invalid:
            print(f"ERROR: Invalid profiles: {', '.join(invalid)}")
            print(f"Valid profiles: {', '.join(DEDUP_PROFILES)}")
            sys.exit(1)
        return profiles


def get_phases_to_run(args) -> List[int]:
    """Determine which phases to run based on arguments."""
    if args.only_phase:
        if args.only_phase not in PIPELINE_PHASES:
            print(f"ERROR: Invalid phase: {args.only_phase}")
            print(f"Valid phases: {', '.join(map(str, PIPELINE_PHASES.keys()))}")
            sys.exit(1)
        return [args.only_phase]

    # Validate phase numbers
    if args.start_phase not in PIPELINE_PHASES:
        print(f"ERROR: Invalid start phase: {args.start_phase}")
        print(f"Valid phases: {', '.join(map(str, PIPELINE_PHASES.keys()))}")
        sys.exit(1)

    if args.end_phase not in PIPELINE_PHASES:
        print(f"ERROR: Invalid end phase: {args.end_phase}")
        print(f"Valid phases: {', '.join(map(str, PIPELINE_PHASES.keys()))}")
        sys.exit(1)

    # Get phases in correct execution order
    phase_order = [2, 3, 4, 5, 7, 6, 8, 9]  # Note: 7 before 6

    # Filter to requested range
    start_idx = phase_order.index(args.start_phase)
    end_idx = phase_order.index(args.end_phase) + 1

    return phase_order[start_idx:end_idx]


def run_script(script_path: Path, args: List[str], phase_name: str,
               script_desc: str, verbose: bool = False) -> Tuple[bool, float]:
    """
    Run a script and return success status and duration.

    Returns:
        Tuple of (success, duration_seconds)
    """
    cmd = [sys.executable, str(script_path)] + args

    print(f"\n{'─' * 80}")
    print(f"Running: {script_desc}")
    print(f"Script: {script_path.name}")
    print(f"Command: {' '.join([str(c) for c in cmd])}")
    print(f"{'─' * 80}\n")

    start_time = time.time()

    try:
        if verbose:
            result = subprocess.run(cmd, check=True)
        else:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            if result.returncode != 0:
                print("STDERR:")
                print(result.stderr)
                return False, time.time() - start_time

        duration = time.time() - start_time
        print(f"\n✓ Completed in {format_duration(duration)}")
        return True, duration

    except subprocess.CalledProcessError as e:
        duration = time.time() - start_time
        print(f"\n✗ Failed after {format_duration(duration)}")
        print(f"Error: {e}")
        return False, duration
    except Exception as e:
        duration = time.time() - start_time
        print(f"\n✗ Failed after {format_duration(duration)}")
        print(f"Unexpected error: {e}")
        return False, duration


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.2f}h"


def print_phase_header(phase_num: int, phase_info: Dict):
    """Print formatted phase header."""
    print("\n" + "=" * 80)
    print(f"PHASE {phase_num}: {phase_info['name']}")
    print("=" * 80)
    print(f"Description: {phase_info['description']}")
    if 'notes' in phase_info:
        print(f"Notes: {phase_info['notes']}")
    print()


def check_phase_inputs(session_dir: Path, phase_info: Dict, profile: Optional[str] = None) -> bool:
    """
    Check if required input files exist for a phase.

    Returns:
        True if all inputs exist, False otherwise
    """
    missing = []

    for input_path in phase_info['required_inputs']:
        # Replace {profile} placeholder if needed
        if profile:
            input_path = input_path.replace('{profile}', profile)

        full_path = session_dir / input_path
        if not full_path.exists():
            missing.append(input_path)

    if missing:
        print(f"WARNING: Missing required inputs:")
        for path in missing:
            print(f"  - {path}")
        return False

    return True


def run_phase(phase_num: int, session_dir: Path, profiles: List[str],
              args, execution_log: Dict) -> bool:
    """
    Run all scripts in a phase.

    Returns:
        True if phase completed successfully
    """
    phase_info = PIPELINE_PHASES[phase_num]

    # Print phase header
    print_phase_header(phase_num, phase_info)

    # Handle manual breakpoint for Phase 8
    if phase_num == 8 and phase_info.get('manual_breakpoint') and not args.skip_web_search:
        print("=" * 80)
        print("MANUAL BREAKPOINT: Web Search Required")
        print("=" * 80)
        print("\nPhase 8.1 will prepare chunks for manual web search.")
        print("After Phase 8.1 completes, you need to:")
        print("  1. Review websearch_chunks/ directory")
        print("  2. Perform manual web searches")
        print("  3. Save results to websearch_chunks/")
        print("  4. Re-run with --start-phase 8 to continue")
        print("\nTo skip this breakpoint, use --skip-web-search")
        print("\nPress Enter to continue with Phase 8.1, or Ctrl+C to exit...")

        if not args.dry_run:
            try:
                input()
            except KeyboardInterrupt:
                print("\n\nExecution paused. Resume later with:")
                print(f"  python run_pipeline.py --session-id {session_dir.name} --start-phase 8")
                return False

    # Track phase timing
    phase_start = time.time()
    phase_log = {
        'phase': phase_num,
        'name': phase_info['name'],
        'scripts': [],
        'success': True,
        'start_time': datetime.now().isoformat()
    }

    # For phases that use profiles (currently just Phase 7)
    if phase_num == 7:
        print(f"Running deduplication for profiles: {', '.join(profiles)}")
        print()

    # Run each script in the phase
    for script_rel_path, script_desc in phase_info['scripts']:
        script_path = SCRIPT_DIR / script_rel_path

        if not script_path.exists():
            print(f"ERROR: Script not found: {script_path}")
            phase_log['success'] = False
            phase_log['error'] = f"Script not found: {script_rel_path}"
            break

        # Build script arguments
        script_args = ['--session-dir', str(session_dir)]

        # Add profile arguments for Phase 7
        if phase_num == 7:
            script_args.extend(['--profiles', ','.join(profiles)])

        # Add config if specified
        if args.config and args.config.exists():
            script_args.extend(['--config', str(args.config)])

        # Dry run
        if args.dry_run:
            print(f"[DRY RUN] Would execute: {script_path.name}")
            print(f"           Args: {' '.join(script_args)}")
            continue

        # Run the script
        success, duration = run_script(
            script_path,
            script_args,
            phase_info['name'],
            script_desc,
            verbose=args.verbose
        )

        # Log script execution
        script_log = {
            'script': script_rel_path,
            'description': script_desc,
            'success': success,
            'duration': duration
        }
        phase_log['scripts'].append(script_log)

        if not success:
            phase_log['success'] = False
            if not args.continue_on_error:
                print(f"\n✗ Phase {phase_num} failed")
                break

    # Record phase duration and completion
    phase_duration = time.time() - phase_start
    phase_log['duration'] = phase_duration
    phase_log['end_time'] = datetime.now().isoformat()
    execution_log['phases'].append(phase_log)

    if args.dry_run:
        return True

    # Print phase summary
    print("\n" + "─" * 80)
    if phase_log['success']:
        print(f"✓ Phase {phase_num} completed successfully in {format_duration(phase_duration)}")
    else:
        print(f"✗ Phase {phase_num} failed after {format_duration(phase_duration)}")
    print("─" * 80)

    return phase_log['success']


def print_execution_summary(execution_log: Dict, session_dir: Path):
    """Print final execution summary."""
    print("\n" + "=" * 80)
    print("EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Session: {session_dir.name}")
    print(f"Start: {execution_log['start_time']}")
    print(f"End: {execution_log['end_time']}")
    print(f"Duration: {format_duration(execution_log['total_duration'])}")
    print()

    # Phase summary
    print("Phase Results:")
    for phase_log in execution_log['phases']:
        status = "✓" if phase_log['success'] else "✗"
        duration = format_duration(phase_log['duration'])
        print(f"  {status} Phase {phase_log['phase']}: {phase_log['name']} ({duration})")

        for script_log in phase_log['scripts']:
            script_status = "✓" if script_log['success'] else "✗"
            script_duration = format_duration(script_log['duration'])
            script_name = Path(script_log['script']).name
            print(f"      {script_status} {script_name} ({script_duration})")

    # Overall status
    print()
    all_success = all(p['success'] for p in execution_log['phases'])
    if all_success:
        print("✓ Pipeline completed successfully!")
    else:
        print("✗ Pipeline completed with errors")

    # Save execution log
    log_file = session_dir / 'execution_log.json'
    with open(log_file, 'w') as f:
        json.dump(execution_log, f, indent=2)
    print(f"\nExecution log saved to: {log_file}")

    # Print key output locations
    print("\nKey Outputs:")
    print(f"  - Session directory: {session_dir}")
    print(f"  - NER union: {session_dir}/02_ner/")
    print(f"  - Dedup results: {session_dir}/07_deduplication/")
    print(f"  - Final inventory: {session_dir}/09_finalization/final_inventory.csv")


def main():
    """Main orchestrator function."""
    args = parse_args()

    # Print header
    print("=" * 80)
    print("Unified Bioresource Pipeline Orchestrator")
    print("=" * 80)
    print(f"Version: 2.0.0")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Setup session
    session_id, session_dir = setup_session(args)

    # Get phases to run
    phases = get_phases_to_run(args)
    print(f"\nPhases to run: {', '.join(map(str, phases))}")

    # Get profiles
    profiles = get_profiles_to_run(args.profiles)
    print(f"Dedup profiles: {', '.join(profiles)}")

    # Dry run mode
    if args.dry_run:
        print("\n" + "=" * 80)
        print("DRY RUN MODE - No scripts will be executed")
        print("=" * 80)

    # Initialize execution log
    execution_log = {
        'session_id': session_id,
        'session_dir': str(session_dir),
        'start_time': datetime.now().isoformat(),
        'phases': [],
        'profiles': profiles
    }

    # Run phases
    start_time = time.time()
    all_success = True

    for phase_num in phases:
        success = run_phase(phase_num, session_dir, profiles, args, execution_log)

        if not success:
            all_success = False
            if not args.continue_on_error:
                print(f"\nStopping at Phase {phase_num} due to failure")
                break

    # Record total duration
    execution_log['total_duration'] = time.time() - start_time
    execution_log['end_time'] = datetime.now().isoformat()
    execution_log['success'] = all_success

    # Print summary
    print_execution_summary(execution_log, session_dir)

    # Exit code
    sys.exit(0 if all_success else 1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
