#!/usr/bin/env python3
"""
Session Utilities - Unified Bioresource Pipeline

Provides standardized session management for all pipeline scripts:
- Session ID generation
- Directory structure creation
- Path resolution for session-based outputs

Session Directory Structure:
    results/{session_id}/
        ├── input/                    # User-provided inputs (from Colab)
        ├── 02_ner/                   # NER union output
        ├── 03_linguistic/            # Linguistic scoring output
        ├── 04_setfit/                # SetFit inference output
        ├── 05_mapping/               # Paper sets & primary resources
        ├── 06_scanning/              # URL scanning (runs after Phase 8)
        ├── 07_deduplication/         # Dedup results by profile
        │   ├── conservative/
        │   ├── balanced/
        │   └── aggressive/
        ├── 08_url_recovery/          # URL recovery outputs
        │   └── websearch_chunks/
        └── 09_finalization/          # Final inventory

Usage:
    from lib.session_utils import (
        generate_session_id,
        create_session_dirs,
        get_session_path,
        add_session_args
    )

    # In script argparse setup:
    add_session_args(parser)

    # Get paths:
    input_file = get_session_path(args.session_dir, '02_ner', 'ner_union.csv')
    output_file = get_session_path(args.session_dir, '03_linguistic', 'high_score_papers.csv')

Created: 2025-12-04
"""

import argparse
import random
import string
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Union


# Phase directory names with numeric prefixes
PHASE_DIRS = {
    'input': 'input',
    '02_ner': '02_ner',
    '03_linguistic': '03_linguistic',
    '04_setfit': '04_setfit',
    '05_mapping': '05_mapping',
    '06_scanning': '06_scanning',
    '07_deduplication': '07_deduplication',
    '08_url_recovery': '08_url_recovery',
    '09_finalization': '09_finalization',
}

# Deduplication profile subdirectories
DEDUP_PROFILES = ['conservative', 'balanced', 'aggressive']

# URL recovery subdirectories
URL_RECOVERY_SUBDIRS = ['websearch_chunks']


def generate_session_id() -> str:
    """
    Generate unique session ID: YYYY-MM-DD-HHMMSS-xxxxx

    Format:
        - Date: YYYY-MM-DD
        - Time: HHMMSS
        - Random suffix: 5 lowercase alphanumeric characters

    Returns:
        str: Session ID (e.g., "2025-12-04-143052-a3f9b")

    Examples:
        >>> session_id = generate_session_id()
        >>> # Returns something like: "2025-12-04-143052-a3f9b"
    """
    timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
    return f"{timestamp}-{random_suffix}"


def create_session_dirs(
    session_dir: Union[str, Path],
    include_input: bool = True,
    include_dedup_profiles: bool = True,
    include_websearch: bool = True
) -> Dict[str, Path]:
    """
    Create session directory structure

    Args:
        session_dir: Path to session directory (e.g., results/2025-12-04-143052-a3f9b)
        include_input: Whether to create input/ directory
        include_dedup_profiles: Whether to create dedup profile subdirs
        include_websearch: Whether to create websearch_chunks/ subdir

    Returns:
        Dict[str, Path]: Dictionary of created directory paths

    Examples:
        >>> dirs = create_session_dirs("results/2025-12-04-143052-a3f9b")
        >>> print(dirs['02_ner'])
        # results/2025-12-04-143052-a3f9b/02_ner
    """
    session_path = Path(session_dir)
    created_dirs = {'session': session_path}

    # Create main phase directories
    for phase_name, dir_name in PHASE_DIRS.items():
        if phase_name == 'input' and not include_input:
            continue
        phase_path = session_path / dir_name
        phase_path.mkdir(parents=True, exist_ok=True)
        created_dirs[phase_name] = phase_path

    # Create deduplication profile subdirectories
    if include_dedup_profiles and '07_deduplication' in created_dirs:
        for profile in DEDUP_PROFILES:
            profile_path = created_dirs['07_deduplication'] / profile
            profile_path.mkdir(parents=True, exist_ok=True)
            created_dirs[f'07_deduplication/{profile}'] = profile_path

    # Create URL recovery subdirectories
    if include_websearch and '08_url_recovery' in created_dirs:
        for subdir in URL_RECOVERY_SUBDIRS:
            subdir_path = created_dirs['08_url_recovery'] / subdir
            subdir_path.mkdir(parents=True, exist_ok=True)
            created_dirs[f'08_url_recovery/{subdir}'] = subdir_path

    return created_dirs


def get_session_path(
    session_dir: Union[str, Path],
    phase: str,
    filename: Optional[str] = None
) -> Path:
    """
    Build path within session directory

    Args:
        session_dir: Session directory path
        phase: Phase name (e.g., '02_ner', '05_mapping', '07_deduplication/aggressive')
        filename: Optional filename to append

    Returns:
        Path: Full path to phase directory or file

    Examples:
        >>> get_session_path('results/session1', '02_ner')
        # Path('results/session1/02_ner')

        >>> get_session_path('results/session1', '02_ner', 'ner_union.csv')
        # Path('results/session1/02_ner/ner_union.csv')

        >>> get_session_path('results/session1', '07_deduplication/aggressive', 'set_c_dedup.csv')
        # Path('results/session1/07_deduplication/aggressive/set_c_dedup.csv')
    """
    session_path = Path(session_dir)
    phase_path = session_path / phase

    if filename:
        return phase_path / filename
    return phase_path


def add_session_args(parser: argparse.ArgumentParser) -> None:
    """
    Add standard session arguments to an argparse parser

    Adds:
        --session-dir: Required session directory path
        --config: Optional pipeline config file

    Args:
        parser: ArgumentParser instance to add arguments to

    Examples:
        >>> parser = argparse.ArgumentParser()
        >>> add_session_args(parser)
        >>> args = parser.parse_args(['--session-dir', 'results/session1'])
    """
    parser.add_argument(
        '--session-dir',
        type=str,
        required=True,
        help='Session directory path (e.g., results/2025-12-04-143052-a3f9b)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/pipeline_config.yaml',
        help='Pipeline configuration file (default: config/pipeline_config.yaml)'
    )


def validate_session_dir(session_dir: Union[str, Path], required_phases: Optional[list] = None) -> bool:
    """
    Validate that session directory exists and has required phase directories

    Args:
        session_dir: Session directory path
        required_phases: List of phase names that must exist (e.g., ['02_ner', '03_linguistic'])

    Returns:
        bool: True if valid, raises ValueError if not

    Raises:
        ValueError: If session dir doesn't exist or is missing required phases
    """
    session_path = Path(session_dir)

    if not session_path.exists():
        raise ValueError(f"Session directory does not exist: {session_path}")

    if required_phases:
        for phase in required_phases:
            phase_path = session_path / phase
            if not phase_path.exists():
                raise ValueError(f"Required phase directory missing: {phase_path}")

    return True


def get_latest_session(base_dir: Union[str, Path] = 'results') -> Optional[str]:
    """
    Get the most recent session ID from the results directory

    Args:
        base_dir: Base results directory

    Returns:
        Optional[str]: Most recent session ID, or None if no sessions exist
    """
    base_path = Path(base_dir)

    if not base_path.exists():
        return None

    # Get all directories that look like session IDs (YYYY-MM-DD-HHMMSS-xxxxx)
    sessions = []
    for item in base_path.iterdir():
        if item.is_dir() and len(item.name) == 22:  # Expected length of session ID
            # Verify it looks like a session ID
            parts = item.name.split('-')
            if len(parts) == 5:
                sessions.append(item.name)

    if not sessions:
        return None

    # Sort by name (which is chronological due to timestamp format)
    sessions.sort(reverse=True)
    return sessions[0]


# Standard file names used across the pipeline
STANDARD_FILES = {
    # Phase 2: NER Union
    '02_ner': {
        'ner_union': 'ner_union.csv',
        'ner_union_pmids': 'ner_union_pmids.txt',
    },
    # Phase 3: Linguistic Scoring
    '03_linguistic': {
        'high_score': 'high_score_papers.csv',
        'medium_score': 'medium_score_papers.csv',
        'low_score': 'low_score_papers.csv',
    },
    # Phase 4: SetFit
    '04_setfit': {
        'setfit_introductions': 'setfit_introductions.csv',
        'setfit_usage': 'setfit_usage.csv',
    },
    # Phase 5: Mapping
    '05_mapping': {
        'set_a': 'set_a_linguistic.csv',
        'set_b': 'set_b_setfit.csv',
        'set_c': 'set_c_union.csv',
        'primary_resources': 'union_papers_with_primary_resources.csv',
        'with_quality': 'papers_with_quality_indicators.csv',
        'with_urls': 'papers_with_urls.csv',
    },
    # Phase 7: Deduplication
    '07_deduplication': {
        'set_c_dedup': 'set_c_dedup.csv',
        'unclear_cases': 'unclear_cases.csv',
        'set_c_final': 'set_c_final.csv',
    },
    # Phase 8: URL Recovery
    '08_url_recovery': {
        'missing_urls': 'missing_urls_prepared.csv',
        'recovered_urls': 'recovered_urls.csv',
        'still_missing': 'still_missing.csv',
        'final_recovery': 'final_url_recovery.csv',
    },
    # Phase 9: Finalization
    '09_finalization': {
        'filtered_novel': 'filtered_novel_resources.csv',
        'transformed': 'transformed_resources.csv',
        'url_checked': 'url_checked_resources.csv',
        'metadata_enriched': 'metadata_enriched_resources.csv',
        'countries_processed': 'countries_processed_resources.csv',
        'final_inventory': 'final_inventory.csv',
        'statistics': 'statistics.json',
    },
    # Input files (from Colab)
    'input': {
        'classification_union': 'classification_union.csv',
        'spacy_ner': 'spacy_ner_results.csv',
        'v2_ner': 'v2_ner_results.csv',
        'setfit_results': 'setfit_introductions.csv',
    },
}


def get_standard_file(
    session_dir: Union[str, Path],
    phase: str,
    file_key: str
) -> Path:
    """
    Get path to a standard pipeline file

    Args:
        session_dir: Session directory path
        phase: Phase name (e.g., '02_ner', '05_mapping')
        file_key: Standard file key (e.g., 'ner_union', 'high_score')

    Returns:
        Path: Full path to the standard file

    Raises:
        KeyError: If phase or file_key is not recognized

    Examples:
        >>> get_standard_file('results/session1', '02_ner', 'ner_union')
        # Path('results/session1/02_ner/ner_union.csv')

        >>> get_standard_file('results/session1', '03_linguistic', 'high_score')
        # Path('results/session1/03_linguistic/high_score_papers.csv')
    """
    if phase not in STANDARD_FILES:
        raise KeyError(f"Unknown phase: {phase}. Valid phases: {list(STANDARD_FILES.keys())}")

    phase_files = STANDARD_FILES[phase]
    if file_key not in phase_files:
        raise KeyError(f"Unknown file key '{file_key}' for phase '{phase}'. Valid keys: {list(phase_files.keys())}")

    filename = phase_files[file_key]
    return get_session_path(session_dir, phase, filename)


if __name__ == "__main__":
    # Test session utilities
    print("Testing Session Utilities")
    print("=" * 80)

    # Test 1: Generate session ID
    session_id = generate_session_id()
    print(f"\n1. Generated session ID: {session_id}")

    # Test 2: Create directories
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        test_session = Path(tmpdir) / session_id
        dirs = create_session_dirs(test_session)
        print(f"\n2. Created {len(dirs)} directories:")
        for name, path in sorted(dirs.items()):
            print(f"   {name}: {path.relative_to(tmpdir)}")

        # Test 3: Get session path
        print("\n3. Session path examples:")
        print(f"   NER union: {get_session_path(test_session, '02_ner', 'ner_union.csv')}")
        print(f"   High score: {get_session_path(test_session, '03_linguistic', 'high_score_papers.csv')}")
        print(f"   Dedup aggressive: {get_session_path(test_session, '07_deduplication/aggressive', 'set_c_dedup.csv')}")

        # Test 4: Get standard file
        print("\n4. Standard file examples:")
        print(f"   NER union: {get_standard_file(test_session, '02_ner', 'ner_union')}")
        print(f"   High score: {get_standard_file(test_session, '03_linguistic', 'high_score')}")
        print(f"   Final inventory: {get_standard_file(test_session, '09_finalization', 'final_inventory')}")

        # Test 5: Validate session
        print("\n5. Validate session:")
        try:
            validate_session_dir(test_session)
            print(f"   Session valid: True")
        except ValueError as e:
            print(f"   Error: {e}")

    print("\n" + "=" * 80)
    print("All tests passed!")
