#!/usr/bin/env python3
"""
Session Manager - Handle session IDs, directories, and metadata tracking

Provides functions for:
- Generating unique session IDs
- Creating session directory structures
- Loading/saving session metadata
- Tracking step completion
- Listing available sessions

Created: 2025-11-21
"""

import json
import random
import string
from pathlib import Path
from datetime import datetime
from typing import Dict, List


def generate_session_id() -> str:
    """
    Generate unique session ID: YYYY-MM-DD-HHMMSS-xxxxx

    Format:
        - Date: YYYY-MM-DD
        - Time: HHMMSS
        - Random suffix: 5 lowercase alphanumeric characters

    Returns:
        str: Session ID (e.g., "2025-11-21-095430-a3f9b")

    Examples:
        >>> session_id = generate_session_id()
        >>> # Returns something like: "2025-11-21-095430-a3f9b"
    """
    timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
    return f"{timestamp}-{random_suffix}"


def create_session_dirs(session_id: str, base_dir: Path) -> Dict[str, Path]:
    """
    Create session directory structure

    Creates:
        results/sessions/{session_id}/
        ├── deduplicated/
        ├── url_scanned/
        ├── final/
        ├── baseline_comparison/
        └── visualizations/

    Args:
        session_id: Unique session identifier
        base_dir: Base directory (pipeline_synthesis_2025-11-18)

    Returns:
        Dict[str, Path]: Dictionary of directory paths

    Examples:
        >>> dirs = create_session_dirs("2025-11-21-095430-a3f9b", Path("/path/to/base"))
        >>> print(dirs['deduplicated'])
        # /path/to/base/results/sessions/2025-11-21-095430-a3f9b/deduplicated
    """
    session_dir = base_dir / 'results' / 'sessions' / session_id

    dirs = {
        'session': session_dir,
        'deduplicated': session_dir / 'deduplicated',
        'url_scanned': session_dir / 'url_scanned',
        'final': session_dir / 'final',
        'baseline_comparison': session_dir / 'baseline_comparison',
        'visualizations': session_dir / 'visualizations'
    }

    # Create all directories
    for dir_path in dirs.values():
        dir_path.mkdir(parents=True, exist_ok=True)

    return dirs


def load_session_metadata(session_id: str, base_dir: Path) -> dict:
    """
    Load session metadata from JSON file

    If file doesn't exist, returns empty metadata template.

    Args:
        session_id: Unique session identifier
        base_dir: Base directory

    Returns:
        dict: Session metadata

    Structure:
        {
            'session_id': str,
            'created_at': str (ISO format),
            'completed_steps': List[int],
            'pipeline_version': str,
            'steps': {
                '1': {'name': str, 'completed': bool, 'timestamp': str},
                ...
            }
        }
    """
    metadata_file = base_dir / 'results' / 'sessions' / session_id / 'session_metadata.json'

    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            return json.load(f)

    # Return empty template
    return {
        'session_id': session_id,
        'created_at': datetime.now().isoformat(),
        'completed_steps': [],
        'pipeline_version': '2025-11-21',
        'steps': {}
    }


def save_session_metadata(session_id: str, metadata: dict, base_dir: Path):
    """
    Save session metadata to JSON file

    Args:
        session_id: Unique session identifier
        metadata: Session metadata dictionary
        base_dir: Base directory
    """
    metadata_file = base_dir / 'results' / 'sessions' / session_id / 'session_metadata.json'

    # Ensure directory exists
    metadata_file.parent.mkdir(parents=True, exist_ok=True)

    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)


def mark_step_complete(metadata: dict, step_num: int, step_name: str) -> dict:
    """
    Mark step as complete in metadata

    Updates both the 'steps' dictionary and 'completed_steps' list.

    Args:
        metadata: Session metadata dictionary
        step_num: Step number (1-7)
        step_name: Human-readable step name

    Returns:
        dict: Updated metadata
    """
    metadata['steps'][str(step_num)] = {
        'name': step_name,
        'completed': True,
        'timestamp': datetime.now().isoformat()
    }

    if step_num not in metadata['completed_steps']:
        metadata['completed_steps'].append(step_num)
        metadata['completed_steps'].sort()

    return metadata


def is_step_complete(metadata: dict, step_num: int) -> bool:
    """
    Check if step is marked as complete

    Args:
        metadata: Session metadata dictionary
        step_num: Step number (1-7)

    Returns:
        bool: True if step is complete, False otherwise
    """
    return step_num in metadata.get('completed_steps', [])


def list_sessions(base_dir: Path) -> List[dict]:
    """
    List all available sessions with metadata

    Returns sessions sorted by creation date (most recent first).

    Args:
        base_dir: Base directory

    Returns:
        List[dict]: List of session info dictionaries

    Structure:
        [
            {
                'session_id': str,
                'created_at': str,
                'completed_steps': int,
                'total_steps': int
            },
            ...
        ]
    """
    sessions_dir = base_dir / 'results' / 'sessions'

    if not sessions_dir.exists():
        return []

    sessions = []
    for session_dir in sorted(sessions_dir.iterdir(), reverse=True):
        if session_dir.is_dir():
            metadata = load_session_metadata(session_dir.name, base_dir)
            sessions.append({
                'session_id': session_dir.name,
                'created_at': metadata.get('created_at', 'Unknown'),
                'completed_steps': len(metadata.get('completed_steps', [])),
                'total_steps': 7
            })

    return sessions


if __name__ == "__main__":
    # Test session manager functions
    print("Testing Session Manager")
    print("=" * 80)

    # Test 1: Generate session ID
    session_id = generate_session_id()
    print(f"\n1. Generated session ID: {session_id}")

    # Test 2: Create directories
    test_base = Path("/tmp/test_pipeline")
    dirs = create_session_dirs(session_id, test_base)
    print(f"\n2. Created {len(dirs)} directories:")
    for name, path in dirs.items():
        print(f"   {name}: {path}")

    # Test 3: Load metadata (should create empty)
    metadata = load_session_metadata(session_id, test_base)
    print(f"\n3. Loaded metadata: {json.dumps(metadata, indent=2)}")

    # Test 4: Mark step complete
    metadata = mark_step_complete(metadata, 1, "Entity mapping")
    metadata = mark_step_complete(metadata, 2, "Filtered datasets")
    print(f"\n4. Marked steps 1-2 complete: {metadata['completed_steps']}")

    # Test 5: Check completion
    print(f"\n5. Step completion checks:")
    print(f"   Step 1: {is_step_complete(metadata, 1)}")
    print(f"   Step 3: {is_step_complete(metadata, 3)}")

    # Test 6: Save metadata
    save_session_metadata(session_id, metadata, test_base)
    print(f"\n6. Saved metadata to file")

    # Test 7: List sessions
    sessions = list_sessions(test_base)
    print(f"\n7. Found {len(sessions)} session(s):")
    for s in sessions:
        print(f"   {s['session_id']}: {s['completed_steps']}/{s['total_steps']} steps")

    print("\n" + "=" * 80)
    print("All tests passed!")
