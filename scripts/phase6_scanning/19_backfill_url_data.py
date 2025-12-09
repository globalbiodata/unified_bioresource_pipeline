#!/usr/bin/env python3
"""
Script 19: Backfill URL Scan Data to Sets A and B

Takes URL scan data from Set C and adds it to Sets A and B based on
matching resource_url or entity name. This ensures all three sets have
identical column structure for comparison.

Created: 2025-11-20
Updated: 2025-12-05 (Session-based refactor)
"""

import argparse
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add lib imports
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from lib.session_utils import get_session_path, validate_session_dir

# Parse command-line arguments
parser = argparse.ArgumentParser(
    description='Backfill URL scan data from Set C to Sets A and B',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Default (aggressive profile):
  python 19_backfill_url_data.py --session-dir results/2025-12-04-143052-a3f9b

  # Specific profile:
  python 19_backfill_url_data.py --session-dir results/2025-12-04-143052-a3f9b --profile balanced
    """
)

parser.add_argument('--session-dir', type=str, required=True,
                    help='Session directory for inputs/outputs')
parser.add_argument('--profile', type=str, default='aggressive',
                    choices=['conservative', 'balanced', 'aggressive'],
                    help='Deduplication profile to use (default: aggressive)')

args = parser.parse_args()

# Validate session directory
SESSION_DIR = Path(args.session_dir).resolve()

if not SESSION_DIR.exists():
    print(f"ERROR: Session directory not found: {SESSION_DIR}")
    sys.exit(1)

try:
    # Validate required phases
    validate_session_dir(SESSION_DIR, required_phases=['07_deduplication', '06_scanning'])
except ValueError as e:
    print(f"ERROR: Invalid session directory: {e}")
    sys.exit(1)

# ============================================================================
# DETERMINE INPUT PATHS
# ============================================================================

# Input paths from deduplication (with profile)
dedup_dir = get_session_path(SESSION_DIR, f'07_deduplication/{args.profile}')
INPUT_SET_A = dedup_dir / 'set_a_linguistic.csv'
INPUT_SET_B = dedup_dir / 'set_b_setfit.csv'
INPUT_SET_C = dedup_dir / 'set_c_final.csv'

# Validate dedup inputs exist
for input_file in [INPUT_SET_A, INPUT_SET_B, INPUT_SET_C]:
    if not input_file.exists():
        print(f"ERROR: Required dedup input file not found: {input_file}")
        sys.exit(1)

# URL scan data from scanning phase
scan_dir = get_session_path(SESSION_DIR, '06_scanning')
URL_SCAN_FILE = scan_dir / 'set_c_url_scan_results.csv'

if not URL_SCAN_FILE.exists():
    print(f"ERROR: URL scan results not found: {URL_SCAN_FILE}")
    sys.exit(1)

# Output directory (same as scanning phase)
OUTPUT_DIR = scan_dir
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Output files
OUTPUT_SET_A = OUTPUT_DIR / f'set_a_linguistic_final_{args.profile}.csv'
OUTPUT_SET_B = OUTPUT_DIR / f'set_b_setfit_final_{args.profile}.csv'
OUTPUT_SET_C = OUTPUT_DIR / f'set_c_union_final_{args.profile}.csv'
STATS_FILE = OUTPUT_DIR / f'backfill_statistics_{args.profile}.txt'

print("="*80)
print("BACKFILLING URL SCAN DATA TO SETS A AND B")
print("="*80)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Session: {SESSION_DIR.name}")
print(f"Profile: {args.profile}")
print(f"\nInput files:")
print(f"  Set A (dedup): {INPUT_SET_A.relative_to(SESSION_DIR)}")
print(f"  Set B (dedup): {INPUT_SET_B.relative_to(SESSION_DIR)}")
print(f"  Set C (dedup): {INPUT_SET_C.relative_to(SESSION_DIR)}")
print(f"  URL scans:     {URL_SCAN_FILE.relative_to(SESSION_DIR)}")
print(f"\nOutput directory: {OUTPUT_DIR.relative_to(SESSION_DIR)}\n")

# ============================================================================
# STEP 1: LOAD ALL DATASETS
# ============================================================================

print("1. Loading datasets...")

try:
    df_a = pd.read_csv(INPUT_SET_A)
    print(f"   Set A loaded: {len(df_a)} resources")
except Exception as e:
    print(f"ERROR: Failed to load Set A from {INPUT_SET_A}")
    print(f"       {e}")
    sys.exit(1)

try:
    df_b = pd.read_csv(INPUT_SET_B)
    print(f"   Set B loaded: {len(df_b)} resources")
except Exception as e:
    print(f"ERROR: Failed to load Set B from {INPUT_SET_B}")
    print(f"       {e}")
    sys.exit(1)

try:
    df_c = pd.read_csv(INPUT_SET_C)
    print(f"   Set C loaded: {len(df_c)} resources")
except Exception as e:
    print(f"ERROR: Failed to load Set C from {INPUT_SET_C}")
    print(f"       {e}")
    sys.exit(1)

try:
    df_url_scans = pd.read_csv(URL_SCAN_FILE)
    # Rename 'url' to 'resource_url' if needed (scanner uses 'url' column)
    if 'url' in df_url_scans.columns and 'resource_url' not in df_url_scans.columns:
        df_url_scans = df_url_scans.rename(columns={'url': 'resource_url'})
    print(f"   URL scans loaded: {len(df_url_scans)} URLs")
except Exception as e:
    print(f"ERROR: Failed to load URL scan data from {URL_SCAN_FILE}")
    print(f"       {e}")
    sys.exit(1)

# Validate required columns
required_cols_a = ['resource_url']
required_cols_b = ['resource_url']
required_cols_c = ['resource_url']
required_cols_scans = ['resource_url']

missing_cols_a = [col for col in required_cols_a if col not in df_a.columns]
missing_cols_b = [col for col in required_cols_b if col not in df_b.columns]
missing_cols_c = [col for col in required_cols_c if col not in df_c.columns]
missing_cols_scans = [col for col in required_cols_scans if col not in df_url_scans.columns]

if missing_cols_a:
    print(f"ERROR: Set A missing required columns: {missing_cols_a}")
    sys.exit(1)
if missing_cols_b:
    print(f"ERROR: Set B missing required columns: {missing_cols_b}")
    sys.exit(1)
if missing_cols_c:
    print(f"ERROR: Set C missing required columns: {missing_cols_c}")
    sys.exit(1)
if missing_cols_scans:
    print(f"ERROR: URL scans missing required columns: {missing_cols_scans}")
    sys.exit(1)

# ============================================================================
# STEP 2: EXTRACT URL SCAN COLUMNS FROM SCAN RESULTS
# ============================================================================

print("\n2. Extracting URL scan columns from scan results...")

# Identify URL scan columns from the scanner output
# The scanner outputs: is_live, status_code, total_score, likelihood, etc.
# We want all columns except the ones already in Sets A/B/C
base_cols = {'resource_url', 'id', 'url', 'entity_long', 'entity_short', 'domain'}
url_cols = [col for col in df_url_scans.columns if col not in base_cols]

# Rename columns with url_ prefix to avoid conflicts
url_cols_renamed = {col: f'url_{col}' for col in url_cols}
df_url_scans = df_url_scans.rename(columns=url_cols_renamed)
url_cols = list(url_cols_renamed.values())

print(f"   Found {len(url_cols)} URL scan columns:")
for col in url_cols:
    print(f"     - {col}")

# Create lookup dataset: resource_url -> URL scan data
url_scan_data = df_url_scans[['resource_url'] + url_cols].copy()
url_scan_data = url_scan_data[url_scan_data['resource_url'].notna()]
print(f"\n   URL scan lookup table: {len(url_scan_data)} URLs")

# ============================================================================
# STEP 3: BACKFILL SET A
# ============================================================================

print("\n3. Backfilling Set A...")

# Merge URL scan data
df_a_final = df_a.merge(url_scan_data, on='resource_url', how='left')

# Count matches (check url_is_live as primary indicator)
matches_a = df_a_final['url_is_live'].notna().sum() if 'url_is_live' in df_a_final.columns else 0
print(f"   Matched URL scan data: {matches_a}/{len(df_a)} resources ({matches_a/len(df_a)*100:.1f}%)")

# Save
try:
    df_a_final.to_csv(OUTPUT_SET_A, index=False)
    print(f"   Saved to: {OUTPUT_SET_A.relative_to(SESSION_DIR)}")
except Exception as e:
    print(f"ERROR: Failed to save Set A to {OUTPUT_SET_A}")
    print(f"       {e}")
    sys.exit(1)

# ============================================================================
# STEP 4: BACKFILL SET B
# ============================================================================

print("\n4. Backfilling Set B...")

# Merge URL scan data
df_b_final = df_b.merge(url_scan_data, on='resource_url', how='left')

# Count matches (check url_is_live as primary indicator)
matches_b = df_b_final['url_is_live'].notna().sum() if 'url_is_live' in df_b_final.columns else 0
print(f"   Matched URL scan data: {matches_b}/{len(df_b)} resources ({matches_b/len(df_b)*100:.1f}%)")

# Save
try:
    df_b_final.to_csv(OUTPUT_SET_B, index=False)
    print(f"   Saved to: {OUTPUT_SET_B.relative_to(SESSION_DIR)}")
except Exception as e:
    print(f"ERROR: Failed to save Set B to {OUTPUT_SET_B}")
    print(f"       {e}")
    sys.exit(1)

# ============================================================================
# STEP 5: BACKFILL SET C
# ============================================================================

print("\n5. Backfilling Set C...")

# Merge URL scan data into Set C
df_c_final = df_c.merge(url_scan_data, on='resource_url', how='left')

# Count matches (check url_is_live as primary indicator)
matches_c = df_c_final['url_is_live'].notna().sum() if 'url_is_live' in df_c_final.columns else 0
print(f"   Matched URL scan data: {matches_c}/{len(df_c)} resources ({matches_c/len(df_c)*100:.1f}%)")

# Save
try:
    df_c_final.to_csv(OUTPUT_SET_C, index=False)
    print(f"   Saved to: {OUTPUT_SET_C.relative_to(SESSION_DIR)}")
except Exception as e:
    print(f"ERROR: Failed to save Set C to {OUTPUT_SET_C}")
    print(f"       {e}")
    sys.exit(1)

# ============================================================================
# STEP 6: VALIDATE COLUMN CONSISTENCY
# ============================================================================

print("\n6. Validating column consistency...")

cols_a = set(df_a_final.columns)
cols_b = set(df_b_final.columns)
cols_c = set(df_c_final.columns)

# Check if all sets have same columns
all_same = (cols_a == cols_b == cols_c)

if all_same:
    print(f"   All three sets have identical columns ({len(cols_a)} columns)")
else:
    print(f"   Warning: Column mismatch detected:")
    print(f"      Set A: {len(cols_a)} columns")
    print(f"      Set B: {len(cols_b)} columns")
    print(f"      Set C: {len(cols_c)} columns")

    # Show differences
    only_a = cols_a - cols_c
    only_b = cols_b - cols_c
    only_c = cols_c - cols_a

    if only_a:
        print(f"\n      Only in A: {only_a}")
    if only_b:
        print(f"      Only in B: {only_b}")
    if only_c:
        print(f"      Only in C: {only_c}")

# ============================================================================
# STEP 7: GENERATE STATISTICS
# ============================================================================

print("\n7. Generating statistics...")

stats = []
stats.append("="*80)
stats.append("URL SCAN DATA BACKFILL STATISTICS")
stats.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
stats.append(f"Session: {SESSION_DIR.name}")
stats.append(f"Profile: {args.profile}")
stats.append("="*80)
stats.append("")

stats.append("INPUT DATASETS:")
stats.append(f"  Set A (Linguistic): {len(df_a)} resources")
stats.append(f"  Set B (SetFit):     {len(df_b)} resources")
stats.append(f"  Set C (Union):      {len(df_c)} resources")
stats.append(f"  URL Scans:          {len(df_url_scans)} URLs")
stats.append("")

stats.append("URL SCAN DATA COVERAGE:")
stats.append(f"  Set A: {matches_a}/{len(df_a)} ({matches_a/len(df_a)*100:.1f}%)")
stats.append(f"  Set B: {matches_b}/{len(df_b)} ({matches_b/len(df_b)*100:.1f}%)")
stats.append(f"  Set C: {matches_c}/{len(df_c)} ({matches_c/len(df_c)*100:.1f}%)")
stats.append("")

stats.append("URL SCAN COLUMNS ADDED:")
for col in url_cols:
    stats.append(f"  - {col}")
stats.append("")

stats.append("COLUMN CONSISTENCY:")
if all_same:
    stats.append(f"  All three sets have identical {len(cols_a)} columns")
else:
    stats.append(f"  Warning: Column mismatch:")
    stats.append(f"     Set A: {len(cols_a)} columns")
    stats.append(f"     Set B: {len(cols_b)} columns")
    stats.append(f"     Set C: {len(cols_c)} columns")
stats.append("")

stats.append("OUTPUT FILES:")
stats.append(f"  {OUTPUT_SET_A.name}")
stats.append(f"  {OUTPUT_SET_B.name}")
stats.append(f"  {OUTPUT_SET_C.name}")
stats.append("")

stats_text = '\n'.join(stats)

try:
    with open(STATS_FILE, 'w') as f:
        f.write(stats_text)
    print(f"   Statistics saved to: {STATS_FILE.relative_to(SESSION_DIR)}")
except Exception as e:
    print(f"ERROR: Failed to save statistics to {STATS_FILE}")
    print(f"       {e}")
    sys.exit(1)

print("\n" + stats_text)

print("\n" + "="*80)
print("COMPLETE!")
print("="*80)
print(f"\nFinal datasets ready for analysis:")
print(f"  Set A: {OUTPUT_SET_A.relative_to(SESSION_DIR)}")
print(f"  Set B: {OUTPUT_SET_B.relative_to(SESSION_DIR)}")
print(f"  Set C: {OUTPUT_SET_C.relative_to(SESSION_DIR)}")
print(f"\nStatistics: {STATS_FILE.relative_to(SESSION_DIR)}")
print(f"\nAll sets now have identical column structure with URL scan data.")
print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
