#!/usr/bin/env python3
"""
Validation script for primary resource mapping (Script 11 output)
Session: 2025-12-04-111420-z381s
"""

import pandas as pd
import sys
from pathlib import Path

# Configuration
SESSION_DIR = Path("/Users/warren/development/GBC/inventory_2022/unified_bioresource_pipeline/2025-12-04-111420-z381s")
FILE_PATH = SESSION_DIR / "05_mapping" / "union_papers_with_primary_resources.csv"

# Expected values from script output
EXPECTED = {
    "total_rows": 7599,
    "status": {
        "ok": 6631,
        "conflict": 952,
        "no_entities": 15,
        "low_score": 1
    },
    "has_primary_long": 4701,
    "has_primary_short": 3432,
    "has_both": 549,
    "has_neither": 15
}

REQUIRED_COLUMNS = [
    "pmid", "title", "abstract", "primary_entity_long",
    "primary_entity_short", "primary_score", "status"
]

def validate_file():
    """Run all validation checks"""
    results = {
        "checks": [],
        "warnings": [],
        "errors": [],
        "samples": {}
    }

    # Check 1: File exists
    if not FILE_PATH.exists():
        results["errors"].append(f"❌ File does not exist: {FILE_PATH}")
        return results
    results["checks"].append(f"✓ File exists: {FILE_PATH}")

    # Load data
    try:
        df = pd.read_csv(FILE_PATH)
        results["checks"].append(f"✓ Successfully loaded CSV file")
    except Exception as e:
        results["errors"].append(f"❌ Failed to load CSV: {e}")
        return results

    # Check 2: Row count
    actual_rows = len(df)
    expected_rows = EXPECTED["total_rows"]
    if actual_rows == expected_rows:
        results["checks"].append(f"✓ Row count matches: {actual_rows} rows")
    else:
        results["errors"].append(f"❌ Row count mismatch: expected {expected_rows}, got {actual_rows}")

    # Check 3: Required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if not missing_cols:
        results["checks"].append(f"✓ All required columns present")
    else:
        results["errors"].append(f"❌ Missing required columns: {missing_cols}")

    # Check 4: Duplicate PMIDs
    dup_pmids = df[df.duplicated(subset=['pmid'], keep=False)]
    if len(dup_pmids) == 0:
        results["checks"].append(f"✓ No duplicate PMIDs")
    else:
        results["errors"].append(f"❌ Found {len(dup_pmids)} duplicate PMIDs")
        results["samples"]["duplicate_pmids"] = dup_pmids['pmid'].tolist()[:10]

    # Check 5: Status value counts
    status_counts = df['status'].value_counts().to_dict()
    results["samples"]["status_counts"] = status_counts

    all_status_match = True
    for status, expected_count in EXPECTED["status"].items():
        actual_count = status_counts.get(status, 0)
        if actual_count == expected_count:
            results["checks"].append(f"✓ Status '{status}' count matches: {actual_count}")
        else:
            results["errors"].append(f"❌ Status '{status}' count mismatch: expected {expected_count}, got {actual_count}")
            all_status_match = False

    # Check for unexpected status values
    expected_statuses = set(EXPECTED["status"].keys())
    actual_statuses = set(status_counts.keys())
    unexpected = actual_statuses - expected_statuses
    if unexpected:
        results["warnings"].append(f"⚠ Unexpected status values found: {unexpected}")

    # Check 6: Primary entity presence for status='ok'
    ok_rows = df[df['status'] == 'ok']
    ok_no_entity = ok_rows[
        (ok_rows['primary_entity_long'].isna() | (ok_rows['primary_entity_long'] == '')) &
        (ok_rows['primary_entity_short'].isna() | (ok_rows['primary_entity_short'] == ''))
    ]
    if len(ok_no_entity) == 0:
        results["checks"].append(f"✓ All status='ok' rows have at least one primary entity")
    else:
        results["errors"].append(f"❌ Found {len(ok_no_entity)} status='ok' rows without any primary entity")
        results["samples"]["ok_no_entity"] = ok_no_entity['pmid'].tolist()[:5]

    # Check 7: No entities for status='no_entities'
    no_entity_rows = df[df['status'] == 'no_entities']
    has_entity = no_entity_rows[
        (~no_entity_rows['primary_entity_long'].isna() & (no_entity_rows['primary_entity_long'] != '')) |
        (~no_entity_rows['primary_entity_short'].isna() & (no_entity_rows['primary_entity_short'] != ''))
    ]
    if len(has_entity) == 0:
        results["checks"].append(f"✓ All status='no_entities' rows have empty primary entities")
    else:
        results["errors"].append(f"❌ Found {len(has_entity)} status='no_entities' rows WITH entities")
        results["samples"]["no_entities_but_has"] = has_entity['pmid'].tolist()

    # Check 8: Primary entity counts
    has_long = df[~df['primary_entity_long'].isna() & (df['primary_entity_long'] != '')].shape[0]
    has_short = df[~df['primary_entity_short'].isna() & (df['primary_entity_short'] != '')].shape[0]
    has_both = df[
        (~df['primary_entity_long'].isna() & (df['primary_entity_long'] != '')) &
        (~df['primary_entity_short'].isna() & (df['primary_entity_short'] != ''))
    ].shape[0]
    has_neither = df[
        (df['primary_entity_long'].isna() | (df['primary_entity_long'] == '')) &
        (df['primary_entity_short'].isna() | (df['primary_entity_short'] == ''))
    ].shape[0]

    if has_long == EXPECTED["has_primary_long"]:
        results["checks"].append(f"✓ Has primary_long count matches: {has_long}")
    else:
        results["warnings"].append(f"⚠ Has primary_long count: expected {EXPECTED['has_primary_long']}, got {has_long}")

    if has_short == EXPECTED["has_primary_short"]:
        results["checks"].append(f"✓ Has primary_short count matches: {has_short}")
    else:
        results["warnings"].append(f"⚠ Has primary_short count: expected {EXPECTED['has_primary_short']}, got {has_short}")

    if has_both == EXPECTED["has_both"]:
        results["checks"].append(f"✓ Has both entities count matches: {has_both}")
    else:
        results["warnings"].append(f"⚠ Has both entities count: expected {EXPECTED['has_both']}, got {has_both}")

    if has_neither == EXPECTED["has_neither"]:
        results["checks"].append(f"✓ Has neither entity count matches: {has_neither}")
    else:
        results["warnings"].append(f"⚠ Has neither entity count: expected {EXPECTED['has_neither']}, got {has_neither}")

    # Check 9: Primary score range
    scores = df[~df['primary_score'].isna()]['primary_score']
    if len(scores) > 0:
        min_score = scores.min()
        max_score = scores.max()
        results["samples"]["score_range"] = {"min": float(min_score), "max": float(max_score)}

        if 0 <= min_score and max_score <= 25:
            results["checks"].append(f"✓ Primary scores in reasonable range (0-25): {min_score:.2f} to {max_score:.2f}")
        else:
            results["warnings"].append(f"⚠ Primary scores outside expected range: {min_score:.2f} to {max_score:.2f}")

    # Check 10: Sample primary entities
    long_entities = df[~df['primary_entity_long'].isna() & (df['primary_entity_long'] != '')]['primary_entity_long'].dropna().sample(min(10, has_long)).tolist()
    short_entities = df[~df['primary_entity_short'].isna() & (df['primary_entity_short'] != '')]['primary_entity_short'].dropna().sample(min(10, has_short)).tolist()

    results["samples"]["primary_long_examples"] = long_entities
    results["samples"]["primary_short_examples"] = short_entities

    # Check 11: Source distribution
    if 'ner_source' in df.columns:
        source_counts = df['ner_source'].value_counts().to_dict()
        results["samples"]["ner_source_distribution"] = source_counts
        results["checks"].append(f"✓ NER source distribution collected")

    return results

def print_report(results):
    """Print validation report"""
    print("=" * 80)
    print("PRIMARY RESOURCE MAPPING VALIDATION REPORT")
    print("Session: 2025-12-04-111420-z381s")
    print("=" * 80)
    print()

    # Passed checks
    print("PASSED CHECKS:")
    print("-" * 80)
    for check in results["checks"]:
        print(f"  {check}")
    print()

    # Warnings
    if results["warnings"]:
        print("WARNINGS:")
        print("-" * 80)
        for warning in results["warnings"]:
            print(f"  {warning}")
        print()

    # Errors
    if results["errors"]:
        print("ERRORS:")
        print("-" * 80)
        for error in results["errors"]:
            print(f"  {error}")
        print()

    # Samples
    print("SAMPLE DATA:")
    print("-" * 80)

    if "status_counts" in results["samples"]:
        print("Status Distribution:")
        for status, count in results["samples"]["status_counts"].items():
            pct = (count / EXPECTED["total_rows"]) * 100
            print(f"  - {status}: {count} ({pct:.1f}%)")
        print()

    if "score_range" in results["samples"]:
        print(f"Primary Score Range: {results['samples']['score_range']['min']:.2f} to {results['samples']['score_range']['max']:.2f}")
        print()

    if "ner_source_distribution" in results["samples"]:
        print("NER Source Distribution:")
        for source, count in results["samples"]["ner_source_distribution"].items():
            print(f"  - {source}: {count}")
        print()

    if "primary_long_examples" in results["samples"]:
        print("Sample Primary Long Entities:")
        for i, entity in enumerate(results["samples"]["primary_long_examples"][:5], 1):
            print(f"  {i}. {entity}")
        print()

    if "primary_short_examples" in results["samples"]:
        print("Sample Primary Short Entities:")
        for i, entity in enumerate(results["samples"]["primary_short_examples"][:5], 1):
            print(f"  {i}. {entity}")
        print()

    # Overall status
    print("=" * 80)
    if results["errors"]:
        print("OVERALL STATUS: ❌ FAIL")
        print(f"Found {len(results['errors'])} errors")
        print("RECOMMENDATION: INVESTIGATE ISSUES BEFORE PROCEEDING")
    elif results["warnings"]:
        print("OVERALL STATUS: ⚠ PASS WITH WARNINGS")
        print(f"Found {len(results['warnings'])} warnings")
        print("RECOMMENDATION: REVIEW WARNINGS, LIKELY SAFE TO PROCEED")
    else:
        print("OVERALL STATUS: ✅ PASS")
        print("RECOMMENDATION: PROCEED TO NEXT STEP")
    print("=" * 80)

    return 0 if not results["errors"] else 1

if __name__ == "__main__":
    results = validate_file()
    exit_code = print_report(results)
    sys.exit(exit_code)
