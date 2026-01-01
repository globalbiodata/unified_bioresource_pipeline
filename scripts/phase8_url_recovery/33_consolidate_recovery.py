#!/usr/bin/env python3
"""
Script 33: Consolidate URL Recovery Results

Purpose: Merge results from abstract and fulltext searches.
         Prepare web search chunks for manual/agent processing.
         Generate agent brief and documentation.

Input:  - abstract_url_results.csv (from script 30)
        - fulltext_url_results.csv (from script 32)
Output: - recovered_urls.csv (all found URLs)
        - still_missing.csv (records for web search)
        - websearch_chunks/chunk_XX.csv
        - websearch_chunks/AGENT_BRIEF.md

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-11-28
"""

import argparse
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

# Add unified_bioresource_pipeline to path for session utils
pipeline_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(pipeline_root))
from lib.session_utils import validate_session_dir, get_session_path


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Consolidate URL recovery results and prepare web search'
    )
    parser.add_argument(
        '--session-dir',
        required=True,
        help='Session directory containing pipeline data'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=70,
        help='Records per web search chunk (default: 70)'
    )
    parser.add_argument(
        '--num-chunks',
        type=int,
        default=6,
        help='Number of web search chunks (default: 6)'
    )

    return parser.parse_args()


def generate_agent_brief() -> str:
    """Generate the agent brief markdown for web search."""
    return '''# URL Recovery Agent Brief

## Your Task
Search the web to find URLs for bioresource databases listed in your assigned chunk file.

---

## CRITICAL: Output File Specification

Your output file **MUST** follow this exact format for automated merging.

### Required Filename Pattern
```
websearch_results_chunk_XX.csv
```
Where `XX` matches your input chunk number (e.g., `websearch_results_chunk_01.csv`)

### Required Columns (in order)
| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `original_record_num` | integer | YES | Copy from input - DO NOT MODIFY |
| `database_name` | string | YES | Copy from input - DO NOT MODIFY |
| `long_database_name` | string | YES | Copy from input - DO NOT MODIFY |
| `found_urls` | string | YES | URL found OR `NOT_FOUND` |
| `url_source` | string | YES | Must be `web_search` or `not_available` |
| `match_quality` | string | YES | `HIGH`, `MEDIUM`, `LOW`, or empty |
| `notes` | string | NO | Optional explanation |

### Column Value Rules

**found_urls:**
- Single URL: `https://example.org/database`
- Multiple URLs: `https://example1.org|https://example2.org` (pipe-separated)
- Not found: `NOT_FOUND` (exact string, all caps)

**url_source:**
- If URL found: `web_search`
- If not found: `not_available`

**match_quality:**
- `HIGH` - URL contains database name/abbreviation
- `MEDIUM` - URL found in clear context
- `LOW` - URL found but connection uncertain
- Empty if `found_urls` is `NOT_FOUND`

### Example Output Row
```csv
original_record_num,database_name,long_database_name,found_urls,url_source,match_quality,notes
123,BioDB,Biological Database,https://biodb.org,web_search,HIGH,Official website found
124,MyData,My Data Resource,NOT_FOUND,not_available,,No dedicated website found
```

---

## What We're Looking For

**WANT:** Dedicated web interfaces for biological databases
- Example: `genome.ucsc.edu`, `www.proteinatlas.org`, `biocyc.org`
- These are websites where users can query, browse, or interact with data

---

## MANDATORY EXCLUSIONS - DO NOT RETURN THESE URLs

**These URL types will be AUTOMATICALLY REJECTED by the merge script.**
If you only find these types of URLs for a database, mark it as `NOT_FOUND`.

### Code Repositories
| Pattern | Example | Why Excluded |
|---------|---------|--------------|
| `github.com/*` | `github.com/user/repo` | Code repository, not web interface |
| `*.github.io/*` | `mydb.github.io` | GitHub Pages hosted |
| `gitlab.com/*` | `gitlab.com/user/repo` | Code repository |
| `bitbucket.org/*` | `bitbucket.org/user/repo` | Code repository |
| `sourceforge.net/*` | `sourceforge.net/projects/x` | Software hosting |

### Data Archives & DOIs
| Pattern | Example | Why Excluded |
|---------|---------|--------------|
| `zenodo.org/*` | `zenodo.org/record/123` | File archive |
| `doi.org/*` | `doi.org/10.1234/xyz` | DOI resolver (not direct interface) |
| `datadryad.org/*` | `datadryad.org/stash/dataset/...` | Data archive |
| `dryad.*/` | `doi.org/10.5061/dryad.xyz` | Dryad DOI |
| `figshare.com/*` | `figshare.com/articles/...` | File sharing |
| `osf.io/*` | `osf.io/abc123` | Open Science Framework |

### File Servers
| Pattern | Example | Why Excluded |
|---------|---------|--------------|
| `ftp://*` | `ftp://ftp.ncbi.nih.gov` | FTP server |
| `ftp.*` | `ftp.ebi.ac.uk` | FTP server |

### Package Repositories
| Pattern | Example | Why Excluded |
|---------|---------|--------------|
| `cran.r-project.org/*` | `cran.r-project.org/package=x` | R package |
| `bioconductor.org/packages/*` | `bioconductor.org/packages/x` | Bioconductor package |
| `pypi.org/*` | `pypi.org/project/x` | Python package |

### Generic Institutional (without specific path)
| Pattern | Example | Why Excluded |
|---------|---------|--------------|
| `*.edu` (root only) | `stanford.edu`, `mit.edu` | Generic institution |
| `*.ac.uk` (root only) | `cam.ac.uk` | Generic institution |

**Note:** Institutional URLs WITH specific database paths ARE acceptable:
- ✅ `genome.ucsc.edu` - specific database
- ✅ `www.ebi.ac.uk/chebi` - specific database path
- ❌ `stanford.edu` - just institution root

---

## Search Strategy
For each record:
1. Search: `"{database_name}" database`
2. Search: `"{long_database_name}" bioinformatics`
3. Look for dedicated web interface in results
4. **If only GitHub/Zenodo/DOI found → mark as `NOT_FOUND`**

## Important Notes
- Dead URLs are OK to record (we track them separately)
- When uncertain, include the URL with LOW quality
- One URL per database is sufficient
- **Preserve original_record_num exactly** - this is the merge key!
- **If the database only exists as code on GitHub, mark `NOT_FOUND`** - we want web interfaces

---

## Checklist Before Submitting
- [ ] File named `websearch_results_chunk_XX.csv`
- [ ] All required columns present
- [ ] `original_record_num` values unchanged from input
- [ ] `url_source` is `web_search` for found URLs
- [ ] No spaces in URLs (use pipe `|` to separate multiple)
- [ ] **NO GitHub, Zenodo, DOI, Dryad, or Figshare URLs included**
'''


def main():
    args = parse_args()

    print(f"=" * 60)
    print(f"Script 33: Consolidate URL Recovery Results")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"=" * 60)

    # Validate session directory
    validate_session_dir(args.session_dir)
    session_dir = args.session_dir

    # Input files from scripts 30 and 32
    abstract_results_path = get_session_path(session_dir, '08_url_recovery', 'abstract_url_results.csv')
    fulltext_results_path = get_session_path(session_dir, '08_url_recovery', 'fulltext_url_results.csv')

    # Output to URL recovery directory
    output_dir = get_session_path(session_dir, '08_url_recovery')
    websearch_dir = output_dir / 'websearch_chunks'
    websearch_dir.mkdir(parents=True, exist_ok=True)

    # Load results
    print(f"\nLoading abstract results: {abstract_results_path}")
    abstract_df = pd.read_csv(abstract_results_path)
    print(f"  Records: {len(abstract_df)}")

    print(f"\nLoading fulltext results: {fulltext_results_path}")
    fulltext_df = pd.read_csv(fulltext_results_path)
    print(f"  Records: {len(fulltext_df)}")

    # Merge results
    # Start with abstract results (all records)
    # Update with fulltext results where abstract was NOT_FOUND

    merged_records = []

    for idx, row in abstract_df.iterrows():
        record = row.to_dict()

        if record['found_urls'] != 'NOT_FOUND':
            # Found in abstract search
            merged_records.append(record)
        else:
            # Check fulltext results
            orig_num = record.get('original_record_num')
            fulltext_match = fulltext_df[
                fulltext_df['original_record_num'] == orig_num
            ]

            if not fulltext_match.empty:
                ft_record = fulltext_match.iloc[0].to_dict()
                if ft_record['found_urls'] != 'NOT_FOUND':
                    # Found in fulltext
                    merged_records.append(ft_record)
                else:
                    # Still not found
                    merged_records.append(record)
            else:
                merged_records.append(record)

    merged_df = pd.DataFrame(merged_records)

    # Split into found and still missing
    found_mask = merged_df['found_urls'] != 'NOT_FOUND'
    found_df = merged_df[found_mask].copy()
    missing_df = merged_df[~found_mask].copy()

    # Save recovered URLs
    recovered_file = output_dir / 'recovered_urls.csv'
    found_df.to_csv(recovered_file, index=False)
    print(f"\nSaved recovered URLs: {recovered_file}")
    print(f"  Records with URLs: {len(found_df)}")

    # Save still missing
    missing_file = output_dir / 'still_missing.csv'
    missing_df.to_csv(missing_file, index=False)
    print(f"\nSaved still missing: {missing_file}")
    print(f"  Records without URLs: {len(missing_df)}")

    # Create web search chunks
    if len(missing_df) > 0:
        print(f"\nCreating web search chunks...")

        # Calculate chunk size
        num_chunks = min(args.num_chunks, len(missing_df))
        chunk_size = math.ceil(len(missing_df) / num_chunks)

        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(missing_df))
            chunk_df = missing_df.iloc[start_idx:end_idx]

            chunk_file = websearch_dir / f'chunk_{i+1:02d}.csv'
            chunk_df.to_csv(chunk_file, index=False)
            print(f"  Created: {chunk_file} ({len(chunk_df)} records)")

        # Generate agent brief
        brief_file = websearch_dir / 'AGENT_BRIEF.md'
        with open(brief_file, 'w') as f:
            f.write(generate_agent_brief())
        print(f"  Created: {brief_file}")

    # Statistics
    total = len(merged_df)
    found_abstract = len(abstract_df[abstract_df['found_urls'] != 'NOT_FOUND'])
    found_fulltext = len(found_df) - found_abstract

    print(f"\n{'=' * 60}")
    print(f"Summary")
    print(f"{'=' * 60}")
    print(f"  Total records processed: {total}")
    print(f"  URLs found in abstract: {found_abstract} ({found_abstract/total*100:.1f}%)")
    print(f"  URLs found in fulltext: {found_fulltext} ({found_fulltext/total*100:.1f}%)")
    print(f"  Total URLs recovered: {len(found_df)} ({len(found_df)/total*100:.1f}%)")
    print(f"  Still missing (need web search): {len(missing_df)} ({len(missing_df)/total*100:.1f}%)")

    if len(found_df) > 0:
        quality_counts = found_df['match_quality'].value_counts()
        print(f"\n  Quality breakdown:")
        for q, c in sorted(quality_counts.items()):
            print(f"    {q}: {c}")

    print(f"\n{'=' * 60}")
    print(f"Output Files")
    print(f"{'=' * 60}")
    print(f"  {recovered_file}")
    print(f"  {missing_file}")
    if len(missing_df) > 0:
        print(f"  {websearch_dir}/chunk_*.csv")
        print(f"  {websearch_dir}/AGENT_BRIEF.md")

    print(f"\nPhase 8 (URL Recovery) complete.")
    print(f"To recover more URLs, run web search agents on the chunk files.")
    print(f"Completed: {datetime.now().isoformat()}")


if __name__ == '__main__':
    main()
