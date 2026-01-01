# Phase 0: Data Acquisition

Scripts for fetching papers from Europe PMC.

## Overview

Phase 0 queries Europe PMC to retrieve candidate papers that might describe bioresources. The query strategy balances recall (capturing all relevant papers) with precision (limiting false positives).

## Script

### query_epmc.py

Queries Europe PMC API and saves results with enhanced metadata.

**Usage:**
```bash
python query_epmc.py config/epmc_query_v5.1.txt -f 2022-01-01 -t 2024-06-30 -o data/my_batch/
```

**Arguments:**
- `query` - Query file or string
- `-f / --from-date` - Start date (YYYY, YYYY-MM, or YYYY-MM-DD)
- `-t / --to-date` - End date (defaults to today)
- `-o / --out-dir` - Output directory (default: out/)

**Output:**
- `query_results.csv` - Papers with 20 metadata fields
- `last_query_dates.txt` - Date range for record-keeping

## Query Strategy

The query (in `config/epmc_query_v5.1.txt`) uses a three-pronged approach:

### 1. MeSH Terms
Medical Subject Headings for database-related papers:
- Databases, Genetic
- Databases, Protein
- Databases, Factual
- Databases, Nucleic Acid
- Knowledge Bases

### 2. Title Keywords
Resource-related terms in paper titles:
- `database*`, `repositor*`, `atlas`, `portal`, `consortium`
- `knowledgebase`, `resource*`, `platform`, `dataset`
- `toolkit`, `toolbox`, `catalog*`, `collection*`, `browser`

### 3. Abstract Patterns
Papers with URLs and resource keywords in abstracts:
- Abstract contains `http*` AND (`database*` OR `repositor*` OR `resource*` OR `collection*` OR `catalog*`)

### Filters
- Excludes retractions/errata: `NOT (TITLE:(retract* OR withdraw* OR erratum))`
- PubMed/PMC only: `SRC:(MED OR PMC)`
- Date range: `FIRST_PDATE:[{0} TO {1}]`

## Metadata Fields

The script extracts 20 fields from EPMC:

**Core (4):**
- `id`, `title`, `abstract`, `publication_date`

**Boolean Flags (8):**
- `hasDbCrossReferences`, `hasData`, `hasSuppl`, `isOpenAccess`
- `inPMC`, `inEPMC`, `hasPDF`, `hasBook`

**Metrics (2):**
- `citedByCount`, `pubYear`

**Rich Metadata (6):**
- `pubType` (JSON array)
- `keywords` (JSON array)
- `meshTerms` (JSON array)
- `journalTitle`
- `journalISSN`
- `authorAffiliations` (JSON array)

## Expected Results

Typical yields by date range:
- **2022:** ~27,000 papers
- **2011-2021:** ~190,000 papers

Most papers are filtered out in Phase 1 (Classification). This broad query ensures high recall.

## Examples

### Fetch a single year
```bash
python query_epmc.py config/epmc_query_v5.1.txt -f 2023-01-01 -t 2023-12-31 -o data/2023/
```

### Fetch recent papers (to today)
```bash
python query_epmc.py config/epmc_query_v5.1.txt -f 2024-01-01 -o data/2024_current/
```

### Use custom query
```bash
python query_epmc.py "TITLE:database AND (SRC:MED) AND (FIRST_PDATE:[{0} TO {1}])" -f 2022-01-01 -t 2022-12-31 -o data/test/
```

## Integration with Pipeline

Output from Phase 0 feeds directly into Phase 1 (Classification):

```bash
# Phase 0: Fetch papers
python scripts/phase0_data_acquisition/query_epmc.py \
    config/epmc_query_v5.1.txt \
    -f 2022-01-01 -t 2024-06-30 \
    -o data/my_batch/

# Phase 1+: Run pipeline
python run_pipeline.py --input-dir data/my_batch/ --profiles aggressive
```

## Notes

- The script handles pagination automatically for large result sets
- API requests are synchronous (no rate limiting needed for reasonable queries)
- Query string uses Python `.format()` for date substitution: `{0}` = from_date, `{1}` = to_date
- Date validation requires YYYY, YYYY-MM, or YYYY-MM-DD format
