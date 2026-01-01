# Phase 0 Quick Reference

## One-Line Command

```bash
python scripts/phase0_data_acquisition/query_epmc.py config/epmc_query_v5.1.txt -f 2022-01-01 -t 2024-06-30 -o data/my_batch/
```

## Output Files

```
data/my_batch/
├── query_results.csv      # Papers with 20 metadata fields
└── last_query_dates.txt   # Date range (e.g., "2022-01-01-2024-06-30")
```

## Key Metadata Fields

| Field | Description |
|-------|-------------|
| `id` | PMID |
| `title` | Paper title |
| `abstract` | Abstract text |
| `publication_date` | First publication date |
| `citedByCount` | Citation count |
| `pubYear` | Publication year |
| `hasDbCrossReferences` | Has database cross-references |
| `hasData` | Has associated data |
| `isOpenAccess` | Open access flag |
| `inPMC` | In PubMed Central |
| `meshTerms` | MeSH terms (JSON) |
| `keywords` | Author keywords (JSON) |
| `journalTitle` | Journal name |

## Common Date Ranges

```bash
# Single year
-f 2023-01-01 -t 2023-12-31

# To current date (omit -t)
-f 2024-01-01

# Specific month
-f 2023-06-01 -t 2023-06-30

# Multi-year range
-f 2011-01-01 -t 2021-12-31
```

## Expected Yields

| Date Range | Papers |
|------------|--------|
| 2022 | ~27,000 |
| 2023 | ~28,000 |
| 2011-2021 | ~190,000 |

## Query Components

**MeSH Terms:** Databases (Genetic, Protein, Factual, Nucleic Acid), Knowledge Bases

**Title Keywords:** database*, repositor*, atlas, portal, consortium, platform, toolkit, etc.

**Abstract Patterns:** http* + resource keywords

**Filters:** Excludes retractions/errata, PubMed/PMC only

## Next Steps

```bash
# After Phase 0, run classification
python run_pipeline.py --input-dir data/my_batch/ --profiles aggressive
```

## Troubleshooting

**No results:** Check date format (YYYY-MM-DD required)

**API error:** Verify internet connection, EPMC may be temporarily down

**Import error:** Ensure pandas and requests are installed: `pip install pandas requests`

## Custom Queries

```bash
# Use custom query string (not file)
python query_epmc.py "TITLE:database AND (SRC:MED) AND (FIRST_PDATE:[{0} TO {1}])" -f 2022-01-01 -t 2022-12-31 -o data/test/
```

Remember: `{0}` = from_date, `{1}` = to_date in query placeholders.
