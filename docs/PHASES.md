# Pipeline Phases

Detailed documentation for each phase of the pipeline.

## Phase Execution Order

**Important:** Phases execute in this order (not numerically):

```
Phase 0 → Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 7 → Phase 6 → Phase 8 → Phase 9
```

Phase 7 (Deduplication) runs before Phase 6 (URL Scanning) to avoid wasting time scanning URLs that will be merged.

---

## Phase 0: Data Acquisition

**Purpose:** Fetch papers from Europe PMC using structured queries to identify candidate bioresource papers.

**GPU Required:** No

**Time:** Varies by date range (typically 1-5 minutes for a year of papers)

### Query Structure

The EPMC query uses a comprehensive search strategy combining:

**MeSH Terms:** Medical Subject Headings for databases and knowledge bases
- `MESH:"Databases, Genetic"`
- `MESH:"Databases, Protein"`
- `MESH:"Databases, Factual"`
- `MESH:"Databases, Nucleic Acid"`
- `MESH:"Knowledge Bases"`

**Title Keywords:** Resource-related terms in paper titles
- Database-related: `database*`, `repositor*`, `catalog*`, `collection*`
- Resource types: `atlas`, `portal`, `consortium`, `knowledgebase`, `resource*`
- Technical: `platform`, `dataset`, `toolkit`, `toolbox`, `browser`

**Abstract Patterns:** URLs and resource mentions in abstracts
- Abstract contains `http*` AND resource keywords
- Identifies papers describing web-accessible resources

**Exclusions:** Filters out non-relevant papers
- Retractions and errata: `NOT (TITLE:(retract* OR withdraw* OR erratum))`
- Source filter: `AND (SRC:(MED OR PMC))` - PubMed and PMC only

### Query Format

The query uses placeholders for date ranges:
```
(MeSH terms OR Title keywords OR Abstract patterns)
NOT (Exclusions)
AND (Source filters)
AND (FIRST_PDATE:[{0} TO {1}])
```

Where `{0}` and `{1}` are replaced with from-date and to-date.

### Running Phase 0

```bash
python scripts/phase0_data_acquisition/query_epmc.py \
    config/epmc_query_v5.1.txt \
    -f 2022-01-01 \
    -t 2024-06-30 \
    -o data/my_batch/
```

**Parameters:**
- First argument: Query file path (or query string)
- `-f / --from-date`: Start date (YYYY, YYYY-MM, or YYYY-MM-DD)
- `-t / --to-date`: End date (defaults to today)
- `-o / --out-dir`: Output directory (defaults to `out/`)

### Outputs

Two files are created in the output directory:

1. **query_results.csv** - Papers with 20 metadata fields:
   - Core: `id`, `title`, `abstract`, `publication_date`
   - Flags: `hasDbCrossReferences`, `hasData`, `hasSuppl`, `isOpenAccess`, `inPMC`, `inEPMC`, `hasPDF`, `hasBook`
   - Metrics: `citedByCount`, `pubYear`
   - Rich metadata: `pubType`, `keywords`, `meshTerms`, `journalTitle`, `journalISSN`, `authorAffiliations`

2. **last_query_dates.txt** - Date range for record-keeping

### Expected Results

Typical query yields:
- 2022 papers: ~27,000 candidates
- 2011-2021 papers: ~190,000 candidates

Most papers will be filtered out in Phase 1 (Classification). This broad search ensures high recall of potential bioresources.

---

## Phase 1: Classification

**Purpose:** Identify papers that describe bioresources (databases, repositories, tools).

**GPU Required:** Yes (for V2), No (for PyCaret)

**Time:** 2-4 hours (V2) + 10-50 minutes (PyCaret)

### Approach: Dual-Model Union

Two complementary classifiers are combined using OR logic:

| Model | Type | Strength | Positive Rate |
|-------|------|----------|---------------|
| **V2 RoBERTa** | Text-based deep learning | High precision | ~8% |
| **PyCaret** | Metadata-based AutoML | High recall | ~25-30% |

**Union Logic:** A paper is positive if EITHER model predicts positive.

### V2 RoBERTa Classifier

- **Base Model:** `allenai/dsp_roberta_base_dapt_biomed_tapt_rct_500`
- **Input:** Concatenated title + abstract
- **Max Length:** 256 tokens
- **Training Data:** ~1,600 manually classified papers
- **Performance:** F1=0.990, Precision=0.999, Recall=0.981

### PyCaret Classifier

- **Type:** AutoML ensemble (blended top 3 models)
- **Features:** 91-112 engineered features including:
  - MeSH terms (target-encoded with Bayesian smoothing)
  - Citation metrics (count, log-transformed, age-ratio)
  - Access flags (isOpenAccess, inPMC, hasData, etc.)
  - Journal encoding
  - Text length features
  - Database keyword presence
- **Training:** 5-10 fold cross-validation with SMOTE for imbalance

### Running Phase 1

**V2 on Colab:**
1. Upload `notebooks/phase1_classification/v2_classification_colab.ipynb`
2. Upload input papers CSV
3. Run all cells
4. Download classification results

**PyCaret Locally:**
```bash
source pycaret_env/bin/activate
python scripts/phase1_classification/02_run_pycaret_local.py \
    --input data/papers.csv \
    --output data/pycaret_results.csv
```

### Outputs

- Classification predictions with confidence scores
- Union of V2 + PyCaret positives

---

## Phase 2: Named Entity Recognition

**Purpose:** Extract bioresource entity mentions (names) from paper text.

**GPU Required:** Yes (for V2), No (for spaCy)

**Time:** 5-10 hours (V2) + minutes (spaCy)

### Approach: Dual-System Union

| System | Type | Strength |
|--------|------|----------|
| **V2 BERT NER** | Statistical token classification | Generalizes to new entities |
| **spaCy Hybrid** | EntityRuler pattern matching | High precision on known patterns |

### Entity Types

- **COM (Compound):** Abbreviations, acronyms, short forms (e.g., "PDB", "GEO", "UniProt")
- **FUL (Full):** Complete descriptive names (e.g., "Protein Data Bank", "Gene Expression Omnibus")

### V2 BERT NER

- **Base Model:** `allenai/dsp_roberta_base_dapt_biomed_tapt_rct_500`
- **Labels:** O, B-COM, I-COM, B-FUL, I-FUL (BIO tagging)
- **Training Data:** 554 manually annotated papers
- **Performance:** F1=0.749, Precision=0.779, Recall=0.722

### spaCy Hybrid NER

The spaCy system uses an EntityRuler with patterns generated through a 4-phase automated process:

1. **Extract Dictionary:** Read labeled bioresource papers to get ~3,700 unique resource names
2. **Enrich Full Names:** Use regex patterns on abstracts to find full names for abbreviations (e.g., "PDB" → "Protein Data Bank")
3. **Generate Patterns:** Create token patterns (for short names) and phrase patterns (for full names)
4. **Relabel COM/FUL:** Convert patterns to match V2 entity types using heuristics (all-caps → COM, multi-word → FUL)

### Union Logic

- Extract unique PMIDs from both NER outputs
- Combine all entities with source attribution
- Deduplicate by (PMID, entity_text, entity_type)

### Running Phase 2

**V2 on Colab:**
1. Upload `notebooks/phase2_ner/v2_ner_colab.ipynb`
2. Upload classification-positive papers
3. Run all cells
4. Download NER results

**spaCy Locally:**
```bash
python scripts/phase2_ner/run_spacy_full_hybrid_local.py \
    --input data/classification_positives.csv \
    --output data/spacy_ner_results.csv
```

**Create Union:**
```bash
python scripts/phase2_ner/06_extract_pmid_union.py \
    --session-dir results/SESSION_ID
```

### Outputs

- `ner_union.csv` - Combined entities with source attribution
- `ner_union_pmids.txt` - Unique PMIDs with entities

---

## Phase 3: Linguistic Scoring

**Purpose:** Score papers based on linguistic patterns to distinguish "introduction" papers (describing new resources) from "usage" papers (using existing resources).

**GPU Required:** No

**Time:** ~30 seconds

### Scoring Formula

| Feature | Points | Description |
|---------|--------|-------------|
| Introduction patterns | +2 | "we present", "we introduce", "here we describe" |
| Title patterns | +1 | Resource name patterns in title |
| URL in abstract | +1 | HTTP/HTTPS URL detected |
| Implementation keywords | +0.5 each (max +2) | "implementation", "architecture", "api", "download", "available at", "freely available", "open source" |
| Usage keywords | -0.5 each (max -2) | "we used", "we employed", "we applied", "results show" |

**Score Range:** Approximately -2 to +6

### Title Score Modifiers

Effective score is adjusted based on title content:
- **+1 boost:** Title contains "database", "archive", "repository", "atlas", "resource", "commons"
- **-1 penalty:** Title contains methodology patterns ("tool for", "method for", "approach for") without data words

### Classification Thresholds

| Category | Score | Action |
|----------|-------|--------|
| **HIGH** | ≥ 2 | Auto-classify as introduction |
| **MEDIUM** | -1 to 2 | Send to SetFit for classification |
| **LOW** | < -1 | Exclude (likely usage paper) |

### Running Phase 3

```bash
python scripts/phase3_linguistic/run_linguistic_scoring.py \
    --session-dir results/SESSION_ID
```

### Outputs

- `high_score_papers.csv` - Auto-classified introductions
- `medium_score_papers.csv` - Needs SetFit classification
- `low_score_papers.csv` - Excluded usage papers

---

## Phase 4: SetFit Classification

**Purpose:** Classify medium-confidence papers using few-shot learning.

**GPU Required:** Recommended (can run on CPU but slower)

**Time:** 10-15 minutes

### SetFit Model

- **Base Model:** `sentence-transformers/all-mpnet-base-v2`
- **Training Data:** 40 examples (20 positive, 20 negative)
- **Positive Sources:**
  - Papers with PubMed types "Database", "Software", "Introductory Journal Article"
  - Papers with linguistic score ≥ 7
- **Negative Sources:** Papers with linguistic score ≤ -2

### Running Phase 4

**On Colab:**
1. Upload `notebooks/phase4_setfit/setfit_inference_colab.ipynb`
2. Upload `medium_score_papers.csv`
3. Run all cells
4. Download SetFit results

**Locally:**
```bash
python scripts/phase4_setfit/08_setfit_inference.py \
    --session-dir results/SESSION_ID
```

### Outputs

- `setfit_introductions.csv` - Papers classified as introductions
- `setfit_usage.csv` - Papers classified as usage

---

## Phase 5: Mapping & Resource Creation

**Purpose:** Create paper sets, assign primary entities, add quality indicators, and extract URLs.

**GPU Required:** No

**Time:** 5-10 minutes

### Scripts

| Script | Purpose |
|--------|---------|
| `09_create_paper_sets.py` | Create Sets A (linguistic), B (SetFit), C (union) |
| `11_create_primary_resources.py` | Score and assign primary entities |
| `12_add_quality_indicators.py` | Add quality flags |
| `13_extract_urls.py` | Extract URLs from abstracts |

### Entity Scoring

Primary entity is selected based on scoring:

| Factor | Points |
|--------|--------|
| Title mention | +10 |
| Abstract mention | +5 |
| Consensus (both NER systems) | +3 |
| V2 NER probability | +0-1 |
| Mention frequency | +count |

### Quality Indicators

- `entity_from_title` - Resource name extracted from title
- `db_keyword_found` - Database keyword in title
- `title_entity_in_ner` - Title entity matches NER primary
- `very_high_conf` - Both db_keyword AND title_entity match

### URL Extraction

URLs extracted from abstracts with context scoring:
- +10 for context phrases ("available at", "accessible at")
- +8 for resource keywords in domain
- +5 for academic/government TLD
- +2 for position in first half of abstract

Excluded domains: github.com, doi.org, twitter.com, etc.

### Running Phase 5

```bash
python run_pipeline.py --session-id SESSION_ID --only-phase 5
```

### Outputs

- `set_a_linguistic.csv` - Papers from linguistic scoring
- `set_b_setfit.csv` - Papers from SetFit
- `set_c_union.csv` - Union with source tracking
- `papers_with_urls.csv` - Papers with extracted URLs

---

## Phase 7: Deduplication

**Purpose:** Merge duplicate resources across papers.

**GPU Required:** No

**Time:** ~5 minutes

**Note:** Runs BEFORE Phase 6 to avoid scanning URLs that will be merged.

### URL Similarity Clustering

- **Threshold:** 0.85 similarity
- **Algorithm:** Union-find with domain blocking optimization
- **Handles:** http/https, www prefix, trailing slashes, path variations

### Domain Blocking

Generic domains are blocked from clustering to prevent false merges:

**Institutional domains:** ac.uk, edu.cn, ac.jp, nih.gov, edu

**Multi-database platforms:** github.io, shinyapps.io, herokuapp.com, netlify.app

### Profile-Based Filtering

Papers are filtered based on the selected profile (aggressive/balanced/conservative). See [CONFIGURATION.md](CONFIGURATION.md) for profile parameters.

### Merge Logic

When duplicates are found:
- Keep earliest paper (by publication date)
- Join PMIDs
- Combine metadata

### Running Phase 7

```bash
python scripts/phase7_deduplication/17_deduplicate_all_sets.py \
    --session-dir results/SESSION_ID \
    --profiles aggressive
```

### Outputs (per profile)

- `set_a_linguistic.csv` - Deduplicated Set A
- `set_b_setfit.csv` - Deduplicated Set B
- `set_c_final.csv` - Final deduplicated set
- `deduplication_stats.txt` - Statistics

---

## Phase 6: URL Scanning

**Purpose:** Validate URLs and score for bioresource indicators.

**GPU Required:** No

**Time:** 75-90 minutes

**Note:** Runs AFTER Phase 7 to avoid scanning merged-away URLs.

### Scanner Features

- Multi-threaded scanning (10 workers default)
- Domain rate limiting (1 req/sec per domain)
- Meta refresh redirect following
- Wayback Machine fallback for dead URLs

### Indicator Scoring

| Score | Indicators |
|-------|------------|
| 5 | NCBI, EBI, NIH, Ensembl, UniProt |
| 4 | "search database", "query database", "download data" |
| 3 | genomics, proteomics, bioinformatics, genome, gene, protein |
| 2 | repository, archive, collection, resource, tool, platform |
| 1 | database, server, portal, web service |
| +5 | Title keywords bonus |

### Likelihood Classifications

| Level | Score |
|-------|-------|
| CRITICAL | ≥ 15 |
| HIGH | ≥ 10 |
| MEDIUM | ≥ 5 |
| LOW | ≥ 1 |
| VERY LOW | < 1 |

### Running Phase 6

```bash
python run_pipeline.py --session-id SESSION_ID --only-phase 6
```

### Outputs

- `scanned_urls.csv` - URL validation results with scores

---

## Phase 8: URL Recovery

**Purpose:** Recover missing URLs from abstracts, fulltext, and web search.

**GPU Required:** No

**Time:** ~5 minutes (automated) + agent time (web search)

### Multi-Stage Process

| Stage | Source | Recovery Rate |
|-------|--------|---------------|
| 1 | Abstract search | ~17% |
| 2 | Fulltext search | ~24% additional |
| 3 | Web search (manual) | Variable |

**Combined automated recovery:** ~37%

### URL Exclusions

The following are excluded from recovery:
- Code repositories: github.com, gitlab.com, bitbucket.org
- Data archives: zenodo.org, figshare.com, dryad
- DOIs: doi.org
- Package managers: cran.r-project.org, pypi.org, bioconductor.org
- FTP servers

### Scripts

| Script | Purpose |
|--------|---------|
| `28_identify_missing_urls.py` | Find records missing URLs |
| `29_fetch_abstracts.py` | Fetch from EPMC API |
| `30_search_abstracts_urls.py` | Search abstracts for URLs |
| `31_fetch_fulltext.py` | Fetch fulltext XML |
| `32_search_fulltext_urls.py` | Search fulltext for URLs |
| `33_consolidate_recovery.py` | Prepare web search chunks |
| `34_merge_websearch_results.py` | Filter and merge results |

### Checkpointing

- Abstract fetch: Saves every 100 PMIDs
- Fulltext fetch: Saves every 50 PMCIDs

### Manual Web Search Breakpoint

Phase 8 includes a breakpoint for manual web search:
1. Automated stages complete
2. Pipeline pauses with `websearch_chunks/` prepared
3. Perform manual web searches
4. Resume pipeline with `--start-phase 8`

Use `--skip-web-search` to bypass the breakpoint.

### Running Phase 8

```bash
# With breakpoint
python run_pipeline.py --session-id SESSION_ID --start-phase 8

# Skip breakpoint
python run_pipeline.py --session-id SESSION_ID --start-phase 8 --skip-web-search
```

### Outputs

- `recovered_urls.csv` - URLs found in abstract/fulltext
- `still_missing.csv` - Records for web search
- `websearch_chunks/` - Prepared chunks for agents
- `final_url_recovery.csv` - All recovered URLs

---

## Phase 9: Finalization

**Purpose:** Transform resources into final inventory format with metadata enrichment.

**GPU Required:** No

**Time:** ~20 minutes

### Scripts

| Script | Purpose |
|--------|---------|
| `23_transform_columns.py` | Column mapping + name sanitization |
| `24_check_urls_with_geo.py` | URL validation + geolocation |
| `25_fetch_epmc_metadata.py` | Fetch authors, grants, citations |
| `26_process_countries.py` | ISO country codes |
| `27_generate_final_inventory.py` | Generate final CSV |

### Name Sanitization

8-step process with modification flags:

1. **Pipe cleaning:** Take first name from pipe-separated values
2. **HTML removal:** Strip HTML tags
3. **Character sanitization:** Unicode to ASCII (μ → "mu", etc.)
4. **Empty name recovery:** Extract from URL domain
5. **Single-letter replacement:** Replace with URL-extracted name
6. **Length validation:** Flag names < 3 chars for review
7. **Auto-capitalization:** Capitalize ≤6 char lowercase acronyms
8. **Duplicate disambiguation:** Add subdomain qualifiers

### URL Validation

- Live check with HTTP HEAD request
- Geolocation via ipinfo.io / ip-api.com
- Wayback Machine fallback for dead URLs

**Blocked patterns:** oxfordjournals.org, bitbucket.org, .pdf, .zip

### EPMC Metadata

Fetched fields:
- `publication_date` (earliest)
- `authors` (joined unique)
- `affiliation`
- `grant_ids`, `grant_agencies`
- `num_citations` (summed)

### Final Inventory Format

24 columns including:
- `ID`, `best_name`, `best_common`, `best_full`
- `extracted_url`, `extracted_url_status`, `wayback_url`
- `publication_date`, `authors`, `affiliation`
- `grant_ids`, `grant_agencies`, `num_citations`
- `affiliation_countries`, `extracted_url_country`
- `name_modification_flags` (for QC)

### Running Phase 9

```bash
python scripts/phase9_finalization/run_phase9.py \
    --session-dir results/SESSION_ID \
    --profile aggressive
```

Optional flags:
- `--skip-geo` - Skip geolocation
- `--skip-wayback` - Skip Wayback lookups
- `--workers N` - Concurrent workers

### Outputs

- `final_inventory.csv` - Main output
- `statistics.json` - Coverage and metrics
- `excluded_no_url.csv` - Resources without valid URLs
