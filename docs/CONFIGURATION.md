# Configuration Reference

## Configuration File

The main configuration file is `config/pipeline_config.yaml`.

## Command-Line Arguments

### run_pipeline.py

```bash
python run_pipeline.py [OPTIONS]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--session-id` | - | Resume existing session ID |
| `--input-dir` | - | Input data directory (for new session) |
| `--results-dir` | `results/` | Base results directory |
| `--start-phase` | 2 | Start from phase N |
| `--end-phase` | 9 | End at phase N |
| `--only-phase` | - | Run only this phase |
| `--profiles` | `all` | Dedup profiles: all, conservative, balanced, aggressive |
| `--dry-run` | False | Preview without executing |
| `--continue-on-error` | False | Continue on failures |
| `--skip-web-search` | False | Skip Phase 8 manual breakpoint |
| `--config` | `config/pipeline_config.yaml` | Config file path |
| `--log-level` | INFO | DEBUG, INFO, WARNING, ERROR |
| `--verbose` | False | Show detailed output |

### Examples

```bash
# New session with aggressive profile
python run_pipeline.py --input-dir data/my_batch/ --profiles aggressive

# Resume from phase 5
python run_pipeline.py --session-id 2025-12-04-143052-abc12 --start-phase 5

# Run only deduplication with balanced profile
python run_pipeline.py --session-id SESSION --only-phase 7 --profiles balanced

# Dry run to preview
python run_pipeline.py --input-dir data/my_batch/ --dry-run

# Skip web search breakpoint
python run_pipeline.py --session-id SESSION --start-phase 8 --skip-web-search
```

## Deduplication Profiles

### Profile Definitions

Located in `config/pipeline_config.yaml`:

```yaml
filtering_profiles:
  conservative:
    description: "High precision, minimize false positives"
    db_keywords:
      - database
      - server
      - portal
      - repository
      - archive
      - atlas
      - map
    linguistic_bypass_threshold: 6
    setfit_threshold: 0.60
    linguistic_high_threshold: 3
    linguistic_low_threshold: 0
    require_url: true

  balanced:
    description: "Recommended - good precision/recall balance"
    db_keywords:
      - database
      - server
      - portal
      - repository
      - archive
      - atlas
      - map
      - bank
      - wiki
      - hub
      - resource
      - browser
      - db
      - base
    linguistic_bypass_threshold: 5
    setfit_threshold: 0.58
    linguistic_high_threshold: 3
    linguistic_low_threshold: 0
    require_url: true

  aggressive:
    description: "Maximum recall, accepts more false positives"
    db_keywords:
      - database
      - server
      - portal
      - repository
      - archive
      - atlas
      - map
      - bank
      - wiki
      - hub
      - resource
      - browser
      - db
      - base
      - tool
      - network
      - collection
      - catalog
      - platform
      - pedia
      - mine
      - cyc
    linguistic_bypass_threshold: 4
    setfit_threshold: 0.55
    linguistic_high_threshold: 2
    linguistic_low_threshold: -1
    require_url: false
```

### Profile Selection

| Profile | Use Case |
|---------|----------|
| **aggressive** | Recommended for comprehensive discovery. Captures more resources but requires more manual review to filter false positives. |
| **balanced** | Good trade-off between recall and precision. Less manual filtering needed. |
| **conservative** | When false positives are costly. High confidence in results. |

### Filter Logic

Papers pass the profile filter through one of two paths:

1. **Linguistic Bypass:** `effective_ling_score >= linguistic_bypass_threshold`
2. **Keyword Match:** Title contains db_keyword AND passes other criteria

## Classification Configuration

```yaml
classification:
  v2:
    model_path: "models/v2_classifier"
    threshold: 0.5
    max_length: 512
    batch_size: 16

  pycaret:
    model_path: "models/pycaret"
    test_mode: false  # true uses 92 features, false uses 112 features
```

## NER Configuration

```yaml
ner:
  v2:
    model_path: "models/v2_ner"
    max_length: 512
    batch_size: 16
    labels:
      - "COM"  # Compound/abbreviation
      - "FUL"  # Full name

  spacy:
    model_path: "models/spacy_hybrid_ner"
    patterns_file: "data/patterns_com_ful.jsonl"
```

## Linguistic Scoring Configuration

```yaml
linguistic:
  high_threshold: 2   # >= threshold = high confidence
  low_threshold: -1   # < threshold = low confidence (exclude)

  # Score weights
  introduction_pattern: 2
  title_pattern: 1
  url_present: 1
  implementation_keyword: 0.5  # max 2
  usage_keyword: -0.5          # max -2
```

## URL Scanning Configuration

```yaml
url_scanning:
  workers: 10              # Concurrent threads
  timeout: 20              # Request timeout (seconds)
  domain_delay: 1.0        # Delay between requests to same domain
  wayback_timeout: 15      # Wayback API timeout
  max_content_size: 512000 # Max bytes to download per page
```

## URL Recovery Configuration

```yaml
url_recovery:
  abstract_rate_limit: 0.1   # Seconds between EPMC API calls
  fulltext_rate_limit: 0.15  # Slightly slower for fulltext
  checkpoint_interval: 100   # Save progress every N records
  chunk_size: 70             # Records per web search chunk
  num_chunks: 6              # Number of web search chunks
```

## Finalization Configuration

```yaml
finalization:
  url_check:
    workers: 10
    timeout: 10
    skip_geo: false       # Skip geolocation lookups
    skip_wayback: false   # Skip Wayback Machine lookups

  epmc:
    chunk_size: 20        # PMIDs per API request

  # Blocked URL patterns (excluded from final inventory)
  blocked_patterns:
    - "oxfordjournals.org"
    - "academic.oup.com/nar"
    - "bitbucket.org"
    - "gitlab.com"
    - "sourceforge.net"
    - ".pdf$"
    - ".xlsx?$"
```

## Checkpoint Configuration

```yaml
checkpoint:
  enabled: true
  checkpoint_file: ".pipeline_checkpoint.json"
  auto_save: true
  save_after_each_phase: true
```

## Environment Variables

Some scripts support environment variables:

| Variable | Description |
|----------|-------------|
| `PYTHONPATH` | Include `src` directory |
| `CUDA_VISIBLE_DEVICES` | GPU selection |
| `HF_HOME` | HuggingFace cache directory |

Example:
```bash
export PYTHONPATH="src:$PYTHONPATH"
export CUDA_VISIBLE_DEVICES=0
```

## Script-Specific Arguments

### Phase 7: Deduplication

```bash
python scripts/phase7_deduplication/17_deduplicate_all_sets.py \
    --session-dir results/SESSION_ID \
    --profiles aggressive,balanced \
    --config config/pipeline_config.yaml
```

### Phase 9: Finalization

```bash
python scripts/phase9_finalization/run_phase9.py \
    --session-dir results/SESSION_ID \
    --profile aggressive \
    --skip-geo \
    --skip-wayback
```

| Argument | Description |
|----------|-------------|
| `--skip-geo` | Skip geolocation lookups |
| `--skip-wayback` | Skip Wayback Machine lookups |
| `--workers N` | Number of concurrent workers |

## Modifying Configuration

1. Copy the default config:
   ```bash
   cp config/pipeline_config.yaml config/my_config.yaml
   ```

2. Edit your copy with desired settings

3. Use with pipeline:
   ```bash
   python run_pipeline.py --config config/my_config.yaml --input-dir data/
   ```
