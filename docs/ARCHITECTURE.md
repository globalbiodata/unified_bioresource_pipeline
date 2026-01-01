# Pipeline Architecture

## System Overview

The Unified Bioresource Pipeline is a multi-phase ML system that processes scientific papers to discover and catalog bioresources (databases, repositories, tools).

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 0: DATA ACQUISITION                                              │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  Query Europe PMC using structured search:                     │    │
│  │  • MeSH terms (Databases, Knowledge Bases)                     │    │
│  │  • Title keywords (database*, repositor*, atlas, portal, etc.) │    │
│  │  • Abstract patterns (http* + resource keywords)               │    │
│  │  Returns ~27k papers/year with 20 metadata fields              │    │
│  └────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 1: CLASSIFICATION                                                │
│  ┌──────────────────┐    ┌──────────────────┐                          │
│  │  V2 RoBERTa      │    │  PyCaret         │                          │
│  │  (Text-based)    │ OR │  (Metadata)      │  = Classification Union  │
│  │  High Precision  │    │  High Recall     │                          │
│  └──────────────────┘    └──────────────────┘                          │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 2: NAMED ENTITY RECOGNITION                                      │
│  ┌──────────────────┐    ┌──────────────────┐                          │
│  │  V2 BERT NER     │    │  spaCy Hybrid    │                          │
│  │  (Statistical)   │ OR │  (EntityRuler)   │  = NER Union             │
│  │  COM/FUL tags    │    │  Pattern-based   │                          │
│  └──────────────────┘    └──────────────────┘                          │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 3: LINGUISTIC SCORING                                            │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  Score papers based on linguistic patterns:                     │    │
│  │  +2 introduction phrases, +1 title patterns, +1 URL present    │    │
│  │  -0.5 usage keywords, produces HIGH/MEDIUM/LOW classification  │    │
│  └────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                   │
              ▼                   ▼                   ▼
        ┌─────────┐         ┌─────────┐         ┌─────────┐
        │  HIGH   │         │ MEDIUM  │         │  LOW    │
        │ (>=2)   │         │ (-1,2)  │         │ (<-1)   │
        │  Auto   │         │ SetFit  │         │ Exclude │
        └────┬────┘         └────┬────┘         └─────────┘
             │                   │
             │                   ▼
             │    ┌─────────────────────────────────────┐
             │    │  PHASE 4: SETFIT CLASSIFICATION    │
             │    │  Few-shot learning on MEDIUM       │
             │    │  papers → Introduction or Usage    │
             │    └──────────────────┬──────────────────┘
             │                       │
             └───────────┬───────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 5: MAPPING & RESOURCE CREATION                                   │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  • Create paper sets (A: linguistic, B: SetFit, C: union)      │    │
│  │  • Score entities (+10 title, +5 abstract, +3 consensus)       │    │
│  │  • Add quality indicators (db_keyword, title_entity_match)     │    │
│  │  • Extract URLs from abstracts with context scoring            │    │
│  └────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 7: DEDUPLICATION  (runs before Phase 6)                          │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  • URL similarity clustering (0.85 threshold)                  │    │
│  │  • Domain blocking (prevent false merges on ac.uk, github.io)  │    │
│  │  • Profile-based filtering (aggressive/balanced/conservative)  │    │
│  │  • Merge duplicates: keep earliest, combine metadata           │    │
│  └────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 6: URL SCANNING  (runs after dedup to avoid wasted scans)        │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  • Multi-threaded URL validation                               │    │
│  │  • Bioresource indicator scoring (NCBI=5, database=1, etc.)    │    │
│  │  • Wayback Machine fallback for dead URLs                      │    │
│  │  • Domain rate limiting (1 req/sec per domain)                 │    │
│  └────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 8: URL RECOVERY                                                  │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  Multi-stage recovery for missing URLs:                        │    │
│  │  1. Search abstracts (~17% recovery)                           │    │
│  │  2. Search fulltext (~24% additional)                          │    │
│  │  3. Optional: Web search via agents                            │    │
│  │  Excludes: GitHub, Zenodo, DOI, FTP, package managers          │    │
│  └────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  PHASE 9: FINALIZATION                                                  │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  • Column transformation and name sanitization                 │    │
│  │  • URL validation with geolocation                             │    │
│  │  • EPMC metadata enrichment (authors, grants, citations)       │    │
│  │  • Country code standardization                                │    │
│  │  • Generate final 24-column inventory                          │    │
│  └────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │   FINAL INVENTORY CSV   │
                    │   (Validated Resources) │
                    └─────────────────────────┘
```

## Phase Execution Order

**Important:** Phases execute in this order:

```
Phase 0 → Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 7 → Phase 6 → Phase 8 → Phase 9
```

Phase 7 (Deduplication) runs **before** Phase 6 (URL Scanning) because:
- Deduplication merges duplicate resources
- Scanning merged-away URLs would waste time and API calls
- This ordering is handled automatically by `run_pipeline.py`

## Data Flow

### Classification Union Strategy

```
V2 RoBERTa (High Precision)     PyCaret (High Recall)
         │                              │
         │    ┌──────────────────┐      │
         └────│      UNION       │──────┘
              │   (OR Logic)     │
              └────────┬─────────┘
                       │
              Papers positive if
              EITHER model says YES
```

**Rationale:** V2 has excellent precision but lower recall on real-world resources. PyCaret rescues many true positives using metadata features.

### NER Union Strategy

```
V2 BERT NER (Statistical)       spaCy Hybrid (Pattern-based)
         │                              │
         │    ┌──────────────────┐      │
         └────│      UNION       │──────┘
              │   (Combine)      │
              └────────┬─────────┘
                       │
              All entities from both
              with source attribution
```

**Rationale:** V2 generalizes to new entities while spaCy has high precision on known patterns.

## Session Management

Each pipeline run creates an isolated session:

```
Session ID Format: YYYY-MM-DD-HHMMSS-xxxxx
Example: 2025-12-04-143052-a3f9b
```

### Session Directory Structure

```
results/{session_id}/
├── input/                    # User-provided inputs
├── 02_ner/
│   ├── ner_union.csv
│   └── ner_union_pmids.txt
├── 03_linguistic/
│   ├── high_score_papers.csv
│   ├── medium_score_papers.csv
│   └── low_score_papers.csv
├── 04_setfit/
│   └── setfit_introductions.csv
├── 05_mapping/
│   ├── set_a_linguistic.csv
│   ├── set_b_setfit.csv
│   ├── set_c_union.csv
│   └── papers_with_urls.csv
├── 06_scanning/
│   └── scanned_urls.csv
├── 07_deduplication/
│   ├── conservative/
│   │   └── set_c_final.csv
│   ├── balanced/
│   │   └── set_c_final.csv
│   └── aggressive/
│       └── set_c_final.csv
├── 08_url_recovery/
│   ├── recovered_urls.csv
│   └── websearch_chunks/
└── 09_finalization/
    ├── final_inventory.csv
    └── statistics.json
```

## Deduplication Profiles

The pipeline supports three filtering profiles:

### Profile Parameters

| Parameter | Conservative | Balanced | Aggressive |
|-----------|-------------|----------|------------|
| **db_keywords** | 7 | 15 | 20 |
| **linguistic_bypass_threshold** | 6 | 5 | 4 |
| **setfit_threshold** | 0.60 | 0.58 | 0.55 |
| **require_url** | Yes | Yes | No |

### Filter Logic

Papers pass the profile filter through one of two paths:

1. **Linguistic Bypass:** `effective_ling_score >= threshold`
   - High-scoring papers bypass keyword requirements

2. **Keyword Match:** Title contains db_keyword AND passes other criteria
   - Keywords: database, server, portal, repository, archive, etc.

### Profile Selection Guide

- **Aggressive (Recommended):** Maximum recall for comprehensive discovery. Requires more manual review to filter false positives.
- **Balanced:** Good precision/recall trade-off. Less manual filtering needed.
- **Conservative:** High precision when false positives are costly.

## Key Design Decisions

### 1. Union Over Intersection
Both classification and NER use union (OR) logic rather than intersection (AND) to maximize recall. False positives are filtered in later phases.

### 2. Phase Reordering
Phase 7 before Phase 6 optimizes resource usage by avoiding URL scans on records that will be merged.

### 3. Multi-Stage URL Recovery
Three-stage approach (abstract → fulltext → web search) balances automation with manual effort for difficult cases.

### 4. Domain Blocking in Deduplication
Generic institutional domains (ac.uk, edu, nih.gov) and multi-database platforms (github.io, shinyapps.io) are blocked from URL clustering to prevent false merges.

### 5. Title Score Modifiers
Linguistic scores are adjusted based on title patterns:
- **+1 boost:** "database", "archive", "repository", "atlas", "resource", "commons"
- **-1 penalty:** "tool for", "method for", "approach for" (without data words)

This reduces false positives from methodology papers.
