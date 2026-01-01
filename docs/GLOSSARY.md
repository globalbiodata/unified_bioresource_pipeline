# Glossary

Terminology used throughout the pipeline documentation.

---

## Core Concepts

### Bioresource
A biological database, repository, tool, or platform that provides access to biological data or computational services. Examples: UniProt, PDB, NCBI GenBank, BLAST.

### Introduction Paper
A scientific paper that introduces or describes a new bioresource. These papers typically present the resource's design, implementation, and availability.

### Usage Paper
A scientific paper that uses an existing bioresource as part of research. These papers cite or mention resources but don't describe them as novel contributions.

### Europe PMC (EPMC)
Europe PubMed Central - a free, open-access database of life sciences literature. The primary source of papers for this pipeline.

---

## Entity Types

### COM (Compound)
An abbreviation, acronym, or short-form name of a bioresource.
- Examples: "PDB", "GEO", "UniProt", "BLAST", "KEGG"
- Typically 2-10 characters, often all capitals

### FUL (Full)
The complete, descriptive name of a bioresource.
- Examples: "Protein Data Bank", "Gene Expression Omnibus", "Kyoto Encyclopedia of Genes and Genomes"
- Multiple words describing the resource

### Primary Entity
The main entity name assigned to a paper, selected based on scoring (title mention, abstract mention, NER consensus).

---

## Pipeline Components

### V2 RoBERTa
The production-ready deep learning model based on RoBERTa (Robustly Optimized BERT Approach), specifically the biomedical-adapted version from Allen AI.

### PyCaret
An open-source, low-code machine learning library for Python used for metadata-based classification in this pipeline.

### spaCy
An open-source NLP library. Used for pattern-based entity extraction via the EntityRuler component.

### EntityRuler
A spaCy component that matches text patterns to extract entities. Uses rule-based patterns rather than statistical learning.

### SetFit
"Sentence Transformer Fine-tuning" - a framework for few-shot text classification that fine-tunes sentence transformers on small labeled datasets.

### Wayback Machine
Internet Archive's web archiving service. Used as a fallback to find archived versions of URLs that are no longer live.

---

## Classification Terms

### Classification Union
The combination of V2 and PyCaret classification results using OR logic. A paper is positive if EITHER model predicts positive.

### NER Union
The combination of V2 BERT NER and spaCy EntityRuler results. All entities from both systems are included with source attribution.

### Linguistic Score
A numeric score assigned to papers based on linguistic patterns. Higher scores indicate higher likelihood of being an introduction paper.

### SetFit Confidence
The probability score from SetFit classification. Higher values indicate stronger confidence in the introduction classification.

---

## Deduplication

### Profile
A set of filtering parameters that control how aggressively the pipeline filters papers. Options: conservative, balanced, aggressive.

### db_keywords
Database-related keywords looked for in paper titles (e.g., "database", "server", "portal", "repository").

### Linguistic Bypass
When a paper's linguistic score is high enough to bypass keyword requirements and be included directly.

### URL Clustering
Grouping resources with similar URLs to identify duplicates. Uses 0.85 similarity threshold.

### Domain Blocking
Preventing certain generic domains (e.g., ac.uk, github.io) from being clustered together to avoid false merges.

---

## Session Management

### Session
A single pipeline run identified by a unique timestamp-based ID. All inputs and outputs are stored in a session directory.

### Session ID
Unique identifier format: `YYYY-MM-DD-HHMMSS-xxxxx`
Example: `2025-12-04-143052-a3f9b`

### Checkpoint
A saved state that allows resuming a failed or interrupted pipeline run.

---

## Paper Sets

### Set A (Linguistic)
Papers classified as introductions based on high linguistic scores (≥ threshold).

### Set B (SetFit)
Papers classified as introductions by the SetFit model from the medium-score group.

### Set C (Union)
The union of Set A and Set B, representing all papers classified as introductions.

### Source Attribution
Tracking whether a paper came from Set A only, Set B only, or both.

---

## URL Processing

### URL Recovery
The process of finding URLs for resources that are missing them, by searching abstracts, fulltext, or the web.

### URL Scanning
Validating URLs by checking if they are live and scoring them for bioresource indicators.

### Wayback URL
An archived URL from the Internet Archive, used when the original URL is no longer accessible.

### Indicator Score
A numeric score based on bioresource-related terms found on a webpage (e.g., "database", "genomics", "NCBI").

### Likelihood Classification
Categorization of URLs based on indicator scores: CRITICAL (≥15), HIGH (≥10), MEDIUM (≥5), LOW (≥1), VERY LOW (<1).

---

## Metadata

### MeSH Terms
Medical Subject Headings - a controlled vocabulary used for indexing PubMed articles. Used as features in PyCaret classification.

### PMID
PubMed Identifier - a unique numeric identifier for each PubMed article.

### PMCID
PubMed Central Identifier - identifier for full-text articles in PMC.

### Affiliation
The institutional affiliation of paper authors.

### Grant IDs
Funding grant identifiers associated with the research.

---

## Data Quality

### Name Sanitization
Cleaning and standardizing resource names (removing HTML, fixing encoding, capitalizing acronyms).

### Modification Flags
Tags indicating what changes were made to a resource name (e.g., PIPE_CLEANED, HTML_REMOVED, CAPITALIZED).

### Disambiguation
Adding qualifiers to duplicate resource names to make them unique (e.g., adding subdomain information).

---

## Technical Terms

### BIO Tagging
A labeling scheme for token classification: B (Beginning), I (Inside), O (Outside). Used for NER.

### Token Classification
Assigning a label to each token (word/subword) in a text. Used for NER.

### Sequence Classification
Assigning a single label to an entire text sequence. Used for paper classification.

### Fine-tuning
Adapting a pre-trained model to a specific task by training on task-specific data.

### Few-shot Learning
Training a model with very few examples (e.g., 20-40). SetFit uses this approach.

### Target Encoding
Encoding categorical features (like journal names) using the target variable's mean, with smoothing.

### Bayesian Smoothing
A technique to reduce overfitting when target-encoding categorical variables with few samples.

---

## File Formats

### CSV
Comma-Separated Values - the primary data format used throughout the pipeline.

### JSONL
JSON Lines - a format with one JSON object per line. Used for spaCy patterns.

### Pickle (.pkl)
Python's binary serialization format. Used for some model files.

---

## Abbreviations

| Abbreviation | Meaning |
|--------------|---------|
| EPMC | Europe PubMed Central |
| NER | Named Entity Recognition |
| LFS | Large File Storage (Git LFS) |
| GPU | Graphics Processing Unit |
| VRAM | Video Random Access Memory |
| OOM | Out of Memory |
| API | Application Programming Interface |
| TLD | Top-Level Domain |
| CV | Cross-Validation |
| SMOTE | Synthetic Minority Over-sampling Technique |
| AutoML | Automated Machine Learning |
