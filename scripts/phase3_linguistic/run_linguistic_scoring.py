#!/usr/bin/env python3
"""
Phase 3: Linguistic Scoring

Applies linguistic pattern detection to NER union papers to score them
for likelihood of being resource introduction papers.

Scoring thresholds (aggressive profile):
    High (>=2): Auto-classify as introduction
    Medium (-1 to 1): Requires SetFit classification
    Low (<-1): Auto-classify as usage/mention

Usage:
    # Session-based (PREFERRED):
    python run_linguistic_scoring.py --session-dir results/2025-12-04-143052-abc12

    # Legacy mode:
    python run_linguistic_scoring.py --auto

    # Custom paths (legacy):
    python run_linguistic_scoring.py \
        --pmid-file data/phase2_ner/ner_union_pmids.txt \
        --classification-file data/phase1_classification/classification_union.csv \
        --output-dir data/phase3_linguistic

Session Mode:
    When --session-dir is provided:
    - Reads from: {session_dir}/02_ner/ner_union_pmids.txt
    - Reads from: {session_dir}/input/classification_union.csv
    - Outputs to: {session_dir}/03_linguistic/

Author: Warren Emmett <warren.emmett@gmail.com>
Date: 2025-12-03
Updated: 2025-12-04 (added session-dir support)
"""

import argparse
import re
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Requires: lib/session_utils.py (run from unified_bioresource_pipeline directory)
# Add lib to path for session utilities
SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

# Import from lib - will fail loudly if lib not found
from lib.session_utils import get_session_path, validate_session_dir

# Legacy paths
DATA_DIR = SCRIPT_DIR.parent.parent / "data"
PHASE2_DIR = DATA_DIR / "phase2_ner"
PHASE1_DIR = DATA_DIR / "phase1_classification"
OUTPUT_DIR = DATA_DIR / "phase3_linguistic"

# Scoring thresholds (aggressive profile for maximum recall)
HIGH_THRESHOLD = 2   # >=2 = high confidence introduction
LOW_THRESHOLD = -1   # <-1 = likely usage paper

# Linguistic patterns
INTRODUCTION_PATTERNS = [
    r'\bwe (present|introduce|describe|developed|implemented|created|built|designed)\b.*\b(database|tool|resource|server|repository|platform|web)\b',
    r'\bhere we (present|describe|introduce|report)\b',
    r'\bthis paper (presents|introduces|describes)\b',
    r'\b(novel|new|updated|comprehensive) (database|tool|resource|server|repository|web)\b',
    r'^[A-Z][a-zA-Z0-9\-]+:\s*(a|an|the)\s+(database|tool|web server|resource|repository)',
    r'\bis (available|accessible) (at|from|via)\b',
    r'\bcan be (accessed|downloaded|found) (at|from)\b',
    r'\bfreely available\b',
]

TITLE_PATTERNS = [
    r'\b(database|server|atlas|repository|resource|portal|archive|browser|tool)\b',
    r':\s*(a|an|the)\s+\w+\s+(database|tool|resource|server|platform)',
    r'\bDB\b|\bdb$',
]

URL_PATTERN = r'https?://[^\s<>"\']+|www\.[^\s<>"\']+'

IMPLEMENTATION_KEYWORDS = [
    'implementation', 'architecture', 'database design', 'system design',
    'data model', 'web interface', 'api', 'download', 'accessible at',
    'available at', 'can be accessed', 'built using', 'developed using',
    'user interface', 'query interface', 'search interface'
]

USAGE_KEYWORDS = [
    'we used', 'we employed', 'we applied', 'analysis revealed',
    'results show', 'we found', 'we observed', 'statistically significant',
    'were analyzed', 'was performed', 'were compared', 'were evaluated'
]


def compute_linguistic_score(title, abstract):
    """Compute linguistic score for a paper"""
    title = str(title) if title else ""
    abstract = str(abstract) if abstract else ""
    text = f"{title} {abstract}".lower()

    score = 0
    evidence = []

    # Check introduction patterns (+2 each)
    has_intro = False
    for pattern in INTRODUCTION_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            has_intro = True
            score += 2
            evidence.append(f"intro_pattern")
            break

    # Check title patterns (+1)
    has_title_pattern = False
    title_lower = title.lower()
    for pattern in TITLE_PATTERNS:
        if re.search(pattern, title_lower, re.IGNORECASE):
            has_title_pattern = True
            score += 1
            evidence.append("title_pattern")
            break

    # Check for URL (+1)
    has_url = bool(re.search(URL_PATTERN, abstract, re.IGNORECASE))
    if has_url:
        score += 1
        evidence.append("has_url")

    # Count implementation keywords (+0.5 each, max +2)
    impl_count = sum(1 for kw in IMPLEMENTATION_KEYWORDS if kw in text)
    impl_score = min(impl_count * 0.5, 2)
    if impl_count > 0:
        score += impl_score
        evidence.append(f"impl_kw={impl_count}")

    # Count usage keywords (-0.5 each, max -2)
    usage_count = sum(1 for kw in USAGE_KEYWORDS if kw in text)
    usage_score = min(usage_count * 0.5, 2)
    if usage_count > 0:
        score -= usage_score
        evidence.append(f"usage_kw={usage_count}")

    return {
        'score': round(score, 1),
        'evidence': "; ".join(evidence) if evidence else "none",
        'has_intro_pattern': has_intro,
        'has_title_pattern': has_title_pattern,
        'has_url': has_url,
        'impl_keywords': impl_count,
        'usage_keywords': usage_count
    }


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Phase 3: Linguistic Scoring for NER union papers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Session-based mode (PREFERRED):
  python run_linguistic_scoring.py --session-dir results/2025-12-04-143052-abc12

  # Legacy mode (auto-detect):
  python run_linguistic_scoring.py --auto

  # Custom paths (legacy):
  python run_linguistic_scoring.py \\
      --pmid-file data/phase2_ner/ner_union_pmids.txt \\
      --classification-file data/phase1_classification/classification_union.csv \\
      --output-dir data/phase3_linguistic
        """
    )

    # Session mode arguments
    parser.add_argument("--session-dir", type=Path,
                        help="Session directory path (e.g., results/2025-12-04-143052-abc12)")

    # Legacy mode arguments
    parser.add_argument("--pmid-file", type=Path, help="Path to NER union PMIDs file (.txt)")
    parser.add_argument("--classification-file", type=Path, help="Path to classification union CSV")
    parser.add_argument("--output-dir", type=Path, help="Output directory for scored papers")
    parser.add_argument("--auto", action="store_true", help="Auto-detect files in legacy paths")

    # Scoring options
    parser.add_argument("--high-threshold", type=float, default=HIGH_THRESHOLD,
                        help=f"High score threshold (default: {HIGH_THRESHOLD})")
    parser.add_argument("--low-threshold", type=float, default=LOW_THRESHOLD,
                        help=f"Low score threshold (default: {LOW_THRESHOLD})")

    return parser.parse_args()


def main():
    args = parse_args()
    start_time = datetime.now()

    print("=" * 80)
    print("PHASE 3: LINGUISTIC SCORING")
    print("=" * 80)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Determine input/output paths
    if args.session_dir:
        # SESSION MODE (PREFERRED)
        print(f"\nMODE: Session-based")
        print(f"Session: {args.session_dir}\n")

        # Validate session directory
        session_path = Path(args.session_dir).resolve()
        if not session_path.exists():
            print(f"ERROR: Session directory not found: {session_path}")
            sys.exit(1)

        # Validate session structure
        try:
            validate_session_dir(session_path, required_phases=['input', '02_ner'])
        except ValueError as e:
            print(f"ERROR: Invalid session directory: {e}")
            sys.exit(1)

        pmid_file = get_session_path(args.session_dir, '02_ner', 'ner_union_pmids.txt')
        classif_file = get_session_path(args.session_dir, 'input', 'classification_union.csv')
        output_dir = get_session_path(args.session_dir, '03_linguistic')

    elif args.pmid_file and args.classification_file:
        # LEGACY: Explicit paths
        print(f"\nMODE: Legacy (explicit paths)")
        pmid_file = args.pmid_file
        classif_file = args.classification_file
        output_dir = args.output_dir if args.output_dir else OUTPUT_DIR

    elif args.auto or not (args.pmid_file or args.classification_file):
        # LEGACY: Auto-detect
        print(f"\nMODE: Legacy (auto-detect)")
        pmid_file = PHASE2_DIR / "ner_union_pmids.txt"
        classif_file = PHASE1_DIR / "classification_union.csv"
        output_dir = OUTPUT_DIR

    else:
        print("ERROR: Must provide --session-dir, or both --pmid-file and --classification-file, or use --auto")
        sys.exit(1)

    # Use custom thresholds if provided
    high_threshold = args.high_threshold
    low_threshold = args.low_threshold

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load NER union PMIDs
    print(f"\nLoading NER union PMIDs from: {pmid_file}")
    if not pmid_file.exists():
        print(f"  ERROR: File not found: {pmid_file}")
        sys.exit(1)
    with open(pmid_file) as f:
        ner_pmids = set(line.strip() for line in f)
    print(f"  Loaded {len(ner_pmids):,} PMIDs")

    # Load classification union (has title/abstract)
    print(f"\nLoading classification union from: {classif_file}")
    if not classif_file.exists():
        print(f"  ERROR: File not found: {classif_file}")
        sys.exit(1)
    df = pd.read_csv(classif_file)
    print(f"  Loaded {len(df):,} papers")

    # Convert publication_id to string for matching (handle NaN)
    df = df.dropna(subset=['publication_id'])
    df['pmid'] = df['publication_id'].astype(float).astype(int).astype(str)

    # Filter to NER union papers only
    print(f"\nFiltering to NER union papers...")
    df = df[df['pmid'].isin(ner_pmids)].copy()
    print(f"  Filtered to {len(df):,} papers")

    # Check for abstracts
    has_abstract = df['abstract'].notna() & (df['abstract'] != '')
    print(f"  Papers with abstracts: {has_abstract.sum():,} ({100*has_abstract.sum()/len(df):.1f}%)")

    # Score all papers
    print(f"\nScoring papers using linguistic patterns...")
    scores = []
    for idx, (i, row) in enumerate(df.iterrows()):
        if idx % 2000 == 0 and idx > 0:
            print(f"  Progress: {idx:,}/{len(df):,} ({100*idx/len(df):.1f}%)")

        score_data = compute_linguistic_score(row['title'], row['abstract'])
        scores.append(score_data)

    # Add scores to dataframe
    score_df = pd.DataFrame(scores)
    for col in score_df.columns:
        df[f'ling_{col}'] = score_df[col].values

    print(f"\nScoring complete for {len(df):,} papers")

    # Categorize by score
    high_score = df[df['ling_score'] >= high_threshold].copy()
    low_score = df[df['ling_score'] < low_threshold].copy()
    medium_score = df[(df['ling_score'] >= low_threshold) & (df['ling_score'] < high_threshold)].copy()

    # Statistics
    print("\n" + "=" * 80)
    print("SCORING RESULTS")
    print("=" * 80)
    print(f"\nTotal papers scored: {len(df):,}")
    print(f"\nScore distribution:")
    print(f"  High (>={high_threshold}):  {len(high_score):,} ({100*len(high_score)/len(df):.1f}%) - LIKELY INTRODUCTIONS")
    print(f"  Medium ({low_threshold} to {high_threshold-1}): {len(medium_score):,} ({100*len(medium_score)/len(df):.1f}%) - NEEDS SETFIT")
    print(f"  Low (<{low_threshold}):     {len(low_score):,} ({100*len(low_score)/len(df):.1f}%) - LIKELY USAGE")

    print(f"\nScore statistics:")
    print(f"  Mean: {df['ling_score'].mean():.2f}")
    print(f"  Median: {df['ling_score'].median():.2f}")
    print(f"  Min: {df['ling_score'].min():.1f}")
    print(f"  Max: {df['ling_score'].max():.1f}")

    print(f"\nPattern frequencies:")
    print(f"  Title pattern:        {df['ling_has_title_pattern'].sum():,} ({100*df['ling_has_title_pattern'].sum()/len(df):.1f}%)")
    print(f"  Introduction phrases: {df['ling_has_intro_pattern'].sum():,} ({100*df['ling_has_intro_pattern'].sum()/len(df):.1f}%)")
    print(f"  URL in abstract:      {df['ling_has_url'].sum():,} ({100*df['ling_has_url'].sum()/len(df):.1f}%)")

    # Save outputs
    print("\n" + "=" * 80)
    print("SAVING OUTPUTS")
    print("=" * 80)

    # All scored papers
    all_file = output_dir / "all_scored_papers.csv"
    df.to_csv(all_file, index=False)
    print(f"\nSaved all scored papers: {all_file}")
    print(f"  Rows: {len(df):,}")

    # High score papers
    high_file = output_dir / "high_score_papers.csv"
    high_score.to_csv(high_file, index=False)
    print(f"\nSaved high score papers: {high_file}")
    print(f"  Rows: {len(high_score):,}")

    # Medium score papers
    medium_file = output_dir / "medium_score_papers.csv"
    medium_score.to_csv(medium_file, index=False)
    print(f"\nSaved medium score papers: {medium_file}")
    print(f"  Rows: {len(medium_score):,}")

    # Low score papers
    low_file = output_dir / "low_score_papers.csv"
    low_score.to_csv(low_file, index=False)
    print(f"\nSaved low score papers: {low_file}")
    print(f"  Rows: {len(low_score):,}")

    # Summary report
    summary_file = output_dir / "scoring_summary.txt"
    with open(summary_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("PHASE 3: LINGUISTIC SCORING SUMMARY\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now()}\n")
        f.write(f"Input: {len(ner_pmids):,} NER union PMIDs\n")
        f.write(f"Profile: Aggressive (High>={high_threshold}, Low<{low_threshold})\n\n")

        f.write("RESULTS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total papers scored: {len(df):,}\n")
        f.write(f"High score (>={high_threshold}): {len(high_score):,} ({100*len(high_score)/len(df):.1f}%)\n")
        f.write(f"Medium score ({low_threshold} to {high_threshold-1}): {len(medium_score):,} ({100*len(medium_score)/len(df):.1f}%)\n")
        f.write(f"Low score (<{low_threshold}): {len(low_score):,} ({100*len(low_score)/len(df):.1f}%)\n\n")

        f.write("PATTERN FREQUENCIES\n")
        f.write("-" * 80 + "\n")
        f.write(f"Title pattern: {df['ling_has_title_pattern'].sum():,} ({100*df['ling_has_title_pattern'].sum()/len(df):.1f}%)\n")
        f.write(f"Introduction phrases: {df['ling_has_intro_pattern'].sum():,} ({100*df['ling_has_intro_pattern'].sum()/len(df):.1f}%)\n")
        f.write(f"URL in abstract: {df['ling_has_url'].sum():,} ({100*df['ling_has_url'].sum()/len(df):.1f}%)\n\n")

        f.write("OUTPUT FILES\n")
        f.write("-" * 80 + "\n")
        f.write(f"high_score_papers.csv: {len(high_score):,} papers\n")
        f.write(f"medium_score_papers.csv: {len(medium_score):,} papers\n")
        f.write(f"low_score_papers.csv: {len(low_score):,} papers\n")
        f.write(f"all_scored_papers.csv: {len(df):,} papers\n")

    print(f"\nSaved summary: {summary_file}")

    # Runtime
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 80)
    print("SUCCESS")
    print("=" * 80)
    print(f"\nCompleted: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")

    print("\n" + "=" * 80)
    print("NEXT STEP")
    print("=" * 80)
    if args.session_dir:
        print("\nRun Phase 4 - SetFit Inference on medium-score papers:")
        print(f"  python scripts/phase4_setfit/08_setfit_inference.py --session-dir {args.session_dir}")
        print(f"  Input: {medium_file}")
        print(f"  Papers to classify: {len(medium_score):,}")
    else:
        print("\nRun Phase 4 - SetFit Inference on medium-score papers:")
        print("  Upload SetFit notebook to Google Colab")
        print(f"  Input: {medium_file}")
        print(f"  Papers to classify: {len(medium_score):,}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
