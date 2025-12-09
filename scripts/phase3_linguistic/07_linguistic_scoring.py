#!/usr/bin/env python3
"""
Script 03: Linguistic Pattern Scoring

Applies linguistic pattern detection to uncertain papers to score them
for likelihood of being resource introduction papers.

Scoring:
    High (≥3): Auto-classify as introduction
    Medium (0-2): Requires ML classification
    Low (<0): Auto-classify as usage/mention

Input:
    - data/filtered/uncertain_papers.csv

Output:
    - data/results/high_score_papers.csv (likely introductions)
    - data/results/medium_score_papers.csv (needs ML)
    - data/results/low_score_papers.csv (likely usage)
    - data/results/linguistic_scoring_report.txt
    - logs/03_linguistic_scoring.log
"""

import sys
from pathlib import Path
import logging
import pandas as pd
from datetime import datetime
import numpy as np

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.config import (
    FILTERED_DIR, RESULTS_DIR, LOGS_DIR,
    HIGH_CONFIDENCE_SCORE, LOW_CONFIDENCE_SCORE,
    LOG_FORMAT, LOG_LEVEL
)
from utils.patterns import compute_linguistic_score

# Configure logging
log_file = LOGS_DIR / "03_linguistic_scoring.log"
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def score_paper(row):
    """Score a single paper using linguistic patterns"""
    title = str(row.get('title', ''))
    abstract = str(row.get('abstract', ''))

    if not abstract or abstract == 'nan':
        # No abstract - can't score effectively
        return {
            'score': 0,
            'evidence': 'No abstract available',
            'has_intro_pattern': False,
            'has_title_pattern': False,
            'has_url': False,
            'impl_keywords': 0,
            'usage_keywords': 0,
            'has_stats': False
        }

    return compute_linguistic_score(title, abstract)


def main():
    """Main execution"""
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("SCRIPT 03: LINGUISTIC PATTERN SCORING")
    logger.info("=" * 80)
    logger.info(f"Started at: {start_time}")

    # Load uncertain papers
    uncertain_file = FILTERED_DIR / "uncertain_papers.csv"
    if not uncertain_file.exists():
        logger.error(f"Uncertain papers file not found: {uncertain_file}")
        logger.error("Run 02_metadata_filtering.py first")
        sys.exit(1)

    logger.info(f"Loading uncertain papers from {uncertain_file}")
    df = pd.read_csv(uncertain_file)
    logger.info(f"Loaded {len(df):,} papers")

    # Check for abstracts
    has_abstract = df['abstract'].notna()
    logger.info(f"Papers with abstracts: {has_abstract.sum():,} ({100*has_abstract.sum()/len(df):.1f}%)")

    if has_abstract.sum() == 0:
        logger.error("ERROR: No papers have abstracts - cannot perform linguistic scoring")
        sys.exit(1)

    # Score all papers
    logger.info("\nScoring papers using linguistic patterns...")
    logger.info("This may take a few minutes...")

    scores = []
    for i, row in df.iterrows():
        if i % 1000 == 0 and i > 0:
            logger.info(f"Progress: {i:,}/{len(df):,} ({100*i/len(df):.1f}%)")

        score_data = score_paper(row)
        scores.append(score_data)

    # Add scores to dataframe
    score_df = pd.DataFrame(scores)
    for col in score_df.columns:
        df[f'ling_{col}'] = score_df[col]

    logger.info(f"Scoring complete for {len(df):,} papers")

    # Categorize by score
    high_score = df[df['ling_score'] >= HIGH_CONFIDENCE_SCORE].copy()
    medium_score = df[(df['ling_score'] >= LOW_CONFIDENCE_SCORE) &
                      (df['ling_score'] < HIGH_CONFIDENCE_SCORE)].copy()
    low_score = df[df['ling_score'] < LOW_CONFIDENCE_SCORE].copy()

    # Log results
    logger.info("\n" + "=" * 80)
    logger.info("SCORING RESULTS")
    logger.info("=" * 80)

    logger.info(f"\nTotal papers scored: {len(df):,}")
    logger.info(f"\nScore distribution:")
    logger.info(f"  High (≥{HIGH_CONFIDENCE_SCORE}): {len(high_score):,} ({100*len(high_score)/len(df):.1f}%) - LIKELY INTRODUCTIONS")
    logger.info(f"  Medium (0-{HIGH_CONFIDENCE_SCORE-1}): {len(medium_score):,} ({100*len(medium_score)/len(df):.1f}%) - NEEDS ML")
    logger.info(f"  Low (<{LOW_CONFIDENCE_SCORE}): {len(low_score):,} ({100*len(low_score)/len(df):.1f}%) - LIKELY USAGE")

    # Score statistics
    logger.info(f"\nScore statistics:")
    logger.info(f"  Mean: {df['ling_score'].mean():.2f}")
    logger.info(f"  Median: {df['ling_score'].median():.2f}")
    logger.info(f"  Std dev: {df['ling_score'].std():.2f}")
    logger.info(f"  Min: {df['ling_score'].min():.0f}")
    logger.info(f"  Max: {df['ling_score'].max():.0f}")

    # Pattern analysis
    logger.info("\n" + "-" * 80)
    logger.info("PATTERN DETECTION ANALYSIS")
    logger.info("-" * 80)

    logger.info(f"\nPattern frequencies:")
    logger.info(f"  Title pattern: {df['ling_has_title_pattern'].sum():,} ({100*df['ling_has_title_pattern'].sum()/len(df):.1f}%)")
    logger.info(f"  Introduction phrases: {df['ling_has_intro_pattern'].sum():,} ({100*df['ling_has_intro_pattern'].sum()/len(df):.1f}%)")
    logger.info(f"  URL in abstract: {df['ling_has_url'].sum():,} ({100*df['ling_has_url'].sum()/len(df):.1f}%)")
    logger.info(f"  Statistical results: {df['ling_has_stats'].sum():,} ({100*df['ling_has_stats'].sum()/len(df):.1f}%)")

    logger.info(f"\nKeyword counts:")
    logger.info(f"  Implementation keywords mean: {df['ling_impl_keywords'].mean():.2f}")
    logger.info(f"  Usage keywords mean: {df['ling_usage_keywords'].mean():.2f}")

    # High score examples
    if len(high_score) > 0:
        logger.info("\n" + "-" * 80)
        logger.info("HIGH SCORE EXAMPLES (Likely Introductions)")
        logger.info("-" * 80)

        for i, row in high_score.nlargest(5, 'ling_score').iterrows():
            logger.info(f"\nScore: {row['ling_score']}")
            logger.info(f"Title: {row['title'][:100]}...")
            logger.info(f"Evidence: {row['ling_evidence']}")
            logger.info(f"Journal: {row.get('journal', 'N/A')}")

    # Low score examples
    if len(low_score) > 0:
        logger.info("\n" + "-" * 80)
        logger.info("LOW SCORE EXAMPLES (Likely Usage)")
        logger.info("-" * 80)

        for i, row in low_score.nsmallest(5, 'ling_score').iterrows():
            logger.info(f"\nScore: {row['ling_score']}")
            logger.info(f"Title: {row['title'][:100]}...")
            logger.info(f"Evidence: {row['ling_evidence']}")
            logger.info(f"Journal: {row.get('journal', 'N/A')}")

    # Save outputs
    logger.info("\n" + "=" * 80)
    logger.info("SAVING OUTPUTS")
    logger.info("=" * 80)

    high_file = RESULTS_DIR / "high_score_papers.csv"
    logger.info(f"\nSaving high score papers to {high_file}")
    high_score.to_csv(high_file, index=False)
    logger.info(f"Saved {len(high_score):,} papers")

    medium_file = RESULTS_DIR / "medium_score_papers.csv"
    logger.info(f"\nSaving medium score papers to {medium_file}")
    medium_score.to_csv(medium_file, index=False)
    logger.info(f"Saved {len(medium_score):,} papers")

    low_file = RESULTS_DIR / "low_score_papers.csv"
    logger.info(f"\nSaving low score papers to {low_file}")
    low_score.to_csv(low_file, index=False)
    logger.info(f"Saved {len(low_score):,} papers")

    # Generate detailed report
    report_file = RESULTS_DIR / "linguistic_scoring_report.txt"
    logger.info(f"\nGenerating detailed report: {report_file}")

    with open(report_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("LINGUISTIC PATTERN SCORING REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now()}\n")
        f.write(f"Input: {uncertain_file}\n\n")

        f.write("SUMMARY\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total papers scored: {len(df):,}\n")
        f.write(f"High score (≥{HIGH_CONFIDENCE_SCORE}): {len(high_score):,} ({100*len(high_score)/len(df):.1f}%)\n")
        f.write(f"Medium score (0-{HIGH_CONFIDENCE_SCORE-1}): {len(medium_score):,} ({100*len(medium_score)/len(df):.1f}%)\n")
        f.write(f"Low score (<{LOW_CONFIDENCE_SCORE}): {len(low_score):,} ({100*len(low_score)/len(df):.1f}%)\n\n")

        f.write("PATTERN FREQUENCIES\n")
        f.write("-" * 80 + "\n")
        f.write(f"Title pattern: {df['ling_has_title_pattern'].sum():,} ({100*df['ling_has_title_pattern'].sum()/len(df):.1f}%)\n")
        f.write(f"Introduction phrases: {df['ling_has_intro_pattern'].sum():,} ({100*df['ling_has_intro_pattern'].sum()/len(df):.1f}%)\n")
        f.write(f"URL in abstract: {df['ling_has_url'].sum():,} ({100*df['ling_has_url'].sum()/len(df):.1f}%)\n")
        f.write(f"Statistical results: {df['ling_has_stats'].sum():,} ({100*df['ling_has_stats'].sum()/len(df):.1f}%)\n\n")

        f.write("TOP 20 HIGH SCORE PAPERS\n")
        f.write("-" * 80 + "\n")
        for i, row in high_score.nlargest(20, 'ling_score').iterrows():
            f.write(f"\nPMID: {row['pmid']}\n")
            f.write(f"Score: {row['ling_score']}\n")
            f.write(f"Title: {row['title']}\n")
            f.write(f"Evidence: {row['ling_evidence']}\n")
            f.write(f"Journal: {row.get('journal', 'N/A')}\n")

        f.write("\n" + "=" * 80 + "\n")
        f.write("TOP 20 LOW SCORE PAPERS\n")
        f.write("-" * 80 + "\n")
        for i, row in low_score.nsmallest(20, 'ling_score').iterrows():
            f.write(f"\nPMID: {row['pmid']}\n")
            f.write(f"Score: {row['ling_score']}\n")
            f.write(f"Title: {row['title']}\n")
            f.write(f"Evidence: {row['ling_evidence']}\n")
            f.write(f"Journal: {row.get('journal', 'N/A')}\n")

    logger.info("Report saved")

    # Runtime
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"\nCompleted at: {end_time}")
    logger.info(f"Total runtime: {duration}")

    logger.info("\n" + "=" * 80)
    logger.info("SUCCESS - Linguistic scoring complete")
    logger.info("=" * 80)

    # Final summary
    logger.info(f"\n📊 WEEK 1 PIPELINE SUMMARY:")
    logger.info(f"\nTotal papers processed: {len(df):,}")
    logger.info(f"\nAuto-classified as INTRODUCTIONS:")
    logger.info(f"  High linguistic score: {len(high_score):,}")
    logger.info(f"\nAuto-classified as USAGE:")
    logger.info(f"  Low linguistic score: {len(low_score):,}")
    logger.info(f"\nNEEDS ML CLASSIFICATION:")
    logger.info(f"  Medium linguistic score: {len(medium_score):,}")

    logger.info(f"\n📁 Output files:")
    logger.info(f"  {high_file}")
    logger.info(f"  {medium_file}")
    logger.info(f"  {low_file}")
    logger.info(f"  {report_file}")

    logger.info(f"\n✅ Week 1 complete! Ready for Week 2 (SetFit ML training)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
