#!/usr/bin/env python3
"""
Purpose: Run query on EuropePMC
Authors: Ana Maria Istrate, Kenneth Schackart, and Warren Emmett <warren.emmett@gmail.com>
"""

import argparse
import os
import re
from datetime import datetime
from typing import List, NamedTuple, Tuple, cast

import pandas as pd
import requests


# ---------------------------------------------------------------------------
class Args(NamedTuple):
    """ Command-line arguments """
    query: str
    from_date: str
    to_date: str
    out_dir: str


# ---------------------------------------------------------------------------
def get_args() -> Args:
    """ Parse command-line arguments """

    parser = argparse.ArgumentParser(
        description=('Query EuropePMC to retrieve articles. '
                     'Saves csv of results and file of query dates'),
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('query',
                        metavar='QUERY',
                        type=str,
                        help='EuropePMC query to run (file or string)')
    parser.add_argument('-f',
                        '--from-date',
                        metavar='DATE',
                        type=str,
                        required=True,
                        help='Articles published after (file or string)')
    parser.add_argument('-t',
                        '--to-date',
                        metavar='DATE',
                        type=str,
                        default=None,
                        help='Articles published before (default: today)')
    parser.add_argument('-o',
                        '--out-dir',
                        metavar='DIR',
                        type=str,
                        default='out/',
                        help='Output directory')

    args = parser.parse_args()

    if os.path.isfile(args.query):
        args.query = open(args.query).read()
    if os.path.isfile(args.from_date):
        args.from_date = open(args.from_date).read()

    date_pattern = re.compile(
        r'''^           # Beginning of date string
            [\d]{4}     # Must start wwith 4 digit year
            (-[\d]{2}   # Optionally 2 digit month
            (-[\d]{2})? # Optionally 2 digit day
            )?          # Finish making month optional
            $           # Followed by nothing else
            ''', re.X)
    for date in [args.from_date, args.to_date]:
        if not re.match(date_pattern, date):
            parser.error(f'Date "{date}" must be one of:\n'
                         '\t\t\tYYYY\n'
                         '\t\t\tYYYY-MM\n'
                         '\t\t\tYYYY-MM-DD')

    return Args(args.query, args.from_date, args.to_date, args.out_dir)


# ---------------------------------------------------------------------------
def make_filenames(outdir: str) -> Tuple[str, str]:
    '''
    Make filenames for output csv file and last date text file

    Parameters:
    `outdir`: Output directory

    Return: Tuple of csv and txt filenames
    '''

    csv_out = os.path.join(outdir, 'query_results.csv')
    txt_out = os.path.join(outdir, 'last_query_dates.txt')

    return csv_out, txt_out


# ---------------------------------------------------------------------------
def test_make_filenames() -> None:
    """ Test make_filenames() """

    assert make_filenames('data/new_query') == (
        'data/new_query/query_results.csv',
        'data/new_query/last_query_date.txt')


# ---------------------------------------------------------------------------
def clean_results(results: List[dict]) -> pd.DataFrame:
    """
    Retrieve enhanced metadata from results of query

    Parameters:
    `results`: JSON-encoded response (nested dictionary)

    Return: Dataframe of results with 24 metadata fields
    """
    import json

    records = []
    for page in results:
        for paper in page.get('resultList').get('result'):  # type: ignore
            # Extract all enhanced metadata fields
            record = {
                # Core fields (original 4)
                'id': paper.get('pmid'),
                'title': paper.get('title'),
                'abstract': paper.get('abstractText'),
                'publication_date': paper.get('firstPublicationDate'),

                # Boolean flags (Tier 1 - 8 fields)
                'hasDbCrossReferences': paper.get('hasDbCrossReferences'),
                'hasData': paper.get('hasData'),
                'hasSuppl': paper.get('hasSuppl'),
                'isOpenAccess': paper.get('isOpenAccess'),
                'inPMC': paper.get('inPMC'),
                'inEPMC': paper.get('inEPMC'),
                'hasPDF': paper.get('hasPDF'),
                'hasBook': paper.get('hasBook'),

                # Citation data
                'citedByCount': paper.get('citedByCount'),

                # Temporal (extract year)
                'pubYear': paper.get('pubYear'),

                # Publication type
                'pubType': _extract_pub_type(paper.get('pubTypeList')),

                # Enhanced features (Tier 2)
                'keywords': _extract_keywords(paper.get('keywordList')),
                'meshTerms': _extract_mesh_terms(paper.get('meshHeadingList')),
                'journalTitle': _extract_journal_title(paper.get('journalInfo')),
                'journalISSN': _extract_journal_issn(paper.get('journalInfo')),
                'authorAffiliations': _extract_author_affiliations(paper.get('authorList')),
            }
            records.append(record)

    return pd.DataFrame(records)


# Helper functions for metadata extraction
def _extract_pub_type(pub_type_list):
    """Extract publication type"""
    if not pub_type_list:
        return None
    pub_types = pub_type_list.get('pubType', [])
    if pub_types:
        import json
        return json.dumps(pub_types)
    return None


def _extract_keywords(keyword_list):
    """Extract keywords"""
    if not keyword_list:
        return None
    keywords = keyword_list.get('keyword', [])
    if keywords:
        import json
        return json.dumps(keywords)
    return None


def _extract_mesh_terms(mesh_heading_list):
    """Extract MeSH terms"""
    if not mesh_heading_list:
        return None
    mesh_headings = mesh_heading_list.get('meshHeading', [])
    if mesh_headings:
        terms = [h.get('descriptorName') for h in mesh_headings if h.get('descriptorName')]
        if terms:
            import json
            return json.dumps(terms)
    return None


def _extract_journal_title(journal_info):
    """Extract journal title"""
    if not journal_info:
        return None
    journal = journal_info.get('journal', {})
    return journal.get('title')


def _extract_journal_issn(journal_info):
    """Extract journal ISSN"""
    if not journal_info:
        return None
    journal = journal_info.get('journal', {})
    issn_list = journal.get('issn', [])
    if issn_list:
        if isinstance(issn_list, list):
            return '|'.join(issn_list)
        return str(issn_list)
    return None


def _extract_author_affiliations(author_list):
    """Extract author affiliations"""
    if not author_list:
        return None
    authors = author_list.get('author', [])
    if not authors:
        return None
    affiliations = set()
    for author in authors:
        affiliation = author.get('affiliation', '')
        if affiliation:
            affiliations.add(affiliation)
    if affiliations:
        import json
        return json.dumps(list(affiliations))
    return None


# ---------------------------------------------------------------------------
def run_query(query: str, from_date: str, to_date: str) -> pd.DataFrame:
    """
    Run query on EuropePMC API

    Parameters:
    `query`: Query to use
    `from_date`: Articles published after this date
    `to_date`: Articles published after this date

    Return: `DataFrame` of returned titles and abstracts
    """

    query = query.format(from_date, to_date)

    prefix = 'https://www.ebi.ac.uk/europepmc/webservices/rest/search?query='
    suffix = '&resultType=core&fromSearchPost=false&format=json'
    url = prefix + query + suffix

    results = requests.get(url)
    if results.status_code != requests.codes.ok:  # pylint: disable=no-member
        results.raise_for_status()

    results_json = cast(dict, results.json())

    result_pages: List[dict] = []
    result_pages.append(results_json)

    while results_json.get('nextPageUrl') is not None:
        results = requests.get(results_json['nextPageUrl'])
        status = results.status_code
        if status != requests.codes.ok:  # pylint: disable=no-member
            results.raise_for_status()

        results_json = cast(dict, results.json())

        result_pages.append(results_json)

    return clean_results(result_pages)


# ---------------------------------------------------------------------------
def main() -> None:
    """ Main function """

    args = get_args()
    out_dir = args.out_dir

    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    out_df, date_out = make_filenames(out_dir)

    if not args.to_date:
        to_date = datetime.today().strftime(r'%Y-%m-%d')
    else:
        to_date = args.to_date

    from_date = args.from_date

    results = run_query(args.query, from_date, to_date)

    results.to_csv(out_df, index=False)
    print(f"{from_date}-{to_date}", file=open(date_out, 'wt'))

    print(f'Done. Wrote 2 files to {out_dir}.')


# ---------------------------------------------------------------------------
if __name__ == '__main__':
    main()
