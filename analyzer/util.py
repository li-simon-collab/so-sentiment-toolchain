"""Module containing mostly file operation related utility functions.

.. module:: util
    :platform: Linux
    :synopsis: Utility functions module.
.. moduleauthor:: Simon Larsén <slarse@kth.se>, Li Ling <liling@kth.se>
"""
import os
import re
import shutil
import tempfile
from itertools import islice
from typing import Callable, Iterable, List, Set

from bs4 import BeautifulSoup
from markdown import markdown

import pandas as pd
from analyzer import LOGGER, database, dbops, stats

DISALLOWED_BODIES_TAGS = ['pre', 'code', 'blockquote']


def _sanitize_document(text: str):
    """Sanitization that's in common for all SO documents."""
    tag_cleaned = _remove_tags_and_disallowed_bodies(text,
                                                     DISALLOWED_BODIES_TAGS)
    whitespace_cleaned = re.sub(r'\s+', ' ', tag_cleaned)
    url_cleaned = re.sub(r'https?://\S+', '', whitespace_cleaned, flags=re.I)
    return url_cleaned.strip()


def sanitize_post(text: str):
    """Sanitize a Stack Overflow Post."""
    pre_cleaned = _sanitize_document(text)
    # sometimes, users incorrectly format their posts with inline code patterns
    # for blocks of code, leaving raw markdown
    md_code_patterns = r'(```.*?```)|(`.*?`)'
    return re.sub(md_code_patterns, '', pre_cleaned, flags=re.I).strip()


def sanitize_comment(text: str):
    """Sanitize a Stack Overflow Comment."""
    return _sanitize_document(markdown(text))


def _remove_tags_and_disallowed_bodies(
        text: str, disallowed_bodies_tags: List[str]) -> str:
    """Return a cleaned version of the text without any HTML tags and stripped
    of the text in the bodies of the disallowed tags.
    """
    soup = BeautifulSoup(text, 'html.parser')
    for tag in soup.find_all(disallowed_bodies_tags):
        tag.decompose()
    return soup.get_text()


def yield_batches(it, batch_size):
    """Yield lists of the specified size from the iterator."""
    exhausted = False
    while not exhausted:
        batch = []
        for _ in range(batch_size):
            try:
                batch.append(next(it))
            except StopIteration:
                exhausted = True
        if batch:  # don't yield empty batches
            yield batch


def generate_comments_csv(num_comments: int, outpath: str, tag: str) -> str:
    """Generate a CSV file and a corresponding index file for comments using
    random sampling."""
    return _generate_csv(num_comments, outpath, database.Comment, tag=tag)


def generate_posts_csv(num_posts: int, outpath: str,
                       post_type: database.PostType, tag: str) -> str:
    """Generate a CSV file and a corresponding index file for posts of the
    specified type using random sampling."""
    return _generate_csv(num_posts, outpath, database.Post, post_type, tag=tag)


def _generate_csv(sample_size: int,
                  outpath: str,
                  model,
                  post_type: database.PostType = None,
                  tag: str = None):
    """Generate a CSV file and a corresponding index file by randomly sampling
    num_posts of documents of the specified type."""
    elems = dbops.get_random_elems_from_database(
        model, sample_size, post_type=post_type, tag=tag)
    return write_sample_and_index_files(outpath, elems)


def write_sample_and_index_files(outpath: str, elems):
    """Given an outpath and a query result (elems), write the texts in the query
    result to outpath, write the indices in the query result to index_filepath.

    Return the index_filepath.
    """
    index_filepath = f'{outpath}.index'
    with open(outpath, 'w') as f, open(index_filepath, 'w') as index:
        for e in elems:
            f.write(f"{e.text}\n")
            index.write(f"{e.id}\n")
    return index_filepath


def find_classifiers(senti4sd_pool_root):
    """Return a list of classifier scripts found in the directory tree starting from
    senti4sd_pool_root.
    """
    script_path = 'ClassificationTask/classificationTask.sh'
    classifiers = [
        os.path.join(senti4sd_pool_root, senti_root, script_path)
        for senti_root in os.listdir(senti4sd_pool_root)
        if 'senti4sd' in senti_root.lower()
        and os.path.isdir(os.path.join(senti4sd_pool_root, senti_root))
    ]
    if not classifiers:
        raise ValueError(f"No classifiers found in {senti4sd_pool_root}.")
    assert classifiers  # check that we found classifiers
    LOGGER.info(
        f"Found {len(classifiers)} classifiers at: {' | '.join(classifiers)}")
    return classifiers


def split_file(rows_per_file: int, inpath: str, dir):
    """Split file from inpath into multiple named tempfiles with delete set to
    false, each containing rows_per_file number of rows.
    
    All split files are placed in dir. The intent is to use this function with dir
    as a TemporaryDirectory.

    Return paths to the subfiles.
    """
    subfiles = []
    with open(inpath, 'r') as infile:
        for i, sli in enumerate(
                iter(lambda: list(islice(infile, rows_per_file)), [])):
            with tempfile.NamedTemporaryFile(
                    mode='w', dir=dir, delete=False) as subfile:
                subfile.writelines(sli)
                subfiles.append(subfile.name)
    return subfiles


def concatenate_predictions(filepaths: list, outpath: str):
    assert filepaths
    LOGGER.info(
        f"Concatenating {len(filepaths)} partial documents into {outpath}")
    shutil.copyfile(filepaths[0], outpath)  # copy first file to get header row
    with open(outpath, 'a') as f:
        for i in range(1, len(filepaths)):
            with open(filepaths[i], 'r') as partial_res:
                next(partial_res)  # skip header row
                f.writelines(partial_res)


def log_exception(pre_msg, e):
    LOGGER.error(f"{pre_msg}\n{type(e).__name__}: {str(e)}")
