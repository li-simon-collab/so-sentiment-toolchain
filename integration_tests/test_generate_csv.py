import os
import tempfile
from itertools import zip_longest
import numpy as np
from analyzer import util, database
from analyzer.database import PostType, Post, Comment

NUM_DOCUMENTS = 1000


def setup_module(module):
    database.setup_session(database.Driver.INTEGRATION)


def setup_function(function):
    np.random.seed(51234)


def test_generate_comments_csv():
    with tempfile.TemporaryDirectory() as tmpdir:
        outfile = os.path.join(tmpdir, 'out.csv')
        index_filepath = util.generate_comments_csv(NUM_DOCUMENTS, outfile,
                                                    None)
        verify_csvs(outfile, index_filepath, Comment)


def test_generate_questions_csv():
    with tempfile.TemporaryDirectory() as tmpdir:
        outfile = os.path.join(tmpdir, 'out.csv')
        index_filepath = util.generate_posts_csv(NUM_DOCUMENTS, outfile,
                                                 PostType.QUESTION, None)
        verify_csvs(outfile, index_filepath, Post, PostType.QUESTION)


def test_generate_answers_csv():
    with tempfile.TemporaryDirectory() as tmpdir:
        outfile = os.path.join(tmpdir, 'out.csv')
        index_filepath = util.generate_posts_csv(NUM_DOCUMENTS, outfile,
                                                 PostType.ANSWER, None)
        verify_csvs(outfile, index_filepath, Post, PostType.ANSWER)


def verify_csvs(document_file, index_file, model, post_type=None):
    """Verify that each document has the text indicated by the ids in the index
    file.
    """
    s = database.Session()
    with open(index_file, 'r') as indices, open(document_file, 'r') as docs:
        ids = [int(id) for id in indices]
        texts = [text[:-1] for text in docs]  # must strip rightmost newline
        query = s.query(model.id, model.text).filter(model.id.in_(ids))

        if post_type is not None:
            query = query.filter(model.post_type_id == post_type.value)

        expected_id_text_pairs = query.all()
        actual_id_text_pairs = sorted(
            zip_longest(ids, texts), key=lambda tup: tup[0])

        for actual, expected in zip_longest(actual_id_text_pairs,
                                            expected_id_text_pairs):
            assert actual == expected
