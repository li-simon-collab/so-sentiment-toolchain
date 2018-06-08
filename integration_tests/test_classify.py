import os
import asyncio
import tempfile
import pytest
from analyzer.classify import classify_sentiment, ClassificationError
from analyzer.stats import read_predictions

DIRNAME = os.path.dirname(__file__)
TEST_ANSWERS_CSV = os.path.abspath(os.path.join(DIRNAME, "test_answers.csv"))
TEST_ANSWERS_PREDICTIONS = os.path.abspath(
    os.path.join(DIRNAME, "test_answers_predictions.csv"))
BOGUS_SENTI4SD_ROOT = os.path.abspath(os.path.join(DIRNAME, "bogus"))
NUM_DOCUMENTS = 100

CLASSIFIER_POOL_DIR = os.path.expanduser("~/software")

EXPECTED_PREDICTIONS = read_predictions(TEST_ANSWERS_PREDICTIONS)

# check file contents
with open(TEST_ANSWERS_CSV) as ans, open(TEST_ANSWERS_PREDICTIONS) as pred:
    # pred has a header row
    assert len(ans.readlines()) + 1 == len(pred.readlines())


def setup_function(function):
    asyncio.set_event_loop(asyncio.new_event_loop())


def test_classify_sentiment_multiple_subfiles_even_split():
    """Note that this test will also run multiple classifiers, if available."""
    divisor = 10
    rows_per_file = NUM_DOCUMENTS // divisor
    # meta assert, check that split is even
    assert rows_per_file * divisor == NUM_DOCUMENTS

    with tempfile.TemporaryDirectory() as tmpdir:
        outfile = os.path.join(tmpdir, 'pred.csv')
        classify_sentiment(rows_per_file, CLASSIFIER_POOL_DIR,
                           TEST_ANSWERS_CSV, outfile)
        actual_predictions = read_predictions(outfile)
    assert all((actual_predictions == EXPECTED_PREDICTIONS).Predicted)


def test_classify_sentiment_multiple_subfiles_uneven_split():
    """Note that this test will also run multiple classifiers, if available."""
    divisor = 7
    rows_per_file = NUM_DOCUMENTS // divisor
    # meta assert, check that split is uneven
    assert rows_per_file * divisor != NUM_DOCUMENTS

    with tempfile.TemporaryDirectory() as tmpdir:
        outfile = os.path.join(tmpdir, 'pred.csv')
        classify_sentiment(rows_per_file, CLASSIFIER_POOL_DIR,
                           TEST_ANSWERS_CSV, outfile)
        actual_predictions = read_predictions(outfile)
    assert all((actual_predictions == EXPECTED_PREDICTIONS).Predicted)


def test_classify_sentiment_single_subfile():
    """Note that this test will run a single classifier regardless of amount
    available.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        outfile = os.path.join(tmpdir, 'pred.csv')
        classify_sentiment(NUM_DOCUMENTS, CLASSIFIER_POOL_DIR,
                           TEST_ANSWERS_CSV, outfile)
        actual_predictions = read_predictions(outfile)
    assert all((actual_predictions == EXPECTED_PREDICTIONS).Predicted)


def test_classify_sentiment_error_in_senti4sd():
    """Test that the program exits gracefully when senti4sd gives non-zero exit
    status."""
    with tempfile.TemporaryDirectory() as tmpdir:
        outfile = os.path.join(tmpdir, 'out.csv')
        with pytest.raises(ClassificationError):
            classify_sentiment(NUM_DOCUMENTS, BOGUS_SENTI4SD_ROOT,
                               TEST_ANSWERS_CSV, outfile)
