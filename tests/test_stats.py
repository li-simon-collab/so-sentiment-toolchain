import math
import os

import pandas as pd
import pytest
from analyzer import stats
from scipy.stats import norm

PREDICTIONS_SAMPLE = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), 'sanitized_comments_predictions.csv'))
NEG_COUNTS = 21
POS_COUNTS = 21
NEUTRAL_COUNTS = 166

CHI2_INDEPENDENT_PREDICTION = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), 'test_chi2_independent_predictions.csv'))


def test_calculate_sample_size_on_small_population():
    # Entire population needed due to small population size
    # alpha_level=0.05,
    # margin_of_error=0.01, population=20
    expected_sample_size = 20
    actual_sample_size = stats.calculate_sample_size(
        population=20,
        alpha_level=0.05,
        accepted_margin_of_error=0.01,
    )
    assert expected_sample_size == actual_sample_size


def test_calculate_sample_size_on_medium_population():
    # A not-very-large sample size
    # alpha_level=0.05,
    # margin_of_error=0.01, population=20000
    expected_sample_size = 6489
    actual_sample_size = stats.calculate_sample_size(
        population=20000, alpha_level=0.05, accepted_margin_of_error=0.01)
    assert expected_sample_size == actual_sample_size


def test_calculate_sample_size_on_large_population():
    # population size > 0.05*sample size, no need to adjust sample size
    # alpha_level=0.05,
    # margin_of_error=0.01, population=20000
    expected_sample_size = 9604
    actual_sample_size = stats.calculate_sample_size(
        population=38670316, alpha_level=0.05, accepted_margin_of_error=0.01)
    assert expected_sample_size == actual_sample_size


def test_calculate_sample_size_99_conf_very_large_population():
    # very large population, 99% confidence interval
    # alpha_level=0.01,
    # margin_of_error=0.01, population=20000
    expected_sample_size = 16588
    actual_sample_size = stats.calculate_sample_size(
        population=38670316, alpha_level=0.01, accepted_margin_of_error=0.01)
    assert expected_sample_size == actual_sample_size

    # very large population, 99% confidence interval
    # alpha_level=0.01,
    # margin_of_error=0.001, population=20000
    expected_sample_size = 1_658_725
    actual_sample_size = stats.calculate_sample_size(
        population=38670316, alpha_level=0.01, accepted_margin_of_error=0.001)
    assert expected_sample_size == actual_sample_size


def test_calculate_margin_of_error_finite_population():
    p = 0.37
    n = 100
    N = 500
    t = norm.ppf(0.975)
    expected_margin_of_error = 0.0900641612200527
    actual_margin_of_error = stats.calculate_margin_of_error(p, n, N, t)
    assert expected_margin_of_error == actual_margin_of_error


def test_calculate_margin_of_error_infinite_population():
    p = 0.37
    n = 100
    N = math.inf
    t = norm.ppf(0.975)
    expected_margin_of_error = 0.10010462346851963
    actual_margin_of_error = stats.calculate_margin_of_error(p, n, N, t)
    assert expected_margin_of_error == actual_margin_of_error


def test_construct_stats_dataframe_from_predictions_csv():
    alpha_level = 0.05  # confidence interval=95%
    dfname = 'test_prediction'
    n = NEG_COUNTS + NEUTRAL_COUNTS + POS_COUNTS
    tval = norm.ppf(0.975)  # confidence interval=95%

    expected_sentiment_prob_neg = NEG_COUNTS / n
    expected_sentiment_prob_neutral = NEUTRAL_COUNTS / n
    expected_sentiment_prob_pos = POS_COUNTS / n

    expected_margin_of_error_neg = 0.04344598960235094
    expected_margin_of_error_neutral = 0.057090112400765704
    expected_margin_of_error_pos = 0.04344598960235094

    actual_dataframe = stats.construct_stats_dataframe_from_predictions_csv(
        PREDICTIONS_SAMPLE, alpha_level, dfname)

    # sentiment probabilities
    assert actual_dataframe.loc[
        stats.NEGATIVE].sentiment_prob == expected_sentiment_prob_neg
    assert actual_dataframe.loc[
        stats.NEUTRAL].sentiment_prob == expected_sentiment_prob_neutral
    assert actual_dataframe.loc[
        stats.POSITIVE].sentiment_prob == expected_sentiment_prob_pos

    # margin of errors
    assert actual_dataframe.loc[
        stats.NEGATIVE].margin_of_error == expected_margin_of_error_neg
    assert actual_dataframe.loc[
        stats.NEUTRAL].margin_of_error == expected_margin_of_error_neutral
    assert actual_dataframe.loc[
        stats.POSITIVE].margin_of_error == expected_margin_of_error_pos


def test_chi2_test_dependent():
    prediction_files = [PREDICTIONS_SAMPLE, PREDICTIONS_SAMPLE]
    confidence_level = 0.95
    expected_chi2_result = True  # Since the prediction_files are identical
    actual_chi2_result = stats.chi2_test_independence(prediction_files,
                                                      confidence_level)
    assert expected_chi2_result == actual_chi2_result

def test_chi2_test_independent():
    prediction_files = [PREDICTIONS_SAMPLE,
            CHI2_INDEPENDENT_PREDICTION]
    confidence_level = 0.95
    expected_chi2_result = False  # Since the prediction_files are identical
    actual_chi2_result = stats.chi2_test_independence(prediction_files,
                                                      confidence_level)
    assert expected_chi2_result == actual_chi2_result


def test_generate_dfname():
    filename = PREDICTIONS_SAMPLE
    expected_filename = 'sanitized'
    actual_filename = stats.generate_dfname_from_filename(filename)
    assert expected_filename == actual_filename


def test_generate_sentiment_counts():
    expected_sentiment_counts = pd.DataFrame.from_dict(
        {
            0: NEUTRAL_COUNTS,
            1: POS_COUNTS,
            -1: NEG_COUNTS,
        }, orient='index')
    expected_sentiment_counts.columns = ['Predicted']
    actual_sentiment_counts = stats.generate_sentiment_counts_dataframe(
        PREDICTIONS_SAMPLE)
    assert expected_sentiment_counts.equals(actual_sentiment_counts)
