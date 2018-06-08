import os
import re
import tempfile

import analyzer
import pytest
from analyzer import cli, database

QUESTIONS_XML = "path/to/questions.xml"
ANSWERS_XML = "path/to/answers.xml"
COMMENTS_XML = "path/to/comments.xml"
OUTPATH = "path/to/results.csv"
NUM = "200"
TAG = "the best tag"
TEST_POPULATION_JSON = os.path.abspath(
    os.path.join(os.path.dirname(__file__), 'test_population.json'))


def test_parse_args_exits_on_empty_args():
    with pytest.raises(SystemExit):
        cli.parse_args([])


def test_parse_args_exits_when_no_subparser_is_given():
    with pytest.raises(SystemExit):
        cli.parse_args(['-d', 'TEST'])


def test_fill_subparser_exits_when_no_document_type_is_given():
    with pytest.raises(SystemExit):
        cli.parse_args([cli.FILL])


def test_fill_subparser_questions():
    questions_args = cli.parse_args([cli.FILL, '-q', QUESTIONS_XML])
    assert questions_args.questions_xml == QUESTIONS_XML and not (
        questions_args.answers_xml or questions_args.comments_xml)


def test_fill_subparser_answers():
    answers_args = cli.parse_args([cli.FILL, '-a', ANSWERS_XML])
    assert answers_args.answers_xml == ANSWERS_XML and not (
        answers_args.questions_xml or answers_args.comments_xml)


def test_fill_subparser_comments():
    comments_args = cli.parse_args([cli.FILL, '-c', COMMENTS_XML])
    assert comments_args.comments_xml == COMMENTS_XML and not (
        comments_args.questions_xml or comments_args.answers_xml)


def test_fill_handler(mocker):
    args = cli.parse_args(
        [cli.FILL, '-q', QUESTIONS_XML, '-a', ANSWERS_XML, '-c', COMMENTS_XML])
    mocker.patch('analyzer.migrate_data.fill_database', autospec=True)
    cli.handle_parsed_args(args)
    analyzer.migrate_data.fill_database.assert_called_once_with(
        QUESTIONS_XML, ANSWERS_XML, COMMENTS_XML, None)


def test_csv_subparser_exits_when_no_outpath_is_given():
    with pytest.raises(SystemExit):
        cli.parse_args([cli.GENERATE_CSV, '-a'])


def test_csv_subparser_exits_when_no_doc_type_is_given():
    with pytest.raises(SystemExit):
        cli.parse_args([cli.GENERATE_CSV, '-o', 'path/to/out'])


def test_csv_subparser_exits_when_multiple_doc_types_are_given():
    with pytest.raises(SystemExit):
        cli.parse_args([cli.GENERATE_CSV, '-o', 'path/to/out', '-a', '-c'])


def test_csv_subparser_for_questions():
    args = cli.parse_args([cli.GENERATE_CSV, '-o', OUTPATH, '-q'])
    assert args.questions and args.outpath == OUTPATH


def test_csv_subparser_for_answers():
    args = cli.parse_args([cli.GENERATE_CSV, '-o', OUTPATH, '-a'])
    assert args.answers and args.outpath == OUTPATH


def test_csv_subparser_for_comments():
    args = cli.parse_args([cli.GENERATE_CSV, '-o', OUTPATH, '-c'])
    assert args.comments and args.outpath == OUTPATH


def test_csv_handler_for_questions(mocker):
    args = cli.parse_args(
        [cli.GENERATE_CSV, '-n', NUM, '-o', OUTPATH, '-q', '-t', TAG])
    mocker.patch('analyzer.util.generate_posts_csv', autospec=True)
    cli.handle_parsed_args(args)
    analyzer.util.generate_posts_csv.assert_called_once_with(
        num_posts=int(NUM),
        outpath=OUTPATH,
        post_type=database.PostType.QUESTION,
        tag=TAG)


def test_csv_handler_for_answers(mocker):
    args = cli.parse_args(
        [cli.GENERATE_CSV, '-n', NUM, '-o', OUTPATH, '-a', '-t', TAG])
    mocker.patch('analyzer.util.generate_posts_csv', autospec=True)
    cli.handle_parsed_args(args)
    analyzer.util.generate_posts_csv.assert_called_once_with(
        num_posts=int(NUM),
        outpath=OUTPATH,
        post_type=database.PostType.ANSWER,
        tag=TAG)


def test_csv_handler_for_comments(mocker):
    args = cli.parse_args(
        [cli.GENERATE_CSV, '-n', NUM, '-o', OUTPATH, '-c', '-t', TAG])
    mocker.patch('analyzer.util.generate_comments_csv', autospec=True)
    cli.handle_parsed_args(args)
    analyzer.util.generate_comments_csv.assert_called_once_with(
        int(NUM), OUTPATH, TAG)


def test_analyze_handler_exits_when_infile_does_not_exist():
    with tempfile.NamedTemporaryFile() as f:
        inpath = f.name
    args = cli.parse_args(
        [cli.ANALYZE, '-i', inpath, '-o', OUTPATH, '-s', 'root'])
    with pytest.raises(SystemExit):
        cli.handle_parsed_args(args)


def test_analyze_handler_exits_when_rpf_too_small():
    rpf = "0"
    with tempfile.NamedTemporaryFile() as f:
        inpath = f.name
        args = cli.parse_args(
            [cli.ANALYZE, '-i', inpath, '-o', OUTPATH, '-s', 'root', '-rpf', rpf])
        with pytest.raises(SystemExit):
            cli.handle_parsed_args(args)

def test_analyze_handler(mocker, monkeypatch):
    sentiroot = 'root'
    rpf = "2000"
    with tempfile.NamedTemporaryFile() as f:
        inpath = f.name
        args = cli.parse_args([
            cli.ANALYZE, '-i', inpath, '-o', OUTPATH, '-s', sentiroot, '-rpf',
            rpf
        ])
        mocker.patch('analyzer.classify.classify_sentiment', autospec=True)
        monkeypatch.setattr('os.path.abspath', lambda x: x)
        cli.handle_parsed_args(args)

    analyzer.classify.classify_sentiment.assert_called_once_with(
        int(rpf), os.path.abspath(sentiroot), inpath, OUTPATH)


def test_teardown_handler(mocker):
    args = cli.parse_args([cli.TEARDOWN])
    mocker.patch('analyzer.database.teardown_database', autospec=True)
    cli.handle_parsed_args(args)
    analyzer.database.teardown_database.assert_called_once_with(
        analyzer.database.Driver.DEV)


def test_plot_handler_no_population_given(mocker):
    with tempfile.NamedTemporaryFile() as f:
        inpath = f.name
        alpha = .5
        width = 100
        args = cli.parse_args([
            cli.PLOT, '-i', inpath, '-i', inpath, '-o', OUTPATH, '-a',
            str(alpha), '-w',
            str(width)
        ])
        mocker.patch('analyzer.stats.plot_predictions', autospec=True)
        cli.handle_parsed_args(args)
    analyzer.stats.plot_predictions.assert_called_once_with(
        [inpath, inpath], alpha, OUTPATH, width, False, False, None)


def test_plot_handler_no_population_given():
    inpath = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), 'sanitized_comments_predictions.csv'))
    alpha = .5
    width = 100
    out = 'plot'

    args = cli.parse_args([
        cli.PLOT,
        '-i',
        inpath,
        '-o',
        out,
        '-a',
        str(alpha),
        '-w',
        str(width),
    ])
    cli.handle_parsed_args(args)
    assert os.path.isfile(f'{out}.svg')


def test_plot_handler_population_file_dont_exist():
    with tempfile.NamedTemporaryFile() as f:
        inpath = f.name
        alpha = .5
        width = 100
        population = 'empty.json'

        args = cli.parse_args([
            cli.PLOT, '-i', inpath, '-i', inpath, '-o', OUTPATH, '-a',
            str(alpha), '-w',
            str(width), '-pop', population
        ])
        with pytest.raises(SystemExit):
            cli.handle_parsed_args(args)


def test_plot_handler_population_file_key_error():
    with tempfile.NamedTemporaryFile() as f:
        inpath = f.name
        alpha = .5
        width = 100

        args = cli.parse_args([
            cli.PLOT, '-i', inpath, '-i', inpath, '-o', OUTPATH, '-a',
            str(alpha), '-w',
            str(width), '-pop', TEST_POPULATION_JSON
        ])
        with pytest.raises(KeyError):
            cli.handle_parsed_args(args)


def test_plot_handler_correct_population_file():
    inpath = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), 'sanitized_comments_predictions.csv'))
    alpha = .5
    width = 100
    out = 'plot'

    args = cli.parse_args([
        cli.PLOT, '-i', inpath, '-o', out, '-a',
        str(alpha), '-w',
        str(width), '-pop', TEST_POPULATION_JSON
    ])
    cli.handle_parsed_args(args)
    assert os.path.isfile(f'{out}.svg')
