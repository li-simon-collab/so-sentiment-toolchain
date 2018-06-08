import os
from collections import defaultdict
from itertools import zip_longest

import numpy

import pytest
from analyzer import database, dbops, migrate_data, util
from analyzer.database import Comment, Post, PostType
from analyzer.dbops import EXTRACT_FIRSTS_FROM_QUERY

TEST_COMMENTS_XML = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "test_comments.xml"))
TEST_POSTS_XML = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "test_posts.xml"))

CSHARP_QUESTION_IDS = {4, 9, 11, 16}
CSHARP_ANSWER_IDS = {7, 12, 21, 22, 27}
CSHARP_COMMENT_IDS = {
    1, 4, 19, 30, 82190368, 82190372, 82190373, 18, 24, 82190381, 16, 82190367,
    2, 82190378, 82190385, 82190386, 82190387, 26, 27, 82190383, 25, 12, 14,
    15, 82190382
}
JAVASCRIPT_QUESTION_IDS = {13}
JAVASCRIPT_ANSWER_IDS = {29}
JAVASCRIPT_COMMENT_IDS = {9, 82190384, 10, 20, 22, 82190371}


def setup_function(function):
    database.setup_database(database.Driver.TEST)
    database.setup_session(database.Driver.TEST)
    migrate_data.fill_database(
        questions_xml=TEST_POSTS_XML,
        answers_xml=TEST_POSTS_XML,
        comments_xml=TEST_COMMENTS_XML)


def teardown_function(function):
    database.teardown_database(database.Driver.TEST)


def test_query_questions_ids_by_tag():
    csharp_query = dbops.query_ids_by_tag('c#', Post, PostType.QUESTION)
    javascript_query = dbops.query_ids_by_tag('javascript', Post,
                                              PostType.QUESTION)
    csharp_question_ids = set(EXTRACT_FIRSTS_FROM_QUERY(csharp_query))
    javascript_question_ids = set(EXTRACT_FIRSTS_FROM_QUERY(javascript_query))
    assert csharp_question_ids == CSHARP_QUESTION_IDS
    assert javascript_question_ids == JAVASCRIPT_QUESTION_IDS


def test_query_answers_ids_by_tag():
    csharp_query = dbops.query_ids_by_tag('c#', Post, PostType.ANSWER)
    javascript_query = dbops.query_ids_by_tag('javascript', Post,
                                              PostType.ANSWER)
    csharp_answer_ids = set(EXTRACT_FIRSTS_FROM_QUERY(csharp_query))
    javascript_answer_ids = set(EXTRACT_FIRSTS_FROM_QUERY(javascript_query))
    assert csharp_answer_ids == CSHARP_ANSWER_IDS
    assert javascript_answer_ids == JAVASCRIPT_ANSWER_IDS


def test_query_comment_ids_by_tag():
    csharp_query = dbops.query_ids_by_tag('c#', Comment)
    javascript_query = dbops.query_ids_by_tag('javascript', Comment)
    csharp_comment_ids = set(EXTRACT_FIRSTS_FROM_QUERY(csharp_query))
    javascript_comment_ids = set(EXTRACT_FIRSTS_FROM_QUERY(javascript_query))
    assert csharp_comment_ids == CSHARP_COMMENT_IDS
    assert javascript_comment_ids == JAVASCRIPT_COMMENT_IDS


def test_get_comments_from_database():
    expected_ids = [1, 2, 4, 82190370, 82190374, 82190387]
    actual_ids = sorted([
        comment.id
        for comment in dbops.get_elems_from_database(expected_ids, Comment)
    ])
    assert actual_ids == expected_ids


def test_get_posts_from_database():
    expected_ids = [4, 6, 7, 18, 27, 29]
    actual_ids = sorted([
        post.id for post in dbops.get_elems_from_database(expected_ids, Post)
    ])
    assert actual_ids == expected_ids


def test_get_random_questions_from_database(monkeypatch):
    num_elems = 5
    expected_question_ids = [4, 6, 19, 24, 25]
    monkeypatch.setattr("numpy.random.choice",
                        lambda *args, **kwargs: expected_question_ids)
    questions = dbops.get_random_elems_from_database(
        Post, num_elems, post_type=PostType.QUESTION)
    for item, expected_id in zip_longest(questions, expected_question_ids):
        assert item.post_type_id == PostType.QUESTION.value
        assert item.id == expected_id


def test_get_random_answers_from_database(monkeypatch):
    num_elems = 5
    expected_answer_ids = [7, 12, 18, 27, 29]
    monkeypatch.setattr("numpy.random.choice",
                        lambda *args, **kwargs: expected_answer_ids)
    answers = dbops.get_random_elems_from_database(
        Post, num_elems, post_type=PostType.ANSWER)
    for item, expected_id in zip_longest(answers, expected_answer_ids):
        assert item.post_type_id == PostType.ANSWER.value
        assert item.id == expected_id


def test_get_random_comments_from_database(monkeypatch):
    num_elems = 5
    expected_comment_ids = [1, 2, 4, 82190386, 82190387]
    monkeypatch.setattr("numpy.random.choice",
                        lambda *args, **kwargs: expected_comment_ids)
    comments = dbops.get_random_elems_from_database(Comment, num_elems)
    for item, expected_id in zip_longest(comments, expected_comment_ids):
        assert item.id == expected_id


def test_get_random_elems_from_database_none_num_elems():
    expected_ids = [
        4, 6, 7, 9, 11, 12, 13, 14, 16, 17, 18, 19, 21, 22, 24, 25, 26, 27, 29,
        57, 59, 60, 63
    ]
    posts = dbops.get_random_elems_from_database(Post)
    for item, expected_id in zip_longest(posts, expected_ids):
        assert item.id == expected_id


def test_get_random_elems_from_database_raises_on_empty_query(monkeypatch):
    """Test that queries that return no results cause a RuntimeError."""
    tag = "java"  # part of considered tags, but not in test set
    with pytest.raises(RuntimeError):
        dbops.get_random_elems_from_database(Post, tag=tag)
