"""Module containing functions that operate on the database. Many of the functions
in this module require memory proportional to the amount of entries in the database.

Tested to work fine with 2 GB of main memory with 5M Posts and 8M Comments.

.. module:: dbops
    :platform: Linux
    :synopsis: Database functions module.
.. moduleauthor:: Simon Larsén <slarse@kth.se>, Li Ling <liling@kth.se>
"""
from itertools import chain
from typing import Sequence

import numpy as np

from analyzer import LOGGER, stats
from analyzer.database import Base, Comment, Post, PostType, Session
from sqlalchemy import not_, or_
from sqlalchemy.orm import aliased

# languages used in our study
LANGUAGES = frozenset(('javascript', 'c', 'c++', 'sql', 'python', 'php',
                       'java', 'c#'))

TAG_FILTER = lambda tag: f"%<{tag}>%"

EXTRACT_FIRSTS_FROM_QUERY = lambda query: [] if query.first() is None else list(zip(*query))[0]


def get_elems_from_database(ids: Sequence[int], model: Base):
    """Return a generator that yields records of the specified model with the
    specified ids.

    NOTE: Does not check if all ids exist, will only return those that do!
    """
    yield from Session().query(model).filter(model.id.in_(ids))


def get_random_elems_from_database(model,
                                   num_elems: int = None,
                                   post_type: PostType = None,
                                   tag: str = None):
    """Get num_elems of random posts/questions/answers/comments from the
    database.

    model: Post or Comment
    num_elems: amount of elements to get from the database.
               If not give, num_elems will be calculated using
               alpha_level = 0.05 (confidence level 95%)
               and margin of error = 0.01.
    post_type: taken into consideration when model is Post,
               0 = all posts, PostType.QUESTION = questions only, PostType.ANSWER = answers only"""
    if tag is not None:
        LOGGER.info(f"Querying {model.__name__} by tag {tag}")
        query = query_ids_by_tag(tag, model, post_type)
    else:
        LOGGER.info(f"Querying {model.__name__}")
        query = query_ids_by_model(model, post_type)

    population = query.count()
    if not population:
        raise RuntimeError("Query returned no results."
                           f"model: {model}\n"
                           f"num_elems: {num_elems}\n"
                           f"post_type: {PostType}\n"
                           f"tag:  {tag}")

    LOGGER.info(f"Found {population} matches.")

    if num_elems is None:
        num_elems = stats.calculate_sample_size(population)
        LOGGER.info(
            f"No sample size provided. Sample size calculated to {num_elems}.")

    ids = EXTRACT_FIRSTS_FROM_QUERY(query)
    selected_ids = [
        int(val)
        for val in np.random.choice(ids, size=num_elems, replace=False)
    ]
    return get_elems_from_database(selected_ids, model)


def query_ids_by_model(model, post_type=None):
    """Query only by model and post type.
    
    Post type is only applicable if model is Post, otherwise ignored.
    """
    session = Session()
    query = session.query(model.id)

    if model == Post:
        if post_type is not None:
            LOGGER.info(f"Filtering by {post_type.name}")
            query = query.filter_by(post_type_id=post_type.value)
    return query


def query_ids_by_tag(tag: str, model: Base, post_type: PostType = None):
    """Return a query that fetches models only if they are associated with the
    specified tag.
    """
    if model == Comment:
        query = _comment_ids_by_tag(tag)
    elif model == Post and post_type == PostType.ANSWER:
        query = _answer_ids_by_tag(tag)
    else:
        query = _filter_question_by_non_overlapping_tags(
            Session().query(model.id), model, tag)
    return query


def _filter_question_by_non_overlapping_tags(query,
                                             filter_model,
                                             tag,
                                             considered_tags=LANGUAGES):
    if tag not in considered_tags:
        raise ValueError(
            f"tag '{tag}' not part of considered tags: {str(considered_tags)}")

    query = query.filter(filter_model.tags.like(TAG_FILTER(tag)))
    for tag_ in considered_tags - {tag}:
        query = query.filter(not_(filter_model.tags.like(TAG_FILTER(tag_))))
    return query


def _comment_ids_by_tag(tag):
    """Return a query that fetches comment ids filtered by the associated post
    having the specified tag.
    """
    session = Session()
    questions = _filter_question_by_non_overlapping_tags(
        session.query(Post.id), Post, tag)
    answers = session.query(Post.id).filter(Post.parent_id.in_(questions))
    return session.query(Comment.id).filter(
        or_(Comment.post_id.in_(questions), Comment.post_id.in_(answers)))


def _answer_ids_by_tag(tag):
    """Return a query that fetches post ids filtered by the parent post having
    the specified tag.
    """
    session = Session()
    question = aliased(Post)
    answer = aliased(Post)
    query = session.query(answer.id).join(question, answer.question)
    return _filter_question_by_non_overlapping_tags(query, question, tag)
