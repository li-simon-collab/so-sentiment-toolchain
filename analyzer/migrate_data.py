"""Module for migrating data from the .xml files in the Stack Overflow data
dump to a SQL database.

.. module:: migrate_data
    :platform: Linux
    :synopsis: Data migration functions.
.. moduleauthor:: Simon Larsén <slarse@kth.se>, Li Ling <liling@kth.se>
"""

from typing import Iterable, Callable, Set
from xml.etree import ElementTree
from functools import partial
from maya import MayaDT
from analyzer import LOGGER
from analyzer.database import Base, commit_all_separately, batch_commit, Post, PostType, Comment
from analyzer.dbops import query_ids_by_model, EXTRACT_FIRSTS_FROM_QUERY
from analyzer.util import sanitize_post, sanitize_comment, yield_batches

BATCH_SIZE = 1000


def fill_database(questions_xml: str = None,
                  answers_xml: str = None,
                  comments_xml: str = None,
                  creation_date_start: MayaDT = None):
    """Fill the database with posts and coments. Text is sanitized first."""
    if questions_xml is not None:
        _migrate_questions_from_xml_to_db(questions_xml, creation_date_start)

    if answers_xml is not None:
        _migrate_answers_from_xml_to_db(answers_xml, creation_date_start)

    if comments_xml is not None:
        _migrate_comments_from_xml_to_db(comments_xml, creation_date_start)


def _xml_to_database(xml_path: str,
                     model_function: Callable[[ElementTree.Element], Base],
                     creation_date_start,
                     post_ids: Set[int] = None):
    """Parse an xml file and add the data to the database.

    post_ids are only applicable for answers and comments, and are ignored for
    questions. An answer or comment is only added to the database if its
    post_id/parent_id is contained within the post_ids set.
    """
    rows = _get_rows_from_xml(xml_path, creation_date_start)
    count = 0
    for batch in yield_batches(rows, BATCH_SIZE):
        model_batch = [
            e for e in (model_function(elem, post_ids) for elem in batch)
            if e is not None
        ]
        committed = len(model_batch)
        if not batch_commit(model_batch):
            committed = commit_all_separately(model_batch)
        count += committed
        LOGGER.info(f"Added: {count}")


def _get_rows_from_xml(filepath: str, creation_date_start: MayaDT):
    """Parse the comments xml file and yield all row elements after the given creation date."""
    parser = iter(ElementTree.iterparse(filepath, events=['start', 'end']))
    _, root = next(parser)
    month = 0
    for event, elem in parser:
        if event == 'end' and elem.tag == 'row':
            cd = MayaDT.from_rfc3339(elem.attrib['CreationDate'])
            if cd.month != month:
                month = cd.month
            if creation_date_start is None or creation_date_start <= cd:
                yield elem
            root.clear()


def _migrate_questions_from_xml_to_db(questions_xml, creation_date_start):
    LOGGER.info(
        f"Migrating questions from {questions_xml} into the database ...")
    _xml_to_database(questions_xml,
                     partial(
                         _post_xml_row_to_model,
                         target_post_type=PostType.QUESTION),
                     creation_date_start)


def _migrate_answers_from_xml_to_db(answers_xml, creation_date_start):
    LOGGER.info("Retrieving question ids ...")
    question_ids = set(EXTRACT_FIRSTS_FROM_QUERY(
        query_ids_by_model(Post, PostType.QUESTION)))
    LOGGER.info(f"Found {len(question_ids)} question ids")
    LOGGER.info(f"Migrating answers from {answers_xml} into the database ...")
    _xml_to_database(answers_xml,
                     partial(
                         _post_xml_row_to_model,
                         target_post_type=PostType.ANSWER),
                     creation_date_start, question_ids)


def _migrate_comments_from_xml_to_db(comments_xml, creation_date_start):
    LOGGER.info("Retrieving post ids ...")
    post_ids = set(EXTRACT_FIRSTS_FROM_QUERY(query_ids_by_model(Post)))
    LOGGER.info(f"Found {len(post_ids)} post ids")
    LOGGER.info(
        f"Migrating comments from {comments_xml} into the database ...")
    _xml_to_database(comments_xml, _comment_xml_row_to_model,
                     creation_date_start, post_ids)


def _post_xml_row_to_model(elem,
                           question_ids: Set[int] = None,
                           target_post_type: PostType = PostType.QUESTION):
    """Convert an xml row from the Posts.xml file to a model. Text is sanitized
    before conversion.
    
    question_ids is only applicable if the target post type is
    PostType.ANSWER. An answer is only added if its parent_id is
    contained in question_ids.
    """
    try:
        post_type = PostType(int(elem.attrib['PostTypeId']))
    except ValueError:  # was not a question or answer
        return None

    # early returns
    if target_post_type != post_type:
        return None
    if target_post_type == PostType.ANSWER and int(
            elem.attrib['ParentId']) not in question_ids:
        return None
    try:
        sanitized = sanitize_post(elem.attrib['Body'])
    except ValueError:
        LOGGER.error(
            f"Sanitization failed for Post with Id={elem.attrib['Id']}")
        return None

    date = MayaDT.from_rfc3339(elem.attrib['CreationDate']).date
    if post_type == PostType.ANSWER:
        title = None
        tags = None
        parent_id = elem.attrib['ParentId']
    else:  # is question
        title = elem.attrib['Title']
        tags = elem.attrib['Tags']
        parent_id = None
    post = Post(
        id=elem.attrib['Id'],
        creation_date=date,
        post_type_id=post_type.value,
        title=title,
        text=sanitized,
        tags=tags,
        parent_id=parent_id)
    return post


def _comment_xml_row_to_model(elem, post_ids: Set[int]):
    """Convert an xml row from the Comments.xml file to a model. Text is
    sanitized before conversion.
    
    Return None if the post_id is not contained in post_ids.
    """
    post_id = int(elem.attrib['PostId'])
    if post_id not in post_ids:
        return None
    try:
        sanitized = sanitize_comment(elem.attrib['Text'])
    except Exception as e:
        LOGGER.error(
            f"Sanitization failed for Comment with Id={elem.attrib['Id']}\n"
            f"{type(e).__name__}\n{str(e)}")
        return None

    date = MayaDT.from_rfc3339(elem.attrib['CreationDate']).date
    comment = Comment(
        id=elem.attrib['Id'],
        creation_date=date,
        text=sanitized,
        post_id=post_id)
    return comment
