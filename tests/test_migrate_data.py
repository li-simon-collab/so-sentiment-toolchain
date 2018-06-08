import os
from xml.etree import ElementTree
from analyzer import migrate_data, database, util

TEST_COMMENTS_XML = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "test_comments.xml"))
TEST_COMMENTS_MISMATCHING_POST_IDS = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "test_comments_mismatch.xml"))
TEST_POSTS_XML = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "test_posts.xml"))
TEST_POSTS_MISMATCHING_PARENT_IDS = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "test_posts_mismatch.xml"))

# fixture
post_base_text = None
post_expected_text = None
code_segment = None
pre_segment = None

# helpers
get_row_ids = lambda filepath: [int(elem.attrib['Id'])
                                for _, elem in ElementTree.iterparse(filepath)
                                if elem.tag == 'row']


def setup_function(function):
    global post_base_text, post_expected_text, code_segment, pre_segment
    post_base_text = "Hi, I have a problem. Here is my code:{}{}{}Can anyone help me?"
    code_segment = "<code> for i in range(10):\n    print(10)\n#wupwup!</code>"
    pre_segment = "< pre> for val in elems:\n\n\n    #do something\nprint(val)</pre>"
    database.setup_database(database.Driver.TEST)
    database.setup_session(database.Driver.TEST)


def teardown_function(function):
    database.teardown_database(database.Driver.TEST)


def test_fill_database():
    migrate_data.fill_database(
        questions_xml=TEST_POSTS_XML,
        answers_xml=TEST_POSTS_XML,
        comments_xml=TEST_COMMENTS_XML)

    session = database.Session()
    # check comments
    # chec posts
    for _, elem in ElementTree.iterparse(TEST_POSTS_XML):
        if elem.tag == 'row':
            actual_elem = session.query(database.Post).get(
                int(elem.attrib['Id']))
            assert actual_elem.text == util.sanitize_post(elem.attrib['Body'])
            assert actual_elem.post_type_id == int(elem.attrib['PostTypeId'])
            if actual_elem.post_type_id == database.PostType.ANSWER.value:
                assert actual_elem.parent_id == int(elem.attrib['ParentId'])

    for _, elem in ElementTree.iterparse(TEST_COMMENTS_XML):
        if elem.tag == 'row':
            actual_elem = session.query(database.Comment).get(
                int(elem.attrib['Id']))
            assert actual_elem.text == util.sanitize_comment(
                elem.attrib['Text'])
            assert actual_elem.post_id == int(elem.attrib['PostId'])


def test_fill_database_skips_comments_with_mismatching_parent_ids():
    """Test that comments that refer to posts that are not in the database are
    skipped.
    """
    migrate_data.fill_database(
        questions_xml=TEST_POSTS_XML,
        answers_xml=TEST_POSTS_XML,
        comments_xml=TEST_COMMENTS_MISMATCHING_POST_IDS)

    expected_ids = [1, 2, 4, 9, 10, 12, 14, 15, 16]
    actual_ids = [
        e[0] for e in database.Session().query(database.Comment.id).order_by(
            database.Comment.id)
    ]
    mismatched_ids = set(
        get_row_ids(TEST_COMMENTS_MISMATCHING_POST_IDS)) - set(expected_ids)
    # meta assert to make sure that we are actually testing something
    assert mismatched_ids
    # the actual test assert
    assert actual_ids == expected_ids


def test_fill_database_skips_answers_with_mismatching_post_ids():
    """Test that answers that refer to questions that are not in the database
    are skipped.
    """
    migrate_data.fill_database(
        questions_xml=TEST_POSTS_MISMATCHING_PARENT_IDS,
        answers_xml=TEST_POSTS_MISMATCHING_PARENT_IDS)
    expected_ids = [4, 7, 11, 27]
    actual_ids = [
        e[0] for e in database.Session().query(database.Post.id).order_by(
            database.Post.id)
    ]
    mismatched_ids = set(
        get_row_ids(TEST_POSTS_MISMATCHING_PARENT_IDS)) - set(expected_ids)
    # meta assert to make sure that we are actually testing something
    assert mismatched_ids
    # the actual assert of the test
    assert actual_ids == expected_ids
