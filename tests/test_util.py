import os
import re
import tempfile

import pytest
from analyzer import util

comment = (
    '@S.Jovan The expected result should look sth. like this:\n[\n{ ""key1"": str10, ""key2"": str20, ""key3"": str30 },\n{ ""key1"": str11, ""key2"": str21, ""key3"": str31 },\n{ ""key1"": str12, ""key2"": str22, ""key3"": str32 },\n...'
)

PREDICTIONS_SAMPLE = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), 'sanitized_comments_predictions.csv'))
NEG_COUNTS = 21
POS_COUNTS = 21
NEUTRAL_COUNTS = 166


def setup_function(function):
    global post_base_text, post_expected_text, code_segment, pre_segment, blockquote_segment
    post_base_text = "Hi, I have a problem. Here is my code:{}{}{}Can anyone help me?"
    code_segment = "<code> for i in range(10):\n    print(10)\n#wupwup!</code>"
    pre_segment = "<pre> for val in elems:\n\n\n    #do something\nprint(val)</pre>"
    blockquote_segment = r"<blockquote>Gzipped data: \x1f\x8b\x08\x00\xf9w[Y\x02\xff%\x8e=\x0e\xc30\x08F\xaf\x82\x98\x91\x05\xe6\xc7\xa6c\xf7\x9e\xa0\xca\x96\xa5[\x86lQ\xee^\xdcN\xf0\xf4\xc1\x83\x0b?\xf8\x00|=\xe7D\x02<\n\xde\x17\xee\xab\xb85%\x82L\x02\xcb\xa6N\xa0\x7fri\xae\xd5K\xe1$\xe83\xc3\x08\x86Z\x81\xa9g-y\x88\xf6\x9a\xf5E\xde\x99\x7f\x96\xb1\xd5\x99\xb3\xfcb\x99\x121D\x1bG\xe7^.\xdcWPO\xdc\xdb\xfd\x05\x0ev\x15\x1d\x99\x00\x00\x00</blockquote>"


def test_sanitize_post_md_code_pattern_is_not_greedy():
    """Test that the markdown code pattern does not remove too much."""
    post = ("`this is code` but a greedy```other code``` pattern\nwould remove"
            "`this whole post`"
            "```along with``` this as well```hehe```")
    expected = "but a greedy pattern would remove this as well"
    sanitized = util.sanitize_post(post)
    assert sanitized == expected


def test_sanitize_post_replaces_all_whitespace_with_single_spaces():
    sanitized = util.sanitize_post(
        post_base_text.format(code_segment, pre_segment, blockquote_segment))
    counter = 0
    for ws in re.findall('\s+', sanitized):
        counter += 1
        assert ws == ' '
    assert counter  # meta assert


def test_sanitize_post_removes_url():
    https_url = "https://hello.world#aweseaf45we23.com"
    http_url = "http://blabla.com#badonk"

    c = "{} and other stuff {} awesome donk {}\n\nhurrdurr".format(
        comment, https_url, http_url)
    sanitized = util.sanitize_post(c)

    assert https_url not in sanitized
    assert http_url not in sanitized


def test_sanitize_post_removes_single_backtick_code():
    markdown_code = '`for i in range(10):\n    print(i)`'
    c = "{} blablabla bla 234 d23r23 {}\nAnd just the finishing touch.".format(
        comment, markdown_code)
    sanitized = util.sanitize_post(c)

    assert markdown_code not in sanitized
    assert '`' not in sanitized
    # and some subpatterns
    assert 'for i in range' not in sanitized
    assert 'range(10)' not in sanitized


def test_sanitize_post_removes_triple_backtick_code():
    markdown_code = '```for i in range(10):\n    print(i)```'
    c = "{} blablabla bla 234 d23r23 {}\nAnd just the finishing touch.".format(
        comment, markdown_code)
    sanitized = util.sanitize_post(c)

    assert markdown_code not in sanitized
    assert '`' not in sanitized
    # and some subpatterns
    assert 'for i in range' not in sanitized
    assert 'range(10)' not in sanitized


def test_sanitize_post_removes_blockquote_segments():
    text = post_base_text.format(blockquote_segment, "\n", "")
    expected_text = post_base_text.format("", " ", "")
    sanitized = util.sanitize_post(text)
    assert sanitized == expected_text


def test_sanitize_post_removes_linefeeds():
    text = "This is a text with \r\n some \u2028 nbbbb \u2029 random \n linefeeds \r and carriege returns \r\n hello \n"
    sanitized = util.sanitize_post(text)
    assert '\n' not in sanitized
    assert '\r' not in sanitized
    assert '\u2028' not in sanitized
    assert '\u2029' not in sanitized


def test_sanitize_post_removes_code_segments():
    text = post_base_text.format("\n", code_segment, "\n")
    # the two newlines are replaced with single space
    expected_text = post_base_text.format(" ", "", "")
    res = util.sanitize_post(text)
    assert res == expected_text


def test_sanitize_post_removes_pre_segments():
    text = post_base_text.format("\n", pre_segment, "\n")
    # the two newlines are replaced with single space
    expected_text = post_base_text.format(" ", "", "")
    res = util.sanitize_post(text)
    assert res == expected_text


def test_sanitize_post_removes_code_pre_and_tags():
    text = post_base_text.format("</a href=https://url.com>", code_segment,
                                 pre_segment)
    expected_text = post_base_text.format("", "", "")
    res = util.sanitize_post(text)
    assert res == expected_text


@pytest.mark.timeout(0.2)
def test_sanitize_post_handles_tag_case_mismatch():
    """Previous version of sanitize post froze due to case mismatch in tags.
    In this particular case, it was the <pre> ... </prE> that cause exponential
    backtracking (we think) to kick in.
    """
    text =\
'''<p><em>"I didn't like this because I have only two C files and it seemed very odd to split the source base at the language level like this"</em></p>

<p>Why does it seem odd? Consider this project:</p>

<pre>
  project1\src\java
  project1\src\cpp
  project1\src\python
</pre>

<p>Or, if you decide to split things up into modules:</p>

<p><pre>
  project1\module1\src\java
  project1\module1\src\cpp
  project1\module2\src\java
  project1\module2\src\python
</prE></p>

<p>I guess it's a matter of personal taste, but the above structure is fairly common, and I think it works quite well once you get used to it.</p>'''
    util.sanitize_post(text)


def test_sanitize_comment_replaces_all_whitespace_with_single_spaces():
    sanitized = util.sanitize_comment(comment)
    counter = 0
    for ws in re.findall('\s+', sanitized):
        counter += 1
        assert ws == ' '
    assert counter  # meta assert


def test_sanitize_comment_removes_url():
    https_url = "https://hello.world#aweseaf45we23.com"
    http_url = "http://blabla.com#badonk"

    c = "{} and other stuff {} awesome donk {}\n\nhurrdurr".format(
        comment, https_url, http_url)
    sanitized = util.sanitize_comment(c)

    assert https_url not in sanitized
    assert http_url not in sanitized


def test_sanitize_comment_leaves_user_mentions():
    sanitized = util.sanitize_comment(comment)
    assert '@S.Jovan' in sanitized


def test_sanitize_comment_strips_leading_and_trailing_ws():
    text = "   there is leading whitespace here <code>some\ncode</code>  "
    sanitized = util.sanitize_comment(text)
    assert sanitized == sanitized.strip()


def test_sanitize_comment_removes_single_backtick_code():
    markdown_code = '`for i in range(10):\n    print(i)`'
    c = "{} blablabla bla 234 d23r23 {}\nAnd just the finishing touch.".format(
        comment, markdown_code)
    sanitized = util.sanitize_comment(c)

    assert markdown_code not in sanitized
    assert '`' not in sanitized
    # and some subpatterns
    assert 'for i in range' not in sanitized
    assert 'range(10)' not in sanitized


def test_sanitize_comment_removes_triple_backtick_code():
    markdown_code = '```for i in range(10):\n    print(i)```'
    c = "{} blablabla bla 234 d23r23 {}\nAnd just the finishing touch.".format(
        comment, markdown_code)
    sanitized = util.sanitize_comment(c)

    assert markdown_code not in sanitized
    assert '`' not in sanitized
    # and some subpatterns
    assert 'for i in range' not in sanitized
    assert 'range(10)' not in sanitized


def test_sanitize_comment_removes_markdown_formatting():
    random_md = "This is ```for i in range(t)``` just a **test** to see that _some_ `inline code` and **other\nmarkdown** stuff is removed."
    sanitized_md = "This is just a test to see that some and other markdown stuff is removed."
    text = post_base_text.format("", random_md, "")
    expected = post_base_text.format("", sanitized_md, "")

    sanitized = util.sanitize_comment(text)

    assert sanitized == expected


def test_sanitize_real_post():
    """Test sanitizing a real post (answer) from SO, authored by Simon Larsén."""
    text =\
    """<p>You can do this in just two lines.</p>

    <pre><code>with open('path/to/file') as f:
        line_lists = [list(line.strip()) for line in f]
    </code></pre>

    <p><code>list</code> on a <code>str</code> object will return a list where each character is an element (as a <code>char</code>). <code>line</code> is stripped first, which removes leading and trailing whitespace. This is assuming that you actually want the characters as <code>char</code>. If you want them parsed to <code>int</code>, this will work:</p>

    <pre><code>with open('path/to/file') as f:
        line_lists = [[int(x) for x in line.strip()] for line in f]
    </code></pre>

    <p>Mind you that there should be some error checking here, the above example will crash if any of the characters cannot be parsed to int.</p>
    """
    expected = "You can do this in just two lines. on a object will return a list where each character is an element (as a ). is stripped first, which removes leading and trailing whitespace. This is assuming that you actually want the characters as . If you want them parsed to , this will work: Mind you that there should be some error checking here, the above example will crash if any of the characters cannot be parsed to int."
    sanitized = util.sanitize_post(text)
    assert sanitized == expected


def test_yield_batches():
    expected = [[0, 1, 2], [3, 4, 5], [6, 7, 8]]
    it = (i for i in range(9))

    actual = [batch for batch in util.yield_batches(it, 3)]

    assert actual == expected
