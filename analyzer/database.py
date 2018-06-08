"""Database utils"""
import enum
import os
from typing import Iterable
from sqlalchemy import (create_engine, Column, Integer, String, DateTime,
                        ForeignKey, Text)
from sqlalchemy.orm import relationship, sessionmaker, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import exc
from analyzer import LOGGER

Base = declarative_base()
# run setup_session before attempting to use the Session
Session = sessionmaker()
_session = None


class PostType(enum.Enum):
    QUESTION = 1
    ANSWER = 2


class Driver(enum.Enum):
    DEV = f'sqlite:///{os.path.abspath(os.path.dirname(__file__))}/dev.sqlite'
    TEST = f'sqlite:///{os.path.abspath(os.path.dirname(__file__))}/test.sqlite'
    EXPERIMENT = os.getenv('DATABASE_URI')
    INTEGRATION = os.getenv('INTEGRATION_TEST_DB_URI')


def setup_session(driver=Driver.DEV):
    """Creates an engine with the provided driver and binds it to the Session
    object.
    """
    global _session
    engine = create_engine(driver.value)
    Session.configure(bind=engine)
    _session = Session(autoflush=False)


def setup_database(driver=Driver.DEV):
    """Instantiates the database for the provided driver. Should only be run
    once for each driver.
    """
    engine = create_engine(driver.value)
    Base.metadata.create_all(engine)


def teardown_database(driver=Driver.DEV):
    """Drop all tables from the database for the provided driver."""
    engine = create_engine(driver.value)
    Base.metadata.drop_all(engine)


def batch_commit(models: Iterable[Base]):
    """Commit all of the models in a single commit.

    Return True if success, otherwise rollback and return false.
    """
    for model in models:
        _session.add(model)
    return _try_commit_and_flush()


def commit_all_separately(models: Iterable[Base]):
    """Commit all of the models, one at a time, ignoring any models that cause
    errors.
    
    Return a count of how many were committed.
    """
    count = 0
    for model in models:
        _session.add(model)
        count = count + 1 if _try_commit_and_flush() else count
    return count


def _try_commit_and_flush():
    """Try to commit and flush the private session.

    Return True if success.
    """
    try:
        _session.commit()
        _session.flush()
    except Exception as e:
        LOGGER.error(
            f"Unexpected exception:\n{type(e).__name__}: {str(e)}\nRolling back"
        )
        _session.rollback()
        return False
    return True


class Post(Base):
    __tablename__ = 'post'
    id = Column(Integer, primary_key=True)
    title = Column(String(250))
    text = Column(Text, nullable=False)
    post_type_id = Column(Integer, nullable=False)
    creation_date = Column(DateTime, nullable=False)
    tags = Column(String(250))
    parent_id = Column(Integer, ForeignKey('post.id'))
    answers = relationship(
        'Post', backref=backref('question', remote_side=[id]))
    comments = relationship('Comment', backref='post')


class Comment(Base):
    __tablename__ = 'comment'
    id = Column(Integer, primary_key=True)
    creation_date = Column(DateTime, nullable=False)
    text = Column(Text, nullable=False)
    post_id = Column(Integer, ForeignKey('post.id'), nullable=False)
