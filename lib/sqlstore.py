"""
This module consolidates Database access for the lkb project.
"""

import logging
import os
import sqlite3
from sqlalchemy import Column, Integer, Text, create_engine, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


Base = declarative_base()


class Request(Base):
    """
    Table containing request information
    """
    __tablename__ = "requests"
    id = Column(Integer, primary_key=True, autoincrement=True)
    hostip = Column(Text)
    version = Column(Text)
    url = Column(Text)
    server = Column(Text)
    referer = Column(Text)
    port = Column(Integer)
    status = Column(Integer)
    bytes = Column(Integer)
    timestamp = Column(Text)
    uagent_id = Column(Integer, ForeignKey('useragents.id'))
    uagent = relationship("UserAgent", foreign_keys=[uagent_id])


class UserAgent(Base):
    """
    Table containing User agents.
    """
    __tablename__ = "useragents"
    id = Column(Integer, primary_key=True, autoincrement=True)
    desc = Column(Text, unique=True)
    browser_family = Column(Text)
    browser_version = Column(Text)
    os_family = Column(Text)
    os_version = Column(Text)
    mobile = Column(Integer)


class FileHash(Base):
    """
    Table containing the file hashes.This is used to check if there is an update on the logfile.
    """
    __tablename__ = "filehash"
    file_id = Column(Text, primary_key=True)
    fh = Column(Text, nullable=False)
    created = Column(Integer, nullable=False)
    modified = Column(Integer, nullable=False)

    def __repr__(self):
        return "<File: {fid} - hash: {h}>".format(fid=self.file_id, h=self.fh)


class DirectConn:
    """
    This class will set up a direct connection to the database. It allows to reset the database,
    in which case the database will be dropped and recreated, including all tables.
    """

    def __init__(self, config):
        """
        To drop a database in sqlite3, you need to delete the file.
        """
        self.db = config['Main']['db']
        self.dbConn = ""
        self.cur = ""

    def connect2db(self):
        """
        Internal method to create a database connection and a cursor. This method is called during object
        initialization.
        Note that sqlite connection object does not test the Database connection. If database does not exist, this
        method will not fail. This is expected behaviour, since it will be called to create databases as well.
        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Datastore object and cursor")
        self.dbConn = sqlite3.connect(self.db)
        self.dbConn.row_factory = sqlite3.Row
        self.cur = self.dbConn.cursor()
        logging.debug("Datastore object and cursor are created")
        return

    def rebuild(self):
        # A drop for sqlite is a remove of the file
        db_name = os.path.basename(self.db)
        try:
            os.remove(self.db)
            logging.info("Database {db} will be recreated".format(db=db_name))
        except FileNotFoundError:
            logging.info("New database {db} will be created".format(db=db_name))
        # Reconnect to the Database
        self.connect2db()
        # Use SQLAlchemy connection to build the database
        conn_string = "sqlite:///{db}".format(db=self.db)
        engine = set_engine(conn_string=conn_string)
        Base.metadata.create_all(engine)


def init_session(db, echo=False):
    """
    This function configures the connection to the database and returns the session object.
    :param db: Name of the sqlite3 database.
    :param echo: True / False, depending if echo is required. Default: False
    :return: session object.
    """
    conn_string = "sqlite:///{db}".format(db=db)
    engine = set_engine(conn_string, echo)
    session = set_session4engine(engine)
    return session


def set_engine(conn_string, echo=False):
    engine = create_engine(conn_string, echo=echo)
    return engine


def set_session4engine(engine):
    session_class = sessionmaker(bind=engine)
    session = session_class()
    return session
