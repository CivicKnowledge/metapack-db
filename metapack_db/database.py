# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from .orm import Base
from .document import Document

class Database(object):

    def __init__(self, ref):

        self.ref = ref

        self.engine = create_engine(ref)

        self.Session = sessionmaker(bind=self.engine)

    def session(self,**kwargs):
        return self.Session(**kwargs)


    def create_tables(self):
        Base.metadata.create_all(self.engine)

class MetatabManager(object):
    """Manages Metatab tables in a database"""
    def __init__(self, database):

        self.database = database

        self.database.create_tables

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def init_tables(self):
        pass




