# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

from sqlalchemy.orm import sessionmaker
from sqlalchemy import String, Integer, Float
from sqlalchemy import Table, Column
from sqlalchemy import create_engine, MetaData

from contextlib import contextmanager

from .orm import Base
from .document import Document
from .resource import Resource
from .term import Term


class Database(object):
    def __init__(self, ref):
        self.ref = ref

        self.engine = create_engine(ref)

        self.Session = sessionmaker(bind=self.engine)

    def session(self, **kwargs):
        return self.Session(**kwargs)

    def create_tables(self):
        Base.metadata.create_all(self.engine)


class MetatabManager(object):
    """Manages Metatab tables in a database"""

    def __init__(self, database):

        self.database = database

        self.database.create_tables()

    @contextmanager
    def session(self):
        """Provide a transactional scope around a series of operations."""
        session = self.database.Session()
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

    def add_doc(self, mt_doc):

        with self.session() as s:
            document = Document()
            document.update_from_doc(mt_doc)
            s.add(document)

            for t in mt_doc.all_terms:
                term = Term(
                    parent_term=t.parent_term_lc,
                    record_term=t.record_term_lc,
                    value=t.value,
                    section=t.section.value if t.section is not None else None,
                    term_value_name=t.term_value_name,
                    properties=t.all_props
                )


                print(term)
                s.add(term)


    def delete_doc(self, id=None, identity=None, name=None):
        """
        Delete a document record but id, identity or name
        :param id:
        :param identity:
        :param name:
        :return:
        """
        with self.session() as s:

            if id is not None:
                s.query(Document).filter_by(id=id).delete()
            elif identity is not None:
                s.query(Document).filter_by(identity=identity).delete()
            elif name is not None:
                s.query(Document).filter_by(name=name).delete()

    def list_tables(self):
        from sqlalchemy.engine import reflection

        inspector = reflection.Inspector.from_engine(self.engine)

        for table_name in inspector.get_table_names():
            yield table_name

    def has_table(self, table_name):
        return table_name in list(self.list_tables())

    def delete_table(self, table_name):

        t = Table(table_name, self.metadata)
        t.drop(self.engine)

        self.metadata = MetaData(bind=self.engine)

    def make_table(self, table_name, columns):

        type_map = {
            'text': String,
            'number': Float,
            'integer': Integer
        }

        sacolumns = []

        for c in columns:
            sacol = Column(c['name'], type_map.get(c['datatype'], String)())
            sacolumns.append(sacol)

        table = Table(table_name, self.metadata, Column('_id', Integer, primary_key=True), *sacolumns)

        table.create()

    def get_table(self, table_name):
        return Table(table_name, self.metadata, autoload=True, autoload_with=self.engine)
