# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    Table,
    inspect
)
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import mapper, relationship

from .orm import Base, JSONEncodedObj, MutationList
from .util import base_encode, tablenamify


class Resource(Base):

    __tablename__ = 'mt_resources'

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey("mt_documents.id"), nullable=False)

    resource_term_id = Column(Integer, ForeignKey("mt_terms.id"), nullable=False)
    resource_term = relationship("Term",   backref='resource', lazy='joined')

    name = Column(String)

    source_url = Column(String) # Resolved URL

    table_name = Column(String)

    # FIXME. This column isn't actually mutable, but this code was easy to cut-and-paste
    schema = Column(MutationList.as_mutable(JSONEncodedObj))

    table_created = Column(Boolean, default=False)
    loaded = Column(Boolean, default=False)

    @property
    def url(self):
        return self.resource_term.value


    @staticmethod
    def make_table_name(document, r):
        """Create a table name from the document id and a resource name"""
        return "d{}".format(document.id)+'_'+tablenamify(r.name)

    @property
    def table(self):
        """Return a SqlAlchemy table for this resource"""

        type_map = {
            'text': Integer,
            'number': Integer,
            'integer': Integer
        }

        sacolumns = []

        for c in self.schema:

            sa_type = type_map.get(c['datatype'], String)

            sacol = Column(c['header'], sa_type)
            sacolumns.append(sacol)

        table = Table(self.table_name, Base.metadata, Column('_id', Integer, primary_key=True), *sacolumns)

        return table


    def make_table(self):
        """Create the table for this resource, including the DDL for the schema"""
        session = inspect(self).session
        manager = session.info['manager']

        if not self.table_created:
            self.table.create(manager.database.engine)

            self.table_created = True

    @property
    def mapper(self):
        """Return the Sqlalchemy Mapper class for this resource"""
        session = inspect(self).session
        manager = session.info['manager']

        table = Table(self.table_name, Base.metadata, autoload=True, autoload_with=manager.database.engine)

        class BareMapper(object):
            """A Class for constructing mappers"""

        return mapper(BareMapper, table)

    def load_resource(self):
        """Load rows into a previously created resource table"""

        from metapack import parse_app_url, get_generator

        if not self.loaded:

            url = parse_app_url(self.source_url)
            g = get_generator(url.get_resource().get_target())

            session = inspect(self).session

            session.bulk_insert_mappings(self.mapper, g.iter_dict)

            self.loaded = True
