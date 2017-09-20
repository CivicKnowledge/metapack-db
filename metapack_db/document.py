# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

from .orm import Base

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship

class Document(Base):

    __tablename__ = 'mt_documents'

    id = Column(Integer, primary_key=True)
    identity = Column(String, nullable=False, unique=True)
    name = Column(String, nullable=False, unique=True)
    name_nv = Column(String, nullable=False)
    table_prefix = Column(String, nullable=False, unique=True)

    title = Column(String)
    description = Column(String)

    dataset = Column(String)
    origin = Column(String)
    space = Column(String)
    time = Column(String)
    grain = Column(String)
    variant = Column(String)

    created = Column(DateTime)
    modified = Column(DateTime)
    issued = Column(DateTime)

    def update_from_doc(self, doc):

        from dateutil.parser import parse

        self.identity = doc.get_value('Root.Identity')
        self.name = doc.get_value('Root.Name')
        self.name_nv = doc.as_version(None)
        self.table_prefix = self.identity[0:3] # This will fail eventually!
        self.title = doc.get_value('Root.Title')
        self.description = doc.get_value('Root.Description')
        self.dataset = doc.get_value('Root.Dataset')
        self.origin = doc.get_value('Root.Origin')
        self.space = doc.get_value('Root.Space')
        self.time = doc.get_value('Root.Time')
        self.grain = doc.get_value('Root.Grain')
        self.variant = doc.get_value('Root.Variant')

        resources = relationship("Resource", cascade="all,delete", backref="document")
        terms = relationship("Term", cascade="all,delete", backref="document")

        #self.created = doc.get_value('Root.Created')
        #self.modified = doc.get_value('Root.Modified')
        #self.issued = doc.get_value('Root.Issued')

