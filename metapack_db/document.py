# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

from .orm import Base

from sqlalchemy import Column, Integer, String, DateTime

class Document(Base):

    __tablename__ = 'mt_document'

    id = Column(Integer, primary_key=True)
    identity = Column(String)
    name = Column(String)
    name_nv = Column(String)
    table_prefix = Column(String)

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
        pass