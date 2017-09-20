# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

from .orm import Base

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy_jsonfield import JSONField

class Term(Base):

    __tablename__ = 'mt_terms'

    id = Column(Integer, primary_key=True)
    document_id = Column(String, ForeignKey("mt_documents.id"), nullable=False)
    parent_term = Column(String, nullable=False)
    record_term = Column(String, nullable=False)
    section = Column(String)

    term_value_name = Column(String)

    value = Column(String)
    properties = Column(JSONField)
