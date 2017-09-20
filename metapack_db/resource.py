# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

from .orm import Base

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy_jsonfield import JSONField

class Resource(Base):

    __tablename__ = 'mt_resources'

    id = Column(Integer, primary_key=True)
    document_id = Column(String, ForeignKey("mt_documents.id"), nullable=False)
    name = Column(String, nullable=False)
    schema = Column(String, nullable=False)
    properties = Column(JSONField)