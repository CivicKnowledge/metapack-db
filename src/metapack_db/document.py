# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

import hashlib
from collections import OrderedDict

import metapack.terms
import metatab.terms
from metapack import MetapackDoc, Resolver
from sqlalchemy import Column, DateTime, Integer, String, inspect
from sqlalchemy.orm import relationship

from .orm import Base, JSONEncodedObj, MutationDict
from .resource import Resource
from .term import Section, Term
from .util import base_encode


class MetapackDbDoc(MetapackDoc):

    def __init__(self, db_doc, clean_cache=False, downloader=None):
        """

        :param package_url:
        :param clean_cache:
        :param downloader:
        """
        super().__init__(None, None, None, None, db_doc.package_url, clean_cache, downloader)

        self._ref = db_doc.ref
        self._input_ref = db_doc.ref

        self.decl_terms = db_doc.decl_terms
        self.decl_sections = db_doc.decl_sections
        self.super_terms = db_doc.super_terms
        self.derived_terms = db_doc.derived_terms

        self._sections = OrderedDict() # Clear out the pre-added root term

        session = inspect(db_doc).session

        for db_t in session.query(Term).filter(Term.document_id == db_doc.id).order_by(Term.id):
            term = db_t.mt_term(self)

            if isinstance(term, metatab.terms.RootSectionTerm):
                self.root = term
                self.add_section(term)
            elif isinstance(term, metatab.terms.SectionTerm):
                self.add_section(term)
                pass
            elif term.parent_term == 'root':
                self.add_term(term, add_section=True)

            if term.parent:
                term.parent.add_child(term)



class Document(Base):

    __tablename__ = 'mt_documents'

    id = Column(Integer, primary_key=True)
    identifier = Column(String, nullable=False, unique=True)
    name = Column(String, nullable=False, unique=True)
    name_nv = Column(String, nullable=False)

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

    ref = Column(String)
    package_url = Column(String)

    # FIXME. These columns aren't actually mutable, but this code was easy to cut-and-paste
    decl_sections = Column(MutationDict.as_mutable(JSONEncodedObj))
    decl_terms = Column(MutationDict.as_mutable(JSONEncodedObj))
    derived_terms = Column(MutationDict.as_mutable(JSONEncodedObj))
    super_terms = Column(MutationDict.as_mutable(JSONEncodedObj))

    resources = relationship("Resource", cascade="all,delete, delete-orphan", backref="document")
    db_terms = relationship("Term", cascade="save-update, merge, delete, delete-orphan", backref="document")

    def update_from_doc(self, doc):

        from dateutil.parser import parse

        self.identifier = doc.get_value('Root.Identifier')
        self.name = doc.get_value('Root.Name')
        self.name_nv = doc.as_version(None)
        # the Identifier is supposed to be unique for datasets, but it won't be between
        # versions and segument updates.
        self.table_prefix = hashlib.sha224(self.name.encode('utf-8')).hexdigest()[0:8]
        self.title = doc.get_value('Root.Title')
        self.description = doc.get_value('Root.Description')

        self.dataset = doc.get_value('Root.Dataset')
        self.origin = doc.get_value('Root.Origin')
        self.space = doc.get_value('Root.Space')
        self.time = doc.get_value('Root.Time')
        self.grain = doc.get_value('Root.Grain')
        self.variant = doc.get_value('Root.Variant')

        self.ref = str(doc.ref)
        self.package_url = str(doc.package_url)
        self.decl_sections = doc.decl_sections
        self.decl_terms = doc.decl_terms
        self.derived_terms = doc.derived_terms
        self.super_terms = doc.super_terms

        #self.created = doc.get_value('Root.Created')
        #self.modified = doc.get_value('Root.Modified')
        #self.issued = doc.get_value('Root.Issued')

    @property
    def sections(self):

        session = inspect(self).session

        return OrderedDict(
            (section.name, section) for section in session.query(Section).filter(Section.document_id == self.id)
        )

    @property
    def terms(self):
        for db_t in self.db_terms:
            yield db_t.mt_term(self)


    @property
    def mt_doc(self):
        """Return the terms in a MetapackDoc"""

        return MetapackDbDoc(self)

    def resource(self, name):
        session = inspect(self).session
        session.query(Resource).filter(Resource.document_id == self.id).filter(Resource.name == name).one()
