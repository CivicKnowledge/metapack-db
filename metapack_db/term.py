# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

from .orm import Base

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy_jsonfield import JSONField
from sqlalchemy.orm import relationship, backref, reconstructor

import metapack.terms
import metatab.terms



class Term(Base):

    __tablename__ = 'mt_terms'

    term_class = metatab.terms.Term

    id = Column(Integer, primary_key=True)
    class_type = Column(String)

    document_id = Column(Integer, ForeignKey("mt_documents.id"), nullable=False)
    # Document defines a back ref to 'document'

    # Parent /  child relationships
    parent_id = Column(Integer, ForeignKey("mt_terms.id"), nullable=True)
    children = relationship("Term",
                            backref=backref('parent', remote_side=[id]), foreign_keys=[parent_id]
                            )

    # Links for sections
    section_id = Column(Integer, ForeignKey("mt_terms.id"), nullable=True)
    terms = relationship("Term",
                            backref=backref('section', remote_side=[id]), foreign_keys=[section_id]
                            )

    parent_term = Column(String, nullable=False)
    record_term = Column(String, nullable=False)

    term_value_name = Column(String)

    value = Column(String)
    properties = Column(JSONField)

    # term values maybe we'll set later
    row = None
    col = None
    file_name = None
    file_type = None
    doc = None



    __mapper_args__ = {
        'polymorphic_on':class_type,
        'polymorphic_identity':'term'
    }

    @property
    def join(self):
        return self.parent_term+'.'+self.record_term

    _mt_term = None

    def mt_term(self, mt_doc):
        """Return the metatab term for this database term"""

        if not self._mt_term:

            self._mt_term = self.term_class(
                term=self.join,
                value=self.value,
                term_args=[],
                doc=mt_doc,
                row=self.row, col=self.col, file_name=self.file_name, file_type=self.file_type,
                parent=self.parent.mt_term(mt_doc) if self.parent else None,
                section=self.section.mt_term(mt_doc) if self.section else None,
            )

            self._mt_term._children = [c.mt_term(mt_doc) for c in self.children ]

        return self._mt_term


    def file_ref(self):
        return 'database'

    def __str__(self):
        from metatab import ELIDED_TERM

        sec_name = 'None' if not self.section else self.section.name

        if self.parent_term == ELIDED_TERM:
            return "{} {}:.{}={}".format(
                self.file_ref(), sec_name, self.record_term, self.value)

        else:
            return "{} {}:{}.{}={}".format(
                self.file_ref(), sec_name, self.parent_term, self.record_term, self.value)


class Section(Term):
    __mapper_args__ = {
        'polymorphic_identity': 'section'
    }

    term_class = metatab.terms.SectionTerm

    @property
    def name(self):
        return self.value


class Root(Section):
    __mapper_args__ = {
        'polymorphic_identity': 'root'
    }

    term_class = metatab.terms.RootSectionTerm


class ResourceTerm(Term):
    __mapper_args__ = {
        'polymorphic_identity': 'resource'
    }

    term_class = metapack.terms.Resource

    @property
    def name(self):
        return self.properties.get('name')

    @property
    def url(self):
        return self.value

class DistributionTerm(Term):
    __mapper_args__ = {
        'polymorphic_identity': 'distribution'
    }

    term_class = metapack.terms.Distribution
