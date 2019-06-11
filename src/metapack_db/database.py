# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""

"""

from contextlib import contextmanager

from metapack import MetapackDoc, parse_app_url
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine
)
from sqlalchemy.orm import load_only, sessionmaker

from .document import Document
from .orm import Base
from .resource import Resource  # Need to import even if not referenced here.
from .term import ResourceTerm, Root, Section, Term


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

        # Should this be done here? Probably not ...
        self.database.create_tables()

        self._session = None

        self._nesting = 0

        self._use_nesting = False
    @contextmanager
    def session(self):
        """Provide a transactional scope around a series of operations."""

        if self._session:
            if self._use_nesting:
                self._session.begin_nested()
            pass
        else:
            assert self._nesting == 0
            self._session = self.database.Session()
            self._session.info['manager'] = self

        try:
            if self._use_nesting:
                self._nesting += 1

            yield self._session
            if self._session:
                self._session.commit()
        except:
            if self._session:
                self._session.rollback()
            raise
        finally:
            if self._session:
                self._session.close()

            if self._use_nesting:
                self._nesting -= 1

            if self._nesting == 0:
                self._session = None

    def init_tables(self):
        pass

    def add_doc(self, mt_doc):
        import metapack.terms
        import metatab
        import metapack_db.resource

        cls_map = {
            metatab.Term: Term,
            metatab.SectionTerm: Section,
            metatab.RootSectionTerm: Root,
            metapack.terms.Resource: ResourceTerm,
            metapack.terms.Distribution: ResourceTerm,
        }

        def add_term(session, document, t):
            term_class = cls_map[type(t)]

            term = term_class(
                document=document,
                parent_term=t.parent_term_lc,
                record_term=t.record_term_lc,
                parent=t.parent._db_term if t.parent is not None else None,
                value=t.value,
                section=t.section._db_term if t.section is not None else None,
                term_value_name=t.term_value_name,
                properties=t.all_props
            )

            session.add(term)

            t._db_term = term

        def add_resource(session, document, t):

            resource = metapack_db.resource.Resource(
                document=document,
                resource_term=t._db_term,
                source_url=str(t.resolved_url),
                table_name = Resource.make_table_name(document,t),
                name=t.name,
                schema = list(t.columns())
            )

            session.add(resource)

        with self.session() as s:
            document = Document()
            document.update_from_doc(mt_doc)
            s.add(document)
            s.commit()

            add_term(s,document, mt_doc.root)

            for s_name, section in mt_doc.sections.items():

                add_term(s,document, section)

                for t in section:
                    add_term(s,document, t)
                    for d in t.descendents:
                        add_term(s,document, d)

                    if t.term_is('Root.Datafile'):
                        add_resource(s,document,t)

            s.commit()
            s.expunge(document)
            return document

    def load(self, url, load_all_resources = False):
        """Load a package and possibly one or all resources, from a url"""

        u = parse_app_url(url)

        d = MetapackDoc(u.clear_fragment())

        db_doc = self.document(name=d.get_value('Root.Name'))

        if not db_doc:
            self.add_doc(d)
            db_doc = self.document(name=d.get_value('Root.Name'))
            assert db_doc

        resources = []

        if load_all_resources:

            for r in self.resources(db_doc):
                self.load_resource(r)
                resources.append(r)

        elif u.target_file:

            r = self.resource(db_doc, u.target_file)

            self.load_resource(r)

            resources.append(d)

        return (db_doc, resources)


    def documents(self):
        """Return a subset of fields from all of the documents that have been loaded into the database"""
        with self.session() as s:
            return s.query(Document)\
                   .options(load_only("id","identifier","name","title","description"))

    def document(self, ref=None, id=None, identifier=None, name=None):
        """
        Delete a document record but id, identity or name
        :param id:
        :param identity:
        :param name:
        :return:
        """

        def f(s,ref=None, id=None, identifier=None, name=None ):
            if id is not None:
                d = s.query(Document).get(id)
            elif identifier is not None:
                d = s.query(Document).filter_by(identifier=identifier).first()
            elif name is not None:
                d = s.query(Document).filter_by(name=name).first()
            elif ref is not None:

                # Metapack URL refs will get re-parsed to have the scheme_extension
                ref = 'metapack+' + ref if not ref.startswith('metapack+') else ref

                d = s.query(Document).filter((Document.ref == ref) | (Document.package_url == ref)).first()

            else:
                return None

            return d

        if self._session:
            return f(self._session, ref, id, identifier, name)
        else:
            with self.session() as s:
                d = f(self._session, ref, id, identifier, name)
                if d:
                    s.expunge(d) # So the doc can be used outside of the session

                return d

    def resources(self, doc):
        """Return the resources for a database document"""

        if self._session:
            # If a session is active, use it, and don't detatch
            return self._session.query(Resource).filter_by(document_id=doc.id).all()
        else:
            # Create a session and detach the object so they can be used outside the session
            with self.session() as s:

                resources = []

                for r in s.query(Resource).filter_by(document_id=doc.id).all():
                    s.expunge(r)
                    resources.append(r)

                return resources

    def resource(self, doc, name):
        """Return the resources for a document"""

        def f(s):
            return s.query(Resource).filter_by(document_id=doc.id).filter_by(name=name).one()

        if self._session:
            return f(self._session)
        with self.session() as s:
            r = f(s)
            s.expunge(r)
            return r


    def create_resource_table(self, table):
        """Create resource table on the database"""
        table.create(self.database.engine)

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


    def load_resource(self, r):

        with self.session() as s:
            dbr = s.query(Resource).get(r.id)
            dbr.make_table()

        with self.session() as s:
            dbr = s.query(Resource).get(r.id)
            dbr.load_resource()
