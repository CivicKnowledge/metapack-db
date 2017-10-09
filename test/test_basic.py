import unittest


from metapack import MetapackDoc
from metapack_db import Database, MetatabManager
from metapack_db.document import Document
from metapack_db.term import Term
from os import remove
from os.path import exists

from sqlalchemy.exc import IntegrityError

def test_data(*paths):
    from os.path import dirname, join, abspath

    return abspath(join(dirname(dirname(abspath(__file__))), 'test-data', *paths))

test_database_path = '/tmp/test.db'


class BasicTests(unittest.TestCase):

    def test_create_and_delete_tables(self):
        from os import remove
        from os.path import exists

        doc = MetapackDoc(test_data('example1.csv'))

        if exists(test_database_path):
            remove(test_database_path)

        db = Database('sqlite:///'+test_database_path)

        db.create_tables()

        mm = MetatabManager(db)

        mm.add_doc(doc)

        with mm.session() as s:
            self.assertEqual(1, len(list(s.query(Document))))
            self.assertEqual(154, len(list(s.query(Term))))

        with self.assertRaises(IntegrityError):
            mm.add_doc(doc)

        with mm.session() as s:
            self.assertEqual(1, len(list(s.query(Document))))
            self.assertEqual(154, len(list(s.query(Term))))

        with mm.session() as s:
            db_doc = mm.document(identifier=doc.get_value('Root.Identifier'))
            self.assertIsNotNone(db_doc)
            s.delete(db_doc)

        with mm.session() as s:
            self.assertEqual(0, len(list(s.query(Document))))
            self.assertEqual(0, len(list(s.query(Term))))

    def test_iterate_doc(self):

        doc = MetapackDoc(test_data('example1.csv'))

        if exists(test_database_path):
            remove(test_database_path)

        db = Database('sqlite:///'+test_database_path)

        mm = MetatabManager(db)

        mm.add_doc(doc)

        with mm.session() as s:
            db_doc = mm.document(identifier=doc.get_value('Root.Identifier'))

            mt_doc = db_doc.mt_doc

            self.assertEqual(23, len(mt_doc.terms))

            self.assertEqual(['http://example.com/example1.csv', 'http://example.com/example2.csv'],
                             [str(r.resolved_url) for r in mt_doc.find("Root.Datafile")])

        with mm.session():
            with mm.session():
                db_doc = mm.document(identifier=doc.get_value('Root.Identifier'))

                mt_doc = db_doc.mt_doc

                self.assertEqual(23, len(mt_doc.terms))

    def test_multiple_docs(self):

        if exists(test_database_path):
            remove(test_database_path)

        db = Database('sqlite:///'+test_database_path)

        mm = MetatabManager(db)

        with mm.session(): # # add_doc session are nested
            mm.add_doc(MetapackDoc(test_data('example1.csv')))
            mm.add_doc(MetapackDoc(test_data('example.com-full-2017-us.csv')))
            mm.add_doc(MetapackDoc('http://library.metatab.org/example.com-simple_example-2017-us-2.csv'))

        with mm.session():
            self.assertEqual(['cfcba102-9d8f-11e7-8adb-3c0754078006', '316821b9-9082-4c9e-8662-db50d9d91135',
                              '96cd659b-94ad-46ae-9c18-4018caa64355' ],
                             [d.identifier for d in mm.documents()])

        doc = mm.document(ref="file:"+test_data('example1.csv'))
        self.assertEquals('cfcba102-9d8f-11e7-8adb-3c0754078006', doc.identifier)

        doc = mm.document(ref='metapack+http://library.metatab.org/example.com-simple_example-2017-us-2.csv')
        self.assertEquals('96cd659b-94ad-46ae-9c18-4018caa64355', doc.identifier)

        doc = mm.document(ref='http://library.metatab.org/example.com-simple_example-2017-us-2.csv')
        self.assertEquals('96cd659b-94ad-46ae-9c18-4018caa64355', doc.identifier)

        doc = mm.document(name=doc.name)
        self.assertEquals('96cd659b-94ad-46ae-9c18-4018caa64355', doc.identifier)

    def test_load_one_resource(self):

        if exists(test_database_path):
            remove(test_database_path)

        db = Database('sqlite:///' + test_database_path)

        mm = MetatabManager(db)

        mm.add_doc(MetapackDoc('http://library.metatab.org/example.com-full-2017-us-1.csv'))

        ident = next(d.identifier for d in mm.documents())
        doc = mm.document(identifier=ident)

        self.assertIsNotNone(doc)

        resources = [ r.name for r in mm.resources(doc) ]

        self.assertEquals(['renter_cost', 'simple-example-altnames', 'simple-example', 'unicode-latin1',
                           'unicode-utf8', 'renter_cost_excel07', 'renter_cost_excel97', 'renter_cost-2',
                           'random-names', 'random-names-fs', 'random-names-csv', 'random-names-xlsx',
                           'random-names-zip', 'sra', 'rowgen', 'simple-fixed'],
                          resources)

        r = mm.resource(doc, 'random-names')

        mm.load_resource(r)

    def test_load_from_url(self):

        if exists(test_database_path):
            remove(test_database_path)

        db = Database('sqlite:///' + test_database_path)

        mm = MetatabManager(db)

        mm.load('http://library.metatab.org/example.com-full-2017-us-1.csv#renter_cost_excel97')

        mm.load('http://library.metatab.org/example.com-full-2017-us-1.csv#random-names')

        mm.load('http://library.metatab.org/example.com-full-2017-us-1.csv#random-names')

if __name__ == '__main__':
    unittest.main()
