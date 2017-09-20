import unittest


from metatab import MetatabDoc
from metapack_db import Database, MetatabManager
from metapack_db.document import Document

from sqlalchemy.exc import IntegrityError

def test_data(*paths):
    from os.path import dirname, join, abspath

    return abspath(join(dirname(dirname(abspath(__file__))), 'test-data', *paths))


class BasicTests(unittest.TestCase):

    def test_create_tables(self):

        doc = MetatabDoc(test_data('example1.csv'))

        db = Database('sqlite:////tmp/test.db')

        db.create_tables()

        mm = MetatabManager(db)

        mm.delete_doc(identity=doc.get_value('Root.Identity'))

        mm.add_doc(doc)

        with self.assertRaises(IntegrityError):
            mm.add_doc(doc)

if __name__ == '__main__':
    unittest.main()
