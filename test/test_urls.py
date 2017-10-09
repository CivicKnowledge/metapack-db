import unittest


from csv import DictReader
from appurl import parse_app_url, match_url_classes

def test_data(*paths):
    from os.path import dirname, join, abspath

    return abspath(join(dirname(dirname(abspath(__file__))), 'test-data', *paths))


class BasicTests(unittest.TestCase):

    def test_app_urls(self):

        with open(test_data('database_urls.csv')) as f:
            for e in DictReader(f):

                u = parse_app_url(e['url'])

                self.assertEquals(str(e['driver']), str(u.driver))
                self.assertEquals(str(e['dialect']), str(u.dialect))

if __name__ == '__main__':
    unittest.main()
