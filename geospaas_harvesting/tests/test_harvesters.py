"""Tests for the harvesters"""
#pylint: disable=protected-access

import logging
import unittest
import unittest.mock as mock

import geospaas_harvesting.harvesters as harvesters

TOP_PACKAGE = 'geospaas_harvesting'

class HarvesterListTestCase(unittest.TestCase):
    """Test the HarvesterList behavior"""

    class TestHarvester(harvesters.Harvester):
        """Dummy Harvester used for tests"""
        def harvest(self):
            pass

    def test_init_no_conf(self):
        """Must be empty if no configuration is given"""
        harvester_list = harvesters.HarvesterList()
        self.assertEqual(harvester_list._harvesters, [])

    def test_init_empty_conf(self):
        """Must be empty if an empty configuration is given"""
        harvester_list = harvesters.HarvesterList({})
        self.assertEqual(harvester_list._harvesters, [])

    def test_init_conf_is_wrong_type(self):
        """
        If the config argument is of the wrong type, an error message must be logged and the
        HarvesterList must remain empty
        """
        with self.assertLogs(f"{TOP_PACKAGE}.harvesters", level=logging.ERROR):
            harvester_list = harvesters.HarvesterList(1)
        self.assertEqual(harvester_list._harvesters, [])

    def test_init_from_config(self):
        """Standard initialization"""
        harvesters_mocks = {
            'Harvester1': self.TestHarvester,
            'Harvester2': self.TestHarvester
        }
        globals_mock = mock.Mock(side_effect=lambda: harvesters_mocks)

        with mock.patch(f"{TOP_PACKAGE}.harvesters.globals", globals_mock):
            harvester_list = harvesters.HarvesterList({
                'harvester1': {
                    'class': 'Harvester1',
                    'urls': ['https://random1.url']
                },
                'harvester2': {
                    'class': 'Harvester2',
                    'urls': ['https://random2.url']
                }
            })

        self.assertEqual(len(harvester_list), 2)
        self.assertIsInstance(harvester_list[0], harvesters.Harvester)
        self.assertIsInstance(harvester_list[1], harvesters.Harvester)

    def test_list_behavior(self):
        """
        A HarvesterList object must have a subset of list functionalities.
        It must be: iterable, subscriptable, have a length and a working append() method
        """
        harvester_list = harvesters.HarvesterList()

        # Test append() method
        harvester_list.append(self.TestHarvester())
        harvester_list.append(self.TestHarvester())
        self.assertIsInstance(harvester_list._harvesters[0], self.TestHarvester)
        self.assertIsInstance(harvester_list._harvesters[1], self.TestHarvester)

        # harvester_list is iterable
        iterator = iter(harvester_list)
        self.assertIsInstance(iterator, harvesters.EndlessHarvesterIterator)

        # harvester_list is subscriptable
        self.assertTrue(callable(getattr(harvester_list, '__getitem__')))
        self.assertIsNotNone(harvester_list[0])

        # harvester_list has a length
        self.assertTrue(callable(getattr(harvester_list, '__len__')))
        self.assertEqual(len(harvester_list), 2)

    def test_endless_iteration(self):
        """The iterator for a HarvesterList must start over at the end of a loop"""
        harvester_list = harvesters.HarvesterList()
        harvester_list.append(self.TestHarvester(id=1))
        harvester_list.append(self.TestHarvester(id=2))

        iterator = iter(harvester_list)
        self.assertEqual(next(iterator).config['id'], 1)
        self.assertEqual(next(iterator).config['id'], 2)
        self.assertEqual(next(iterator).config['id'], 1)


class PODAACHarvesterTestCase(unittest.TestCase):
    """Test the PO.DAAC harvester"""

    class MockCrawler():
        """Mock crawler class which iterates over a defined set of URLs"""

        TEST_DATA = {
            'https://random1.url': ['ressource_1', 'ressource_2'],
            'https://random2.url': ['ressource_a', 'ressource_b', 'ressource_c']
        }

        def __init__(self, root_url):
            """Build a list of URLs which will be returned by the iterator"""
            self.data = []
            try:
                for uri in self.TEST_DATA[root_url]:
                    self.data.append(f'{root_url}/{uri}')
            except KeyError:
                pass

            self.current_index = 0

        def __iter__(self):
            return self

        def __next__(self):
            """Return the next element in the list of URLs"""
            try:
                result = self.current_index
                self.current_index += 1
                return self.data[result]
            except IndexError:
                raise StopIteration

    class MockIngester():
        """Mock class """
        INGESTED_URLS = []

        def ingest(self, urls):
            """Appends the URLs in the 'urls' iterable to the list of ingested URLs"""
            for url in urls:
                self.INGESTED_URLS.append(url)

    def test_correct_conf_loading(self):
        """Test that a correct configuration file is used the proper way"""
        urls = ['https://random1.url', 'https://random2.url']

        harvester = harvesters.PODAACHarvester(urls=urls)
        self.assertDictEqual(harvester.config, {'urls': urls})

    def test_empty_conf_loading(self):
        """An exception must be raised if the configuration file is empty"""
        with self.assertRaises(harvesters.HarvesterConfigurationError):
            _ = harvesters.PODAACHarvester()

    def test_conf_without_url_loading(self):
        """
        An exception must be raised if the configuration file does not contain a 'urls' key in the
        harvester's section
        """
        with self.assertRaises(harvesters.HarvesterConfigurationError):
            _ = harvesters.PODAACHarvester(nonsense='arg')

    def test_all_urls_ingested(self):
        """Tests that all root URLs are explored"""

        with mock.patch(
                f'{TOP_PACKAGE}.crawlers.OpenDAPCrawler',
                self.MockCrawler), mock.patch(
                    f'{TOP_PACKAGE}.ingesters.DDXIngester',
                    self.MockIngester) as mock_ingester:
            harvester = harvesters.PODAACHarvester(
                urls=['https://random1.url', 'https://random2.url'])
            harvester.harvest()

            self.assertListEqual(
                mock_ingester.INGESTED_URLS,
                ['https://random1.url/ressource_1',
                 'https://random1.url/ressource_2',
                 'https://random2.url/ressource_a',
                 'https://random2.url/ressource_b',
                 'https://random2.url/ressource_c'])
