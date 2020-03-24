"""Tests for the harvesters"""
#pylint: disable=protected-access

import logging
import unittest
import unittest.mock as mock

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.ingesters as ingesters
import geospaas_harvesting.harvesters as harvesters

from .stubs import StubHarvester

TOP_PACKAGE = 'geospaas_harvesting'


class HarvesterListTestCase(unittest.TestCase):
    """Test the HarvesterList behavior"""

    class TestHarvester(harvesters.Harvester):
        """Dummy Harvester used for tests"""
        def __init__(self, **config):
            self.config = config

        def _create_crawlers(self):
            pass

        def _create_ingester(self):
            pass

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

    def test_iterate_over_empty_list(self):
        """StopIteration must be raised if the list is empty"""
        harvester_list = harvesters.HarvesterList({})
        iterator = iter(harvester_list)
        with self.assertRaises(StopIteration):
            _ = next(iterator)

    def test_init_conf_is_wrong_type(self):
        """
        If the config argument is of the wrong type, an error message must be logged and the
        HarvesterList must remain empty
        """
        with self.assertLogs(harvesters.LOGGER, level=logging.ERROR):
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

    def test_init_wrong_harvester_class(self):
        """An error must be logged if one of the harvesters has a inexistent class"""
        with self.assertLogs(harvesters.LOGGER, level=logging.ERROR):
            _ = harvesters.HarvesterList({
                'harvester1': {
                    'class': 'InexistentHarvester'
                }
            })

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


class HarvesterTestCase(unittest.TestCase):
    """Test the base harvester"""

    def test_exception_on_base_harvester_create_crawlers(self):
        """
        An exception is raised if an attempt is made to call the _create_crawlers() method of the
        base Harvester class
        """
        with mock.patch.object(harvesters.Harvester, '__init__', return_value=None):
            base_harvester = harvesters.Harvester()
            with self.assertRaises(NotImplementedError):
                base_harvester._create_crawlers()

    def test_exception_on_base_harvester_create_ingester(self):
        """
        An exception is raised if an attempt is made to call the _create_ingester() method of the
        base Harvester class
        """
        with mock.patch.object(harvesters.Harvester, '__init__', return_value=None):
            base_harvester = harvesters.Harvester()
            with self.assertRaises(NotImplementedError):
                base_harvester._create_ingester()

    def test_correct_conf_loading(self):
        """Test that a correct configuration file is used the proper way"""
        urls = ['https://random1.url', 'https://random2.url']

        harvester = StubHarvester(urls=urls)
        # This assertion relies on the fact that StubHarvester uses StubCrawler which stores the
        # root URL as an attribute. It wouldn't work with any harvester and crawler
        self.assertListEqual([c.root_url for c in harvester._crawlers], urls)

    def test_empty_conf_loading(self):
        """An exception must be raised if the configuration file is empty"""
        with self.assertRaises(harvesters.HarvesterConfigurationError):
            _ = StubHarvester()

    def test_conf_without_url_loading(self):
        """
        An exception must be raised if the configuration file does not contain a 'urls' key in the
        harvester's section
        """
        with self.assertRaises(harvesters.HarvesterConfigurationError):
            _ = StubHarvester(nonsense='arg')

    def test_all_urls_ingested(self):
        """Tests that all root URLs are explored"""
        harvester = StubHarvester(urls=['https://random1.url', 'https://random2.url'])
        with self.assertLogs(ingesters.LOGGER):
            harvester.harvest()

        self.assertListEqual(
            harvester._ingester.ingested_urls,
            ['https://random1.url/ressource_1',
             'https://random1.url/ressource_2',
             'https://random1.url/ressource_3',
             'https://random2.url/ressource_a',
             'https://random2.url/ressource_b',
             'https://random2.url/ressource_c'])

    def test_podaac_harvester(self):
        """The PODAAC harvester has the correct crawler and ingester"""
        harvester = harvesters.PODAACHarvester(urls=[''])
        self.assertIsInstance(harvester._current_crawler, crawlers.OpenDAPCrawler)
        self.assertIsInstance(harvester._ingester, ingesters.DDXIngester)
