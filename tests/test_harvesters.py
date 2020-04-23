"""Tests for the harvesters"""
#pylint: disable=protected-access

import unittest
import unittest.mock as mock

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.ingesters as ingesters
import geospaas_harvesting.harvesters as harvesters

from .stubs import StubHarvester, StubIngester

TOP_PACKAGE = 'geospaas_harvesting'


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
        with self.assertLogs(StubIngester.LOGGER):
            harvester.harvest()

        self.assertListEqual(
            harvester._ingester.ingested_urls,
            ['https://random1.url/ressource_1',
             'https://random1.url/ressource_2',
             'https://random1.url/ressource_3',
             'https://random2.url/ressource_a',
             'https://random2.url/ressource_b',
             'https://random2.url/ressource_c'])

class ChildHarvestersTestCase(unittest.TestCase):
    """Tests for the Harvesters which inherit from the base Harvester class"""

    def test_podaac_harvester(self):
        """The PODAAC harvester has the correct crawler and ingester"""
        harvester = harvesters.PODAACHarvester(urls=[''], max_fetcher_threads=1, max_db_threads=1)
        self.assertIsInstance(harvester._current_crawler, crawlers.OpenDAPCrawler)
        self.assertIsInstance(harvester._ingester, ingesters.DDXIngester)

    def test_copernicus_sentinel_harvester(self):
        """The Copernicus Sentinel harvester has the correct crawler and ingester"""
        harvester = harvesters.CopernicusSentinelHarvester(url='', search_terms=[''],
                                                           max_fetcher_threads=1, max_db_threads=1,
                                                           username='test', password='TEST')
        self.assertIsInstance(harvester._current_crawler, crawlers.CopernicusOpenSearchAPICrawler)
        self.assertIsInstance(harvester._ingester, ingesters.CopernicusODataIngester)
