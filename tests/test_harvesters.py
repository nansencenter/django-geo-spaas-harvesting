"""Tests for the harvesters"""
#pylint: disable=protected-access

import unittest
import unittest.mock as mock
from datetime import datetime

from geospaas.vocabularies.models import Parameter

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.harvesters as harvesters
import geospaas_harvesting.ingesters as ingesters
from geospaas_harvesting.harvesters import HarvesterConfigurationError

from .stubs import StubHarvester, StubIngester


class HarvesterTestCase(unittest.TestCase):
    """Test the base harvester"""

    def setUp(self):
        self.patcher_param_count = mock.patch.object(Parameter.objects, 'count')
        self.mock_param_count = self.patcher_param_count.start()
        self.mock_param_count.return_value = 2

    def tearDown(self):
        self.patcher_param_count.stop()

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

    def test_get_time_range_standard(self):
        """Get a standard time range from the configuration"""
        harvester = StubHarvester(urls=[''], time_range=['20191215161800', '20191215201800'])
        self.assertTupleEqual(harvester.get_time_range(), (
            datetime(2019, 12, 15, 16, 18, 00), datetime(2019, 12, 15, 20, 18, 00)
        ))

    def test_get_time_range_no_lower_limit(self):
        """Get a time range without a lower limit from the configuration"""
        harvester = StubHarvester(urls=[''], time_range=[None, '20191215201800'])
        self.assertTupleEqual(harvester.get_time_range(), (
            None, datetime(2019, 12, 15, 20, 18, 00)
        ))

    def test_get_time_range_no_upper_limit(self):
        """Get a time range without an upper limit from the configuration"""
        harvester = StubHarvester(urls=[''], time_range=['20191215161800', None])
        self.assertTupleEqual(harvester.get_time_range(), (
            datetime(2019, 12, 15, 16, 18, 00), None
        ))

    def test_get_time_range_no_conf(self):
        """Get (None, None) when `time_range` is empty in the configuration"""
        harvester = StubHarvester(urls=[''])
        self.assertTupleEqual(harvester.get_time_range(), (None, None))

    def test_raise_error_on_invalid_time_range(self):
        """An error must be raised if the first value of the time range is superior to the second"""
        harvester = StubHarvester(urls=[''], time_range=['20191215201800', '20191215161800'])
        with self.assertRaises(ValueError):
            _ = harvester.get_time_range()

    def test_raise_error_on_invalid_date_format(self):
        """
        An error must be raised if the format of one or both of the dates is not parseable by
        dateutil
        """
        harvester = StubHarvester(urls=[''], time_range=['some_string', 'some_other_string'])
        with self.assertRaises(ValueError):
            _ = harvester.get_time_range()

    def test_raise_error_on_wrong_time_range_length(self):
        """
        An error must be raised if the time_range length is different than 2 in the configuration
        """
        harvester = StubHarvester(urls=[''], time_range=['20191215161800'])
        with self.assertRaises(ValueError):
            _ = harvester.get_time_range()


class ChildHarvestersTestCase(unittest.TestCase):
    """Tests for the Harvesters which inherit from the base Harvester class"""

    def setUp(self):
        self.patcher_param_count = mock.patch.object(Parameter.objects, 'count')
        self.mock_param_count = self.patcher_param_count.start()
        self.mock_param_count.return_value = 2

    def tearDown(self):
        self.patcher_param_count.stop()

    def test_podaac_harvester(self):
        """The PODAAC harvester has the correct crawler and ingester"""
        harvester = harvesters.PODAACHarvester(urls=[''], max_fetcher_threads=1, max_db_threads=1)
        self.assertIsInstance(harvester._current_crawler, crawlers.PODAACCrawler)
        self.assertIsInstance(harvester._ingester, ingesters.DDXIngester)

    def test_copernicus_sentinel_harvester(self):
        """The Copernicus Sentinel harvester has the correct crawler and ingester"""
        harvester = harvesters.CopernicusSentinelHarvester(url='', search_terms=[''],
                                                           max_fetcher_threads=1, max_db_threads=1,
                                                           username='test', password='TEST')
        self.assertIsInstance(harvester._current_crawler, crawlers.CopernicusOpenSearchAPICrawler)
        self.assertIsInstance(harvester._ingester, ingesters.CopernicusODataIngester)


class HarvesterExceptTestCase(unittest.TestCase):
    def test_except_create_crawler(self):
        """shall return exception in the case of incorrect class of crawler"""
        class test_class_harvester(harvesters.WebDirectoryHarvester):
            ingester = ingesters.DDXIngester
        with self.assertRaises(HarvesterConfigurationError):
            test_class_harvester()

    def test_except_create_ingester(self):
        """shall return exception in the case of incorrect class of ingester"""
        class test_class_harvester2(harvesters.WebDirectoryHarvester):
            crawler = crawlers.OpenDAPCrawler
        with self.assertRaises(HarvesterConfigurationError):
            test_class_harvester2()

    def test_except_create_without_ingester_or_crawler(self):
        """shall return exception in the case of lack of ingester or crawler """
        class test_class_harvester3(harvesters.WebDirectoryHarvester):
            pass
        with self.assertRaises(HarvesterConfigurationError):
            test_class_harvester3()
