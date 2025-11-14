# pylint: disable=protected-access
"""Tests for the base classes used by providers"""

import logging
import unittest
import unittest.mock as mock
from datetime import datetime, timezone as tz

from shapely.geometry.polygon import Polygon

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.ingesters as ingesters
import geospaas_harvesting.normalizers.raw
import geospaas_harvesting.providers as providers
import geospaas_harvesting.utils as utils


class ProviderTestCase(unittest.TestCase):
    """Tests for the base Provider class"""

    class TestCrawler(crawlers.Crawler):
        """Test crawler"""
        name = 'test'
        def crawl(self):
            return []

    def setUp(self) -> None:
        mock.patch('geospaas_harvesting.crawlers.index', {'test': self.TestCrawler}).start()
        self.provider = providers.Provider(
            name='test',
            config={
                'crawler': {
                    'name': 'test',
                    'username': 'user',
                    'password': 'pass',
                },
                'normalizer': {'name': 'raw'},
                'ingester': {},
            })
        self.addCleanup(mock.patch.stopall)

    def test_search(self):
        """Check that search() produces a SearchResult with the right
        arguments
        """
        self.maxDiff = None
        results = self.provider.search(
            crawler={
                'time_range': ['2023-01-01', '2023-01-02'],
                'location': 'POLYGON((30 40,20 40,20 20,30 40))',
                'username': 'user2',
            }
        )
        self.assertEqual(
            results.search_info,
            providers.SearchResults(
                self.TestCrawler(
                    time_range=[datetime(2023, 1, 1, tzinfo=tz.utc),
                                datetime(2023, 1, 2, tzinfo=tz.utc)],
                    location='POLYGON ((30 40, 20 40, 20 20, 30 40))',
                    username='user2', password='pass'),
                geospaas_harvesting.normalizers.raw.RawMetadataNormalizer(max_threads=1),
                ingesters.Ingester()
            ).search_info)

    def test_repr(self):
        """Test provider representation"""
        self.assertEqual(
            repr(self.provider),
            "Provider(name='test', config={'crawler': {'name': 'test', 'username': 'user', 'password': '******'}, 'normalizer': {'name': 'raw'}, 'ingester': {}})")

    def test_str(self):
        """Test string representation"""
        self.assertEqual(
            str(self.provider),
            "Provider: test (normalizer: raw, crawler: test)")


class SearchResultsTestCase(unittest.TestCase):
    """Tests for the SearchResults class"""

    def setUp(self):
        self.crawler = mock.MagicMock()
        self.ingester = mock.MagicMock()
        self.mock_dataset_infos = [mock.Mock(), mock.Mock()]
        self.crawler.__iter__.return_value = iter(self.mock_dataset_infos)
        self.filter = mock.MagicMock()
        self.search_results = providers.SearchResults(
            crawler=self.crawler,
            filters=[self.filter],
            ingester=self.ingester)

    def test_repr(self):
        """Check the string representation of a SearchResults object"""
        self.assertEqual(
            repr(self.search_results),
            f"SearchResults for crawler: {self.crawler}")

    def test_equality(self):
        """Test equality operator between SearchResults objects"""
        self.assertEqual(
            self.search_results,
            providers.SearchResults(self.crawler, [self.filter]))
        self.assertNotEqual(
            self.search_results,
            providers.SearchResults(mock.MagicMock(), [mock.MagicMock()]))
        self.assertNotEqual(
            self.search_results,
            providers.SearchResults(self.crawler, [self.filter, mock.MagicMock()]))

    def test_iterable(self):
        """A SearchResults object should be iterable"""
        self.assertEqual(iter(self.search_results), self.search_results)
        self.crawler.set_initial_state.assert_called_once()
        self.crawler.__iter__.assert_called_once()

    def test_iterator(self):
        """A SearchResults object should be its own iterator"""

        search_results_iterator = iter(self.search_results)

        self.assertEqual(next(search_results_iterator), self.mock_dataset_infos[0])
        self.assertEqual(next(search_results_iterator), self.mock_dataset_infos[1])
        with self.assertRaises(StopIteration):
            next(search_results_iterator)
        self.filter.assert_has_calls([
            mock.call(self.mock_dataset_infos[0]),
            mock.call(self.mock_dataset_infos[1])
        ], any_order=True)

    def test_filter(self):
        """Test filtering dataset_infos"""
        self.filter.side_effect = [True, False]
        self.assertListEqual(
            list(self.search_results),
            [self.mock_dataset_infos[0]])

    def test_save(self):
        """Test saving the search results to the database"""
        with self.assertLogs(providers.logger, level=logging.INFO):
            self.search_results.save()
        self.ingester.ingest.assert_called_once()
