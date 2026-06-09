# pylint: disable=protected-access
"""Tests for the base classes used by providers"""

import logging
import unittest
import unittest.mock as mock
from datetime import datetime, timezone as tz

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.ingesters as ingesters
import geospaas_harvesting.normalizers.raw
import geospaas_harvesting.providers as providers


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

    def test_crawler_class(self):
        """Test getting a crawler class from the provider's config"""
        mock_crawler = mock.Mock()
        with mock.patch('geospaas_harvesting.crawlers.index', {'test': mock_crawler}):
            self.assertEqual(self.provider.crawler_class, mock_crawler)
            with self.assertRaises(ValueError):
                providers.Provider(name='test', config={'crawler': {'name': 't'}}).crawler_class

    def test_normalizer_class(self):
        """Test getting a normalizer class from the provider's config
        """
        mock_normalizer = mock.Mock()
        with mock.patch('geospaas_harvesting.normalizers.index', {'raw': mock_normalizer}):
            self.assertEqual(self.provider.normalizer_class, mock_normalizer)
            with self.assertRaises(ValueError):
                providers.Provider(
                    name='test', config={'normalizer': {'name': 'n'}}).normalizer_class


class SearchResultsTestCase(unittest.TestCase):
    """Tests for the SearchResults class"""

    def setUp(self):
        self.crawler = mock.MagicMock()
        self.ingester = mock.MagicMock()
        self.normalizer = mock.MagicMock()
        self.mock_dataset_infos = [mock.Mock(), mock.Mock()]
        self.normalizer.normalize_stream.return_value = iter(self.mock_dataset_infos)
        self.search_results = providers.SearchResults(
            crawler=self.crawler,
            normalizer=self.normalizer,
            ingester=self.ingester)

    def test_iterable(self):
        """A SearchResults object should be iterable"""
        try:
            iter(self.search_results)
        except TypeError:
            self.fail("SearchResult objects should be iterable")

    def test_iterator(self):
        """A SearchResults object should be its own iterator"""

        search_results_iterator = iter(self.search_results)

        self.assertEqual(next(search_results_iterator), self.mock_dataset_infos[0])
        self.assertEqual(next(search_results_iterator), self.mock_dataset_infos[1])
        with self.assertRaises(StopIteration):
            next(search_results_iterator)

    def test_save(self):
        """Test saving the search results to the database"""
        with self.assertLogs(providers.logger, level=logging.INFO):
            self.search_results.save()
        self.ingester.ingest.assert_called_once()
