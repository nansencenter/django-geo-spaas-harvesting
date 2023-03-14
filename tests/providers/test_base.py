# pylint: disable=protected-access
"""Tests for the base classes used by providers"""

import logging
import unittest
import unittest.mock as mock
from datetime import datetime, timezone as tz

from shapely.geometry.polygon import Polygon

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.providers.base as providers_base


class ProviderTestCase(unittest.TestCase):
    """Tests for the base Provider class"""

    def setUp(self) -> None:
        self.provider = providers_base.Provider(name='test', username='user', password='pass')

    def test_search(self):
        """Check that search() produces a crawler with the right
        arguments
        """
        with mock.patch.object(self.provider, '_make_crawler') as mock_make_crawler:
            results = self.provider.search(
                start_time='2023-01-01',
                end_time='2023-01-02',
                location='POLYGON((30 40,20 40,20 20,30 40))'
            )
        mock_make_crawler.assert_called_once_with({
            'start_time': datetime(2023, 1, 1, tzinfo=tz.utc),
            'end_time': datetime(2023, 1, 2, tzinfo=tz.utc),
            'location': Polygon(((30, 40), (20, 40), (20, 20), (30, 40)))
        })
        self.assertEqual(
            results,
            providers_base.SearchResults(mock_make_crawler.return_value, filters=[]))

    def test_repr(self):
        """Test provider representation"""
        self.assertEqual(repr(self.provider), 'Provider(name=test, username=user, password=*)')

    def test_str(self):
        """Test string representation"""
        self.assertEqual(
            str(self.provider),
            f"test provider, {str(self.provider.search_parameters_parser)}")

    def test_abstract_make_crawler(self):
        """_make_crawler should raise a NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.provider._make_crawler({})


class FilterMixinTestCase(unittest.TestCase):
    """Tests for the FilterMixin class"""

    def test_make_filters(self):
        """No filters are created by default"""
        self.assertListEqual(providers_base.FilterMixin()._make_filters({}), [])


class TimeFilterMixinTestCase(unittest.TestCase):
    """Tests for the TimeFilterMixin class"""

    def test_filter_methods(self):
        """Test the filter methods. Done with subtests for easier setup
        """
        time_filter_mixin = providers_base.TimeFilterMixin()
        time_filter_mixin._mixin_start_time = datetime(2023, 1, 1, tzinfo=tz.utc)
        time_filter_mixin._mixin_end_time = datetime(2023, 1, 2, tzinfo=tz.utc)

        with self.subTest('time_coverage_end superior to reference start time'):
            self.assertTrue(
                time_filter_mixin._time_coverage_end_gt(crawlers.DatasetInfo(
                    'http://foo',
                    {'time_coverage_end': datetime(2023, 1, 1, 1, tzinfo=tz.utc)})
                ))
        with self.subTest('time_coverage_end equal to reference start time'):
            self.assertFalse(
                time_filter_mixin._time_coverage_end_gt(crawlers.DatasetInfo(
                    'http://foo',
                    {'time_coverage_end': datetime(2023, 1, 1, tzinfo=tz.utc)})
                ))
        with self.subTest('time_coverage_end inferior to reference start time'):
            self.assertFalse(
                time_filter_mixin._time_coverage_end_gt(crawlers.DatasetInfo(
                    'http://foo',
                    {'time_coverage_end': datetime(2022, 1, 1, tzinfo=tz.utc)})
                ))

        with self.subTest('time_coverage_start superior to reference end time'):
            self.assertFalse(
                time_filter_mixin._time_coverage_start_lte(crawlers.DatasetInfo(
                    'http://foo',
                    {'time_coverage_start': datetime(2023, 1, 2, 1, tzinfo=tz.utc)})
                ))
        with self.subTest('time_coverage_start equal to reference end time'):
            self.assertTrue(
                time_filter_mixin._time_coverage_start_lte(crawlers.DatasetInfo(
                    'http://foo',
                    {'time_coverage_start': datetime(2023, 1, 2, tzinfo=tz.utc)})
                ))
        with self.subTest('time_coverage_start inferior to reference end time'):
            self.assertTrue(
                time_filter_mixin._time_coverage_start_lte(crawlers.DatasetInfo(
                    'http://foo',
                    {'time_coverage_start': datetime(2023, 1, 1, tzinfo=tz.utc)})
                ))

    def test_make_filters(self):
        """Test that the proper filters are created"""
        time_filter_mixin = providers_base.TimeFilterMixin()
        self.assertListEqual(
            time_filter_mixin._make_filters({
                'start_time': datetime(2023, 1, 1, tzinfo=tz.utc),
                'end_time': datetime(2023, 1, 2, tzinfo=tz.utc),}),
            [time_filter_mixin._time_coverage_end_gt, time_filter_mixin._time_coverage_start_lte])
        self.assertEqual(time_filter_mixin._mixin_start_time, datetime(2023, 1, 1, tzinfo=tz.utc))
        self.assertEqual(time_filter_mixin._mixin_end_time, datetime(2023, 1, 2, tzinfo=tz.utc))

    def test_make_filters_no_start_time(self):
        """Test filters creation when no start time is given"""
        time_filter_mixin = providers_base.TimeFilterMixin()
        self.assertListEqual(
            time_filter_mixin._make_filters({'end_time': datetime(2023, 1, 2, tzinfo=tz.utc)}),
            [time_filter_mixin._time_coverage_start_lte])
        self.assertIsNone(time_filter_mixin._mixin_start_time)
        self.assertEqual(time_filter_mixin._mixin_end_time, datetime(2023, 1, 2, tzinfo=tz.utc))

    def test_make_filters_no_end_time(self):
        """Test filters creation when no end time is given"""
        time_filter_mixin = providers_base.TimeFilterMixin()
        self.assertListEqual(
            time_filter_mixin._make_filters({'start_time': datetime(2023, 1, 1, tzinfo=tz.utc)}),
            [time_filter_mixin._time_coverage_end_gt])
        self.assertIsNone(time_filter_mixin._mixin_end_time)
        self.assertEqual(time_filter_mixin._mixin_start_time, datetime(2023, 1, 1, tzinfo=tz.utc))


class SearchResultsTestCase(unittest.TestCase):
    """Tests for the SearchResults class"""

    def setUp(self):
        self.crawler = mock.MagicMock()
        self.mock_dataset_infos = [mock.Mock(), mock.Mock()]
        self.crawler.__iter__.return_value = iter(self.mock_dataset_infos)
        self.filter = mock.MagicMock()
        self.search_results = providers_base.SearchResults(self.crawler, [self.filter])

    def test_repr(self):
        """Check the string representation of a SearchResults object"""
        self.assertEqual(
            repr(self.search_results),
            f"SearchResults for crawler: {self.crawler}")

    def test_equality(self):
        """Test equality operator between SearchResults objects"""
        self.assertEqual(
            self.search_results,
            providers_base.SearchResults(self.crawler, [self.filter]))
        self.assertNotEqual(
            self.search_results,
            providers_base.SearchResults(mock.MagicMock(), [mock.MagicMock()]))
        self.assertNotEqual(
            self.search_results,
            providers_base.SearchResults(self.crawler, [self.filter, mock.MagicMock()]))

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
        with mock.patch('geospaas_harvesting.ingesters.Ingester.ingest') as mock_ingest:
            with self.assertLogs(providers_base.logger, level=logging.INFO):
                self.search_results.save()
        mock_ingest.assert_called_once()
