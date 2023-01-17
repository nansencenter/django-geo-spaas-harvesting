"""Test suite for crawlers"""
# pylint: disable=protected-access

import ftplib
import logging
import os
import re
import requests
import unittest
import unittest.mock as mock
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import ParseResult

import requests

import geospaas_harvesting.crawlers as crawlers


class InvalidMetadataErrorTestCase(unittest.TestCase):
    """Tests for InvalidMetadataError"""
    def test_instanciation(self):
        """Test the correct creation of an InvalidMetadataError object"""
        error = crawlers.InvalidMetadataError('message', missing_fields=('foo', 'bar'))
        self.assertTupleEqual(error.args, ('message',))
        self.assertSetEqual(error.missing_fields, {'foo', 'bar'})

    def test_str(self):
        """Test the string representation of a InvalidMetadataError object"""
        string = str(crawlers.InvalidMetadataError(missing_fields=('foo', 'bar')))
        # we use a set to store missing fields so it can come out in
        # one order or the other
        self.assertIn(string, ('Missing fields: foo,bar', 'Missing fields: bar,foo'))


class DatasetInfoTestCase(unittest.TestCase):
    """Tests for DatasetInfo"""

    def test_instanciation(self):
        """Test the correct creation of a DatasetInfo object"""
        dataset_info = crawlers.DatasetInfo('url', metadata={'foo': 'bar'})
        self.assertEqual(dataset_info.url, 'url')
        self.assertDictEqual(dataset_info.metadata, {'foo': 'bar'})


class NormalizedDatasetInfo(unittest.TestCase):
    """Tests for NormalizedDatasetInfo"""

    def test_instanciation(self):
        """Test the correct creation of a DatasetInfo object"""
        with mock.patch(
                'geospaas_harvesting.crawlers.NormalizedDatasetInfo.check_metadata'
        ) as mock_check_metadata:
            crawlers.NormalizedDatasetInfo('url', metadata={'foo': 'bar'})
        mock_check_metadata.assert_called()

    def test_check_metadata(self):
        """Check that no exception is raised using correct metadata
        """
        metadata = {
            'entry_title': 'title',
            'entry_id': 'id',
            'summary': 'sum-up',
            'time_coverage_start': 'start time',
            'time_coverage_end': 'end time',
            'platform': 'satellite',
            'instrument': 'sar',
            'location_geometry': 'somewhere',
            'provider': 'someone',
            'iso_topic_category': 'ocean',
            'gcmd_location': 'surface',
            'dataset_parameters': ['params'],
        }
        try:
            crawlers.NormalizedDatasetInfo('url', metadata)
        except crawlers.InvalidMetadataError:
            self.fail("InvalidMetadataError should not be raised")

    def test_check_metadata_error(self):
        """Test that an exception is raised in case of invalid metadata
        """
        with self.assertRaises(crawlers.InvalidMetadataError):
            crawlers.NormalizedDatasetInfo('url', {'foo': 'bar'})


class BaseCrawlerTestCase(unittest.TestCase):
    """Tests for the base Crawler"""

    def test_abstract_set_initial_state(self):
        """
        A NotImplementedError should be raised if the set_initial_state() method
        is accessed directly on the Crawler class
        """
        crawler = crawlers.Crawler()
        with self.assertRaises(NotImplementedError):
            crawler.set_initial_state()

    def test_exception_on_iter(self):
        """An exception must be raised if the __iter__ method is not overloaded"""
        base_crawler = crawlers.Crawler()
        with self.assertRaises(NotImplementedError):
            _ = iter(base_crawler)

    def test_iter(self):
        """__iter__() should return self"""
        crawler = crawlers.Crawler()
        crawler.crawl = lambda: []
        self.assertIsInstance(iter(crawler), crawlers.CrawlerIterator)

    def test_http_get_retry(self):
        """Test that _http_get retries the request when a connection
        error or a server error occurs
        """
        http_500_error = requests.HTTPError()
        http_500_error.response = mock.MagicMock(status_code=500)

        with mock.patch('geospaas_harvesting.utils.http_request') as mock_request, \
                mock.patch('time.sleep') as mock_sleep:
            mock_request.side_effect=(
                requests.ConnectionError,
                requests.ConnectTimeout,
                requests.ReadTimeout,
                http_500_error,
                mock.Mock())
            with self.assertLogs(crawlers.Crawler.logger, level=logging.WARNING):
                crawlers.Crawler._http_get('url')

            self.assertEqual(len(mock_request.mock_calls), 5)
            self.assertListEqual(mock_sleep.mock_calls, [mock.call(30 * (2**i)) for i in range(4)])

    def test_http_get_fails_eventually(self):
        """Test that _http_get retries the request when a connection
        error or a server error occurs, then logs an error and returns None
        if the problem persists
        """
        with mock.patch('geospaas_harvesting.utils.http_request') as mock_request, \
                mock.patch('time.sleep') as mock_sleep:
            mock_request.side_effect = requests.ConnectionError

            with self.assertLogs(crawlers.Crawler.logger, level=logging.ERROR):
                self.assertIsNone(crawlers.Crawler._http_get('url'))

            self.assertEqual(len(mock_request.mock_calls), 5)
            self.assertEqual(len(mock_sleep.mock_calls), 5)

    def test_http_get_no_retry_error(self):
        """_http_get should not retry the request if the error is not a
        connection error or a server error
        """
        with mock.patch('geospaas_harvesting.utils.http_request') as mock_request:
            mock_request.side_effect = requests.TooManyRedirects
            with self.assertLogs(crawlers.Crawler.logger, level=logging.ERROR):
                self.assertIsNone(crawlers.Crawler._http_get('url'))

    def test_abstract_get_normalized_attributes(self):
        """get_normalized_attributes() should raise a NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            crawlers.Crawler().get_normalized_attributes({})

    def test_add_url(self):
        """Test adding a dataset's url to its raw attributes dictionary
        """
        raw_attributes = {'bar': 'baz'}
        crawlers.Crawler.add_url('foo', raw_attributes)
        self.assertDictEqual(
            raw_attributes,
            {'url': 'foo', 'bar': 'baz'})

    def test_add_url_already_present(self):
        """Don't add the url if it is already there
        """
        raw_attributes = {'url': 'baz'}
        crawlers.Crawler.add_url('foo', raw_attributes)
        self.assertDictEqual(
            raw_attributes,
            {'url': 'baz'})

class DirectoryCrawlerTestCase(unittest.TestCase):
    """Tests for the DirectoryCrawler"""

    def test_instantiation(self):
        """Test the correct instantiation of a DirectoryCrawler
        """
        crawler = crawlers.DirectoryCrawler(
            'https://foo/bar.nc',
            time_range=(
                datetime(2020, 1, 1, tzinfo=timezone.utc),
                datetime(2020, 1, 2, tzinfo=timezone.utc)),
            include='.*')
        self.assertIsInstance(crawler, crawlers.Crawler)
        self.assertEqual(
            crawler.root_url,
            ParseResult(scheme='https', netloc='foo', path='/bar.nc',
                        params='', query='', fragment=''))
        self.assertEqual(
            crawler.time_range,
            (datetime(2020, 1, 1, tzinfo=timezone.utc),
             datetime(2020, 1, 2, tzinfo=timezone.utc)))
        self.assertListEqual(crawler._results, [])
        self.assertListEqual(crawler._to_process, ['/bar.nc'])

    def test_abstract_list_folder_contents(self):
        """
        A NotImplementedError should be raised if the _list_folder_contents() method
        is accessed directly on the DirectoryCrawler class
        """
        crawler = crawlers.DirectoryCrawler('')
        with self.assertRaises(NotImplementedError):
            crawler._list_folder_contents('')

    def test_is_folder(self):
        """
        A NotImplementedError should be raised if the _is_folder() method
        is accessed directly on the DirectoryCrawler class
        """
        crawler = crawlers.DirectoryCrawler('')
        with self.assertRaises(NotImplementedError):
            crawler._is_folder('')

    def test_get_download_url(self):
        """
        The get_download_url() method of the DirectoryCrawler
        should return the resource URL unchanged
        """
        crawler = crawlers.DirectoryCrawler('https://foo')
        self.assertEqual(crawler.get_download_url('bar'), 'https://foo/bar')

    def test_base_url(self):
        """The base_url property should return the root_url without path"""
        crawler = crawlers.DirectoryCrawler('http://foo/bar')
        self.assertEqual(crawler.base_url, 'http://foo')

    def test_set_initial_state(self):
        """set_initial_state() should set the right values for _urls and _to_process"""
        crawler = crawlers.DirectoryCrawler('http://foo/bar')
        crawler._results = None
        crawler._to_process = None
        crawler.set_initial_state()
        self.assertListEqual(crawler._results, [])
        self.assertListEqual(crawler._to_process, ['/bar'])

    def test_add_url_to_return(self):
        """
        _add_url_to_return() should add the full URL corresponding
        to the path if it fits in the time range constraint
        """
        crawler = crawlers.DirectoryCrawler('http://foo/bar')
        crawler.logger = mock.Mock()
        crawler._add_url_to_return('/bar/baz.nc')
        self.assertListEqual(crawler._results, [crawlers.DatasetInfo('http://foo/bar/baz.nc')])

    def test_add_folder_to_process(self):
        """_add_folder_to_process() should add the path of the folder
        if it fits in the time range constraint
        """
        crawler = crawlers.DirectoryCrawler('http://foo/bar')
        crawler.logger = mock.Mock()
        crawler._to_process = []
        crawler._add_folder_to_process('/bar/baz')
        self.assertListEqual(crawler._to_process, ['/bar/baz'])

    def test_process_folder_with_file(self):
        """_process_folder() should feed the _urls stack
        with only file paths which are included
        """
        crawler = crawlers.DirectoryCrawler('http://foo/bar', include='\.nc$')
        crawler.EXCLUDE = re.compile(r'\.h5$')
        crawler.logger = mock.Mock()
        with mock.patch.object(crawler, '_list_folder_contents') as mock_folder_contents, \
                mock.patch.object(crawler, '_is_folder', return_value=False), \
                mock.patch.object(crawler, '_add_url_to_return') as mock_add_url:
            mock_folder_contents.return_value = ['/bar/baz.nc', '/bar/qux.gz']
            crawler._process_folder('')
        mock_add_url.assert_called_once_with('/bar/baz.nc')

    def test_process_folder_with_folder(self):
        """_process_folder() should feed the _to_process stack
        with folder paths which are not excluded
        """
        crawler = crawlers.DirectoryCrawler('http://foo/bar', include='baz')
        crawler.EXCLUDE = re.compile(r'qux')
        crawler.logger = mock.Mock()
        with mock.patch.object(crawler, '_list_folder_contents') as mock_folder_contents, \
                mock.patch.object(crawler, '_is_folder', return_value=True), \
                mock.patch.object(crawler, '_add_folder_to_process') as mock_add_folder:
            mock_folder_contents.return_value = ['/bar/baz', '/bar/qux']
            crawler._process_folder('')
        mock_add_folder.assert_called_once_with('/bar/baz')

    def test_get_year_folder_coverage(self):
        """Get the correct time range from a year folder"""
        self.assertEqual(
            crawlers.DirectoryCrawler._folder_coverage(
                'https://test-opendap.com/folder/2019/contents.html'),
            (datetime(2019, 1, 1, tzinfo=timezone.utc), datetime(2020, 1, 1, tzinfo=timezone.utc))
        )

    def test_get_month_folder_coverage(self):
        """Get the correct time range from a month folder"""
        self.assertEqual(
            crawlers.DirectoryCrawler._folder_coverage(
                'https://test-opendap.com/folder/2019/02/contents.html'),
            (datetime(2019, 2, 1, tzinfo=timezone.utc), datetime(2019, 3, 1, tzinfo=timezone.utc))
        )
        self.assertEqual(
            crawlers.DirectoryCrawler._folder_coverage(
                'https://test-opendap.com/folder/201902/contents.html'),
            (datetime(2019, 2, 1, tzinfo=timezone.utc), datetime(2019, 3, 1, tzinfo=timezone.utc))
        )

    def test_get_day_of_month_folder_coverage(self):
        """Get the correct time range from a day of month folder"""
        self.assertEqual(
            crawlers.DirectoryCrawler._folder_coverage(
                'https://test-opendap.com/folder/2019/02/14/contents.html'),
            (datetime(2019, 2, 14, tzinfo=timezone.utc), datetime(2019, 2, 15, tzinfo=timezone.utc))
        )
        self.assertEqual(
            crawlers.DirectoryCrawler._folder_coverage(
                'https://test-opendap.com/folder/20190214/contents.html'),
            (datetime(2019, 2, 14, tzinfo=timezone.utc), datetime(2019, 2, 15, tzinfo=timezone.utc))
        )

    def test_get_day_of_year_folder_coverage(self):
        """Get the correct time range from a day of year folder"""
        self.assertEqual(
            crawlers.DirectoryCrawler._folder_coverage(
                'https://test-opendap.com/folder/2019/046/contents.html'),
            (datetime(2019, 2, 15, tzinfo=timezone.utc), datetime(2019, 2, 16, tzinfo=timezone.utc))
        )

    def test_none_when_no_folder_coverage(self):
        """
        The `_folder_coverage` method should return `None` if no time range is inferred from the
        folder's path
        """
        self.assertEqual(
            crawlers.DirectoryCrawler._folder_coverage(
                'https://test-opendap.com/folder/contents.html'), (None, None))
        self.assertEqual(
            crawlers.DirectoryCrawler._folder_coverage(
                'https://test-opendap.com/folder/046/contents.html'),
            (None, None)
        )
        self.assertEqual(
            crawlers.DirectoryCrawler._folder_coverage(
                'https://test-opendap.com/folder/02/contents.html'),
            (None, None)
        )

    def test_intersects_time_range_finite_limits(self):
        """
        Test the behavior of the `_intersects_time_range` method with a finite time range limitation
        `time_range[0]` and `time_range[1]` are the limits defined in the crawler
        `start_time` and `stop_time` are the limits of the time range which is tested against the
        crawler's condition
        """
        crawler = crawlers.DirectoryCrawler(
            '', time_range=(datetime(2019, 2, 14), datetime(2019, 2, 20)))

        # start_time < time_range[0] < stop_time < time_range[1]
        self.assertTrue(crawler._intersects_time_range(
            datetime(2019, 2, 10), datetime(2019, 2, 17)))
        # start_time < time_range[0] == stop_time < time_range[1]
        self.assertTrue(crawler._intersects_time_range(
            datetime(2019, 2, 10), datetime(2019, 2, 14)))
        # time_range[0] < start_time < time_range[1] < stop_time
        self.assertTrue(crawler._intersects_time_range(
            datetime(2019, 2, 17), datetime(2019, 2, 25)))
        # time_range[0] < start_time == time_range[1] < stop_time
        self.assertTrue(crawler._intersects_time_range(
            datetime(2019, 2, 20), datetime(2019, 2, 25)))
        # time_range[0] < start_time < stop_time < time_range[1]
        self.assertTrue(crawler._intersects_time_range(
            datetime(2019, 2, 15), datetime(2019, 2, 19)))
        # start_time < time_range[0] < time_range[1] < stop_time
        self.assertTrue(crawler._intersects_time_range(
            datetime(2019, 2, 13), datetime(2019, 2, 25)))
        # start_time < stop_time < time_range[0] < time_range[1]
        self.assertFalse(crawler._intersects_time_range(
            datetime(2019, 2, 10), datetime(2019, 2, 13)))
        # time_range[0] < time_range[1] < start_time < stop_time
        self.assertFalse(crawler._intersects_time_range(
            datetime(2019, 2, 25), datetime(2019, 2, 26)))
        # no start_time < time_range[0] < time_range[1] < stop_time
        self.assertTrue(crawler._intersects_time_range(None, datetime(2019, 2, 27)))
        # no start_time < time_range[0] < stop_time < time_range[1]
        self.assertTrue(crawler._intersects_time_range(None, datetime(2019, 2, 17)))
        # no start_time < stop_time < time_range[0] < time_range[1]
        self.assertFalse(crawler._intersects_time_range(None, datetime(2019, 2, 10)))
        # start_time < time_range[0] < time_range[1] < no stop time
        self.assertTrue(crawler._intersects_time_range(datetime(2019, 2, 10), None))
        # time_range[0] < start_time < time_range[1] < no stop time
        self.assertTrue(crawler._intersects_time_range(datetime(2019, 2, 18), None))
        # time_range[0] < time_range[1] < start_time < no stop time
        self.assertFalse(crawler._intersects_time_range(datetime(2019, 2, 21), None))

    def test_intersects_time_range_no_lower_limit(self):
        """
        Test the behavior of the `_intersects_time_range` method without a lower limit for the
        crawler's time range.
        `time_range[1]` is the upper limit defined in the crawler
        `start_time` and `stop_time` are the limits of the time range which is tested against the
        crawler's condition
        """
        crawler = crawlers.DirectoryCrawler('', time_range=(None, datetime(2019, 2, 20)))

        # no lower limit < time_range[1] < start_time < stop_time
        self.assertFalse(crawler._intersects_time_range(
            datetime(2019, 2, 25), datetime(2019, 2, 26)))
        # no lower limit < start_time < time_range[1] < stop_time
        self.assertTrue(crawler._intersects_time_range(
            datetime(2019, 2, 18), datetime(2019, 2, 26)))
        # no lower limit < start_time < stop_time < time_range[1]
        self.assertTrue(crawler._intersects_time_range(
            datetime(2019, 2, 18), datetime(2019, 2, 19)))
        # no lower limit and no start time
        self.assertTrue(crawler._intersects_time_range(None, datetime(2019, 2, 21)))
        # no lower limit and no stop_time, with intersection
        self.assertTrue(crawler._intersects_time_range(datetime(2019, 2, 19), None))
        # no lower limit and no stop_time, without intersection
        self.assertFalse(crawler._intersects_time_range(datetime(2019, 2, 21), None))

    def test_intersects_time_range_no_upper_limit(self):
        """
        Test the behavior of the `_intersects_time_range` method without an upper limit for the
        crawler's time range.
        `time_range[0]` is the upper limit defined in the crawler
        `start_time` and `stop_time` are the limits of the time range which is tested against the
        crawler's condition
        """
        crawler = crawlers.DirectoryCrawler('', time_range=(datetime(2019, 2, 20), None))

        # start_time < stop_time < time_range[0] < no upper limit
        self.assertFalse(crawler._intersects_time_range(
            datetime(2019, 2, 10), datetime(2019, 2, 15)))
        # start_time < time_range[0] < stop_time < no upper limit
        self.assertTrue(crawler._intersects_time_range(
            datetime(2019, 2, 18), datetime(2019, 2, 26)))
        # time_range[0] < start_time < stop_time < no upper limit
        self.assertTrue(crawler._intersects_time_range(
            datetime(2019, 2, 21), datetime(2019, 2, 25)))
        # no upper limit and no stop_time
        self.assertTrue(crawler._intersects_time_range(datetime(2019, 2, 21), None))
        # no upper limit and no start_time, with intersection
        self.assertTrue(crawler._intersects_time_range(None, datetime(2019, 2, 21)))
        # no upper limit and no start_time, without intersection
        self.assertFalse(crawler._intersects_time_range(None, datetime(2019, 2, 19)))

    def test_abstract_get_normalized_attributes(self):
        """get_normalized_attributes is abstract in DirectoryCrawler"""
        with self.assertRaises(NotImplementedError):
            crawlers.DirectoryCrawler('').get_normalized_attributes(mock.Mock())

    def test_crawl(self):
        """Test crawling"""
        crawler = crawlers.DirectoryCrawler('https://foo/bar.nc')
        crawler._results = ['foo', 'bar']

        with mock.patch.object(crawler, '_process_folder') as mock_process_folder:
            generator = crawler.crawl()
            self.assertEqual(next(generator), 'bar')
            self.assertEqual(next(generator), 'foo')
            self.assertListEqual(crawler._results, [])
            with self.assertRaises(StopIteration):
                next(generator)
            mock_process_folder.assert_called()


class LocalDirectoryCrawlerTestCase(unittest.TestCase):
    """Tests for LocalDirectoryCrawler"""

    def setUp(self):
        self.crawler = crawlers.LocalDirectoryCrawler('')

    def test_list_folder_contents(self):
        """_list_folder_contents() should return the absolute
        path of all files contained in the folder"""
        with mock.patch('os.listdir', return_value=['foo', 'bar', 'baz']), \
                mock.patch.object(self.crawler, '_is_folder', return_value=True):
            base_dir_name = 'base_dir'
            self.assertListEqual(
                self.crawler._list_folder_contents(base_dir_name),
                [
                    os.path.join(base_dir_name, 'foo'),
                    os.path.join(base_dir_name, 'bar'),
                    os.path.join(base_dir_name, 'baz'),
                ]
            )

    def test_list_folder_contents_file_path(self):
        """When given a file path, _list_folder_contents() should
        return a list containing only this file path
        """
        with mock.patch.object(self.crawler, '_is_folder', return_value=False):
            file_path = '/foo/bar.nc'
            self.assertListEqual(
                self.crawler._list_folder_contents(file_path),
                [file_path]
            )

    def test_is_folder(self):
        """_is_folder() should return True if the
        path points to a folder, False otherwise"""
        with mock.patch('os.path.isdir', return_value=True):
            self.assertTrue(self.crawler._is_folder(''), "_is_folder() should return True")
        with mock.patch('os.path.isdir', return_value=False):
            self.assertFalse(self.crawler._is_folder(''), "_is_folder() should return False")

    def test_abstract_get_normalized_attributes(self):
        """get_normalized_attributes is abstract in LocalDirectoryCrawler"""
        with self.assertRaises(NotImplementedError):
            crawlers.LocalDirectoryCrawler('').get_normalized_attributes(mock.Mock())


class HTMLDirectoryCrawlerTestCase(unittest.TestCase):
    """Tests for the HTMLDirectoryCrawler crawler"""

    def test_prepend_parent_path(self):
        """
        Should prepend all the paths with the parent_path, except if they already start with it
        """
        parent_path = '/foo'
        paths = ['/foo/bar', 'baz']
        self.assertEqual(
            crawlers.HTMLDirectoryCrawler._prepend_parent_path(parent_path, paths),
            ['/foo/bar', '/foo/baz']
        )

    def test_abstract_get_normalized_attributes(self):
        """The get_normalized_attribute is abstract in
        HTMLDirectoryCrawler
        """
        with self.assertRaises(NotImplementedError):
            crawlers.HTMLDirectoryCrawler('').get_normalized_attributes(None)


class OpenDAPCrawlerTestCase(unittest.TestCase):
    """Tests for the OpenDAP crawler"""

    TEST_DATA = {
        'root': {
            'urls': ["https://test-opendap.com"],
            'file_path': "data/opendap/root.html"},
        'root_duplicates': {
            'urls': ["https://test2-opendap.com"],
            'file_path': "data/opendap/root_duplicates.html"},
        'dataset': {
            'urls': [
                'https://test-opendap.com/dataset.nc',
                'https://test2-opendap.com/dataset.nc',
                'https://test-opendap.com/folder/dataset.nc',
                'https://test-opendap.com/folder/2019/02/14/20190214120000_dataset.nc',
                'https://test-opendap.com/folder/2019/02/14/20190214000000_dataset.nc'
            ],
            'file_path': None},
        'full_ddx': {
            'urls': ["https://opendap.jpl.nasa.gov/opendap/full_dataset.nc.ddx"],
            'file_path': "data/opendap/full_ddx.xml"},
        'short_ddx': {
            'urls': ["https://test-opendap.com/short_dataset.nc.ddx"],
            'file_path': "data/opendap/short_ddx.xml"},
        'no_ns_ddx': {
            'urls': ["https://test-opendap.com/no_ns_dataset.nc.ddx"],
            'file_path': "data/opendap/ddx_no_ns.xml"},
        'folder': {
            'urls': [
                'https://test-opendap.com/folder/contents.html',
                'https://test2-opendap.com/folder/contents.html'
            ],
            'file_path': 'data/opendap/folder/contents.html'},
        'folder_year': {
            'urls': ['https://test-opendap.com/folder/2019/contents.html'],
            'file_path': 'data/opendap/folder/2019/contents.html'},
        'folder_month': {
            'urls': ['https://test-opendap.com/folder/2019/02/contents.html'],
            'file_path': 'data/opendap/folder/2019/02/contents.html'},
        'folder_day_of_month': {
            'urls': ['https://test-opendap.com/folder/2019/02/14/contents.html'],
            'file_path': 'data/opendap/folder/2019/02/14/contents.html'},
        'folder_day_of_year': {
            'urls': ['https://test-opendap.com/folder/2019/046/contents.html'],
            'file_path': 'data/opendap/folder/2019/046/contents.html'},
        'empty': {
            'urls': ['http://empty.com'],
            'file_path': 'data/empty.html'},
        'inexistent': {
            'urls': ['http://random.url'],
            'file_path': None}
    }

    def request_side_effect(self, method, url, **kwargs):
        """Side effect function used to mock calls to requests.get().text"""
        if method != 'GET':
            return None

        data_file_relative_path = None
        for test_data in self.TEST_DATA.values():
            if url in test_data['urls']:
                data_file_relative_path = test_data['file_path']
                break

        response = requests.Response()

        if data_file_relative_path:
            # Open data file as binary stream so it can be used to mock a requests response
            data_file = open(os.path.join(os.path.dirname(__file__), data_file_relative_path), 'rb')
            # Store opened files so they can be closed when the test is finished
            self.opened_files.append(data_file)

            response.status_code = 200
            response.raw = data_file
        else:
            response.status_code = 404

        return response

    def setUp(self):
        # Mock requests.request()
        self.patcher_request = mock.patch('geospaas_harvesting.crawlers.utils.http_request')
        self.mock_request = self.patcher_request.start()
        self.mock_request.side_effect = self.request_side_effect

        # Initialize a list of opened files which will be closed in tearDown()
        self.opened_files = []

    def tearDown(self):
        self.patcher_request.stop()
        # Close any files opened during the test
        for opened_file in self.opened_files:
            opened_file.close()

    def test_get_correct_html_contents(self):
        """Test that the _http_get() method returns the correct HTML string"""
        data_file = open(os.path.join(os.path.dirname(__file__), 'data/opendap/root.html'))
        html = data_file.read()
        data_file.close()

        html_from_method = crawlers.OpenDAPCrawler._http_get(self.TEST_DATA['root']['urls'][0])

        self.assertEqual(html, html_from_method)

    @mock.patch('logging.Logger.error')
    def test_get_html_logs_error_on_http_status(self, mock_error_logger):
        """Test that an exception is raised in case of HTTP error code"""
        _ = crawlers.OpenDAPCrawler._http_get(self.TEST_DATA['inexistent']['urls'][0])
        mock_error_logger.assert_called_once()

    def test_get_right_number_of_links(self):
        """Test that the crawler gets the correct number of links from a test page"""
        links = {}
        for sample in ('root', 'empty'):
            data_file = open(os.path.join(
                os.path.dirname(__file__),
                self.TEST_DATA[sample]['file_path']))
            html = data_file.read()
            data_file.close()
            links[sample] = crawlers.OpenDAPCrawler._get_links(html)

        self.assertEqual(len(links['root']), 4)
        self.assertEqual(len(links['empty']), 0)

    def test_link_extractor_error(self):
        """In case of error, LinkExtractor must use a logger"""
        parser = crawlers.LinkExtractor()
        with self.assertLogs(parser.logger, level=logging.ERROR):
            parser.error('some message')

    def test_process_folder(self):
        """
        Explore root page and make sure the _url and _to_process attributes of the crawler have the
        right values
        """
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root']['urls'][0], include=r'\.nc$')
        with self.assertLogs(crawler.logger, level=logging.DEBUG):
            crawler._process_folder(crawler._to_process.pop())
        self.assertListEqual(
            crawler._results,
            [crawlers.DatasetInfo(self.TEST_DATA['dataset']['urls'][0])])
        self.assertListEqual(crawler._to_process, ['/folder/contents.html'])

    def test_process_folder_with_duplicates(self):
        """If the same URL is present twice in the page, it should only be processed once"""
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root_duplicates']['urls'][0],
        include='\.nc$')
        with self.assertLogs(crawler.logger, level=logging.DEBUG):
            crawler._process_folder(crawler._to_process.pop())
        self.assertListEqual(
            crawler._results,
            [crawlers.DatasetInfo(self.TEST_DATA['dataset']['urls'][1])])
        self.assertListEqual(crawler._to_process, ['/folder/contents.html'])

    def test_process_folder_with_time_restriction(self):
        """
        Process a folder and make sure the _url and _to_process
        attributes of the crawler have the right values according to a
        time restriction.
        Since the precision of the time restriction is limited to the
        folder level for DirectoryCrawlers, all datasets in a folder
        whose time coverage intersects the crawler's time range are
        selected, even if the timestamp of a dataset does not intersect
        the crawler's time range.
        """
        crawler = crawlers.OpenDAPCrawler(
            self.TEST_DATA['folder_day_of_year']['urls'][0], include=r'\.nc$',
            time_range=(datetime(2019, 2, 15, 11, 0, 0), datetime(2019, 2, 15, 13, 0, 0)))
        with self.assertLogs(crawler.logger, level=logging.DEBUG):
            crawler._process_folder(crawler._to_process.pop())
        self.assertListEqual(
            crawler._results,
            [
                crawlers.DatasetInfo(
                    'https://test-opendap.com/folder/2019/046/20190215000000_dataset.nc'),
                crawlers.DatasetInfo(
                    'https://test-opendap.com/folder/2019/046/20190215120000_dataset.nc'),
            ]
        )
        self.assertListEqual(crawler._to_process, [])

    def test_get_xml_namespace(self):
        """Get xml namespace from the test data DDX file"""
        test_file_path = os.path.join(
            os.path.dirname(__file__),
            self.TEST_DATA['short_ddx']['file_path'])

        with open(test_file_path, 'rb') as test_file:
            root = ET.parse(test_file).getroot()

        self.assertEqual(
            crawlers.OpenDAPCrawler('')._get_xml_namespace(root),
            'http://xml.opendap.org/ns/DAP/3.2#')

    def test_logging_if_no_xml_namespace(self):
        """A warning must be logged if no namespace has been found, and an empty string returned"""
        test_file_path = os.path.join(
            os.path.dirname(__file__),
            self.TEST_DATA['no_ns_ddx']['file_path'])

        with open(test_file_path, 'rb') as test_file:
            root = ET.parse(test_file).getroot()

        crawler = crawlers.OpenDAPCrawler('')
        with self.assertLogs(crawler.logger, level=logging.WARNING):
            namespace = crawlers.OpenDAPCrawler('')._get_xml_namespace(root)
        self.assertEqual(namespace, '')

    def test_extract_global_attributes(self):
        """Get nc_global attributes from the test data DDX file"""
        test_file_path = os.path.join(
            os.path.dirname(__file__),
            self.TEST_DATA['short_ddx']['file_path'])

        with open(test_file_path, 'rb') as test_file:
            root = ET.parse(test_file).getroot()

        self.assertDictEqual(
            crawlers.OpenDAPCrawler('')._extract_attributes(root),
            {
                'Conventions': 'CF-1.7, ACDD-1.3',
                'raw_dataset_parameters': [],
                'title': 'VIIRS L2P Sea Surface Skin Temperature'
            }
        )

    def test_get_normalized_attributes(self):
        """Test that the correct attributes are extracted from a DDX file"""
        with mock.patch(
                'geospaas_harvesting.crawlers.MetadataHandler.get_parameters') as mock_get_params:
            _ = crawlers.OpenDAPCrawler('').get_normalized_attributes(
                crawlers.DatasetInfo("https://opendap.jpl.nasa.gov/opendap/full_dataset.nc"))
        mock_get_params.assert_called_with({
                'Conventions': 'CF-1.7, ACDD-1.3',
                'title': 'VIIRS L2P Sea Surface Skin Temperature',
                'summary': (
                    "Sea surface temperature (SST) retrievals produced at the NASA OBPG for the "
                    "Visible Infrared Imaging\n                Radiometer Suite (VIIRS) sensor on "
                    "the Suomi National Polar-Orbiting Partnership (Suomi NPP) platform.\n         "
                    "       These have been reformatted to GHRSST GDS version 2 Level 2P "
                    "specifications by the JPL PO.DAAC. VIIRS\n                SST algorithms "
                    "developed by the University of Miami, RSMAS"),
                'references': 'GHRSST Data Processing Specification v2r5',
                'institution': (
                    "NASA Jet Propulsion Laboratory"
                    " (JPL) Physical Oceanography Distributed Active Archive Center\n              "
                    "  (PO.DAAC)/NASA Goddard Space Flight Center (GSFC), Ocean Biology Processing "
                    "Group (OBPG)/University of\n                Miami Rosential School of Marine "
                    "and Atmospheric Science (RSMAS)"),
                'history': ("VIIRS L2P created at JPL PO.DAAC"
                    " by combining OBPG SNPP_SST and SNPP_SST3, and outputing to the\n             "
                    "   GHRSST GDS2 netCDF file format"),
                'comment': ("L2P Core without DT analysis "
                    "or other ancillary fields; Day, Start Node:Ascending, End\n                "
                    "Node:Ascending; WARNING Some applications are unable to properly handle signed"
                    " byte values. If values\n                are encountered > 127, please "
                    "subtract 256 from this reported value; Quicklook"),
                'license': 'GHRSST and PO.DAAC protocol allow data use as free and open.',
                'id': 'VIIRS_NPP-JPL-L2P-v2016.2',
                'naming_authority': 'org.ghrsst',
                'product_version': '2016.2',
                'uuid': 'b6ac7651-7b02-44b0-942b-c5dc3c903eba',
                'gds_version_id': '2.0',
                'netcdf_version_id': '4.1',
                'date_created': '20200101T211816Z',
                'file_quality_level': '3',
                'spatial_resolution': '750 m',
                'start_time': '20200101T000001Z',
                'time_coverage_start': '20200101T000001Z',
                'stop_time': '20200101T000559Z',
                'time_coverage_end': '20200101T000559Z',
                'northernmost_latitude': '9.47472000',
                'southernmost_latitude': '-15.3505001',
                'easternmost_longitude': '-142.755005',
                'westernmost_longitude': '-175.084000',
                'geospatial_lat_max': '9.47472000',
                'geospatial_lat_min': '-15.3505001',
                'geospatial_lon_max': '-142.755005',
                'geospatial_lon_min': '-175.084000',
                'source': ("VIIRS sea surface temperature observations from the Ocean Biology "
                           "Processing Group (OBPG)"),
                'platform': 'Suomi-NPP',
                'sensor': 'VIIRS',
                'metadata_link': (
                    'http://podaac.jpl.nasa.gov/ws/metadata/dataset/?format=iso&shortName='
                    'VIIRS_NPP-JPL-L2P-v2016.2\n            '),
                'keywords': (
                    'Oceans > Ocean Temperature > Sea Surface Temperature > '
                    'Skin Sea Surface Temperature'),
                'keywords_vocabulary': ('NASA Global Change Master Directory (GCMD) Science '
                                        'Keywords'),
                'standard_name_vocabulary': 'NetCDF Climate and Forecast (CF) Metadata Conventions',
                'geospatial_lat_units': 'degrees_north',
                'geospatial_lat_resolution': '0.00749999983',
                'geospatial_lon_units': 'degrees_east',
                'geospatial_lon_resolution': '0.00749999983',
                'acknowledgment': (
                    'The VIIRS L2P sea surface temperature data are sponsored by NASA'),
                'creator_name': 'JPL PO.DAAC',
                'creator_email': 'ghrsst@jpl.nasa.gov',
                'creator_url': 'http://podaac.jpl.nasa.gov',
                'project': 'Group for High Resolution Sea Surface Temperature',
                'publisher_name': 'The GHRSST Project Office',
                'publisher_url': 'http://www.ghrsst.org',
                'publisher_email': 'ghrsst-po@nceo.ac.uk',
                'processing_level': 'L2P',
                'cdm_data_type': 'swath',
                'startDirection': 'Ascending',
                'endDirection': 'Ascending',
                'day_night_flag': 'Day',
                'raw_dataset_parameters': ['sea_ice_area_fraction'],
                'url': 'https://opendap.jpl.nasa.gov/opendap/full_dataset.nc',
            })

    def test_get_ddx_url(self):
        """Test utility function which transforms download links into
        metadata links for OpenDAP
        """
        self.assertEqual(
            crawlers.OpenDAPCrawler.get_ddx_url('https://foo/bar.nc.ddx'),
            'https://foo/bar.nc.ddx')
        self.assertEqual(
            crawlers.OpenDAPCrawler.get_ddx_url('https://foo/bar.nc'),
            'https://foo/bar.nc.ddx')
        self.assertEqual(
            crawlers.OpenDAPCrawler.get_ddx_url('https://foo/bar.nc.dods'),
            'https://foo/bar.nc.ddx')


class ThreddsCrawlerTestCase(unittest.TestCase):
    """Tests for the Thredds crawler"""

    @mock.patch("geospaas_harvesting.crawlers.ThreddsCrawler._http_get")
    @mock.patch("geospaas_harvesting.crawlers.ThreddsCrawler._get_links")
    def test_get_download_url(self, mock_get_link, mock_http_get):
        """
        Test the functionality of "get_download_url" method for OpenDAP crawler of OSISAF project
        """
        mock_get_link.return_value = [
            '/thredds/dodsC/osisaf/met.no/ice/amsr2_conc/2019/11/'
            'ice_conc_nh_polstere-100_amsr2_201911301200.nc.html',
            '/thredds/fileServer/osisaf/met.no/ice/amsr2_conc/2019/11/'
            'ice_conc_nh_polstere-100_amsr2_201911301200.nc'
        ]
        # The value of this variable is not used in this test, it is here for reference
        catalog_url = (
            'https://thredds.met.no/thredds/catalog/osisaf/met.no/ice/amsr2_conc/2019/11/'
            'catalog.html?dataset=osisaf/met.no/ice/amsr2_conc/2019/11/'
            'ice_conc_nh_polstere-100_amsr2_201911301200.nc'
        )
        crawler = crawlers.ThreddsCrawler('https://thredds.met.no/thredds/osisaf/osisaf.html')
        request_link = crawler.get_download_url(catalog_url)
        self.assertEqual(
            request_link,
            'https://thredds.met.no/thredds/fileServer/osisaf/met.no/ice/amsr2_conc/2019/11/'
            'ice_conc_nh_polstere-100_amsr2_201911301200.nc'
        )

    @mock.patch("geospaas_harvesting.crawlers.ThreddsCrawler._http_get")
    @mock.patch("geospaas_harvesting.crawlers.ThreddsCrawler._get_links")
    def test_get_download_url_no_direct_download_link(self, mock_get_link, mock_http_get):
        """
        The get_download_url() method of the Thredds crawler
        must return None if no valid download URL is found
        """
        mock_get_link.return_value = ['/thredds/dodsC/osisaf/met.no/ice_conc201911301200.nc.dods']
        self.assertIsNone(crawlers.ThreddsCrawler('').get_download_url("dummy"))

    def test_get_ddx_url(self):
        """Test utility function which transforms download links into
        metadata links for Thredds
        """
        self.assertEqual(
            crawlers.ThreddsCrawler.get_ddx_url(
                'https://thredds.met.no/thredds/fileServer/osisaf/met.no/ice/conc/2023/01/'
                'ice_conc_sh_polstere-100_multi_202301141200.nc'),
            'https://thredds.met.no/thredds/dodsC/osisaf/met.no/ice/conc/2023/01/'
            'ice_conc_sh_polstere-100_multi_202301141200.nc.ddx')

    def test_get_ddx_url_error(self):
        """get_ddx_url() should raise an exception when the provided
        URL is not a Thredds fileserver URL
        """
        with self.assertRaises(ValueError):
            crawlers.ThreddsCrawler.get_ddx_url('https://foo/bar.nc')


class HTTPPaginatedAPICrawlerTestCase(unittest.TestCase):
    """Tests for the HTTPPaginatedAPICrawler base class"""

    def test_get_page_size(self):
        """Test page_size getter"""
        with mock.patch(
                'geospaas_harvesting.crawlers.HTTPPaginatedAPICrawler.PAGE_SIZE_NAME', 'size'):
            self.assertEqual(
                crawlers.HTTPPaginatedAPICrawler('https://foo', page_size=10).page_size,
                10)

    def test_get_page_offset(self):
        """Test page_offset getter"""
        self.assertEqual(crawlers.HTTPPaginatedAPICrawler('foo', initial_offset=10).page_offset, 10)

    def test_set_page_offset(self):
        """Test page_offset setter"""
        crawler = crawlers.HTTPPaginatedAPICrawler('https://foo')
        crawler.page_offset = 12
        self.assertEqual(crawler.page_offset, 12)

    def test_set_initial_state(self):
        """Test that set_initial_state correctly resets the crawler"""
        crawler = crawlers.HTTPPaginatedAPICrawler('https://foo')
        # Set non-default offset and _urls values
        crawler.request_parameters['params'][crawler.PAGE_OFFSET_NAME] = 200
        crawler._results = ['url1', 'url2']

        crawler.set_initial_state()
        self.assertEqual(
            crawler.request_parameters['params'][crawler.PAGE_OFFSET_NAME], crawler.initial_offset)
        self.assertListEqual(crawler._results, [])

    def test_get_next_page(self):
        """_get_next_page() should get the page at the current offset,
        then increment the offset
        """
        crawler = crawlers.HTTPPaginatedAPICrawler('https://foo')
        with mock.patch.object(crawler, '_http_get', return_value='foo'), \
                self.assertLogs(crawler.logger, level=logging.DEBUG):
            self.assertEqual(crawler._get_next_page(), 'foo')
            self.assertEqual(crawler.request_parameters['params'][crawler.PAGE_OFFSET_NAME], 1)

    def test_abstract_get_datasets_info(self):
        """_get_datasets_info() should raise a NotImplementedError
        when called directly from HTTPPaginatedAPICrawler
        """
        crawler = crawlers.HTTPPaginatedAPICrawler('foo')
        with self.assertRaises(NotImplementedError):
            crawler._get_datasets_info('')

    def test_abstract_get_normalized_attributes(self):
        """get_normalized_attributes() should raise a NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            crawlers.HTTPPaginatedAPICrawler('https://foo').get_normalized_attributes({})

    def test_crawl(self):
        """Test the crawling mechanism for HTTP paginated APIs"""
        crawler = crawlers.HTTPPaginatedAPICrawler('https://foo')
        crawler._results = [crawlers.DatasetInfo('bar')]
        with mock.patch.object(crawler, '_get_datasets_info', return_value=False), \
                mock.patch.object(crawler, '_get_next_page'):
            self.assertListEqual(list(crawler.crawl()), [crawlers.DatasetInfo('bar')])


class FTPCrawlerTestCase(unittest.TestCase):
    """Tests for the FTP crawler"""

    def emulate_cwd_of_ftp(self, name):
        """passes in the case of "", ".." or "folder_name" in order to resemble the behavior of cwd
        of ftplib. Otherwise (encountering a filename) raise the proper exception """
        if name not in ["..", "folder_name", ""]:
            raise ftplib.error_perm

    @mock.patch('ftplib.FTP', autospec=True)
    def test_ftp_correct_navigation(self, mock_ftp):
        """check that file URLs and folders paths are added to the right stacks"""

        test_crawler = crawlers.FTPCrawler('ftp://foo', include='\.gz$')
        test_crawler.ftp.nlst.return_value = ['file1.gz', 'folder_name', 'file3.bb', 'file2.gz', ]
        test_crawler.ftp.cwd = self.emulate_cwd_of_ftp
        test_crawler.ftp.host = ''
        with self.assertLogs('geospaas_harvesting.crawlers.FTPCrawler', level=logging.DEBUG):
            test_crawler._process_folder('')
        # '.gz' files must be in the "_urls" list
        # Other type of files should not be in the "_urls" list
        self.assertEqual(
            test_crawler._results,
            [
                crawlers.DatasetInfo('ftp://foo/file1.gz'),
                crawlers.DatasetInfo('ftp://foo/file2.gz')
            ])
        # folder with 'folder_name' must be in the "_to_process" list
        self.assertCountEqual(['/', 'folder_name'], test_crawler._to_process)

    @mock.patch('geospaas_harvesting.crawlers.ftplib.FTP.login')
    def test_ftp_correct_exception(self, mock_ftp):
        """set_initial_state() should not raise an error in case of
        503 or 230 responses from FTP.login(), but it should for
        other error codes.
        """

        test_crawler = crawlers.FTPCrawler(
            'ftp://', username="d", password="d", include='\.gz$')

        mock_ftp.side_effect = ftplib.error_perm("503")
        test_crawler.set_initial_state()

        mock_ftp.side_effect = ftplib.error_perm("230")
        test_crawler.set_initial_state()

        mock_ftp.side_effect = ftplib.error_perm("999")
        with self.assertRaises(ftplib.error_perm):
            test_crawler.set_initial_state()

    def test_ftp_incorrect_entry(self):
        """Shall return 'ValueError' when there is an incorrect entry in ftp address of
        the configuration file """
        with self.assertRaises(ValueError):
            crawlers.FTPCrawler('ft:///')

    def test_retry_on_timeout_decorator_timeout_error(self):
        """The retry_on_timeout decorator should re-create
        the connection when a FTP timeout error occurs, and
        and re-run the method in which the error occurred once
        """
        with mock.patch('ftplib.FTP'):
            crawler = crawlers.FTPCrawler('ftp://foo')
            crawler.ftp.nlst.side_effect = ftplib.error_temp('421')

            with self.assertRaises(ftplib.error_temp), \
                 self.assertLogs(crawler.logger, level=logging.INFO) as log_cm:
                crawler._list_folder_contents('/')

            self.assertEqual(log_cm.records[0].getMessage(), "Re-initializing the FTP connection")

    def test_retry_on_timeout_decorator_connection_error(self):
        """The retry_on_timeout decorator should try to re-create
        the when a connection error occurs, and re-run the method
        in which the error occurred 5 times
        """
        with mock.patch('ftplib.FTP'):
            crawler = crawlers.FTPCrawler('ftp://foo')

            for error in (ConnectionError, ConnectionRefusedError, ConnectionResetError):
                crawler.ftp.nlst.side_effect = error

                with mock.patch.object(crawler, 'connect') as mock_connect:
                    with self.assertRaises(error), \
                        self.assertLogs(crawler.logger, level=logging.INFO):
                        crawler._list_folder_contents('/')
                self.assertEqual(mock_connect.call_count, 5)

    def test_no_retry_on_non_timeout_ftp_errors(self):
        """FTP errors other than timeouts should not trigger a retry"""
        with mock.patch('ftplib.FTP'):
            crawler = crawlers.FTPCrawler('ftp://foo')
            crawler.ftp.nlst.side_effect = ftplib.error_temp('422')

            with mock.patch.object(crawler, 'connect') as mock_connect:
                with self.assertRaises(ftplib.error_temp):
                    crawler._list_folder_contents('/')
                mock_connect.assert_not_called()

    def test_getstate(self):
        """Test pickling an FTPCrawler"""
        with mock.patch('ftplib.FTP', return_value=mock.Mock(spec_set=ftplib.FTP)):
            crawler = crawlers.FTPCrawler('ftp://foo/bar')
        expected_result = crawler.__dict__.copy()
        expected_result['ftp'] = None
        self.assertDictEqual(crawler.__getstate__(), expected_result)

    def test_setstate(self):
        """Test unpickling an FTPCrawler"""
        state = {
            '_metadata_handler': crawlers.MetadataHandler(crawlers.GeoSPaaSMetadataNormalizer),
            '_results': [],
            '_to_process': ['/bar'],
            'ftp': None,
            'include': None,
            'max_threads': 1,
            'password': 'anonymous',
            'root_url': ParseResult(
                scheme='ftp', netloc='foo', path='/bar', params='', query='', fragment=''),
            'time_range': (None, None),
            'username': 'anonymous'
        }
        ftp_mock = mock.Mock(spec_set=ftplib.FTP)
        with mock.patch('ftplib.FTP', return_value=ftp_mock):
            crawler = crawlers.FTPCrawler.__new__(crawlers.FTPCrawler)
            crawler.__setstate__(state)
        expected_result = state.copy()
        expected_result['ftp'] = ftp_mock
        self.assertDictEqual(crawler.__dict__, expected_result)

    def test_get_normalized_attributes(self):
        """Test that the attributes are gotten using metanorm, and the
        geospaas_service attributes are set to 'ftp'
        """
        with mock.patch('ftplib.FTP', return_value=mock.Mock(spec_set=ftplib.FTP)):
            crawler = crawlers.FTPCrawler('ftp://foo')
        with mock.patch.object(crawler, '_metadata_handler') as mock_handler:
            mock_handler.get_parameters.return_value = {'foo': 'bar'}
            self.assertDictEqual(
                    crawler.get_normalized_attributes(crawlers.DatasetInfo('ftp://uri')),
                    {
                        'foo': 'bar',
                        'geospaas_service_name': 'ftp',
                        'geospaas_service': 'ftp'
                    })
            mock_handler.get_parameters.assert_called_once_with({'url': 'ftp://uri'})
