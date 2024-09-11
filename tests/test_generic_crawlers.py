"""Test suite for crawlers"""
# pylint: disable=protected-access

import ftplib
import io
import logging
import os
import pickle
import re
import shutil
import tempfile
import threading
import unittest
import unittest.mock as mock
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import ParseResult

import requests

import geospaas.catalog.managers
import geospaas_harvesting.crawlers as crawlers


class DatasetInfoTestCase(unittest.TestCase):
    """Tests for DatasetInfo"""

    def test_instanciation(self):
        """Test the correct creation of a DatasetInfo object"""
        dataset_info = crawlers.DatasetInfo('url', metadata={'foo': 'bar'})
        self.assertEqual(dataset_info.url, 'url')
        self.assertDictEqual(dataset_info.metadata, {'foo': 'bar'})

    def test_equality(self):
        """Test equality between two DatasetInfo objects"""
        self.assertEqual(
            crawlers.DatasetInfo('foo', {'bar': 'baz'}),
            crawlers.DatasetInfo('foo', {'bar': 'baz'}))
        self.assertNotEqual(
            crawlers.DatasetInfo('foo', {'bar': 'baz'}),
            crawlers.DatasetInfo('foo', {'bar': 'quz'}))

    def test_representation(self):
        """Test string reprensentation of DatasetInfo objects"""
        self.assertEqual(
            repr(crawlers.DatasetInfo('https://foo', {'a': 1})),
            "DatasetInfo(url='https://foo', metadata={'a': 1})")


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

    def test_iter(self):
        """__iter__() should return self"""
        crawler = crawlers.Crawler()
        crawler.crawl = lambda: []
        self.assertIsInstance(iter(crawler), crawlers.CrawlerIterator)

    def test_abstract_crawl(self):
        """The crawl method should raise a NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            crawlers.Crawler().crawl()

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
                crawlers.Crawler()._http_get('url', max_tries=5, wait_time=30)

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

            with self.assertLogs(crawlers.Crawler.logger, level=logging.WARNING), \
                 self.assertRaises(RuntimeError):
                crawlers.Crawler()._http_get('url')

            self.assertEqual(len(mock_request.mock_calls), 5)
            self.assertEqual(len(mock_sleep.mock_calls), 5)

    def test_http_get_no_retry_error(self):
        """_http_get should not retry the request if the error is not a
        connection error or a server error
        """
        with mock.patch('geospaas_harvesting.utils.http_request') as mock_request:
            mock_request.side_effect = requests.TooManyRedirects
            with self.assertRaises(requests.RequestException):
                self.assertIsNone(crawlers.Crawler()._http_get('url'))

    def test_http_get_error_on_404_status(self):
        """Test that an exception is raised in case of HTTP error code"""
        response = requests.Response()
        response.status_code = 404
        with mock.patch('geospaas_harvesting.utils.http_request') as mock_request:
            mock_request.side_effect = requests.HTTPError(response=response)
            with self.assertRaises(requests.HTTPError):
                crawlers.Crawler()._http_get('http://foo')

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


class CrawlerIteratorTestCase(unittest.TestCase):
    """Tests for CrawlerIterator.
    """

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.old_ingestion_path = crawlers.CrawlerIterator.FAILED_INGESTIONS_PATH
        self.old_max_failed = crawlers.CrawlerIterator.MAX_FAILED
        crawlers.CrawlerIterator.FAILED_INGESTIONS_PATH = self.tmp_dir
        crawlers.CrawlerIterator.MAX_FAILED = 2

    def tearDown(self):
        crawlers.CrawlerIterator.FAILED_INGESTIONS_PATH = self.old_ingestion_path
        crawlers.CrawlerIterator.MAX_FAILED = self.old_max_failed
        shutil.rmtree(self.tmp_dir)

    class TestCrawler(crawlers.Crawler):
        """Crawler used for testing the CrawlerIterator"""

        def crawl(self):
            for url in ['https://foo', 'https://bar', 'https://baz']:
                yield crawlers.DatasetInfo(url)

        def set_initial_state(self):
            pass

        def get_normalized_attributes(self, dataset_info, **kwargs):
            #used for testing error management
            if dataset_info.url == 'https://bar':
                raise RuntimeError()
            elif dataset_info.url == 'https://baz':
                # bypass the broad exception catch in
                # _thread_get_normalized_attributes() to check handling
                # of exceptions happening in that method
                raise BaseException() # pylint: disable=broad-exception-raised

            return {'foo': 'bar'}

    def test_iterating(self):
        """Test iterating over normalization results"""
        crawler = self.TestCrawler()
        with self.assertLogs(crawlers.CrawlerIterator.logger, level=logging.ERROR) as scm:
            crawler_iterator = iter(crawler)
            crawler_iterator.manager_thread.join()

        self.assertIs(scm.records[0].exc_info[0], RuntimeError)
        self.assertIs(scm.records[1].exc_info[0], BaseException)

        results = list(crawler_iterator)

        self.assertListEqual(results, [crawlers.DatasetInfo('https://foo', {'foo': 'bar'})])

        failed_ingestion_files = os.listdir(self.tmp_dir)
        self.assertEqual(len(failed_ingestion_files), 1)
        self.assertTrue(failed_ingestion_files[0].endswith(crawler_iterator.RECOVERY_SUFFIX))

    def test_pickle_list_elements(self):
        """Test pickling a list of objects"""
        # create a crawler iterator without starting the processing threads
        with mock.patch('threading.Thread'):
            crawler_iterator = iter(self.TestCrawler())
        objects_to_pickle = [1, 'one', 2.2]
        reference = list(objects_to_pickle)  # needed because the list will be cleared
        with tempfile.TemporaryDirectory() as tmp_dir, self.assertLogs(crawler_iterator.logger):
            file_path = os.path.join(tmp_dir, 'random_objects.pickle')
            # pickle various objects to a temporary file
            crawler_iterator._pickle_list_elements(objects_to_pickle, file_path)

            # retrieve the pickled objects and check they are the same
            # as the ones which were pickled
            unpickled_objects = []
            with open(file_path, 'rb') as pickle_file:
                while True:
                    try:
                        unpickled_objects.append(pickle.load(pickle_file))
                    except EOFError:
                        break

            self.assertListEqual(unpickled_objects, reference)
            self.assertFalse(objects_to_pickle)  # check that the list has been cleared

    def test_thread_manage_failed_ingestions(self):
        """Test the processing of failed ingestions"""
        # create a crawler iterator without starting the processing threads
        with mock.patch('threading.Thread'):
            crawler_iterator = iter(self.TestCrawler())

        # start the thread
        thread = threading.Thread(target=crawler_iterator._thread_manage_failed_normalizing)
        with self.assertLogs(crawler_iterator.logger, level=logging.INFO) as log_manager:
            thread.start()
            # put two items in the failed queue (one more than the
            # max number of items per file)
            items_to_pickle = [
                (crawlers.DatasetInfo('foo', {}), RuntimeError()),
                (crawlers.DatasetInfo('baz', {}), ValueError()),
                (crawlers.DatasetInfo('quux', {}), KeyError())
            ]
            for item in items_to_pickle:
                crawler_iterator._failed.put(item)
            # stop the thread
            crawler_iterator._failed.put(crawlers.Stop)
            # wait for the thread to stop
            thread.join()

        # check that one file is created
        failed_dir_contents = os.listdir(self.tmp_dir)
        self.assertEqual(len(failed_dir_contents), 1)

        with open(os.path.join(self.tmp_dir, failed_dir_contents[0]), 'rb') as pickle_file:
            pickled_objects = [
                pickle.load(pickle_file) for _ in range(len(items_to_pickle))
            ]

            with self.assertRaises(EOFError):
                pickle.load(pickle_file)

        # check the contents of the file
        self.assertTrue(all(
            (to_pickle[0] == result[0],
             type(to_pickle[1]) == type(result[1]) and to_pickle[1].args == result[1].args)
            for to_pickle, result in zip(items_to_pickle, pickled_objects)
        ))

        # check that the dump method has been called twice
        # (because the number of items exceeds the max number
        # of items per file)
        dump_messages = 0
        for record in log_manager.records:
            if record.getMessage().startswith('Dumping items to'):
                dump_messages += 1
        self.assertEqual(dump_messages, 2)

    def test_keyboard_interruption(self):
        """Test that keyboard interrupts are managed properly"""
        mock_futures = (mock.Mock(), KeyboardInterrupt)
        with mock.patch('concurrent.futures.ThreadPoolExecutor.submit',
                        side_effect=mock_futures) as mock_submit, \
             mock.patch('concurrent.futures.as_completed') as mock_as_completed:
            with self.assertLogs(crawlers.CrawlerIterator.logger, level=logging.DEBUG):
                crawler_iterator = iter(self.TestCrawler())
                crawler_iterator.manager_thread.join()
            mock_futures[0].cancel.assert_called()


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

    def test_equality(self):
        """Test equality of two DirectoryCrawler objects"""
        self.assertEqual(
            crawlers.DirectoryCrawler(
                'http://foo', (datetime(2024, 1, 2), datetime(2024, 1, 3)),
                r'.*\.nc', 'user', 'pass'),
            crawlers.DirectoryCrawler(
                'http://foo', (datetime(2024, 1, 2), datetime(2024, 1, 3)),
                r'.*\.nc', 'user', 'pass'))
        self.assertNotEqual(
            crawlers.DirectoryCrawler(
                'http://foo', (datetime(2024, 1, 2), datetime(2024, 1, 3)),
                r'.*\.nc', 'user', 'pass'),
            crawlers.DirectoryCrawler(
                'http://foo', (datetime(2024, 1, 2), datetime(2024, 1, 3)),
                r'.*\.nc', 'user', 'password'))

    def test_http_get_with_auth(self):
        """If no username and password are provided, HTTP requests
        should not have an 'auth' parameter
        """
        crawler = crawlers.DirectoryCrawler('', username='user', password='pass')
        with mock.patch('geospaas_harvesting.crawlers.Crawler._http_get') as mock_get:
            crawler._http_get('http://foo/bar')
            crawler._http_get('http://foo/bar', request_parameters={'quz': 'qux'})
        mock_get.assert_has_calls((
            mock.call('http://foo/bar', request_parameters={'auth': ('user', 'pass')},
                      max_tries=5, wait_time=5),
            mock.call('http://foo/bar', request_parameters={'quz': 'qux', 'auth': ('user', 'pass')},
                      max_tries=5, wait_time=5),
        ))

    def test_http_get_no_auth(self):
        """If no username and password are provided, HTTP requests
        should not have an 'auth' parameter
        """
        crawler = crawlers.DirectoryCrawler('')
        with mock.patch('geospaas_harvesting.crawlers.Crawler._http_get') as mock_get:
            crawler._http_get('http://foo/bar')
        mock_get.assert_called_with('http://foo/bar', request_parameters=None,
                                    max_tries=5, wait_time=5)

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

    def test_strip_folder_page(self):
        """_strip_folder_page() should remove the index page from a
        folder path
        """
        self.assertEqual(
            crawlers.HTMLDirectoryCrawler._strip_folder_page('/foo/bar/contents.html'),
            '/foo/bar')
        self.assertEqual(
            crawlers.HTMLDirectoryCrawler._strip_folder_page('/foo/bar/'),
            '/foo/bar')
        self.assertEqual(
            crawlers.HTMLDirectoryCrawler._strip_folder_page('/foo/bar'),
            '/foo/bar')

    def test_get_right_number_of_links(self):
        """Test that the crawler gets the correct number of links from a test page"""
        with open(os.path.join(
                os.path.dirname(__file__), 'data', 'opendap', 'root.html')) as data_file:
            html = data_file.read()
        self.assertEqual(len(crawlers.HTMLDirectoryCrawler._get_links(html)), 4)

        with open(os.path.join(os.path.dirname(__file__), 'data', 'empty.html')) as data_file:
            html = data_file.read()
        self.assertEqual(len(crawlers.HTMLDirectoryCrawler._get_links(html)), 0)

    def test_link_extractor_error(self):
        """In case of error, LinkExtractor must use a logger"""
        parser = crawlers.LinkExtractor()
        with self.assertLogs(parser.logger, level=logging.ERROR):
            parser.error('some message')

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

    def test_list_folder_contents(self):
        """Test listing a folder's contents"""
        with mock.patch('geospaas_harvesting.crawlers.Crawler._http_get') as mock_http_get:
            mock_http_get.return_value.text = (
                '<html>'
                '<a href="bar/contents.html">folder/</a>'
                '<a href="baz/">folder/</a>'
                '<html/>')
            crawler = crawlers.HTMLDirectoryCrawler('')
            self.assertListEqual(
                crawler._list_folder_contents('/foo/contents.html'),
                ['/foo/bar/contents.html', '/foo/baz/'])

    def test_list_folder_contents_no_auth(self):
        """If no username and password are provided, HTTP requests
        should not have an 'auth' parameter
        """
        with mock.patch('geospaas_harvesting.crawlers.Crawler._http_get') as mock_http_get:
            mock_http_get.return_value.text = '<html><html/>'
            crawler = crawlers.HTMLDirectoryCrawler('http://foo')
            crawler._list_folder_contents('/bar')
            mock_http_get.assert_called_once_with('http://foo/bar', request_parameters={},
                                                  max_tries=5, wait_time=5)

    def test_list_folder_contents_with_auth(self):
        """If a username and password are provided, HTTP requests
        should have an 'auth' parameter
        """
        with mock.patch('geospaas_harvesting.crawlers.Crawler._http_get') as mock_http_get:
            mock_http_get.return_value.text = '<html><html/>'
            crawler = crawlers.HTMLDirectoryCrawler('http://foo', username='user', password='pass')
            crawler._list_folder_contents('/bar')
        mock_http_get.assert_called_once_with('http://foo/bar',
                                              request_parameters={'auth': ('user', 'pass')},
                                              max_tries=5, wait_time=5)

    def test_get_normalized_attributes(self):
        """Test that the attributes are gotten using metanorm, and the
        geospaas_service attributes are set
        """
        crawler = crawlers.HTMLDirectoryCrawler('http://foo')
        with mock.patch.object(crawler, '_metadata_handler') as mock_handler:
            mock_handler.get_parameters.return_value = {'foo': 'bar'}
            self.assertDictEqual(
                    crawler.get_normalized_attributes(crawlers.DatasetInfo('ftp://uri')),
                    {
                        'foo': 'bar',
                        'geospaas_service_name': geospaas.catalog.managers.HTTP_SERVICE_NAME,
                        'geospaas_service': geospaas.catalog.managers.HTTP_SERVICE
                    })
            mock_handler.get_parameters.assert_called_once_with({'url': 'ftp://uri'})


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

    def test_equality(self):
        """Test the equality operator between crawlers"""
        self.assertEqual(crawlers.HTTPPaginatedAPICrawler('http://foo'),
                         crawlers.HTTPPaginatedAPICrawler('http://foo'))
        self.assertEqual(
            crawlers.HTTPPaginatedAPICrawler('http://foo',
                                    username='user', password='pass', search_terms={'bar': 'baz'}),
            crawlers.HTTPPaginatedAPICrawler('http://foo',
                                    username='user', password='pass', search_terms={'bar': 'baz'}))
        self.assertNotEqual(crawlers.HTTPPaginatedAPICrawler('http://foo'),
                            crawlers.HTTPPaginatedAPICrawler('http://bar'))

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
        response = requests.Response()
        response.status_code = 200
        response.raw = io.BytesIO(b'foo')
        with mock.patch.object(crawler, '_http_get', return_value=response), \
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


class ERDDAPTableCrawlerTestCase(unittest.TestCase):
    """Tests for ERDDAPTableCrawler"""

    TEST_DATA_PATH = os.path.join(os.path.dirname(__file__), 'data', 'erddap')

    def test_url_check(self):
        """ERDDAPTableCrawler's url should end with .json"""
        with self.assertRaises(ValueError):
            crawlers.ERDDAPTableCrawler('http://foo', ['bar'])

    def test_equality(self):
        """Test equality of two DirectoryCrawler objects"""
        self.assertEqual(
            crawlers.ERDDAPTableCrawler('http://foo/ArgoFloats.json', ['platform_number']),
            crawlers.ERDDAPTableCrawler('http://foo/ArgoFloats.json', ['platform_number']))
        self.assertNotEqual(
            crawlers.ERDDAPTableCrawler('http://foo/ArgoFloats.json', ['platform_number']),
            crawlers.ERDDAPTableCrawler('http://foo/ArgoFloats.json', ['platform_number'],
                                        longitude_attr='lon', latitude_attr='lat'))

    def test_get_ids(self):
        """Test gettings identifiers which match search terms"""
        response_path = os.path.join(self.TEST_DATA_PATH, 'ids.json')
        response = requests.Response()
        response.status_code = 200
        response.raw = open(response_path, 'rb')
        crawler = crawlers.ERDDAPTableCrawler('http://foo/ArgoFloats.json', ['platform_number'],
                                              search_terms=['time>=2024-01-01T00:00:00Z',
                                                            'time<=2024-01-01T01:00:00Z'])
        with mock.patch.object(crawler, '_http_get', return_value=response):
            ids = crawler.get_ids()
            self.assertListEqual(
                list(ids),
                [["3901480"], ["5905121"], ["5905267"], ["5905498"], ["5905533"], ["5905765"],
                 ["5905878"], ["5906337"], ["5906912"], ["5906993"], ["6902906"], ["6903060"]])
        response.raw.close()

    def test_get_ids_error(self):
        """An error message must be logged if an error happens when
        fetching IDs
        """
        error = requests.HTTPError(response=mock.Mock(content='error message'))
        crawler = crawlers.ERDDAPTableCrawler('http://foo/ArgoFloats.json', ['platform_number'])
        with mock.patch.object(crawler, '_http_get', side_effect=error):
            with self.assertLogs(logger=crawler.logger, level=logging.ERROR), \
                 self.assertRaises(requests.HTTPError):
                list(crawler.get_ids())

    def test_crawl(self):
        """Test the DatasetInfo objects returned by the crawler"""
        ids = [["3901480"], ["5905121"], ["5905267"]]
        crawler = crawlers.ERDDAPTableCrawler(
            'http://foo/ArgoFloats.json', ['platform_number'],
            position_qc_attr='position_qc', variables=['foo', 'bar'])
        with mock.patch.object(crawler, 'get_ids', return_value=ids):
            self.assertListEqual(
                list(crawler.crawl()),
                [
                    crawlers.DatasetInfo(
                        'http://foo/ArgoFloats.json?time,longitude,latitude,position_qc,foo,bar'
                        '&platform_number="3901480"',
                        {'id_attributes': {'platform_number': '3901480'}}),
                    crawlers.DatasetInfo(
                        'http://foo/ArgoFloats.json?time,longitude,latitude,position_qc,foo,bar'
                        '&platform_number="5905121"',
                        {'id_attributes': {'platform_number': '5905121'}}),
                    crawlers.DatasetInfo(
                        'http://foo/ArgoFloats.json?time,longitude,latitude,position_qc,foo,bar'
                        '&platform_number="5905267"',
                        {'id_attributes': {'platform_number': '5905267'}}),
                ])

    def test_check_qc(self):
        """Test the QC validation"""
        crawler = crawlers.ERDDAPTableCrawler('foo.json', ['bar'], valid_qc_codes=('1', '2'))
        self.assertTrue(crawler._check_qc('1'))
        self.assertTrue(crawler._check_qc('2'))
        self.assertFalse(crawler._check_qc('0'))
        self.assertFalse(crawler._check_qc('3'))
        self.assertFalse(crawler._check_qc(1))

    def test_make_coverage_url(self):
        """Test making the URL to get a dataset's temporal and spatial
        coverage
        """
        self.assertEqual(
            crawlers.ERDDAPTableCrawler('https://foo.json', ['id'],
                                        longitude_attr='lon', latitude_attr='lat', time_attr='time',
                                        position_qc_attr='pos_qc',
                                        variables=['bar', 'baz'])._make_coverage_url(),
            'https://foo.json?time,lon,lat,pos_qc&distinct()&orderBy("time")'
        )

    def test_get_coverage(self):
        """Test getting the temporal and spatial coverage for one
        dataset
        """
        crawler = crawlers.ERDDAPTableCrawler(
            'https://foo.json', ['platform_number'],
            longitude_attr='longitude', latitude_attr='latitude',
            time_attr='time',
            position_qc_attr='position_qc', time_qc_attr='time_qc')

        response = requests.Response()
        response.status_code = 200
        response.raw = open(os.path.join(self.TEST_DATA_PATH, 'coverage.json'), 'rb')

        expected_trajectory = [
            (-11.863, -0.126),
            (-13.83, -0.035),
            (-15.744, 0.68),
            (-16.674, 0.76),
            (-17.133, 1.21),
            (-17.74, 1.403),
            (-17.734, 1.263),
            (-17.189, 1.756),
            (-16.437, 1.191),
            (-16.039, 1.409),
            (-15.451, 1.177),
            (-15.075, 1.132),
            (-14.329, 1.182),
            (-13.586, 1.285),
            (-13.488, 1.766),
            (-13.593, 2.086),
            (-14.115, 2.533),
            (-15.016, 2.923),
            (-15.901, 3.11),
            (-16.634, 3.042),
            (-16.874, 3.115),
            (-17.081, 3.125),
            (-17.515, 3.171),
            (-17.623, 3.318),
            (-17.668, 3.358),
            (-17.332, 3.699),
            (-16.714, 3.96),
            (-15.962, 4.15),
            (-15.254, 3.998),
            (-14.585, 4.127),
            (-14.048, 4.175),
            (-13.926, 4.17),
            (-13.769, 4.183),
            (-13.47, 4.276),
            (-13.134, 4.322),
            (-12.887, 4.221),
            (-12.702, 4.292),
            (-12.415, 4.275),
            (-12.116, 4.126),
            (-11.792, 3.997),
            (-11.3, 3.732),
            (-10.925, 3.94),
            (-10.152, 3.852),
            (-9.558, 4.015),
            (-9.756, 4.6),
            (-10.046, 5.203),
            (-9.934, 5.179),
            (-9.612, 4.975),
        ]

        with mock.patch.object(crawler, '_http_get', return_value=response) as mock_http_get:
            self.assertTupleEqual(
                crawler.get_coverage({'platform_number': '13858'}),
                (("1997-07-28T20:26:20Z", "1998-12-27T20:00:25Z"), expected_trajectory))
        mock_http_get.assert_called_once_with(
            crawler._make_coverage_url(),
            request_parameters={'params': {'platform_number': '"13858"'}})
        response.raw.close()

    def test_get_coverage_error(self):
        """`get_coverage` must raise an exception when the coverage
        cannot be determined
        """
        crawler = crawlers.ERDDAPTableCrawler(
            'https://foo.json', ['platform_number'], valid_qc_codes=(1,))
        with mock.patch.object(crawler, '_http_get') as mock_http_get:
            mock_http_get.return_value.json.return_value = {
                'table': {
                    'rows': [
                        ["1997-07-28T20:26:20Z", -11.863, -0.126, "3", "1"],
                        ["1997-08-09T01:52:41Z", -13.83, -0.035, "5", "1"],
                        ["1997-08-19T20:44:44Z", -15.744, 0.68, "3", "1"],
                        ["1997-08-30T20:12:43Z", -16.674, 0.76, "4", "1"],
                        ["1997-09-10T21:03:19Z", -17.133, 1.21, "4", "1"],
                    ]
                }
            }
            with self.assertRaises(RuntimeError):
                crawler.get_coverage({'platform_number': '123456'})

    def test_get_coverage_http_error(self):
        """`get_coverage` must raise an exception when an HTTP error
        happens
        """
        crawler = crawlers.ERDDAPTableCrawler('https://foo.json', ['platform_number'])
        error = requests.HTTPError(response=mock.MagicMock())
        with mock.patch.object(crawler, '_http_get', side_effect=error):
            with self.assertRaises(requests.HTTPError), \
                 self.assertLogs(crawler.logger, logging.ERROR):
                crawler.get_coverage({'platform_number': '123456'})

    def test_make_product_metadata_url(self):
        """Test creating the URL to a product's metadata"""
        self.assertEqual(
            crawlers.ERDDAPTableCrawler(
                'https://erddap.ifremer.fr/erddap/tabledap/ArgoFloats.json', ['id']
            )._make_product_metadata_url(),
            'https://erddap.ifremer.fr/erddap/info/ArgoFloats/index.json')

        with self.assertRaises(RuntimeError):
            crawlers.ERDDAPTableCrawler('https://foo.json', ['id'])._make_product_metadata_url()

    def test_get_product_metadata(self):
        """Test getting a product's metadata"""
        crawler = crawlers.ERDDAPTableCrawler(
            'https://erddap.ifremer.fr/erddap/tabledap/ArgoFloats.json', ['id'])
        with mock.patch.object(crawler, '_http_get') as mock_http_get:
            result = crawler.get_product_metadata()
        self.assertEqual(result, mock_http_get.return_value.json.return_value)
        mock_http_get.assert_called_with(crawler._make_product_metadata_url())

    def test_get_product_metadata_http_error(self):
        """`get_coverage` must raise an exception when an HTTP error
        happens
        """
        crawler = crawlers.ERDDAPTableCrawler(
            'https://erddap.ifremer.fr/erddap/tabledap/ArgoFloats.json', ['id'])
        error = requests.HTTPError
        with mock.patch.object(crawler, '_http_get', side_effect=error):
            with self.assertRaises(error), self.assertLogs(crawler.logger, logging.ERROR):
                crawler.get_product_metadata()

    def test_get_normalized_attributes(self):
        """Test attributes normalization"""
        dataset_info = crawlers.DatasetInfo('https://foo.json?id=bar',
                                            {'id_attributes': {'platform_number':'bar'}})
        crawler = crawlers.ERDDAPTableCrawler('https://foo.json', ['id'])
        with mock.patch.object(crawler, 'get_coverage') as mock_get_coverage, \
             mock.patch.object(crawler, 'get_product_metadata') as mock_get_product_metadata, \
             mock.patch.object(crawler._metadata_handler, 'get_parameters') as mock_get_parameters:
            mock_get_coverage.return_value = (('date1', 'date2'), [(1, 2), (3, 4)])
            mock_get_product_metadata.return_value = {'baz': 'qux'}
            mock_get_parameters.return_value = {'key1': 'value1', 'key2': 'value2'}
            result = crawler.get_normalized_attributes(dataset_info)
        self.assertDictEqual(
            result,
            {
                'key1': 'value1',
                'key2': 'value2',
                'geospaas_service_name': geospaas.catalog.managers.HTTP_SERVICE_NAME,
                'geospaas_service': geospaas.catalog.managers.HTTP_SERVICE
            })