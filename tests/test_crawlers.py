"""Test suite for crawlers"""

import ftplib
import logging
import os
import unittest
import unittest.mock as mock
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests

import geospaas_harvesting.crawlers as crawlers


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


class WebDirectoryCrawlerTestCase(unittest.TestCase):
    """Tests for the WebDirectoryCrawler"""

    def test_abstract_list_folder_contents(self):
        """
        A NotImplementedError should be raised if the _list_folder_contents() method
        is accessed directly on the WebDirectoryCrawler class
        """
        crawler = crawlers.WebDirectoryCrawler('')
        with self.assertRaises(NotImplementedError):
            crawler._list_folder_contents('')

    def test_is_folder(self):
        """
        A NotImplementedError should be raised if the _is_folder() method
        is accessed directly on the WebDirectoryCrawler class
        """
        crawler = crawlers.WebDirectoryCrawler('')
        with self.assertRaises(NotImplementedError):
            crawler._is_folder('')

    def test_is_file(self):
        """
        A NotImplementedError should be raised if the _is_file() method
        is accessed directly on the WebDirectoryCrawler class
        """
        crawler = crawlers.WebDirectoryCrawler('')
        with self.assertRaises(NotImplementedError):
            crawler._is_file('')

    def test_get_download_url(self):
        """
        The get_download_url() method of the WebDirectoryCrawler
        should return the resource URL unchanged
        """
        crawler = crawlers.WebDirectoryCrawler('')
        self.assertEqual(crawler.get_download_url('foo'), 'foo')


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
                'https://test-opendap.com/folder/2019/02/14/20190214000000_dataset.nc',
                'https://test-opendap.com/folder/2019/046/20190215000000_dataset.nc'
            ],
            'file_path': None},
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

    def requests_get_side_effect(self, url):
        """Side effect function used to mock calls to requests.get().text"""
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
        # Mock requests.get()
        self.patcher_requests_get = mock.patch.object(crawlers.requests, 'get')
        self.mock_requests_get = self.patcher_requests_get.start()
        self.mock_requests_get.side_effect = self.requests_get_side_effect

        # Initialize a list of opened files which will be closed in tearDown()
        self.opened_files = []

    def tearDown(self):
        self.patcher_requests_get.stop()
        # Close any files opened during the test
        for opened_file in self.opened_files:
            opened_file.close()

    def test_instantiation(self):
        """Test the correct instantiation of an Opendap crawler"""
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root']['urls'][0])
        self.assertIsInstance(crawler, crawlers.Crawler)
        self.assertEqual(crawler.root_url, urlparse(self.TEST_DATA['root']['urls'][0]))
        self.assertEqual(crawler.time_range, (None, None))
        self.assertListEqual(crawler._urls, [])
        self.assertListEqual(crawler._to_process, [''])

    def test_set_initial_state(self):
        """Tests that the set_initial_state() method sets the correct values"""
        # Create a crawler and start iterating to set a non-initial state
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root']['urls'][0])
        with self.assertLogs(crawler.LOGGER):
            next(iter(crawler))

        crawler.set_initial_state()
        self.assertListEqual(crawler._to_process, [crawler.root_url.path])
        self.assertListEqual(crawler._urls, [])

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
        with self.assertLogs(parser.LOGGER, level=logging.ERROR):
            parser.error('some message')

    def test_process_folder(self):
        """
        Explore root page and make sure the _url and _to_process attributes of the crawler have the
        right values
        """
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root']['urls'][0])
        with self.assertLogs(crawler.LOGGER):
            crawler._process_folder(crawler._to_process.pop())
        self.assertListEqual(crawler._urls, [self.TEST_DATA['dataset']['urls'][0]])
        self.assertListEqual(crawler._to_process, ['/folder/contents.html'])

    def test_process_folder_with_duplicates(self):
        """If the same URL is present twice in the page, it should only be processed once"""
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root_duplicates']['urls'][0])
        with self.assertLogs(crawler.LOGGER):
            crawler._process_folder(crawler._to_process.pop())
        self.assertListEqual(crawler._urls, [self.TEST_DATA['dataset']['urls'][1]])
        self.assertListEqual(crawler._to_process, ['/folder/contents.html'])

    def test_process_folder_with_time_restriction_discriminated_by_timestamp(self):
        """
        Explore a page and make sure the _url and _to_process attributes of the crawler have the
        right values according to a time restriction
        """
        crawler = crawlers.OpenDAPCrawler(
            self.TEST_DATA['folder_day_of_year']['urls'][0],
            time_range=(datetime(2019, 2, 15, 11, 0, 0), datetime(2019, 2, 15, 13, 0, 0)))
        with self.assertLogs(crawler.LOGGER):
            crawler._process_folder(crawler._to_process.pop())
        self.assertListEqual(crawler._urls,
                             ['https://test-opendap.com/folder/2019/046/20190215120000_dataset.nc'])
        self.assertListEqual(crawler._to_process, [])

    def test_process_folder_with_time_restriction_discriminated_by_folder_coverage(self):
        """
        Explore a page and make sure the _url and _to_process attributes of the crawler have the
        right values according to a time restriction
        """
        crawler = crawlers.OpenDAPCrawler(
            self.TEST_DATA['folder_day_of_year']['urls'][0],
            time_range=(datetime(2019, 2, 11, 11, 0, 0), datetime(2019, 2, 11, 13, 0, 0)))
        with self.assertLogs(crawler.LOGGER):
            crawler._process_folder(crawler._to_process.pop())
        self.assertListEqual(crawler._urls, [])
        self.assertListEqual(crawler._to_process, [])

    def test_iterating(self):
        """Test the call to the __iter__ method"""
        crawler = crawlers.OpenDAPCrawler(
            self.TEST_DATA['root']['urls'][0],
            time_range=(datetime(2019, 2, 14, 0, 0, 0), datetime(2019, 2, 14, 9, 0, 0)))
        crawler_iterator = iter(crawler)

        # Test the values returned by the iterator
        with self.assertLogs(crawler.LOGGER):
            self.assertEqual(next(crawler_iterator), self.TEST_DATA['dataset']['urls'][0])
            self.assertEqual(next(crawler_iterator), self.TEST_DATA['dataset']['urls'][2])
            self.assertEqual(next(crawler_iterator), self.TEST_DATA['dataset']['urls'][3])

        # Test that a StopIteration is returned at the end. The nested context managers are
        # necessary because the StopIteration exception is raised inside an 'except KeyError:' block
        with self.assertRaises(StopIteration):
            with self.assertRaises(KeyError):
                next(crawler)

    def test_get_year_folder_coverage(self):
        """Get the correct time range from a year folder"""
        crawler = crawlers.OpenDAPCrawler('')
        self.assertEqual(
            crawler._folder_coverage('https://test-opendap.com/folder/2019/contents.html'),
            (datetime(2019, 1, 1, 0, 0, 0), datetime(2019, 12, 31, 23, 59, 59))
        )

    def test_get_month_folder_coverage(self):
        """Get the correct time range from a month folder"""
        crawler = crawlers.OpenDAPCrawler('')
        self.assertEqual(
            crawler._folder_coverage('https://test-opendap.com/folder/2019/02/contents.html'),
            (datetime(2019, 2, 1, 0, 0, 0), datetime(2019, 2, 28, 23, 59, 59))
        )

    def test_get_day_of_month_folder_coverage(self):
        """Get the correct time range from a day of month folder"""
        crawler = crawlers.OpenDAPCrawler('')
        self.assertEqual(
            crawler._folder_coverage('https://test-opendap.com/folder/2019/02/14/contents.html'),
            (datetime(2019, 2, 14, 0, 0, 0), datetime(2019, 2, 14, 23, 59, 59))
        )

    def test_get_day_of_year_folder_coverage(self):
        """Get the correct time range from a day of year folder"""
        crawler = crawlers.OpenDAPCrawler('')
        self.assertEqual(
            crawler._folder_coverage('https://test-opendap.com/folder/2019/046/contents.html'),
            (datetime(2019, 2, 15, 0, 0, 0), datetime(2019, 2, 15, 23, 59, 59))
        )

    def test_none_when_no_folder_coverage(self):
        """
        The `_folder_coverage` method should return `None` if no time range is inferred from the
        folder's path
        """
        crawler = crawlers.OpenDAPCrawler('')
        self.assertEqual(
            crawler._folder_coverage('https://test-opendap.com/folder/contents.html'), (None, None))
        self.assertEqual(
            crawler._folder_coverage('https://test-opendap.com/folder/046/contents.html'),
            (None, None)
        )
        self.assertEqual(
            crawler._folder_coverage('https://test-opendap.com/folder/02/contents.html'),
            (None, None)
        )

    def test_get_dataset_timestamp(self):
        """Get the correct date from a dataset prefixed by a timestamp"""
        crawler = crawlers.OpenDAPCrawler('')
        self.assertEqual(
            crawler._dataset_timestamp('20190214090812_dataset_name.nc'),
            datetime(2019, 2, 14, 9, 8, 12),
        )

    def test_none_when_no_dataset_timestamp(self):
        """
        The `_dataset_timestamp` method should return `None` if no timestamp is found in the
        dataset's name
        """
        crawler = crawlers.OpenDAPCrawler('')
        self.assertEqual(crawler._dataset_timestamp('dataset_name.nc'), None)

    def test_intersects_time_range_finite_limits(self):
        """
        Test the behavior of the `_intersects_time_range` method with a finite time range limitation
        `time_range[0]` and `time_range[1]` are the limits defined in the crawler
        `start_time` and `stop_time` are the limits of the time range which is tested against the
        crawler's condition
        """
        crawler = crawlers.OpenDAPCrawler(
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
        crawler = crawlers.OpenDAPCrawler('', time_range=(None, datetime(2019, 2, 20)))

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
        crawler = crawlers.OpenDAPCrawler('', time_range=(datetime(2019, 2, 20), None))

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


class CopernicusOpenSearchAPICrawlerTestCase(unittest.TestCase):
    """Tests for the Copernicus OpenSearch API crawler"""
    BASE_URL = 'https://scihub.copernicus.eu/dhus/search'
    SEARCH_TERMS = '(platformname:Sentinel-1 OR platformname:Sentinel-3) AND NOT L0'
    PAGE_SIZE = 2
    TEST_DATA = {
        'page1': {
            'offset': 0,
            'file_path': "data/copernicus_opensearch/page1.xml"},
        'page2': {
            'offset': 2,
            'file_path': "data/copernicus_opensearch/page2.xml"},
        'page3': {
            'offset': 4,
            'file_path': 'data/copernicus_opensearch/page3.xml'}
    }

    def requests_get_side_effect(self, url, **request_parameters):
        """Side effect function used to mock calls to requests.get().text"""
        data_file_relative_path = None
        for test_data in self.TEST_DATA.values():
            if (url == self.BASE_URL
                    and request_parameters['params']['q'].startswith(f"({self.SEARCH_TERMS}) AND ")
                    and request_parameters['params']['start'] == test_data['offset']
                    and request_parameters['params']['rows'] == self.PAGE_SIZE):
                data_file_relative_path = test_data['file_path']

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
        # Mock requests.get()
        self.patcher_requests_get = mock.patch.object(crawlers.requests, 'get')
        self.mock_requests_get = self.patcher_requests_get.start()
        self.mock_requests_get.side_effect = self.requests_get_side_effect

        # Initialize a list of opened files which will be closed in tearDown()
        self.opened_files = []

        self.crawler = crawlers.CopernicusOpenSearchAPICrawler(
            url=self.BASE_URL, search_terms=self.SEARCH_TERMS, username='user', password='pass',
            page_size=self.PAGE_SIZE, initial_offset=0)

    def tearDown(self):
        self.patcher_requests_get.stop()
        # Close any files opened during the test
        for opened_file in self.opened_files:
            opened_file.close()

    def test_instantiation(self):
        """Test the correct instantiation of a Copernicus OpenSearch API crawler"""
        self.assertIsInstance(self.crawler, crawlers.Crawler)
        self.assertEqual(self.crawler.url, self.BASE_URL)
        self.assertEqual(self.crawler.initial_offset, 0)
        self.assertDictEqual(self.crawler.request_parameters, {
            'params': {
                'q': f"({self.SEARCH_TERMS}) AND (beginposition:[1-01-01T00:00:00Z TO NOW])",
                'start': 0,
                'rows': self.PAGE_SIZE,
                'orderby': 'beginposition asc'
            },
            'auth': ('user', 'pass')
        })
        self.assertEqual(self.crawler._urls, [])

    def test_build_parameters_with_standard_time_range(self):
        """Build the request parameters with a time range composed of two datetime objects"""
        request_parameters = crawlers.CopernicusOpenSearchAPICrawler._build_request_parameters(
            search_terms=self.SEARCH_TERMS, username='user', password='pass',
            page_size=self.PAGE_SIZE, initial_offset=0,
            time_range=(datetime(2020, 2, 10, tzinfo=timezone.utc),
                        datetime(2020, 2, 11, tzinfo=timezone.utc)))

        self.assertDictEqual(request_parameters, {
            'params': {
                'q': f"({self.SEARCH_TERMS}) AND " +
                     "(beginposition:[2020-02-10T00:00:00Z TO 2020-02-11T00:00:00Z])",
                'start': 0,
                'rows': self.PAGE_SIZE,
                'orderby': 'beginposition asc'
            },
            'auth': ('user', 'pass')
        })

    def test_build_parameters_with_time_range_without_lower_limit(self):
        """Build the request parameters with a time range in which the first element is None"""
        request_parameters = crawlers.CopernicusOpenSearchAPICrawler._build_request_parameters(
            search_terms=self.SEARCH_TERMS, username='user', password='pass',
            page_size=self.PAGE_SIZE, initial_offset=0,
            time_range=(None, datetime(2020, 2, 11, tzinfo=timezone.utc)))
        self.assertEqual(request_parameters['params']['q'], f"({self.SEARCH_TERMS}) AND " +
                         "(beginposition:[1-01-01T00:00:00Z TO 2020-02-11T00:00:00Z])")

    def test_build_parameters_with_time_range_without_upper_limit(self):
        """Build the request parameters with a time range in which the second element is None"""
        request_parameters = crawlers.CopernicusOpenSearchAPICrawler._build_request_parameters(
            search_terms=self.SEARCH_TERMS, username='user', password='pass',
            page_size=self.PAGE_SIZE, initial_offset=0,
            time_range=(datetime(2020, 2, 10, tzinfo=timezone.utc), None))
        self.assertEqual(request_parameters['params']['q'], f"({self.SEARCH_TERMS}) AND " +
                         "(beginposition:[2020-02-10T00:00:00Z TO NOW])")

    def test_build_parameters_without_time_range(self):
        """
        Build the request parameters with a time range in which the both elements are None
        The result is equivalent to a search without a time condition.
        """
        request_parameters = crawlers.CopernicusOpenSearchAPICrawler._build_request_parameters(
            search_terms=self.SEARCH_TERMS, username='user', password='pass',
            page_size=self.PAGE_SIZE, initial_offset=0,
            time_range=(None, None))
        self.assertEqual(request_parameters['params']['q'], f"({self.SEARCH_TERMS}) AND " +
                         "(beginposition:[1-01-01T00:00:00Z TO NOW])")

    def test_set_initial_state(self):
        """Tests that the set_initial_state() method sets the correct values"""
        # Create a crawler and start iterating to set a non-initial state
        with self.assertLogs(self.crawler.LOGGER):
            next(iter(self.crawler))

        self.crawler.set_initial_state()
        self.assertEqual(self.crawler.request_parameters['params']['start'],
                         self.crawler.initial_offset)
        self.assertListEqual(self.crawler._urls, [])

    def test_get_next_page(self):
        """Test the next page content"""
        current_folder = os.path.dirname(__file__)

        with open(os.path.join(current_folder, self.TEST_DATA['page1']['file_path']), 'r') as dfh:
            with self.assertLogs(self.crawler.LOGGER):
                self.assertEqual(self.crawler._get_next_page(), dfh.read())
        with open(os.path.join(current_folder, self.TEST_DATA['page2']['file_path']), 'r') as dfh:
            with self.assertLogs(self.crawler.LOGGER):
                self.assertEqual(self.crawler._get_next_page(), dfh.read())
        with open(os.path.join(current_folder, self.TEST_DATA['page3']['file_path']), 'r') as dfh:
            with self.assertLogs(self.crawler.LOGGER):
                self.assertEqual(self.crawler._get_next_page(), dfh.read())

    def test_iterating(self):
        """Tests that the correct values are returned when iterating"""
        expected_urls = [
            "https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('d023819a-60d3-4b5e-bb81-645294d73b5b')/$value",
            "https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('87ddb795-dab4-4985-85f4-c390c9cdd65b')/$value",
            "https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('b54171e1-078b-4234-ae0a-7b27abb14baa')/$value",
            "https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('e2842bc8-8b3e-4161-a88c-84c2b43e60f9')/$value"
        ]

        with self.assertLogs(self.crawler.LOGGER):
            for i, url in enumerate(self.crawler):
                self.assertEqual(url, expected_urls[i])


class FTPCrawlerTestCase(unittest.TestCase):
    """Tests for the FTP crawler"""

    def emulate_cwd_of_ftp(self, name):
        """passes in the case of "", ".." or "folder_name" in order to resemble the behavior of cwd
        of ftplib. Otherwise (encountering a filename) raise the proper exception """
        if name not in ["..", "folder_name", ""]:
            raise ftplib.error_perm

    @mock.patch('geospaas_harvesting.crawlers.ftplib.FTP', autospec=True)
    def test_ftp_correct_navigation(self, mock_ftp):
        """ shall categorize the specific file names (based on specific 'fileformat' which is
        revealed in the configuration file) as well as folder(s) inside the ftp resource """
        test_crawler = crawlers.FTPCrawler('ftp://foo', files_suffixes='.gz')
        test_crawler.ftp.nlst.return_value = ['file1.gz', 'folder_name', 'file3.bb', 'file2.gz', ]
        test_crawler.ftp.cwd = self.emulate_cwd_of_ftp
        test_crawler.ftp.host = ''
        with self.assertLogs('geospaas_harvesting.crawlers.FTPCrawler'):
            test_crawler._process_folder('')
        # '.gz' files must be in the "_urls" list
        # Other type of files should not be in the "_urls" list
        self.assertCountEqual(['ftp://foo/file1.gz', 'ftp://foo/file2.gz'], test_crawler._urls)
        # folder with 'folder_name' must be in the "_to_process" list
        self.assertCountEqual(['/', 'folder_name'], test_crawler._to_process)

    @mock.patch('geospaas_harvesting.crawlers.ftplib.FTP.login')
    def test_ftp_correct_exception(self, mock_ftp):
        """ shall return the costume 'ConnectionError'
         instead of 'ftplib.error_perm' in order to continue the harvesting process in the case of
         redundant or repetitive login attempt(s)
         after the first login attempt. "nlst" is placed after "login" in source code. So reach "nlst"
         means passing the login code. """
        test_crawler = crawlers.FTPCrawler(
            'ftp://', username="d", password="d", files_suffixes='.gz')

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
