"""Test suite for crawlers"""
#pylint: disable=protected-access

import logging
import os
import unittest
import unittest.mock as mock

import requests

import geospaas_harvesting.crawlers as crawlers


class BaseCrawlerTestCase(unittest.TestCase):
    """Tests for the base Crawler"""

    def test_exception_on_iter(self):
        """An exception must be raised if the __iter__ method is not overloaded"""
        base_crawler = crawlers.Crawler()
        with self.assertRaises(NotImplementedError):
            _ = iter(base_crawler)

class OpenDAPCrawlerTestCase(unittest.TestCase):
    """Tests for the OpenDAP crawler"""

    TEST_DATA = {
        'root': {
            'url': "https://test-opendap.com",
            'file_path': "data/opendap/root.html"},
        'root_duplicates': {
            'url': "https://test2-opendap.com",
            'file_path': "data/opendap/root_duplicates.html"},
        'root_dataset': {
            'url': 'https://test-opendap.com/dataset.nc',
            'file_path': None},
        'root_dataset_2': {
            'url': 'https://test2-opendap.com/dataset.nc',
            'file_path': None},
        'folder': {
            'url': 'https://test-opendap.com/folder/contents.html',
            'file_path': 'data/opendap/folder.html'},
        'folder_2': {
            'url': 'https://test2-opendap.com/folder/contents.html',
            'file_path': 'data/opendap/folder.html'},
        'folder_dataset': {
            'url': 'https://test-opendap.com/folder/dataset.nc',
            'file_path': None},
        'empty': {
            'url': 'http://empty.com',
            'file_path': 'data/empty.html'},
        'inexistent': {
            'url': 'http://random.url',
            'file_path': None}
    }

    def requests_get_side_effect(self, url):
        """Side effect function used to mock calls to requests.get().text"""
        data_file_relative_path = None
        for test_data in self.TEST_DATA.values():
            if url == test_data['url']:
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

    def tearDown(self):
        self.patcher_requests_get.stop()
        # Close any files opened during the test
        for opened_file in self.opened_files:
            opened_file.close()

    def test_instantiation(self):
        """Test the correct instantiation of an Opendap crawler"""
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root']['url'])
        self.assertIsInstance(crawler, crawlers.Crawler)
        self.assertListEqual(crawler._urls, [])
        self.assertListEqual(crawler._to_process, [self.TEST_DATA['root']['url']])

    def test_set_initial_state(self):
        """Tests that the set_initial_state() method sets the correct values"""
        #Create a crawler and start iterating to set a non-initial state
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root']['url'])
        with self.assertLogs(crawler.LOGGER):
            next(iter(crawler))

        crawler.set_initial_state()
        self.assertListEqual(crawler._to_process, [crawler.root_url])
        self.assertListEqual(crawler._urls, [])

    def test_get_correct_html_contents(self):
        """Test that the _http_get() method returns the correct HTML string"""
        data_file = open(os.path.join(os.path.dirname(__file__), 'data/opendap/root.html'))
        html = data_file.read()
        data_file.close()

        html_from_method = crawlers.OpenDAPCrawler._http_get(self.TEST_DATA['root']['url'])

        self.assertEqual(html, html_from_method)

    @mock.patch('logging.Logger.error')
    def test_get_html_logs_error_on_http_status(self, mock_error_logger):
        """Test that an exception is raised in case of HTTP error code"""
        _ = crawlers.OpenDAPCrawler._http_get(self.TEST_DATA['inexistent']['url'])
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

    def test_explore_page(self):
        """
        Explore root page and make sure the _url and _to_process attributes of the crawler have the
        right values
        """
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root']['url'])
        with self.assertLogs(crawler.LOGGER):
            crawler._explore_page(crawler._to_process.pop())
        self.assertListEqual(crawler._urls, [self.TEST_DATA['root_dataset']['url']])
        self.assertListEqual(crawler._to_process, [self.TEST_DATA['folder']['url']])

    def test_explore_page_with_duplicates(self):
        """If the same URL is present twice in the page, it should only be processed once"""
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root_duplicates']['url'])
        with self.assertLogs(crawler.LOGGER):
            crawler._explore_page(crawler._to_process.pop())
        self.assertListEqual(crawler._urls, [self.TEST_DATA['root_dataset_2']['url']])
        self.assertListEqual(crawler._to_process, [self.TEST_DATA['folder_2']['url']])

    def test_iterating(self):
        """Test the call to the __iter__ method"""
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root']['url'])
        crawler_iterator = iter(crawler)

        # Test the values returned by the iterator
        with self.assertLogs(crawler.LOGGER):
            self.assertEqual(next(crawler_iterator), self.TEST_DATA['root_dataset']['url'])
            self.assertEqual(next(crawler_iterator), self.TEST_DATA['folder_dataset']['url'])

        # Test that a StopIteration is returned at the end. The nested context managers are
        # necessary because the StopIteration exception is raised inside an 'except KeyError:' block
        with self.assertRaises(StopIteration):
            with self.assertRaises(KeyError):
                next(crawler)


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
                    and request_parameters['params']['q'] == self.SEARCH_TERMS
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
            self.BASE_URL, self.SEARCH_TERMS, 'user', 'pass',
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
        self.assertEqual(self.crawler.search_terms, self.SEARCH_TERMS)
        self.assertEqual(self.crawler._credentials, ('user', 'pass'))
        self.assertEqual(self.crawler.page_size, self.PAGE_SIZE)
        self.assertEqual(self.crawler.initial_offset, 0)
        self.assertEqual(self.crawler.offset, 0)
        self.assertEqual(self.crawler._urls, [])

    def test_set_initial_state(self):
        """Tests that the set_initial_state() method sets the correct values"""
        #Create a crawler and start iterating to set a non-initial state
        with self.assertLogs(self.crawler.LOGGER):
            next(iter(self.crawler))

        self.crawler.set_initial_state()
        self.assertEqual(self.crawler.offset, self.crawler.initial_offset)
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
            "https://scihub.copernicus.eu/dhus/odata/v1/Products('d023819a-60d3-4b5e-bb81-645294d73b5b')/$value", # pylint:disable=line-too-long
            "https://scihub.copernicus.eu/dhus/odata/v1/Products('87ddb795-dab4-4985-85f4-c390c9cdd65b')/$value", # pylint:disable=line-too-long
            "https://scihub.copernicus.eu/dhus/odata/v1/Products('b54171e1-078b-4234-ae0a-7b27abb14baa')/$value",  # pylint:disable=line-too-long
            "https://scihub.copernicus.eu/dhus/odata/v1/Products('e2842bc8-8b3e-4161-a88c-84c2b43e60f9')/$value"  # pylint:disable=line-too-long
        ]

        with self.assertLogs(self.crawler.LOGGER):
            for i, url in enumerate(self.crawler):
                self.assertEqual(url, expected_urls[i])
