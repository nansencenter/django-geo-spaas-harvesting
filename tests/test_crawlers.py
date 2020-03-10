"""Test suite for crawlers"""
#pylint: disable=protected-access

import os
import unittest
import unittest.mock as mock

import requests

import geospaas_harvesting.crawlers as crawlers


class OpenDAPCrawlerTestCase(unittest.TestCase):
    """Tests for the OpenDAP crawler"""

    TEST_DATA = {
        'root': {
            'url': "https://test-opendap.com",
            'file_path': "data/opendap_root.html"},
        'root_dataset': {
            'url': 'https://test-opendap.com/dataset.nc',
            'file_path': None},
        'folder': {
            'url': 'https://test-opendap.com/folder/contents.html',
            'file_path': 'data/opendap_folder.html'},
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
        self.patcher_requests_get = mock.patch('geospaas_harvesting.crawlers.requests.get')
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
        self.assertEqual(crawler.root_url, self.TEST_DATA['root']['url'])
        self.assertEqual(crawler._urls, set())
        self.assertEqual(crawler._to_process, set())

    def test_get_correct_html_contents(self):
        """Test that the _get_html_page() method returns the correct HTML string"""
        data_file = open(os.path.join(os.path.dirname(__file__), 'data/opendap_root.html'))
        html = data_file.read()
        data_file.close()

        html_from_method = crawlers.OpenDAPCrawler._get_html_page(self.TEST_DATA['root']['url'])

        self.assertEqual(html, html_from_method)

    @mock.patch('logging.Logger.error')
    def test_get_html_logs_error_on_http_status(self, mock_error_logger):
        """Test that an exception is raised in case of HTTP error code"""
        _ = crawlers.OpenDAPCrawler._get_html_page(self.TEST_DATA['inexistent']['url'])
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

    def test_explore_page(self):
        """
        Explore root page and make sure the _url and _to_process attributes of the crawler have the
        right values
        """
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root']['url'])
        crawler._explore_page(self.TEST_DATA['root']['url'])
        self.assertSetEqual(crawler._urls, set([self.TEST_DATA['root_dataset']['url']]))
        self.assertSetEqual(crawler._to_process, set([self.TEST_DATA['folder']['url']]))

    def test_iterating(self):
        """Test the call to the __iter__ method"""
        crawler = crawlers.OpenDAPCrawler(self.TEST_DATA['root']['url'])
        crawler_iterator = iter(crawler)

        # Test the initial state of the crawler
        self.assertSetEqual(crawler._urls, set([self.TEST_DATA['root_dataset']['url']]))
        self.assertSetEqual(crawler._to_process, set([self.TEST_DATA['folder']['url']]))

        # Test the values returned by the iterator
        self.assertEqual(next(crawler_iterator), self.TEST_DATA['root_dataset']['url'])
        self.assertEqual(next(crawler_iterator), self.TEST_DATA['folder_dataset']['url'])

        # Test that a StopIteration is returned at the end. The nested context managers are
        # necessary because the StopIteration exception is raised inside an 'except KeyError:' block
        with self.assertRaises(StopIteration):
            with self.assertRaises(KeyError):
                next(crawler)


if __name__ == '__main__':
    unittest.main()
