import io
import logging
import unittest
import unittest.mock as mock

import requests

import geospaas_harvesting.crawlers.base as crawlers_base
import geospaas_harvesting.crawlers.paginated_api as crawlers_paginated_api


class HTTPPaginatedAPICrawlerTestCase(unittest.TestCase):
    """Tests for the HTTPPaginatedAPICrawler base class"""

    def test_equality(self):
        """Test the equality operator between crawlers"""
        self.assertEqual(
            crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(url='http://foo'),
            crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(url='http://foo'))
        self.assertEqual(
            crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(
                url='http://foo', username='user', password='pass', search_terms={'bar': 'baz'}),
            crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(
                url='http://foo', username='user', password='pass', search_terms={'bar': 'baz'}))
        self.assertNotEqual(
            crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(url='http://foo'),
            crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(url='http://bar'))

    def test_get_page_size(self):
        """Test page_size getter"""
        with mock.patch(
                'geospaas_harvesting.crawlers.paginated_api.HTTPPaginatedAPICrawler.PAGE_SIZE_NAME',
                'size'):
            crawler = crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(
                url='https://foo', page_size=10)
            crawler.set_initial_state()
            self.assertEqual(crawler.page_size, 10)

    def test_get_page_offset(self):
        """Test page_offset getter"""
        crawler = crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(
                url='foo', initial_offset=10)
        crawler.set_initial_state()
        self.assertEqual(crawler.page_offset, 10)

    def test_set_page_offset(self):
        """Test page_offset setter"""
        crawler = crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(url='https://foo')
        crawler.page_offset = 12
        self.assertEqual(crawler.page_offset, 12)

    def test_set_initial_state(self):
        """Test that set_initial_state correctly resets the crawler"""
        crawler = crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(url='https://foo')
        # Set non-default offset and _urls values
        crawler.request_parameters['params'][crawler.PAGE_OFFSET_NAME] = 200
        crawler.page_offset = 15

        crawler.set_initial_state()
        self.assertEqual(
            crawler.request_parameters['params'][crawler.PAGE_OFFSET_NAME], crawler.initial_offset)

    def test_get_next_page(self):
        """_get_next_page() should get the page at the current offset,
        then increment the offset
        """
        crawler = crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(url='https://foo')
        crawler.set_initial_state()
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
        crawler = crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(url='foo')
        with self.assertRaises(NotImplementedError):
            crawler._get_datasets_info('')

    def test_abstract_get_entries(self):
        """_get_entries() should raise a NotImplementedError
        when called directly from HTTPPaginatedAPICrawler
        """
        crawler = crawlers_paginated_api.HTTPPaginatedAPICrawler.from_kwargs(url='foo')
        with self.assertRaises(NotImplementedError):
            crawler._get_entries('')

    def test_crawl(self):
        """Test the crawling mechanism for HTTP paginated APIs"""
        class TestHTTPAPICrawler(crawlers_paginated_api.HTTPPaginatedAPICrawler):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.ran = False

            def _get_next_page(self):
                if self.ran:
                    return ''
                else:
                    self.ran = True
                    return 'https://foo/bar.nc;https://foo/baz.nc'

            def _get_entries(self, page:str):
                if page:
                    return page.split(';')
                else:
                    return None

            def _get_datasets_info(self, entries):
                return [crawlers_base.DatasetInfo(url) for url in entries]

        crawler = TestHTTPAPICrawler.from_kwargs(url='https://foo')
        self.assertListEqual(
            list(crawler.crawl()),
            [crawlers_base.DatasetInfo('https://foo/bar.nc'),
             crawlers_base.DatasetInfo('https://foo/baz.nc')])
