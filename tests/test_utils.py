"""Tests for the geospaas_harvesting.utils module"""

import unittest
import unittest.mock as mock

import geospaas_harvesting.utils as utils

class UtilsTestCase(unittest.TestCase):
    """Tests for utilities"""

    def test_should_strip_auth(self):
        """The authentication headers should be stripped if a
        redirection outside of the current domain happens
        """
        with utils.TrustDomainSession() as session:
            self.assertFalse(session.should_strip_auth('https://scihub.copernicus.eu/foo/bar',
                                                       'https://apihub.copernicus.eu/foo/bar'))

            self.assertFalse(session.should_strip_auth('https://scihub.copernicus.eu/foo/bar',
                                                       'https://scihub.copernicus.eu/baz'))

            self.assertFalse(session.should_strip_auth('http://scihub.copernicus.eu:80/foo/bar',
                                                       'https://scihub.copernicus.eu:443/foo/bar'))

            self.assertTrue(session.should_strip_auth('https://scihub.copernicus.eu/foo/bar',
                                                      'https://www.website.com/foo/bar'))

            self.assertTrue(session.should_strip_auth('https://scihub.copernicus.eu/foo/bar',
                                                      'https://foo.com/bar'))

    def test_http_get_with_auth(self):
        """If the `auth` argument is provided, the request should be
        executed inside a TrustDomainSession
        """
        with mock.patch('requests.Session.get', return_value='response') as mock_get:
            self.assertEqual(
                utils.http_get('url', stream=False, auth=('username', 'password')),
                'response'
            )
            mock_get.assert_called_once_with('url', stream=False)

    def test_http_get_without_auth(self):
        """If the `auth` argument is not provided, the request should
        simply be executed using requests.get()
        """
        with mock.patch('requests.get', return_value='response') as mock_get:
            self.assertEqual(
                utils.http_get('url', stream=True),
                'response'
            )
            mock_get.assert_called_once_with('url', stream=True)
