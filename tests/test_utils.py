"""Tests for the geospaas_harvesting.utils module"""
import io
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

    def test_http_request_with_auth(self):
        """If the `auth` argument is provided, the request should be
        executed inside a TrustDomainSession
        """
        with mock.patch('requests.Session.request', return_value='response') as mock_request:
            self.assertEqual(
                utils.http_request('GET', 'url', stream=False, auth=('username', 'password')),
                'response'
            )
            mock_request.assert_called_once_with('GET', 'url', stream=False)

    def test_http_request_without_auth(self):
        """If the `auth` argument is not provided, the request should
        simply be executed using requests.get()
        """
        with mock.patch('requests.request', return_value='response') as mock_request:
            self.assertEqual(
                utils.http_request('GET', 'url', stream=True),
                'response'
            )
            mock_request.assert_called_once_with('GET', 'url', stream=True)

    def test_yaml_parsing(self):
        """Test YAML parsing with environment variable retrieval"""
        yaml_content="""---
        foo: bar
        baz: !ENV ENV_VAR
        """
        buffer = io.StringIO(yaml_content)
        with mock.patch('builtins.open', return_value=buffer), \
             mock.patch('os.environ', {'ENV_VAR': 'qux'}):
            self.assertDictEqual(
                utils.read_yaml_file(''),
                {'foo': 'bar', 'baz': 'qux'})
