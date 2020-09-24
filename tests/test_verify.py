""" Test the verification code """
import unittest.mock as mock

import django
from geospaas.catalog.models import Dataset, DatasetURI

import geospaas_harvesting.verify as verify
from tests.test_ingesters import IngesterTestCase


class VerifierTestCase(django.test.TransactionTestCase):
    """ Test the verification code """

    class FakeResponseHtml:
        headers = {'content-type': 'html'}
        status_code=200

    class FakeResponseText:
        headers = {'content-type': 'text'}
        status_code=200

    class FakeResponseIncorrectStatusCode:
        headers = {'content-type': 'aaa'}
        status_code=504

    class SampleDownloadResponse:
        headers = {'content-type': 'application/x-netcdf;charset=ISO-8859-1'}
        status_code=200

    @mock.patch("builtins.open",autospec=True)
    @mock.patch('requests.head')
    def test_download_link_responded_with_html(self, mock_request, mock_open):
        """Shall write dataset to file from database because of unhealthy download link"""
        mock_request.return_value = self.FakeResponseHtml()
        dataset, _ = IngesterTestCase._create_dummy_dataset(IngesterTestCase, 'test')
        IngesterTestCase._create_dummy_dataset_uri(IngesterTestCase, 'http://test.uri/dataset', dataset)
        IngesterTestCase._create_dummy_dataset_uri(IngesterTestCase, 'http://anotherhost/dataset', dataset)
        verify.main(filename='')
        self.assertTrue(mock_open.mock_calls[2][1][0].startswith('http://test.uri/dataset'))
        self.assertTrue(mock_open.mock_calls[3][1][0].startswith('http://anotherhost/dataset'))

    @mock.patch("builtins.open",autospec=True)
    @mock.patch('requests.head')
    def test_download_link_responded_with_incorrect_status_code(self, mock_request, mock_open):
        """Shall write dataset to file from database because of unhealthy download link"""
        mock_request.return_value = self.FakeResponseIncorrectStatusCode()
        dataset, _ = IngesterTestCase._create_dummy_dataset(IngesterTestCase, 'test')
        IngesterTestCase._create_dummy_dataset_uri(IngesterTestCase, 'http://test.uri/dataset', dataset)
        IngesterTestCase._create_dummy_dataset_uri(IngesterTestCase, 'http://anotherhost/dataset', dataset)
        verify.main(filename='')
        self.assertEqual(len(mock_open.mock_calls), 5)
        self.assertTrue(mock_open.mock_calls[2][1][0].startswith('http://test.uri/dataset'))
        self.assertTrue(mock_open.mock_calls[3][1][0].startswith('http://anotherhost/dataset'))

    @mock.patch("builtins.open",autospec=True)
    @mock.patch('requests.head')
    def test_download_link_responded_with_text(self, mock_request, mock_open):
        """Shall write dataset to file from database because of unhealthy download link"""
        mock_request.return_value = self.FakeResponseText()
        dataset, _ = IngesterTestCase._create_dummy_dataset(IngesterTestCase, 'test')
        IngesterTestCase._create_dummy_dataset_uri(IngesterTestCase, 'http://test.uri/dataset', dataset)
        IngesterTestCase._create_dummy_dataset_uri(IngesterTestCase, 'http://anotherhost/dataset', dataset)
        verify.main(filename='')
        self.assertEqual(len(mock_open.mock_calls), 5)
        self.assertTrue(mock_open.mock_calls[2][1][0].startswith('http://test.uri/dataset'))
        self.assertTrue(mock_open.mock_calls[3][1][0].startswith('http://anotherhost/dataset'))

    @mock.patch("builtins.open",autospec=True)
    @mock.patch('requests.head')
    def test_download_link_responded_correctly(self, mock_request, mock_open):
        """Shall not remove dataset from database because of healthy download link"""
        mock_request.return_value = self.SampleDownloadResponse()
        dataset, _ = IngesterTestCase._create_dummy_dataset(IngesterTestCase, 'test')
        IngesterTestCase._create_dummy_dataset_uri(IngesterTestCase, 'http://test.uri/dataset', dataset)
        IngesterTestCase._create_dummy_dataset_uri(IngesterTestCase, 'http://anotherhost/dataset', dataset)
        verify.main(filename='')
        self.assertEqual(len(mock_open.mock_calls), 3)
