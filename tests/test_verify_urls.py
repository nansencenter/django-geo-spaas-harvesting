""" Test the verification code """
import os
import tempfile
import unittest.mock as mock

import django
from geospaas_harvesting import verify_urls

from tests.test_ingesters import IngesterTestCase


class VerifierTestCase(django.test.TransactionTestCase):
    """ Test the verification code """

    @mock.patch('requests.head')
    def test_download_link_responded_correctly(self, mock_request):
        """Shall not write dataset to file from database because of healthy download link"""
        mock_request.return_value.status_code = 200
        dataset, _ = IngesterTestCase._create_dummy_dataset(IngesterTestCase, 'test')
        IngesterTestCase._create_dummy_dataset_uri(
            IngesterTestCase, 'http://test.uri/dataset', dataset)
        IngesterTestCase._create_dummy_dataset_uri(
            IngesterTestCase, 'http://anotherhost/dataset', dataset)
        with tempfile.TemporaryDirectory() as tmpdirname:
            os.chdir(tmpdirname)
            verify_urls.main()
            file_content = open(os.listdir()[0], "r").read()
            self.assertEqual('', file_content)
        os.chdir('..')

    @mock.patch('requests.head')
    def test_download_link_responded_with_incorrect_status_code(self, mock_request):
        """Shall write dataset to file from database because of unhealthy download link"""
        mock_request.return_value.status_code = 504
        dataset, _ = IngesterTestCase._create_dummy_dataset(IngesterTestCase, 'test')
        IngesterTestCase._create_dummy_dataset_uri(
            IngesterTestCase, 'http://test.uri/dataset', dataset)
        IngesterTestCase._create_dummy_dataset_uri(
            IngesterTestCase, 'http://anotherhost/dataset', dataset)
        with tempfile.TemporaryDirectory() as tmpdirname:
            os.chdir(tmpdirname)
            verify_urls.main()
            file_content = open(os.listdir()[0], "r").read()
            self.assertIn('http://test.uri/dataset', file_content)
            self.assertIn('http://anotherhost/dataset', file_content)
        os.chdir('..')
