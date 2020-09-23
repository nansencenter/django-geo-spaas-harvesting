""" Test the verification code """
import unittest.mock as mock
import django
from geospaas.catalog.models import Dataset, DatasetURI
import geospaas_harvesting.verify as verify
from tests.test_ingesters import IngesterTestCase as itc

class VerifierTestCase(django.test.TransactionTestCase):
    """ Test the verification code """

    class FakeResponseHtml:
        headers={'content-type':'html'}

    class FakeResponseText:
        headers={'content-type':'text'}

    class SampleDownloadResponse:
        headers={'content-type':'netcdf'}

    @mock.patch("builtins.open")
    @mock.patch('requests.head')
    def test_download_link_responded_with_html(self,mock_request,mock_open):
        """Shall remove dataset from database because of unhealthy download link"""
        mock_request.return_value = self.FakeResponseHtml()
        dataset, _ = itc._create_dummy_dataset(itc,'test')
        itc._create_dummy_dataset_uri(itc,'http://test.uri/dataset', dataset)
        itc._create_dummy_dataset_uri(itc,'http://anotherhost/dataset', dataset)
        verify.main()
        self.assertEqual(DatasetURI.objects.all().count(),0)
        self.assertEqual(Dataset.objects.all().count(),0)

    @mock.patch("builtins.open")
    @mock.patch('requests.head')
    def test_download_link_responded_with_text(self,mock_request,mock_open):
        """Shall remove dataset from database because of unhealthy download link"""
        mock_request.return_value = self.FakeResponseText()
        dataset, _ = itc._create_dummy_dataset(itc,'test')
        itc._create_dummy_dataset_uri(itc,'http://test.uri/dataset', dataset)
        itc._create_dummy_dataset_uri(itc,'http://anotherhost/dataset', dataset)
        verify.main()
        self.assertEqual(DatasetURI.objects.all().count(),0)
        self.assertEqual(Dataset.objects.all().count(),0)

    @mock.patch("builtins.open")
    @mock.patch('requests.head')
    def test_download_link_responded_correctly(self,mock_request,mock_open):
        """Shall not remove dataset from database because of healthy download link"""
        mock_request.return_value = self.SampleDownloadResponse()
        dataset, _ = itc._create_dummy_dataset(itc,'test')
        itc._create_dummy_dataset_uri(itc,'http://test.uri/dataset', dataset)
        itc._create_dummy_dataset_uri(itc,'http://anotherhost/dataset', dataset)
        verify.main()
        self.assertEqual(DatasetURI.objects.all().count(),2)
        self.assertEqual(Dataset.objects.all().count(),1)
