"""Test suite for ingesters"""

import logging
import unittest.mock as mock
from datetime import datetime, timezone

import django.db
import django.db.utils
import django.test
from django.contrib.gis.geos.geometry import GEOSGeometry
from geospaas.catalog.models import Dataset, DatasetURI
from geospaas.vocabularies.models import DataCenter, ISOTopicCategory, Parameter

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.ingesters as ingesters


class IngesterTestCase(django.test.TransactionTestCase):
    """Test the base ingester class"""

    def setUp(self):
        self.patcher_param_count = mock.patch.object(Parameter.objects, 'count')
        self.mock_param_count = self.patcher_param_count.start()
        self.mock_param_count.return_value = 2
        self.ingester = ingesters.Ingester()

    def tearDown(self):
        self.patcher_param_count.stop()

    def test_max_db_threads_type_error(self):
        """An exception should be raised if the max_db_threads argument
        to the constructor is not an integer
        """
        with self.assertRaises(TypeError):
            ingesters.Ingester(max_db_threads='2')

    def _create_dummy_dataset(self, title):
        """Create dummy dataset for testing purposes"""

        data_center = DataCenter(short_name='test')
        data_center.save()
        iso_topic_category = ISOTopicCategory(name='TEST')
        iso_topic_category.save()
        dataset = Dataset(entry_title=title,
                          ISO_topic_category=iso_topic_category,
                          data_center=data_center)
        dataset.save()
        return (dataset, True)

    def _create_dummy_dataset_uri(self, uri, dataset):
        """Create dummy dataset URI for testing purposes"""
        dataset_uri = DatasetURI(uri=uri, dataset=dataset)
        dataset_uri.save()
        return (dataset_uri, True)

    def test_check_existing_uri(self):
        """The _uri_exists() method must return True if a URI already exists, False otherwise"""

        uri = 'http://test.uri/dataset'
        self.assertFalse(self.ingester._uri_exists(uri))

        dataset, _ = self._create_dummy_dataset('test')
        self._create_dummy_dataset_uri(uri, dataset)
        self.assertTrue(self.ingester._uri_exists(uri))

    def test_ingest_dataset(self):
        """Test ingesting a dataset from a NormalizedDatasetInfo object"""
        parameters = [
            Parameter(standard_name='parameter', short_name='param', units='bananas'),
            Parameter(standard_name='parameter', short_name='param', units='apples'),
            Parameter(standard_name='latitude', short_name='lat', units='degrees_north')
        ]
        for p in parameters:
            p.save()

        dataset_info = crawlers.NormalizedDatasetInfo('some_url', {
            'entry_title': 'title',
            'entry_id': 'id',
            'summary': 'sum-up',
            'time_coverage_start': '2022-01-01',
            'time_coverage_end': '2022-01-02',
            'platform': {
                'Series_Entity': 'Space-based Platforms',
                'Category': '',
                'Short_Name': '',
                'Long_Name': '',
            },
            'instrument': {
                'Short_Name': 'sar',
                'Category': '',
                'Class': '',
                'Type': '',
                'Subtype': '',
                'Long_Name': '',
            },
            'location_geometry': 'POINT(10 11)',
            'provider': {
                'Short_Name': 'someone',
                'Bucket_Level0': '',
                'Bucket_Level1': '',
                'Bucket_Level2': '',
                'Bucket_Level3': '',
                'Long_Name': '',
                'Data_Center_URL': '',
            },
            'iso_topic_category': {'iso_topic_category': 'oceans'},
            'gcmd_location': {
                'Location_Category': 'vertical location',
                'Location_Type': 'sea surface',
                'Location_Subregion1': '',
                'Location_Subregion2': '',
                'Location_Subregion3': '',
            },
            'dataset_parameters': [
                {
                    'standard_name': 'parameter',
                    'short_name': 'param',
                    'units': 'bananas'
                },
                {
                    'standard_name': 'latitude',
                    'short_name': 'lat',
                    'units': 'degrees_north'
                }
            ],
        })

        self.ingester._ingest_dataset(dataset_info)  # pylint: disable=protected-access
        dataset = Dataset.objects.last()
        self.assertEqual(dataset.entry_title, 'title')
        self.assertEqual(dataset.summary, 'sum-up')
        self.assertEqual(dataset.time_coverage_start,
                         datetime(2022, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(dataset.time_coverage_end,
                         datetime(2022, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(dataset.source.platform.series_entity, 'Space-based Platforms')
        self.assertEqual(dataset.source.instrument.short_name, 'sar')
        self.assertEqual(dataset.geographic_location.geometry,
                         GEOSGeometry(('POINT(10 11)'), srid=4326))
        self.assertEqual(dataset.data_center.short_name, 'someone')
        self.assertEqual(dataset.ISO_topic_category.name, 'oceans')
        self.assertEqual(dataset.gcmd_location.category, 'vertical location')
        self.assertEqual(dataset.gcmd_location.type, 'sea surface')
        self.assertListEqual(list(dataset.parameters.all()), [parameters[0]])


    def test_ingest_same_uri_twice(self):
        """Ingestion of the same URI must not happen twice and the attempt must be logged"""

        uri = 'http://test.uri/dataset'
        dataset, _ = self._create_dummy_dataset('test')
        self._create_dummy_dataset_uri(uri, dataset)
        # we get away with not using a NormalizedDatasetInfo because
        # the ingestion is interrupted before it becomes a problem
        dataset_info = crawlers.DatasetInfo(uri, {})

        with self.assertLogs(self.ingester.logger, level=logging.INFO) as logger_cm:
            self.ingester._ingest_dataset(dataset_info)

        self.assertTrue(any((
            record.getMessage().endswith('already present in the database')
            for record in logger_cm.records)))
        self.assertEqual(Dataset.objects.count(), 1)

    def test_log_on_ingestion_error(self):
        """The cause of the error must be logged if an exception is raised while ingesting"""
        with mock.patch.object(ingesters.Ingester, '_ingest_dataset') as mock_ingest_dataset:
            mock_ingest_dataset.side_effect = TypeError
            with self.assertLogs(self.ingester.logger, level=logging.ERROR) as logger_cm:
                self.ingester.ingest([crawlers.DatasetInfo(('some_url', {}))])
            self.assertEqual(logger_cm.records[0].message,
                             "Error during ingestion")
            self.assertIs(logger_cm.records[0].exc_info[0], TypeError)

    def test_log_on_ingestion_success(self):
        """All ingestion successes must be logged"""
        with mock.patch.object(ingesters.Ingester, '_ingest_dataset') as mock_ingest_dataset:
            mock_ingest_dataset.return_value = ('some_url', True, True)
            with self.assertLogs(self.ingester.logger, level=logging.INFO) as logger_cm:
                self.ingester.ingest([crawlers.DatasetInfo('some_url', {})])
                self.assertEqual(logger_cm.records[0].message,
                                 "Successfully created dataset from url: 'some_url'")

    def test_log_error_on_dataset_created_with_existing_uri(self):
        """
        An error must be logged if a dataset is created during ingestion, even if its URI already
        exists in the database (this should not be possible)
        """
        with mock.patch.object(ingesters.Ingester, '_ingest_dataset') as mock_ingest_dataset:
            mock_ingest_dataset.return_value = ('some_url', True, False)
            with self.assertLogs(self.ingester.logger, level=logging.WARNING) as logger_cm:
                self.ingester.ingest([crawlers.DatasetInfo('some_url', {})])
            self.assertEqual(logger_cm.records[0].message,
                             "The Dataset URI 'some_url' was not created.")

    def test_keyboard_interruption(self):
        """Test that keyboard interrupts are managed properly"""
        mock_futures = (mock.Mock(), KeyboardInterrupt)
        with mock.patch('concurrent.futures.ThreadPoolExecutor.submit',
                        side_effect=mock_futures) as mock_submit, \
                mock.patch('concurrent.futures.as_completed') as mock_as_completed:
            with self.assertRaises(KeyboardInterrupt), \
                 self.assertLogs(self.ingester.logger, level=logging.DEBUG):
                self.ingester.ingest([mock.Mock(), mock.Mock()])
            mock_futures[0].cancel.assert_called()
