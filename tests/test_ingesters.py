"""Test suite for ingesters"""

import json
import logging
import os
import time
import unittest.mock as mock
import xml.etree.ElementTree as ET
from collections import OrderedDict
from datetime import datetime
import pythesint as pti
import django.db
import django.db.utils
import django.test
import requests
from dateutil.tz import tzutc
from django.contrib.gis.geos.geometry import GEOSGeometry
from geospaas.catalog.models import Dataset, DatasetURI
from geospaas.vocabularies.models import DataCenter, ISOTopicCategory, Parameter

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

    def test_safety_exception(self):
        """ Raise the safety exception in the case of an empty parameters in the vocabulary  """
        self.mock_param_count.return_value = 0 #No parameter in the vocabulary
        with self.assertRaises(RuntimeError):
            ingesters.Ingester()

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

    def test_get_normalized_attributes_must_be_implemented(self):
        """An error must be raised if the _get_normalized_attributes() method is not implemented"""
        with self.assertRaises(NotImplementedError), self.assertLogs(self.ingester.LOGGER):
            self.ingester._get_normalized_attributes('')

    def test_ingest_same_uri_twice(self):
        """Ingestion of the same URI must not happen twice and the attempt must be logged"""

        uri = 'http://test.uri/dataset'
        dataset, _ = self._create_dummy_dataset('test')
        self._create_dummy_dataset_uri(uri, dataset)

        with mock.patch.object(ingesters.Ingester, '_ingest_dataset') as mock_ingest_dataset:
            with self.assertLogs(self.ingester.LOGGER, level=logging.INFO) as logger_cm:
                self.ingester.ingest([uri])

        self.assertTrue(logger_cm.records[0].msg.endswith('is already present in the database'))
        self.assertEqual(Dataset.objects.count(), 1)
        self.assertFalse(mock_ingest_dataset.called)

    def test_log_on_ingestion_error(self):
        """The cause of the error must be logged if an exception is raised while ingesting"""
        self.ingester._to_ingest.put(('some_url', {}))
        self.ingester._to_ingest.put(None)
        with mock.patch.object(ingesters.Ingester, '_ingest_dataset') as mock_ingest_dataset:
            mock_ingest_dataset.side_effect = TypeError
            with self.assertLogs(self.ingester.LOGGER, level=logging.ERROR) as logger_cm:
                self.ingester._thread_ingest_dataset()
            self.assertEqual(logger_cm.records[0].message,
                             "Ingestion of the dataset at 'some_url' failed")

    def test_log_on_ingestion_database_error(self):
        """
        The cause of the error must be logged if a database exception is raised while ingesting
        """
        patcher = mock.patch.object(ingesters.DataCenter.objects, 'get_or_create')
        with patcher as mock_ingest_dataset:
            mock_ingest_dataset.side_effect = django.db.utils.OperationalError
            with self.assertLogs(self.ingester.LOGGER, level=logging.ERROR) as logger_cm:
                self.ingester._ingest_dataset('', {'provider': ''})
            self.assertTrue(logger_cm.records[0].message.startswith('Database insertion failed'))

    def test_log_on_ingestion_success(self):
        """All ingestion successes must be logged"""
        self.ingester._to_ingest.put(('some_url', {}))
        self.ingester._to_ingest.put(None)
        with mock.patch.object(ingesters.Ingester, '_ingest_dataset') as mock_ingest_dataset:
            mock_ingest_dataset.return_value = (True, True)
            with self.assertLogs(self.ingester.LOGGER, level=logging.INFO) as logger_cm:
                self.ingester._thread_ingest_dataset()
                self.assertEqual(logger_cm.records[0].message,
                                 "Successfully created dataset from url: 'some_url'")

    def test_log_error_on_dataset_created_with_existing_uri(self):
        """
        An error must be logged if a dataset is created during ingestion, even if its URI already
        exists in the database (this should not be possible)
        """
        self.ingester._to_ingest.put(('some_url', {}))
        self.ingester._to_ingest.put(None)
        with mock.patch.object(ingesters.Ingester, '_ingest_dataset') as mock_ingest_dataset:
            mock_ingest_dataset.return_value = (True, False)
            with self.assertLogs(self.ingester.LOGGER, level=logging.ERROR) as logger_cm:
                self.ingester._thread_ingest_dataset()
            self.assertEqual(logger_cm.records[0].message,
                             "The Dataset's URI already exists. This should never happen.")

    def test_log_on_dataset_already_ingested_from_different_uri(self):
        """A message must be logged if a dataset was already ingested from a different URI"""
        self.ingester._to_ingest.put(('some_url', {}))
        self.ingester._to_ingest.put(None)
        with mock.patch.object(ingesters.Ingester, '_ingest_dataset') as mock_ingest_dataset:
            mock_ingest_dataset.return_value = (False, True)
            with self.assertLogs(self.ingester.LOGGER, level=logging.INFO) as logger_cm:
                self.ingester._thread_ingest_dataset()
            self.assertEqual(logger_cm.records[0].message,
                             "Dataset at 'some_url' already exists in the database.")

    def test_log_on_metadata_fetching_error(self):
        """A message must be logged if an error occurs while fetching the metadata for a dataset"""
        with mock.patch.object(ingesters.Ingester, '_get_normalized_attributes') as mock_normalize:
            mock_normalize.side_effect = TypeError
            with self.assertLogs(self.ingester.LOGGER, level=logging.ERROR) as logger_cm:
                self.ingester._thread_get_normalized_attributes('some_url')
            self.assertEqual(logger_cm.records[0].message, "Could not get metadata from 'some_url'")

    def test_fetching_threads_stop_on_keyboard_interrupt(self):
        """
        Test that the scheduled threads which fetch datasets metadata are stopped when a
        KeyboardInterrupt (i.e. SIGINT or SIGTERM) occurs. If the exception is not correctly
        handled, this test will hang.
        """

        ingester = ingesters.Ingester(max_fetcher_threads=100, max_db_threads=1)
        fetcher_patcher = mock.patch.object(ingesters.Ingester, '_thread_get_normalized_attributes')
        uri_exists_patcher = mock.patch.object(ingesters.Ingester, '_uri_exists')

        def sleep():
            time.sleep(1)

        def uri_exists(uri):
            if uri < 99:
                return False
            else:
                raise KeyboardInterrupt

        with fetcher_patcher as fetcher_mock, uri_exists_patcher as uri_exists_mock:
            fetcher_mock.side_effect = sleep
            uri_exists_mock.side_effect = uri_exists
            with self.assertRaises(KeyboardInterrupt):
                ingester.ingest(range(100))


class MetanormIngesterTestCase(django.test.TestCase):
    """Test the base metadata ingester class"""

    def setUp(self):
        self.patcher_param_count = mock.patch.object(Parameter.objects, 'count')
        self.mock_param_count = self.patcher_param_count.start()
        self.mock_param_count.return_value = 2
        self.ingester = ingesters.MetanormIngester()
        self.value_for_testing = {
            'entry_title': 'title_value',
            'summary': 'summary_value',
            'time_coverage_start': datetime(
                year=2020, month=1, day=1, hour=0, minute=0, second=1, tzinfo=tzutc()),
            'time_coverage_end': datetime(
                year=2020, month=1, day=1, hour=0, minute=5, second=59, tzinfo=tzutc()),
            'platform': OrderedDict([
                ('Category', 'platform_category'),
                ('Series_Entity', 'platform_series_entity'),
                ('Short_Name', 'platform_short_name'),
                ('Long_Name', 'platform_long_name')]),
            'instrument': OrderedDict([('Category', 'instrument_category'),
                                       ('Class', 'instrument_class'),
                                       ('Type', 'instrument_type'),
                                       ('Subtype', 'instrument_subtype'),
                                       ('Short_Name', 'instrument_short_name'),
                                       ('Long_Name', 'instrument_long_name')]),
            'location_geometry': GEOSGeometry(('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'), srid=4326),
            'provider': OrderedDict([('Bucket_Level0', 'provider_bucket_level0'),
                                     ('Bucket_Level1', 'provider_bucket_level1'),
                                     ('Bucket_Level2', 'provider_bucket_level2'),
                                     ('Bucket_Level3', 'provider_bucket_level3'),
                                     ('Short_Name', 'provider_short_name'),
                                     ('Long_Name', 'provider_long_name'),
                                     ('Data_Center_URL', 'provider_data_center_url')]),
            'iso_topic_category': OrderedDict([('iso_topic_category', 'category_value')]),
            'gcmd_location': OrderedDict([('Location_Category', 'gcmd_location_category'),
                                          ('Location_Type', 'gcmd_location_type'),
                                          ('Location_Subregion1', 'gcmd_location_subregion1'),
                                          ('Location_Subregion2', 'gcmd_location_subregion2'),
                                          ('Location_Subregion3', 'gcmd_location_subregion3')]),
            'dataset_parameters': [pti.get_wkv_variable('surface_backwards_scattering_coefficient_of_radar_wave'),
                                   {'standard_name': 'latitude'}, {'standard_name': 'longitude'}, ]
        }

    def tearDown(self):
        self.patcher_param_count.stop()

    def test_get_normalized_attributes_must_be_implemented(self):
        """An error must be raised if the _get_normalized_attributes() method is not implemented"""
        with self.assertRaises(NotImplementedError), self.assertLogs(self.ingester.LOGGER):
            self.ingester._get_normalized_attributes('')

    def test_ingest_from_metadata(self):
        """Test that a dataset is created with the correct values from metadata"""
        datasets_count = Dataset.objects.count()

        # Create a dataset from these values
        self.ingester._ingest_dataset('http://test.uri/dataset', self.value_for_testing)

        self.assertTrue(Dataset.objects.count() == datasets_count + 1)
        inserted_dataset = Dataset.objects.latest('id')

        # Check that the dataset was created correctly
        self.assertEqual(inserted_dataset.entry_title, 'title_value')
        self.assertEqual(inserted_dataset.summary, 'summary_value')
        self.assertEqual(inserted_dataset.time_coverage_start,
                         self.value_for_testing['time_coverage_start'])
        self.assertEqual(inserted_dataset.time_coverage_end,
                         self.value_for_testing['time_coverage_end'])

        self.assertEqual(inserted_dataset.source.platform.category, 'platform_category')
        self.assertEqual(inserted_dataset.source.platform.series_entity, 'platform_series_entity')
        self.assertEqual(inserted_dataset.source.platform.short_name, 'platform_short_name')
        self.assertEqual(inserted_dataset.source.platform.long_name, 'platform_long_name')

        self.assertEqual(inserted_dataset.source.instrument.category, 'instrument_category')
        self.assertEqual(inserted_dataset.source.instrument.instrument_class, 'instrument_class')
        self.assertEqual(inserted_dataset.source.instrument.type, 'instrument_type')
        self.assertEqual(inserted_dataset.source.instrument.subtype, 'instrument_subtype')
        self.assertEqual(inserted_dataset.source.instrument.short_name, 'instrument_short_name')
        self.assertEqual(inserted_dataset.source.instrument.long_name, 'instrument_long_name')

        self.assertEqual(inserted_dataset.geographic_location.geometry,
                         GEOSGeometry(('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'), srid=4326))

        self.assertEqual(inserted_dataset.data_center.bucket_level0, 'provider_bucket_level0')
        self.assertEqual(inserted_dataset.data_center.bucket_level1, 'provider_bucket_level1')
        self.assertEqual(inserted_dataset.data_center.bucket_level2, 'provider_bucket_level2')
        self.assertEqual(inserted_dataset.data_center.bucket_level3, 'provider_bucket_level3')
        self.assertEqual(inserted_dataset.data_center.short_name, 'provider_short_name')
        self.assertEqual(inserted_dataset.data_center.long_name, 'provider_long_name')
        self.assertEqual(inserted_dataset.data_center.data_center_url, 'provider_data_center_url')

        self.assertEqual(inserted_dataset.ISO_topic_category.name, 'category_value')

        self.assertEqual(inserted_dataset.gcmd_location.category, 'gcmd_location_category')
        self.assertEqual(inserted_dataset.gcmd_location.type, 'gcmd_location_type')
        self.assertEqual(inserted_dataset.gcmd_location.subregion1, 'gcmd_location_subregion1')
        self.assertEqual(inserted_dataset.gcmd_location.subregion2, 'gcmd_location_subregion2')
        self.assertEqual(inserted_dataset.gcmd_location.subregion3, 'gcmd_location_subregion3')

    def test_ingest_from_metadata_string_geometry(self):
        """Test that a dataset is created with the correct values from metadata with WKT string """
        self.value_for_testing['location_geometry'] = 'POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'
        # Create a dataset from these values
        self.ingester._ingest_dataset('http://test.uri/dataset', self.value_for_testing)
        inserted_dataset = Dataset.objects.latest('id')
        self.assertEqual(inserted_dataset.geographic_location.geometry,
                         GEOSGeometry(('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'), srid=4326))

class DDXIngesterTestCase(django.test.TestCase):
    """Test the DDXIngester"""

    TEST_DATA = {
        'full_ddx': {
            'url': "https://test-opendap.com/full_dataset.nc.ddx",
            'file_path': "data/opendap/full_ddx.xml"},
        'full_ddx_2': {
            'url': "https://test-opendap2.com/full_dataset.nc.ddx",
            'file_path': "data/opendap/full_ddx.xml"},
        'short_ddx': {
            'url': "https://test-opendap.com/short_dataset.nc.ddx",
            'file_path': "data/opendap/short_ddx.xml"},
        'no_ns_ddx': {
            'url': "https://test-opendap.com/no_ns_dataset.nc.ddx",
            'file_path': "data/opendap/ddx_no_ns.xml"},
    }

    def requests_get_side_effect(self, url, **kwargs):
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
        self.patcher_param_count = mock.patch.object(Parameter.objects, 'count')
        self.mock_param_count = self.patcher_param_count.start()
        self.mock_param_count.return_value = 2
        # Mock requests.get()
        self.patcher_requests_get = mock.patch('geospaas_harvesting.ingesters.requests.get')
        self.mock_requests_get = self.patcher_requests_get.start()
        self.mock_requests_get.side_effect = self.requests_get_side_effect
        self.opened_files = []

    def tearDown(self):
        self.patcher_requests_get.stop()
        self.patcher_param_count.stop()
        # Close any files opened during the test
        for opened_file in self.opened_files:
            opened_file.close()

    def test_get_xml_namespace(self):
        """Get xml namespace from the test data DDX file"""
        test_file_path = os.path.join(
            os.path.dirname(__file__),
            self.TEST_DATA['short_ddx']['file_path'])

        with open(test_file_path, 'rb') as test_file:
            root = ET.parse(test_file).getroot()

        ingester = ingesters.DDXIngester()

        self.assertEqual(ingester._get_xml_namespace(root), 'http://xml.opendap.org/ns/DAP/3.2#')

    def test_logging_if_no_namespace(self):
        """A warning must be logged if no namespace has been found, and an empty string returned"""
        test_file_path = os.path.join(
            os.path.dirname(__file__),
            self.TEST_DATA['no_ns_ddx']['file_path'])

        with open(test_file_path, 'rb') as test_file:
            root = ET.parse(test_file).getroot()

        ingester = ingesters.DDXIngester()

        with self.assertLogs(ingester.LOGGER, level=logging.WARNING):
            namespace = ingester._get_xml_namespace(root)

        self.assertEqual(namespace, '')

    def test_extract_global_attributes(self):
        """Get nc_global attributes from the test data DDX file"""
        test_file_path = os.path.join(
            os.path.dirname(__file__),
            self.TEST_DATA['short_ddx']['file_path'])

        with open(test_file_path, 'rb') as test_file:
            root = ET.parse(test_file).getroot()

        ingester = ingesters.DDXIngester()
        self.assertDictEqual(
            ingester._extract_attributes(root),
            {
                'Conventions': 'CF-1.7, ACDD-1.3',
                'raw_dataset_parameters': [],
                'title': 'VIIRS L2P Sea Surface Skin Temperature'
            }
        )

    def test_get_normalized_attributes(self):
        """Test that the correct attributes are extracted from a DDX file"""
        ingester = ingesters.DDXIngester()
        normalized_parameters = ingester._get_normalized_attributes(
            self.TEST_DATA['full_ddx']['url'])

        self.assertEqual(normalized_parameters['entry_title'],
                         'VIIRS L2P Sea Surface Skin Temperature')
        self.assertEqual(normalized_parameters['summary'], (
            'Sea surface temperature (SST) retrievals produced at the NASA OBPG for the Visible I' +
            'nfrared Imaging\n                Radiometer Suite (VIIRS) sensor on the Suomi Nation' +
            'al Polar-Orbiting Partnership (Suomi NPP) platform.\n                These have been' +
            ' reformatted to GHRSST GDS version 2 Level 2P specifications by the JPL PO.DAAC. VII' +
            'RS\n                SST algorithms developed by the University of Miami, RSMAS'))
        self.assertEqual(normalized_parameters['time_coverage_start'], datetime(
            year=2020, month=1, day=1, hour=0, minute=0, second=1, tzinfo=tzutc()))
        self.assertEqual(normalized_parameters['time_coverage_end'], datetime(
            year=2020, month=1, day=1, hour=0, minute=5, second=59, tzinfo=tzutc()))

        self.assertEqual(normalized_parameters['instrument']['Short_Name'], 'VIIRS')
        self.assertEqual(normalized_parameters['instrument']['Long_Name'],
                         'Visible-Infrared Imager-Radiometer Suite')
        self.assertEqual(normalized_parameters['instrument']['Category'],
                         'Earth Remote Sensing Instruments')
        self.assertEqual(normalized_parameters['instrument']['Subtype'],
                         'Imaging Spectrometers/Radiometers')
        self.assertEqual(normalized_parameters['instrument']['Class'],
                         'Passive Remote Sensing')

        self.assertEqual(normalized_parameters['platform']['Short_Name'], 'SUOMI-NPP')
        self.assertEqual(normalized_parameters['platform']['Long_Name'],
                         'Suomi National Polar-orbiting Partnership')
        self.assertEqual(normalized_parameters['platform']['Category'],
                         'Earth Observation Satellites')
        self.assertEqual(normalized_parameters['platform']['Series_Entity'],
                         'Joint Polar Satellite System (JPSS)')

        self.assertEqual(normalized_parameters['location_geometry'], GEOSGeometry(
            'POLYGON((' +
            '-175.084000 -15.3505001,' +
            '-142.755005 -15.3505001,' +
            '-142.755005 9.47472000,' +
            '-175.084000 9.47472000,' +
            '-175.084000 -15.3505001))',
            srid=4326
        ))

        self.assertEqual(normalized_parameters['provider']['Short_Name'],
                         'The GHRSST Project Office')
        self.assertEqual(normalized_parameters['provider']['Long_Name'],
                         'The GHRSST Project Office')
        self.assertEqual(normalized_parameters['provider']['Data_Center_URL'],
                         'http://www.ghrsst.org')

        self.assertEqual(normalized_parameters['iso_topic_category']
                         ['iso_topic_category'], 'Oceans')

        self.assertEqual(normalized_parameters['gcmd_location']
                         ['Location_Category'], 'VERTICAL LOCATION')
        self.assertEqual(normalized_parameters['gcmd_location']['Location_Type'], 'SEA SURFACE')

    def test_ingest_dataset_twice_different_urls(self):
        """The same dataset must not be ingested twice even if it is present at different URLs"""
        initial_datasets_count = Dataset.objects.count()

        ingester = ingesters.DDXIngester()
        with self.assertLogs(ingester.LOGGER):
            ingester.ingest([self.TEST_DATA['full_ddx']['url']])
        self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

        with self.assertLogs(ingester.LOGGER, level=logging.INFO) as logger_cm:
            ingester.ingest([self.TEST_DATA['full_ddx_2']['url']])

        self.assertTrue(logger_cm.records[0].msg.endswith('already exists in the database.'))
        self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

    def test_function_named_prepare_url(self):
        """ test the functionality of 'prepare_url' for a ddxingester """
        input_url = 'https://opendap.jpl.nasa.gov/opendap/allData/ghrsst/data/GDS2/L2P/VIIRS_NPP/NAVO/v1/2014/005/20140105235906-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv01.0.nc'
        output_url = 'https://opendap.jpl.nasa.gov/opendap/allData/ghrsst/data/GDS2/L2P/VIIRS_NPP/NAVO/v1/2014/005/20140105235906-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv01.0.nc.ddx'
        ingester = ingesters.DDXIngester()
        self.assertEqual(output_url, ingester.prepare_url(input_url))

    def test_function_named_prepare_url2(self):
        """ no change when a ddx file has been given to the function """
        input_url = 'https://opendap.jpl.nasa.gov/opendap/allData/ghrsst/data/GDS2/L2P/VIIRS_NPP/NAVO/v1/2014/005/20140105235906-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv01.0.nc.ddx'
        output_url = 'https://opendap.jpl.nasa.gov/opendap/allData/ghrsst/data/GDS2/L2P/VIIRS_NPP/NAVO/v1/2014/005/20140105235906-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv01.0.nc.ddx'
        ingester = ingesters.DDXIngester()
        self.assertEqual(output_url, ingester.prepare_url(input_url))

    def test_function_named_prepare_url3(self):
        """ test the functionality of 'prepare_url' for a OSISAF ingester """
        input_url = 'https://thredds.met.no/thredds/dodsC/osisaf/met.no/ice/amsr2_conc/2019/11/ice_conc_nh_polstere-100_amsr2_201911011200.nc.dods'
        output_url = 'https://thredds.met.no/thredds/dodsC/osisaf/met.no/ice/amsr2_conc/2019/11/ice_conc_nh_polstere-100_amsr2_201911011200.nc.ddx'
        ingester = ingesters.DDXIngester()
        self.assertEqual(output_url, ingester.prepare_url(input_url))


class CopernicusODataIngesterTestCase(django.test.TestCase):
    """Test the CopernicusODataIngester"""
    fixtures = [os.path.join(os.path.dirname(__file__), "fixtures", "harvest")]
    TEST_DATA = {
        'full': {
            'url': "https://scihub.copernicus.eu/full?$format=json&$expand=Attributes",
            'file_path': "data/copernicus_opensearch/full.json"}
    }

    def requests_get_side_effect(self, url, **kwargs):  # pylint: disable=unused-argument
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
            raise requests.exceptions.HTTPError()

        return response

    def setUp(self):
        self.ingester = ingesters.CopernicusODataIngester()
        # Mock requests.get()
        self.patcher_requests_get = mock.patch.object(ingesters.requests, 'get')
        self.mock_requests_get = self.patcher_requests_get.start()
        self.mock_requests_get.side_effect = self.requests_get_side_effect
        self.opened_files = []

        self.patcher_param_count = mock.patch.object(Parameter.objects, 'count')
        self.mock_param_count = self.patcher_param_count.start()
        self.mock_param_count.return_value = 2

    def tearDown(self):
        self.patcher_requests_get.stop()
        self.patcher_param_count.stop()
        # Close any files opened during the test
        for opened_file in self.opened_files:
            opened_file.close()

    def test_instantiation(self):
        """Test that the attributes of the CopernicusODataIngester are correctly initialized"""
        ingester = ingesters.CopernicusODataIngester(username='test', password='test')
        self.assertEqual(ingester._credentials, ('test', 'test'))

    def test_build_metadata_url(self):
        """Test that the metadata URL is correctly built from the dataset URL"""
        test_url = 'http://scihub.copernicus.eu/dataset/$value'
        expected_result = 'http://scihub.copernicus.eu/dataset?$format=json&$expand=Attributes'

        self.assertEqual(self.ingester._build_metadata_url(test_url), expected_result)

    def test_get_raw_metadata(self):
        """Test that the raw metadata is correctly fetched"""
        raw_metadata = self.ingester._get_raw_metadata('https://scihub.copernicus.eu/full/$value')
        test_file_path = os.path.join(
            os.path.dirname(__file__), self.TEST_DATA['full']['file_path'])

        with open(test_file_path, 'rb') as test_file_handler:
            self.assertDictEqual(json.load(test_file_handler), raw_metadata)

    def test_log_on_inexistent_metadata_page(self):
        """An error must be logged in case the metadata URL points to nothing"""
        with self.assertLogs(self.ingester.LOGGER, level=logging.ERROR):
            self.ingester._get_raw_metadata('http://nothing/$value')

    def test_log_on_invalid_dataset_url(self):
        """An An error must be logged in case the dataset URL does not match the ingester's regex"""
        with self.assertLogs(self.ingester.LOGGER, level=logging.ERROR):
            self.ingester._get_raw_metadata('')

    def test_get_normalized_attributes(self):
        """Test that the correct attributes are extracted from Sentinel-SAFE JSON metadata"""
        normalized_parameters = self.ingester._get_normalized_attributes(
            'https://scihub.copernicus.eu/full/$value')

        self.assertEqual(normalized_parameters['entry_title'],
                         'S1A_IW_GRDH_1SDV_20200318T062305_20200318T062330_031726_03A899_F558')
        self.assertEqual(normalized_parameters['summary'], (
            'Date: 2020-03-18T06:23:05.976Z, Instrument: SAR-C, Mode: IW, ' +
            'Satellite: Sentinel-1, Size: 1.65 GB'))
        self.assertEqual(normalized_parameters['time_coverage_start'], datetime(
            year=2020, month=3, day=18, hour=6, minute=23, second=5,
            tzinfo=tzutc()))
        self.assertEqual(normalized_parameters['time_coverage_end'], datetime(
            year=2020, month=3, day=18, hour=6, minute=23, second=30,
            tzinfo=tzutc()))

        self.assertEqual(normalized_parameters['instrument']['Short_Name'], 'SENTINEL-1 C-SAR')
        self.assertEqual(normalized_parameters['instrument']['Long_Name'], '')
        self.assertEqual(normalized_parameters['instrument']['Category'],
                         'Earth Remote Sensing Instruments')
        self.assertEqual(normalized_parameters['instrument']['Subtype'], '')
        self.assertEqual(normalized_parameters['instrument']['Class'], 'Active Remote Sensing')

        self.assertEqual(normalized_parameters['platform']['Short_Name'], 'SENTINEL-1A')
        self.assertEqual(normalized_parameters['platform']['Long_Name'], 'SENTINEL-1A')
        self.assertEqual(normalized_parameters['platform']['Category'],
                         'Earth Observation Satellites')
        self.assertEqual(normalized_parameters['platform']['Series_Entity'], 'SENTINEL-1')

        self.assertEqual(normalized_parameters['location_geometry'], GEOSGeometry(
            'MULTIPOLYGON(((' +
            '-0.694377 50.983601,' +
            '-0.197663 52.476219,' +
            '-4.065843 52.891499,' +
            '-4.436811 51.396446,' +
            '-0.694377 50.983601)))',
            srid='4326'  # TODO: check whether this should be an integer in metanorm
        ))

        self.assertEqual(normalized_parameters['provider']
                         ['Bucket_Level0'], 'MULTINATIONAL ORGANIZATIONS')
        self.assertEqual(normalized_parameters['provider']['Bucket_Level1'], '')
        self.assertEqual(normalized_parameters['provider']['Bucket_Level2'], '')
        self.assertEqual(normalized_parameters['provider']['Bucket_Level3'], '')
        self.assertEqual(normalized_parameters['provider']['Short_Name'], 'ESA/EO')
        self.assertEqual(normalized_parameters['provider']['Long_Name'],
                         'Observing the Earth, European Space Agency')
        self.assertEqual(normalized_parameters['provider']['Data_Center_URL'],
                         'http://www.esa.int/esaEO/')

        self.assertEqual(normalized_parameters['iso_topic_category']
                         ['iso_topic_category'], 'Oceans')

        self.assertEqual(normalized_parameters['gcmd_location']
                         ['Location_Category'], 'VERTICAL LOCATION')
        self.assertEqual(normalized_parameters['gcmd_location']['Location_Type'], 'SEA SURFACE')
        self.assertEqual(normalized_parameters['gcmd_location']['Location_Subregion1'], '')
        self.assertEqual(normalized_parameters['gcmd_location']['Location_Subregion2'], '')
        self.assertEqual(normalized_parameters['gcmd_location']['Location_Subregion3'], '')

    def test_parameter_assignment_for_attributes(self):
        """Shall assign the correct parameter to dataset
        from Sentinel-SAFE JSON metadata (only one time execution)"""

        value_for_testing = {
            'entry_title': 'title_value',
            'summary': 'summary_value',
            'time_coverage_start': datetime(
                year=2020, month=1, day=1, hour=0, minute=0, second=1, tzinfo=tzutc()),
            'time_coverage_end': datetime(
                year=2020, month=1, day=1, hour=0, minute=5, second=59, tzinfo=tzutc()),
            'platform': OrderedDict([
                ('Category', 'platform_category'),
                ('Series_Entity', 'platform_series_entity'),
                ('Short_Name', 'platform_short_name'),
                ('Long_Name', 'platform_long_name')]),
            'instrument': OrderedDict([('Category', 'instrument_category'),
                                       ('Class', 'instrument_class'),
                                       ('Type', 'instrument_type'),
                                       ('Subtype', 'instrument_subtype'),
                                       ('Short_Name', 'instrument_short_name'),
                                       ('Long_Name', 'instrument_long_name')]),
            'location_geometry': GEOSGeometry(('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'), srid=4326),
            'provider': OrderedDict([('Bucket_Level0', 'provider_bucket_level0'),
                                     ('Bucket_Level1', 'provider_bucket_level1'),
                                     ('Bucket_Level2', 'provider_bucket_level2'),
                                     ('Bucket_Level3', 'provider_bucket_level3'),
                                     ('Short_Name', 'provider_short_name'),
                                     ('Long_Name', 'provider_long_name'),
                                     ('Data_Center_URL', 'provider_data_center_url')]),
            'iso_topic_category': OrderedDict([('iso_topic_category', 'category_value')]),
            'gcmd_location': OrderedDict([('Location_Category', 'gcmd_location_category'),
                                          ('Location_Type', 'gcmd_location_type'),
                                          ('Location_Subregion1', 'gcmd_location_subregion1'),
                                          ('Location_Subregion2', 'gcmd_location_subregion2'),
                                          ('Location_Subregion3', 'gcmd_location_subregion3')]),
            'dataset_parameters': [pti.get_wkv_variable('surface_backwards_scattering_coefficient_of_radar_wave')]
        }
        duplicate_value_for_testing = value_for_testing.copy()
        created_dataset, created_dataset_uri = self.ingester._ingest_dataset(
            'https://scihub.copernicus.eu/full/$value', value_for_testing)
        self.assertEqual(Dataset.objects.count(), 1)
        self.assertEqual(Dataset.objects.first().datasetparameter_set.count(), 1)
        # the parameter that has added (by above variable of "value_for_testing") to the dataset
        # should be equal to the first object of parameter table
        # which is created by fixtures inside the database
        self.assertEqual(
            Dataset.objects.first().datasetparameter_set.first().parameter, Parameter.objects.first())
        self.assertEqual(created_dataset, True)
        self.assertEqual(created_dataset_uri, True)

        # No parameter or dataset should be added for the second time of executing this command
        # with same normalized attributes (same variable of "value_for_testing")
        created_dataset, created_dataset_uri = self.ingester._ingest_dataset(
            'https://scihub.copernicus.eu/full/$value', duplicate_value_for_testing)

        self.assertEqual(Dataset.objects.count(), 1)
        self.assertEqual(Dataset.objects.first().datasetparameter_set.count(), 1)
        self.assertEqual(
            Dataset.objects.first().datasetparameter_set.first().parameter, Parameter.objects.first())
        self.assertEqual(created_dataset, False)
        self.assertEqual(created_dataset_uri, False)


class NansatIngesterTestCase(django.test.TestCase):
    """Test the NansatIngester"""

    def setUp(self):
        self.patcher_param_count = mock.patch.object(Parameter.objects, 'count')
        self.mock_param_count = self.patcher_param_count.start()
        self.mock_param_count.return_value = 2

    def tearDown(self):
        self.patcher_param_count.stop()

    def test_normalize_netcdf_attributes_with_nansat(self):
        """Test the ingestion of a netcdf file using nansat"""
        ingester = ingesters.NansatIngester()
        normalized_attributes = ingester._get_normalized_attributes(
            os.path.join(os.path.dirname(__file__), 'data/nansat/arc_metno_dataset.nc'))

        self.assertEqual(normalized_attributes['entry_title'], 'NONE')
        self.assertEqual(normalized_attributes['summary'], 'NONE')
        self.assertEqual(normalized_attributes['time_coverage_start'], datetime(
            year=2017, month=5, day=18, hour=0, minute=0, second=0))
        self.assertEqual(normalized_attributes['time_coverage_end'], datetime(
            year=2017, month=5, day=27, hour=0, minute=0, second=0))

        self.assertEqual(normalized_attributes['instrument']['Short_Name'], 'Computer')
        self.assertEqual(normalized_attributes['instrument']['Long_Name'], 'Computer')
        self.assertEqual(normalized_attributes['instrument']['Category'],
                         'In Situ/Laboratory Instruments')
        self.assertEqual(normalized_attributes['instrument']['Subtype'], '')
        self.assertEqual(normalized_attributes['instrument']['Class'], 'Data Analysis')

        self.assertEqual(normalized_attributes['platform']['Short_Name'], 'MODELS')
        self.assertEqual(normalized_attributes['platform']['Long_Name'], '')
        self.assertEqual(normalized_attributes['platform']['Category'], 'Models/Analyses')
        self.assertEqual(normalized_attributes['platform']['Series_Entity'], '')

        expected_geometry = GEOSGeometry(
            'POLYGON ((20.704223629342 89.99989256067724, 24.995696372329 89.99987077812315, ' +
            '28.03728216024454 89.99984848089234, 30.2939221300575 89.99982586658082, ' +
            '32.02978515699697 89.99980304437932, 33.40416281560302 89.99978007899939, ' +
            '34.5181331485164 89.99975701103389, 35.43865303831961 89.99973386715732, ' +
            '36.21171468995888 89.99971066558552, 36.86989764584402 89.99968741916564, ' +
            '37.60881652342161 89.99965636990923, 37.60881652342161 89.99965636990923, ' +
            '33.5815529218009 89.99965234415896, 29.66530387656485 89.99964664479596, ' +
            '25.89035287122399 89.99963935115623, 22.28000271854232 89.99963055765173, ' +
            '18.850402039079 89.99962036849348, 15.61098853367965 89.99960889274531, ' +
            '12.5653483799073 89.99959624009551, 9.712286628119696 89.99958251753553, ' +
            '7.046939023377033 89.99956782697018, 4.128626239815715 89.99954934720112, ' +
            '4.128626239815715 89.99954934720112, 1.379076461628385 89.99957255048969, ' +
            '-0.883102001486741 89.99958924373496, -3.332737430412262 89.99960524311808, ' +
            '-5.983994159666929 89.99962046088012, -8.849993050812204 89.99963479929851, ' +
            '-11.94175268863764 89.99964815085281, -15.26682844732823 89.99966039912611, ' +
            '-18.82767840387169 89.99967142071603, -22.61986494804042 89.99968108841456, ' +
            '-26.63030830092556 89.99968927582337, -26.63030830092556 89.99968927582337, ' +
            '-24.77514056883192 89.99971674199658, -22.90124981623723 89.99973970559563, ' +
            '-20.66768923166103 89.99976233715211, -17.96913974015701 89.9997845320129, ' +
            '-14.66023223730137 89.99980614012912, -10.53918372862823 89.99982694157781, ' +
            '-5.328234781979837 89.99984660783163, 1.34000778089971 89.99986464317887, ' +
            '9.898259944152521 89.99988030809297, 20.704223629342 89.99989256067724))',
            srid=4326
        )

        # This fails, which is why string representations are compared. Any explanation is welcome.
        # self.assertTrue(normalized_attributes['location_geometry'].equals(expected_geometry))
        self.assertEqual(str(normalized_attributes['location_geometry']), str(expected_geometry))

        self.assertEqual(normalized_attributes['provider']['Short_Name'], 'NERSC')
        self.assertEqual(normalized_attributes['provider']['Long_Name'],
                         'Nansen Environmental and Remote Sensing Centre')
        self.assertEqual(normalized_attributes['provider']['Data_Center_URL'],
                         'http://www.nersc.no/main/index2.php')

        self.assertEqual(
            normalized_attributes['iso_topic_category']['iso_topic_category'], 'Oceans')

        self.assertEqual(
            normalized_attributes['gcmd_location']['Location_Category'], 'VERTICAL LOCATION')
        self.assertEqual(normalized_attributes['gcmd_location']['Location_Type'], 'SEA SURFACE')

    # TODO: make this work
    # def test_ingest_dataset_twice_different_urls(self):
    #     """The same dataset must not be ingested twice even if it is present at different URLs"""
    #     initial_datasets_count = Dataset.objects.count()

    #     ingester = ingesters.NansatIngester()
    #     ingester.ingest([os.path.join(os.path.dirname(__file__), 'data/nansat/arc_metno_dataset.nc')])
    #     self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

    #     with self.assertLogs(ingester.LOGGER, level=logging.INFO) as logger_cm:
    #         ingester.ingest([
    #             os.path.join(os.path.dirname(__file__), 'data/nansat/arc_metno_dataset_2.nc')])

    #     self.assertTrue(logger_cm.records[0].msg.endswith('already exists in the database.'))
    #     self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)
