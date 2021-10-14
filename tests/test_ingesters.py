"""Test suite for ingesters"""

import json
import logging
import os
import time
import unittest.mock as mock
import xml.etree.ElementTree as ET
from collections import OrderedDict
from datetime import datetime

import django.db
import django.db.utils
import django.test
import numpy as np
import pythesint as pti
import requests
from dateutil.tz import tzutc
from django.contrib.gis.geos.geometry import GEOSGeometry
from geospaas.catalog.models import Dataset, DatasetURI
from geospaas.vocabularies.models import DataCenter, ISOTopicCategory, Parameter

import geospaas_harvesting.ingesters as ingesters
from geospaas.catalog.managers import (DAP_SERVICE_NAME, FILE_SERVICE_NAME,
                                       LOCAL_FILE_SERVICE, OPENDAP_SERVICE)


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
        self.mock_param_count.return_value = 0  # No parameter in the vocabulary
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

    def test_get_download_url(self):
        """get_download_url() should return the download URL from the
        information provided by a crawler.
        """
        self.assertEqual(self.ingester.get_download_url('url'), 'url')

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
                             "The Dataset URI 'some_url' was not created.")

    def test_log_on_dataset_already_ingested_from_different_uri(self):
        """A message must be logged if a dataset was already ingested from a different URI"""
        self.ingester._to_ingest.put(('some_url', {}))
        self.ingester._to_ingest.put(None)
        with mock.patch.object(ingesters.Ingester, '_ingest_dataset') as mock_ingest_dataset:
            mock_ingest_dataset.return_value = (False, True)
            with self.assertLogs(self.ingester.LOGGER, level=logging.INFO) as logger_cm:
                self.ingester._thread_ingest_dataset()
            self.assertEqual(logger_cm.records[0].message,
                             "Dataset at 'some_url' was not created.")

    def test_log_on_metadata_fetching_error(self):
        """A message must be logged if an error occurs while fetching the metadata for a dataset"""
        with mock.patch.object(ingesters.Ingester, '_get_normalized_attributes') as mock_normalize:
            mock_normalize.side_effect = TypeError
            with self.assertLogs(self.ingester.LOGGER, level=logging.ERROR) as logger_cm:
                self.ingester._thread_get_normalized_attributes('some_url', 'some_url')
            self.assertEqual(logger_cm.records[0].message, "Could not get metadata for 'some_url'")

    def test_fetching_threads_stop_on_keyboard_interrupt(self):
        """Test that the scheduled threads which fetch datasets
        metadata are stopped when a KeyboardInterrupt (i.e. SIGINT or
        SIGTERM) occurs.
        """
        def range_error_generator(size):
            """Yields integers from 0 to size - 1,
            then raise a KeyboardInterrupt"""
            for i in range(size):
                yield i
            raise KeyboardInterrupt

        ingester = ingesters.Ingester(max_fetcher_threads=1, max_db_threads=1)

        # The test is done by scheduling 5 tasks which sleep for 0.5s,
        # then raising a KeyboardInterrupt exception. Usually, by this
        # point one task is running and the others are scheduled. If
        # the scheduled tasks are correctly canceled, only the running
        # task is executed, so the test takes a bit more than 0.5s.
        # If the tasks are not cancelled, they all run and the test
        # takes more than 2.5s.
        # This way of testing is far from ideal, good ideas are
        # welcome.
        with mock.patch.object(ingester,
                               '_thread_get_normalized_attributes',
                               side_effect=lambda x: time.sleep(0.5)):
            start = time.monotonic()
            with self.assertRaises(KeyboardInterrupt):
                ingester.ingest(range_error_generator(5))
            stop = time.monotonic()
        self.assertLess(stop - start, 2.5)


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
            'url': "https://opendap.jpl.nasa.gov/opendap/full_dataset.nc.ddx",
            'file_path': "data/opendap/full_ddx.xml"},
        'short_ddx': {
            'url': "https://test-opendap.com/short_dataset.nc.ddx",
            'file_path': "data/opendap/short_ddx.xml"},
        'no_ns_ddx': {
            'url': "https://test-opendap.com/no_ns_dataset.nc.ddx",
            'file_path': "data/opendap/ddx_no_ns.xml"},
    }

    def request_side_effect(self, method, url, **kwargs):
        """Side effect function used to mock calls to requests.get().text"""
        if method != 'GET':
            return None
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
        self.patcher_request = mock.patch('geospaas_harvesting.ingesters.requests.request')
        self.mock_request = self.patcher_request.start()
        self.mock_request.side_effect = self.request_side_effect
        self.opened_files = []

    def tearDown(self):
        self.patcher_request.stop()
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

    @mock.patch('xml.etree.ElementTree.parse')
    @mock.patch('geospaas_harvesting.ingesters.DDXIngester._extract_attributes')
    @mock.patch('metanorm.handlers.MetadataHandler.get_parameters')
    def test_existence_url_in_raw_attributes(self, mock_get_parameters, mock_extatt, mock_etree):
        """the 'url' field in the "get_parameters" call of function must be present as an
        input argument. This is a strict need of metanorm for creating the 'entry_id' based
        on this 'url' field for this ingester."""
        mock_extatt.return_value = {}
        ingester = ingesters.DDXIngester()
        ingester._get_normalized_attributes('test_url')
        self.assertIn(({'url': 'test_url'},), mock_get_parameters.call_args)

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
            "https://opendap.jpl.nasa.gov/opendap/full_dataset.nc")

        self.assertEqual(normalized_parameters['entry_title'],
                         'VIIRS L2P Sea Surface Skin Temperature')
        self.assertEqual(normalized_parameters['summary'], ('Description: ' +
            'Sea surface temperature (SST) retrievals produced at the NASA OBPG for the Visible I' +
            'nfrared Imaging\n                Radiometer Suite (VIIRS) sensor on the Suomi Nation' +
            'al Polar-Orbiting Partnership (Suomi NPP) platform.\n                These have been' +
            ' reformatted to GHRSST GDS version 2 Level 2P specifications by the JPL PO.DAAC. VII' +
            'RS\n                SST algorithms developed by the University of Miami, RSMAS;' +
            'Processing level: 2P'))
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

        self.assertEqual(normalized_parameters['platform']['Short_Name'], 'Suomi-NPP')
        self.assertEqual(normalized_parameters['platform']['Long_Name'],
                         'Suomi National Polar-orbiting Partnership')
        self.assertEqual(normalized_parameters['platform']['Category'],
                         'Earth Observation Satellites')
        self.assertEqual(normalized_parameters['platform']['Series_Entity'],
                         'Joint Polar Satellite System (JPSS)')

        self.assertEqual(normalized_parameters['location_geometry'], (
            'POLYGON(('
            '-175.084000 -15.3505001,'
            '-142.755005 -15.3505001,'
            '-142.755005 9.47472000,'
            '-175.084000 9.47472000,'
            '-175.084000 -15.3505001))'
        ))

        self.assertEqual(normalized_parameters['provider']['Short_Name'],
                         'NASA/JPL/PODAAC')
        self.assertEqual(normalized_parameters['provider']['Long_Name'],
                         'Physical Oceanography Distributed Active Archive Center,' +
                         ' Jet Propulsion Laboratory, NASA')
        self.assertEqual(normalized_parameters['provider']['Data_Center_URL'],
                         'https://podaac.jpl.nasa.gov/')

        self.assertEqual(normalized_parameters['iso_topic_category']
                         ['iso_topic_category'], 'Oceans')

        self.assertEqual(normalized_parameters['gcmd_location']
                         ['Location_Category'], 'VERTICAL LOCATION')
        self.assertEqual(normalized_parameters['gcmd_location']['Location_Type'], 'SEA SURFACE')
        self.assertEqual(normalized_parameters['entry_id'], 'full_dataset')

    def test_ingest_dataset_twice_different_urls(self):
        """The same dataset must not be ingested twice in the case of second time execution of
        'ingest' command with the same url. """
        initial_datasets_count = Dataset.objects.count()
        ingester = ingesters.DDXIngester()
        with self.assertLogs(ingester.LOGGER):
            ingester.ingest(["https://opendap.jpl.nasa.gov/opendap/full_dataset.nc"])
        self.assertEqual(Dataset.objects.count(), initial_datasets_count + 1)

        with self.assertLogs(ingester.LOGGER, level=logging.INFO) as logger_cm:
            ingester.ingest(["https://opendap.jpl.nasa.gov/opendap/full_dataset.nc"])

        self.assertTrue(logger_cm.records[0].msg.endswith('already present in the database'))
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


class ThreddsIngesterTestCase(django.test.TestCase):
    """Test the ThreddsIngester"""

    def test_prepare_url(self):
        """Should return a DDX URL from a Thredds URL, or raise a
        ValueError if the URL is invalid
        """
        self.assertEqual(
            ingesters.ThreddsIngester.prepare_url(
                'https://foo.com/thredds/fileServer/bar/baz/dataset.nc'),
            'https://foo.com/thredds/dodsC/bar/baz/dataset.nc.ddx'
        )

        with self.assertRaises(ValueError):
            ingesters.ThreddsIngester.prepare_url('Https://foo/bar.nc')


class CopernicusODataIngesterTestCase(django.test.TestCase):
    """Test the CopernicusODataIngester"""
    fixtures = [os.path.join(os.path.dirname(__file__), "fixtures", "harvest")]
    TEST_DATA = {
        'full': {
            'url': "https://scihub.copernicus.eu/apihub/odata/v1/full"
                   "?$format=json&$expand=Attributes",
            'file_path': "data/copernicus_opensearch/full.json"}
    }

    def request_side_effect(self, method, url, **kwargs):  # pylint: disable=unused-argument
        """Side effect function used to mock calls to requests.get().text"""
        if method != 'GET':
            return None
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
        self.patcher_request = mock.patch.object(ingesters.requests, 'request')
        self.mock_request = self.patcher_request.start()
        self.mock_request.side_effect = self.request_side_effect
        self.opened_files = []

        self.patcher_param_count = mock.patch.object(Parameter.objects, 'count')
        self.mock_param_count = self.patcher_param_count.start()
        self.mock_param_count.return_value = 2

    def tearDown(self):
        self.patcher_request.stop()
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
        raw_metadata = self.ingester._get_raw_metadata(
            'https://scihub.copernicus.eu/apihub/odata/v1/full/$value')
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
            'https://scihub.copernicus.eu/apihub/odata/v1/full/$value')

        self.assertEqual(normalized_parameters['entry_title'],
                         'S1A_IW_GRDH_1SDV_20200318T062305_20200318T062330_031726_03A899_F558')
        self.assertEqual(normalized_parameters['summary'], (
            'Description: Date=2020-03-18T06:23:05.976Z, '
            'Instrument name=Synthetic Aperture Radar (C-band), '
            'Mode=IW, Satellite=Sentinel-1, Size=1.65 GB, Timeliness Category=Fast-24h'
            ';Processing level: 1'))
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

        self.assertEqual(normalized_parameters['platform']['Short_Name'], 'Sentinel-1A')
        self.assertEqual(normalized_parameters['platform']['Long_Name'], 'Sentinel-1A')
        self.assertEqual(normalized_parameters['platform']['Category'],
                         'Earth Observation Satellites')
        self.assertEqual(normalized_parameters['platform']['Series_Entity'], 'Sentinel-1')

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
            'https://scihub.copernicus.eu/apihub/odata/v1/full/$value', value_for_testing)
        self.assertEqual(Dataset.objects.count(), 1)
        self.assertEqual(Dataset.objects.first().parameters.count(), 1)
        # the parameter that has added (by above variable of "value_for_testing") to the dataset
        # should be equal to the first object of parameter table
        # which is created by fixtures inside the database
        self.assertEqual(
            Dataset.objects.first().parameters.first(), Parameter.objects.first())
        self.assertEqual(created_dataset, True)
        self.assertEqual(created_dataset_uri, True)

        # No parameter or dataset should be added for the second time of executing this command
        # with same normalized attributes (same variable of "value_for_testing")
        created_dataset, created_dataset_uri = self.ingester._ingest_dataset(
            'https://scihub.copernicus.eu/apihub/odata/v1/full/$value', duplicate_value_for_testing)

        self.assertEqual(Dataset.objects.count(), 1)
        self.assertEqual(Dataset.objects.first().parameters.count(), 1)
        self.assertEqual(
            Dataset.objects.first().parameters.first(), Parameter.objects.first())
        self.assertEqual(created_dataset, False)
        self.assertEqual(created_dataset_uri, False)


class APIPayloadIngesterTestCase(django.test.TestCase):
    """Tests for the APIIngester"""

    fixtures = [os.path.join(os.path.dirname(__file__), "fixtures", "harvest")]

    def setUp(self):
        self.ingester = ingesters.APIPayloadIngester()

    def test_get_normalized_attributes(self):
        """_get_normalized_attributes() should add the download URL to
        the raw attributes, get the attributes from metanorm and
        add service information
        """
        dataset_info = {'services': {'download': {'url': 'http://something'}}}
        with mock.patch.object(
            self.ingester._metadata_handler, 'get_parameters', return_value={'foo': 'bar'}), \
                mock.patch.object(self.ingester, 'add_url') as mock_add_url:
            self.assertDictEqual(
                self.ingester._get_normalized_attributes(dataset_info),
                {
                    'foo': 'bar',
                    'geospaas_service': ingesters.HTTP_SERVICE,
                    'geospaas_service_name': ingesters.HTTP_SERVICE_NAME
                }
            )
            mock_add_url.assert_called_once()


class CreodiasEOFinderIngesterTestCase(django.test.TestCase):
    """Test the CreodiasEOFinderIngester"""

    fixtures = [os.path.join(os.path.dirname(__file__), "fixtures", "harvest")]

    def setUp(self):
        self.ingester = ingesters.CreodiasEOFinderIngester()

    def test_get_download_url(self):
        """Test that the download URL is correctly extracted from the
        dataset information"""
        dataset_info = {'services': {'download': {'url': 'http://something'}}}
        self.assertEqual(self.ingester.get_download_url(dataset_info), 'http://something')


class EarthdataCMRIngesterTestCase(django.test.TestCase):
    """Test the EarthdataCMRIngester"""

    fixtures = [os.path.join(os.path.dirname(__file__), "fixtures", "harvest")]

    def setUp(self):
        self.ingester = ingesters.EarthdataCMRIngester()

    def test_get_download_url(self):
        """Test that the download URL is correctly extracted from the
        dataset information"""
        dataset_info = {'umm': {'RelatedUrls': [{'URL': 'http://something', 'Type': 'GET DATA'}]}}
        self.assertEqual(self.ingester.get_download_url(dataset_info), 'http://something')


class FTPIngesterTestCase(django.test.TestCase):
    """Test the FTPIngester"""

    def setUp(self):
        self.patcher_param_count = mock.patch.object(Parameter.objects, 'count')
        self.mock_param_count = self.patcher_param_count.start()
        self.mock_param_count.return_value = 2

    def tearDown(self):
        self.patcher_param_count.stop()

    def test_get_normalized_attributes(self):
        """Test that the attributes are gotten using metanorm, and the
        geospaas_service attributes are set to 'ftp'
        """
        ingester = ingesters.FTPIngester()
        with mock.patch.object(ingester, '_metadata_handler') as mock_handler:
            mock_handler.get_parameters.return_value = {'foo': 'bar'}
            self.assertDictEqual(ingester._get_normalized_attributes('ftp://uri'), {
                'foo': 'bar',
                'geospaas_service_name': 'ftp',
                'geospaas_service': 'ftp'
            })
            mock_handler.get_parameters.assert_called_once_with({'url': 'ftp://uri'})


class NansatIngesterTestCase(django.test.TestCase):
    """Test the NansatIngester"""

    def setUp(self):
        self.patcher_param_count = mock.patch.object(Parameter.objects, 'count')
        self.mock_param_count = self.patcher_param_count.start()
        self.mock_param_count.return_value = 2

        self.patcher_get_metadata = mock.patch('geospaas_harvesting.ingesters.Nansat')
        self.mock_get_metadata = self.patcher_get_metadata.start()

        self.mock_get_metadata.return_value.get_border_wkt.return_value = (
            'POLYGON((24.88 68.08,22.46 68.71,19.96 69.31,17.39 69.87,24.88 68.08))')

    def tearDown(self):
        self.patcher_param_count.stop()
        self.patcher_get_metadata.stop()

    def test_normalize_netcdf_attributes_with_nansat(self):
        """Test the ingestion of a netcdf file using nansat"""
        self.mock_get_metadata.return_value.get_metadata.side_effect = [
            {'bulletin_type': 'Forecast', 'Conventions': 'CF-1.4', 'field_date': '2017-05-29',
             'field_type': 'Files based on file type nersc_daily',
             'filename': '/vsimem/343PBWM116.vrt', 'Forecast_range': '10 days',
             'history': '20170521:Created by program hyc2proj, version V0.3',
             'institution': 'MET Norway, Henrik Mohns plass 1, N-0313 Oslo, Norway',
             'instrument':
             '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
             '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
             '"Long_Name": "Computer"}',
             'platform':
             '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
             '"Long_Name": ""}',
             'references': 'http://marine.copernicus.eu', 'source': 'NERSC-HYCOM model fields',
             'time_coverage_end': '2017-05-27T00:00:00', 'time_coverage_start':
             '2017-05-18T00:00:00',
             'title':
             'Arctic Ocean Physics Analysis and Forecast, 12.5km daily mean '
             '(dataset-topaz4-arc-myoceanv2-be)',
             'dataset_parameters': '["surface_backwards_scattering_coefficient_of_radar_wave"]'}]
        ingester = ingesters.NansatIngester()
        normalized_attributes = ingester._get_normalized_attributes('')
        self.assertEqual(normalized_attributes['entry_title'], 'NONE')
        self.assertEqual(normalized_attributes['summary'], 'NONE')
        self.assertEqual(normalized_attributes['time_coverage_start'], datetime(
            year=2017, month=5, day=18, hour=0, minute=0, second=0, tzinfo=tzutc()))
        self.assertEqual(normalized_attributes['time_coverage_end'], datetime(
            year=2017, month=5, day=27, hour=0, minute=0, second=0, tzinfo=tzutc()))

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

        expected_geometry = GEOSGeometry((
            'POLYGON((24.88 68.08,22.46 68.71,19.96 69.31,17.39 69.87,24.88 68.08))'), srid=4326)

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
        self.assertEqual(
            normalized_attributes['dataset_parameters'],
            [
                OrderedDict(
                    [('standard_name', 'surface_backwards_scattering_coefficient_of_radar_wave'),
                     ('canonical_units', '1'),
                     ('definition',
                         'The scattering/absorption/attenuation coefficient is assumed to be an '
                         'integral over all wavelengths, unless a coordinate of '
                         'radiation_wavelength is included to specify the wavelength. Scattering of'
                         ' radiation is its deflection from its incident path without loss of '
                         'energy. Backwards scattering refers to the sum of scattering into all '
                         'backward angles i.e. scattering_angle exceeding pi/2 radians. A '
                         'scattering_angle should not be specified with this quantity.')
                     ])
            ])

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

    def test_exception_handling_of_bad_development_of_mappers(self):
        """Test the exception handling of bad development of 'dataset_parameters' of metadata.
        ANY mapper should return a python list as 'dataset_parameters' of metadata."""
        self.mock_get_metadata.return_value.get_metadata.side_effect = [
            {
            'time_coverage_end': '2017-05-27T00:00:00', 'time_coverage_start':
                '2017-05-18T00:00:00',
                'platform':
                '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
                '"Long_Name": ""}',
                'instrument':
                '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
                '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
                '"Long_Name": "Computer"}',
                'dataset_parameters': "{}"}]
        ingester = ingesters.NansatIngester()
        with self.assertRaises(TypeError) as err:
            normalized_attributes = ingester._get_normalized_attributes('')
        self.assertEqual(
            err.exception.args[0],
            "Can't ingest '': the 'dataset_parameters' section of the metadata returned by nansat "
            "is not a JSON list")

    def test_no_dataset_parameters(self):
        """If no "dataset_parameters" attribute is present in the
        nansat metadata, normalized_attributes['dataset_parameters']
        should be set to an empty list
        """
        self.mock_get_metadata.return_value.get_metadata.return_value = {
            'time_coverage_end': '2017-05-27T00:00:00',
            'time_coverage_start': '2017-05-18T00:00:00',
            'platform':
                '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
                '"Long_Name": ""}',
            'instrument':
                '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
                '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
                '"Long_Name": "Computer"}'
        }
        ingester = ingesters.NansatIngester()
        self.assertListEqual(ingester._get_normalized_attributes('')['dataset_parameters'], [])

    def test_usage_of_nansat_ingester_with_http_protocol_in_the_OPENDAP_cases(self):
        """LOCALHarvester(which uses NansatIngester) can be used for `OPENDAP provided` files """
        ingester = ingesters.NansatIngester()
        self.mock_get_metadata.return_value.get_metadata.side_effect = [{
            'time_coverage_end': '2017-05-27T00:00:00', 'time_coverage_start':
                '2017-05-18T00:00:00',
                'platform':
                '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
                '"Long_Name": ""}',
                'instrument':
                '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
                '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
                '"Long_Name": "Computer"}',
        }]
        normalized_attributes = ingester._get_normalized_attributes('http://')
        self.assertEqual(normalized_attributes['geospaas_service_name'], DAP_SERVICE_NAME)
        self.assertEqual(normalized_attributes['geospaas_service'], OPENDAP_SERVICE)

    def test_usage_of_nansat_ingester_with_local_file(self):
        """LOCALHarvester(which uses NansatIngester) can be used for local files """
        ingester = ingesters.NansatIngester()
        self.mock_get_metadata.return_value.get_metadata.side_effect = [{
            'time_coverage_end': '2017-05-27T00:00:00', 'time_coverage_start':
                '2017-05-18T00:00:00',
                'platform':
                '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
                '"Long_Name": ""}',
                'instrument':
                '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
                '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
                '"Long_Name": "Computer"}',
        }]
        normalized_attributes = ingester._get_normalized_attributes('/src/blabla')
        self.assertEqual(normalized_attributes['geospaas_service_name'], FILE_SERVICE_NAME)
        self.assertEqual(normalized_attributes['geospaas_service'], LOCAL_FILE_SERVICE)


    def test_exception_handling_of_bad_inputting_of_nansat_ingester_with_ftp_protocol(self):
        """LOCALHarvester(which uses NansatIngester) is only for local file addresses"""
        ingester = ingesters.NansatIngester()
        self.mock_get_metadata.return_value.get_metadata.side_effect = ['']
        with self.assertRaises(ValueError) as err:
            normalized_attributes = ingester._get_normalized_attributes('ftp://')
        self.assertEqual(
            err.exception.args[0],
            "Can't ingest 'ftp://': nansat can't open remote ftp files")

    def test_reprojection_based_on_gcps(self):
        """Nansat ingester should reproject if there is any GC point in the metadata"""
        self.mock_get_metadata.return_value.vrt.dataset.GetGCPs.return_value = True
        self.mock_get_metadata.return_value.get_metadata.side_effect = [{
            'time_coverage_end': '2017-05-27T00:00:00', 'time_coverage_start':
                '2017-05-18T00:00:00',
                'platform':
                '{"Category": "Models/Analyses", "Series_Entity": "", "Short_Name": "MODELS", '
                '"Long_Name": ""}',
                'instrument':
                '{"Category": "In Situ/Laboratory Instruments", "Class": "Data Analysis", '
                '"Type": "Environmental Modeling", "Subtype": "", "Short_Name": "Computer", '
                '"Long_Name": "Computer"}',
        }]
        ingester = ingesters.NansatIngester()
        normalized_attributes = ingester._get_normalized_attributes('')
        self.mock_get_metadata.return_value.reproject_gcps.assert_called_once()


class NetCDFIngesterTestCase(django.test.TestCase):
    """Test the NetCDFIngester"""

    def  setUp(self):
        mock.patch('geospaas_harvesting.ingesters.Parameter.objects.count', return_value=2).start()
        self.addCleanup(mock.patch.stopall)

        self.ingester = ingesters.NetCDFIngester()

    class MockVariable(mock.Mock):
        def __init__(self, data, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._data = np.array(data)
            self.shape = self._data.shape
            self.dimensions = kwargs.get('dimensions', {})

        def __iter__(self):
            """Make the class iterable"""
            return iter(self._data)

        def __getitem__(self, i):
            """Make the class subscriptable"""
            return self._data[i]

        def __array__(self, *args, **kwargs):
            """Make the class numpy-array-like"""
            return self._data


    class MaskedMockVariable(MockVariable):
        def __init__(self, data, *args, **kwargs):
            super().__init__(data, *args, **kwargs)
            self._data = np.ma.masked_values(data, 1e10)


    def test_get_raw_attributes(self):
        """Test reading raw attributes from a netCDF file"""
        attributes = {
            'attr1': 'value1',
            'attr2': 'value2'
        }

        # In the netCDF4 lib, the netCDF attributes of a dataset are
        # accessed using the __dict__ attribute of the Python Dataset
        # object. This messes with the internal workings of Python
        # objects and makes mocking quite hard.
        # Here, it is necessary to mock _get_geometry_wkt() and
        # _get_geometry_wkt(), because since we are mocking its
        # __dict__, the mocked dataset does not behave as expected when
        # calling these methods on it.
        with mock.patch('netCDF4.Dataset') as mock_dataset, \
             mock.patch.object(self.ingester, '_get_geometry_wkt', return_value='wkt'), \
             mock.patch.object(self.ingester, '_get_parameter_names', return_value=['param']):
            mock_dataset.return_value.__dict__ = attributes

            self.assertDictEqual(
                self.ingester._get_raw_attributes('/foo/bar'),
                {
                    **attributes,
                    'url': '/foo/bar',
                    'geometry': 'wkt',
                    'raw_dataset_parameters': ['param']
                }
            )

    def test_get_parameter_names(self):
        """_get_parameter_names() should return the names of the
        variables of the dataset
        """
        mock_variable1 = mock.Mock()
        mock_variable1.standard_name = 'standard_name_1'

        mock_dataset = mock.Mock()
        mock_dataset.variables = {
            'var1': mock_variable1,
            'var2': 'variable2' # does not have a "standard_name" attribute
        }

        self.assertListEqual(self.ingester._get_parameter_names(mock_dataset), ['standard_name_1'])

    def test_get_normalized_attributes(self):
        """_get_normalized_attributes() should use metanorm to
        normalize the raw attributes
        """
        with mock.patch.object(self.ingester, '_get_raw_attributes'), \
             mock.patch.object(self.ingester, '_metadata_handler') as mock_metadata_handler:
            mock_metadata_handler.get_parameters.return_value = {'param': 'value'}
            # Local path
            self.assertDictEqual(
                self.ingester._get_normalized_attributes('/foo/bar.nc'),
                {
                    'param': 'value',
                    'geospaas_service': ingesters.LOCAL_FILE_SERVICE,
                    'geospaas_service_name': ingesters.FILE_SERVICE_NAME
                }
            )
            # HTTP URL
            self.assertDictEqual(
                self.ingester._get_normalized_attributes('http://foo/bar.nc'),
                {
                    'param': 'value',
                    'geospaas_service': ingesters.HTTP_SERVICE,
                    'geospaas_service_name': ingesters.HTTP_SERVICE_NAME
                }
            )

    def test_get_trajectory(self):
        """Test getting a trajectory from a netCDF dataset"""
        mock_dataset = mock.Mock()
        mock_dataset.dimensions = {}
        mock_dataset.variables = {
            'LONGITUDE': self.MockVariable((1, 3, 5)),
            'LATITUDE': self.MockVariable((2, 4, 6))
        }
        self.assertEqual(
            self.ingester._get_geometry_wkt(mock_dataset),
            'LINESTRING (1 2, 3 4, 5 6)'
        )

    def test_get_point(self):
        """Test getting a WKT point when the shape of the latitude and
        longitude is (1,)"""
        mock_dataset = mock.Mock()
        mock_dataset.dimensions = {}
        mock_dataset.variables = {
            'LONGITUDE': self.MockVariable((1,)),
            'LATITUDE': self.MockVariable((2,))
        }
        self.assertEqual(
            self.ingester._get_geometry_wkt(mock_dataset),
            'POINT (1 2)'
        )

    def test_get_deduplicated_point(self):
        """Test getting a WKT point when that point is referenced
        multiple times in the dataset
        """
        mock_dataset = mock.Mock()
        mock_dataset.dimensions = {}
        mock_dataset.variables = {
            'LONGITUDE': self.MockVariable((1, 1, 1)),
            'LATITUDE': self.MockVariable((2, 2, 2))
        }
        self.assertEqual(
            self.ingester._get_geometry_wkt(mock_dataset),
            'POINT (1 2)'
        )

    def test_get_polygon_from_coordinates_lists(self):
        """Test getting a polygonal coverage from a dataset when the
        latitude and longitude are multi-dimensional and of the same
        shape
        """
        mock_dataset = mock.Mock()
        mock_dataset.dimensions = {}
        mock_dataset.variables = {
            'LONGITUDE': self.MockVariable((
                (1, 1, 2),
                (2, 0, 3),
            )),
            'LATITUDE': self.MockVariable((
                (1, 2, 3),
                (4, 0, 4),
            ))
        }
        self.assertEqual(
            self.ingester._get_geometry_wkt(mock_dataset),
            'POLYGON ((0 0, 2 4, 3 4, 1 1, 0 0))'
        )

    @mock.patch('geospaas_harvesting.ingesters.np.ma.isMaskedArray', return_value=True)
    def test_get_polygon_from_coordinates_lists_with_masked_array(self, mock_isMaskedArray):
        """Test getting a polygonal coverage from a dataset when the
        latitude and longitude are multi-dimensional masked_array
        """
        mock_dataset = mock.Mock()
        mock_dataset.dimensions = {}
        mock_dataset.variables = {
            'LONGITUDE': self.MaskedMockVariable((
                (1, 1e10, 1e10),
                (2, 0, 3),
            )),
            'LATITUDE': self.MaskedMockVariable((
                (1, 1e10, 1e10),
                (4, 0, 4),
            ))
        }
        self.assertEqual(
            self.ingester._get_geometry_wkt(mock_dataset),
            'POLYGON ((0 0, 2 4, 3 4, 1 1, 0 0))'
        )

    @mock.patch('geospaas_harvesting.ingesters.np.ma.isMaskedArray', return_value=True)
    def test_get_polygon_from_coordinates_lists_with_masked_array_1d_case(self, mock_isMaskedArray):
        """Test getting a polygonal coverage from a dataset when the
        latitude and longitude are 1d masked_array as an abstracted version of 2d lon and lat values
        """
        mock_dataset = mock.Mock()
        mock_dataset.dimensions = {}
        mock_dataset.variables = {
            'LONGITUDE': self.MaskedMockVariable(
                (1,1e10, 1e10, 2, 0, 3, 1),dimensions=['LONGITUDE','LATITUDE']
                                                ),
            'LATITUDE': self.MaskedMockVariable(
                (1, 1e10, 1e10, 4, 0, 4, 1),dimensions=['LONGITUDE','LATITUDE']
                                               ),
        }
        self.assertEqual(
            self.ingester._get_geometry_wkt(mock_dataset),
            'POLYGON ((0 0, 0 4, 3 4, 3 0, 0 0))'
        )

    def test_get_polygon_from_1d_lon_lat(self):
        """Test getting a polygonal coverage from a dataset when the
        latitude and longitude are one-dimensional and of different
        shapes
        """
        mock_dataset = mock.Mock()
        mock_dataset.dimensions = {}
        mock_dataset.variables = {
            'LONGITUDE': self.MockVariable((1, 2, 3)),
            'LATITUDE': self.MockVariable((1, 2)),
            'DATA': self.MockVariable('some_data', dimensions=('LONGITUDE', 'LATITUDE'))
        }
        self.assertEqual(
            self.ingester._get_geometry_wkt(mock_dataset),
            'POLYGON ((1 1, 1 2, 3 2, 3 1, 1 1))'
        )

    def test_get_polygon_from_1d_lon_lat_same_shape(self):
        """Test getting a polygonal coverage from a dataset when the
        latitude and longitude are one-dimensional and have the same
        shape
        """
        mock_dataset = mock.Mock()
        mock_dataset.dimensions = {}
        mock_dataset.variables = {
            'LONGITUDE': self.MockVariable((1, 2)),
            'LATITUDE': self.MockVariable((1, 2)),
            'DATA': self.MockVariable('some_data', dimensions=('LONGITUDE', 'LATITUDE'))
        }
        self.assertEqual(
            self.ingester._get_geometry_wkt(mock_dataset),
            'POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1))'
        )

    def test_error_on_unsupported_case(self):
        """An error should be raised if the dataset has longitude and
        latitude arrays of different lengths and no variable is
        dependent on latitude and longitude
        """
        mock_dataset = mock.Mock()
        mock_dataset.dimensions = {}
        mock_dataset.variables = {
            'LONGITUDE': self.MockVariable((1, 1, 1, 1)),
            'LATITUDE': self.MockVariable((2, 2, 2))
        }
        with self.assertRaises(ValueError):
            self.ingester._get_geometry_wkt(mock_dataset)
