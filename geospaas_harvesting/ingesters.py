"""
Ingesters which, given a set of (possibly remote) files, add those files' metadata
in the GeoSPaaS catalog
"""
import io
import json
import logging
import re
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

import requests
from django.contrib.gis.geos import WKTReader

import pythesint as pti
from geospaas.catalog.managers import (DAP_SERVICE_NAME, FILE_SERVICE_NAME,
                                       LOCAL_FILE_SERVICE, OPENDAP_SERVICE)
from geospaas.catalog.models import (Dataset, DatasetURI, GeographicLocation,
                                     Source)
from geospaas.utils.utils import nansat_filename
from geospaas.vocabularies.models import (DataCenter, Instrument,
                                          ISOTopicCategory, Location, Platform)
from metanorm.errors import MetadataNormalizationError
from metanorm.handlers import GeospatialMetadataHandler
from nansat import Nansat

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class Ingester():
    """Base class for ingesters"""

    DATASET_PARAMETER_NAMES = [
        'entry_title',
        'summary',
        'time_coverage_start',
        'time_coverage_end',
        'platform',
        'instrument',
        'location_geometry',
        'provider',
        'iso_topic_category',
        'gcmd_location'
    ]

    def __init__(self):
        #TODO: why is this here??
        self._metadata_handler = GeospatialMetadataHandler(self.DATASET_PARAMETER_NAMES)

    def _uri_exists(self, uri):
        """Checks if the given URI already exists in the database"""
        return bool(DatasetURI.objects.filter(uri=uri))

    def ingest(self, urls, *args, **kwargs):
        """Adds the metadata from the files located at the URLs in the GeoSPaaS database"""
        raise NotImplementedError('The ingest() method was not implemented')


class DDXIngester(Ingester):
    """Ingests metadata in DDX format from an OpenDAP server"""

    GLOBAL_ATTRIBUTES_NAME = 'NC_GLOBAL'
    NAMESPACE_REGEX = r'^\{(\S+)\}Dataset$'

    def _get_xml_namespace(self, root):
        """Try to get the namespace for the XML tag in the document from the root tag"""
        try:
            namespace_prefix = re.match(self.NAMESPACE_REGEX, root.tag)[1]  # first matched group
        except TypeError:
            namespace_prefix = ''
            LOGGER.warning('Could not find XML namespace while reading DDX metadata')
        return namespace_prefix

    def _extract_global_attributes(self, root):
        """Extracts the global attributes of a dataset from a DDX document"""
        LOGGER.debug("Getting the dataset's global attributes.")
        namespaces = {'default': self._get_xml_namespace(root)}
        global_attributes = {}
        for attribute in root.findall(
                f"./default:Attribute[@name='{self.GLOBAL_ATTRIBUTES_NAME}']/default:Attribute",
                namespaces):

            global_attributes[attribute.get('name')] = attribute.find(
                "./default:value", namespaces).text

        return global_attributes

    def ingest(self, urls, *args, **kwargs):
        """
        Iterate over the URLs, get metadata from the OpenDAP server and create geospaas Datasets
        which are added to the catalog database
        """
        for url in urls:
            # This check is not strictly necessary because the get_or_create() method takes care of
            # idempotence, but it increases performance because we don't go through the normalizing
            # chain
            if self._uri_exists(url):
                LOGGER.info("%s is already present in the database.", url)
            else:
                try:
                    LOGGER.debug('Ingesting %s ...', url)
                    ddx_url = url if url.endswith('.ddx') else url + '.ddx'

                    # Get the metadata from the dataset as an XML tree
                    stream = io.BytesIO(requests.get(ddx_url, stream=True).content)

                    # Get all the global attributes of the Dataset into a dictionary
                    dataset_global_attributes = self._extract_global_attributes(
                        ET.parse(stream).getroot())

                    # Get the parameters needed to create a geospaas catalog dataset from the
                    # global attributes
                    normalized_attributes = self._metadata_handler.get_parameters(
                        dataset_global_attributes)

                    # Create the objects with which the dataset has relationships
                    # (or get them if they already exist)
                    data_center, _ = DataCenter.objects.get_or_create(
                        normalized_attributes.pop('provider'))

                    geographic_location, _ = GeographicLocation.objects.get_or_create(
                        geometry=normalized_attributes.pop('location_geometry'))

                    location, _ = Location.objects.get_or_create(
                        normalized_attributes.pop('gcmd_location'))

                    iso_topic_category, _ = ISOTopicCategory.objects.get_or_create(
                        normalized_attributes.pop('iso_topic_category'))

                    platform, _ = Platform.objects.get_or_create(
                        normalized_attributes.pop('platform'))

                    instrument, _ = Instrument.objects.get_or_create(
                        normalized_attributes.pop('instrument'))

                    source, _ = Source.objects.get_or_create(
                        platform=platform,
                        instrument=instrument,
                        specs='')

                    # Create Dataset in the database. The normalized_attributes dict contains the
                    # "basic parameter", which are not objects in the database.
                    # The objects we just created in the database are passed separately.
                    dataset, created_dataset = Dataset.objects.get_or_create(
                        **normalized_attributes,
                        data_center=data_center,
                        geographic_location=geographic_location,
                        gcmd_location=location,
                        ISO_topic_category=iso_topic_category,
                        source=source)

                    #Create the URI for the created Dataset in the database
                    _, created_dataset_uri = DatasetURI.objects.get_or_create(
                        name=DAP_SERVICE_NAME,
                        service=OPENDAP_SERVICE,
                        uri=url,
                        dataset=dataset
                    )

                    if created_dataset:
                        LOGGER.info("Successfully created dataset from url: '%s'", url)
                    else:
                        LOGGER.info("Dataset at '%s' already exists in the database.", url)

                    if not created_dataset_uri:
                        LOGGER.error("The Dataset's URI already exists. This should never happen.")
                except MetadataNormalizationError as error:
                    LOGGER.error('Ingestion failed due to the following error: %s', str(error))


class NansatIngester(Ingester):
    """
    Ingester class using Nansat to open files or streams
    """

    def ingest(self, urls, *args, **kwargs):
        """
        Ingest one Dataset per file that has not previously been ingested.
        urls: iterable of URLs to import
        """

        n_points = int(kwargs.get('n_points', 10))
        nansat_options = kwargs.get('nansat_options', {})

        for url in urls:
            if self._uri_exists(url):
                LOGGER.info("%s is already present in the database.", url)
            else:
                LOGGER.debug('Ingesting %s ...', url)

                # Open file with Nansat
                n = Nansat(nansat_filename(url), **nansat_options)

                # get metadata from Nansat and get objects from vocabularies
                n_metadata = n.get_metadata()

                # set compulsory metadata (source)
                platform, _ = Platform.objects.get_or_create(json.loads(n_metadata['platform']))
                instrument, _ = Instrument.objects.get_or_create(
                    json.loads(n_metadata['instrument']))
                specs = n_metadata.get('specs', '')
                source, _ = Source.objects.get_or_create(
                    platform=platform, instrument=instrument, specs=specs)

                default_char_fields = {
                    'entry_id': lambda: 'NERSC_' + str(uuid.uuid4()),
                    'entry_title': lambda: 'NONE',
                    'summary': lambda: 'NONE',
                }

                # set optional CharField metadata from Nansat or from default_char_fields
                options = {}
                for name in default_char_fields:
                    if name not in n_metadata:
                        LOGGER.warning('%s is not provided in Nansat metadata!', name)
                        options[name] = default_char_fields[name]()
                    else:
                        options[name] = n_metadata[name]

                default_foreign_keys = {
                    'gcmd_location': {
                        'model': Location,
                        'value': pti.get_gcmd_location('SEA SURFACE')},
                    'data_center': {
                        'model': DataCenter,
                        'value': pti.get_gcmd_provider('NERSC')},
                    'ISO_topic_category': {
                        'model': ISOTopicCategory,
                        'value': pti.get_iso19115_topic_category('Oceans')},
                }

                # set optional ForeignKey metadata from Nansat or from default_foreign_keys
                for name in default_foreign_keys:
                    value = default_foreign_keys[name]['value']
                    model = default_foreign_keys[name]['model']
                    if name not in n_metadata:
                        LOGGER.warning('%s is not provided in Nansat metadata!', name)
                    else:
                        try:
                            value = json.loads(n_metadata[name])
                        except json.JSONDecodeError:
                            LOGGER.warning(
                                '%s value of %s  metadata provided in Nansat is wrong!',
                                n_metadata[name], name)
                    options[name], _ = model.objects.get_or_create(value)

                # Find coverage to set number of points in the geolocation
                if len(n.vrt.dataset.GetGCPs()) > 0:
                    n.reproject_gcps()
                geolocation = GeographicLocation.objects.get_or_create(
                    geometry=WKTReader().read(n.get_border_wkt(nPoints=n_points)))[0]

                # create dataset
                dataset, created_dataset = Dataset.objects.get_or_create(
                    time_coverage_start=n.get_metadata('time_coverage_start'),
                    time_coverage_end=n.get_metadata('time_coverage_end'),
                    source=source,
                    geographic_location=geolocation,
                    **options)

                url_scheme = urlparse(url).scheme
                if 'http' in url_scheme:
                    service_name = DAP_SERVICE_NAME
                    service = OPENDAP_SERVICE
                else:
                    service_name = FILE_SERVICE_NAME
                    service = LOCAL_FILE_SERVICE
                # create dataset URI
                _, created_dataset_uri = DatasetURI.objects.get_or_create(
                    name=service_name, service=service, uri=url, dataset=dataset)

                if created_dataset:
                    LOGGER.info("Successfully created dataset from url: '%s'", url)
                else:
                    LOGGER.info("Dataset at '%s' already exists in the database.", url)

                if not created_dataset_uri:
                    LOGGER.error("The Dataset's URI already exists. This should never happen.")
