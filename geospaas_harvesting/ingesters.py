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
from metanorm.handlers import GeospatialMetadataHandler
from nansat import Nansat

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class Ingester():
    """Base class for ingesters"""

    @staticmethod
    def _uri_exists(uri):
        """Checks if the given URI already exists in the database"""
        return bool(DatasetURI.objects.filter(uri=uri))

    def _ingest_dataset(self, url, *args, **kwargs):
        """
        Ingests one dataset from a URL. Should return a couple of booleans respectively indicating
        whether a Dataset and a DatasetURI were created.
        """
        raise NotImplementedError('The _ingest_dataset() method was not implemented')

    def ingest(self, urls, *args, **kwargs):
        """Iterate over the URLs, and ingest each dataset"""
        for url in urls:
            if self._uri_exists(url):
                LOGGER.info("'%s' is already present in the database", url)
            else:
                try:
                    LOGGER.debug("Ingesting '%s'", url)
                    (created_dataset, created_dataset_uri) = self._ingest_dataset(url,
                                                                                  *args,
                                                                                  **kwargs)
                except Exception:  # pylint: disable=broad-except
                    LOGGER.error("Ingestion of the dataset at '%s' failed", url, exc_info=True)
                else:
                    if created_dataset:
                        LOGGER.info("Successfully created dataset from url: '%s'", url)
                    else:
                        LOGGER.info("Dataset at '%s' already exists in the database.", url)

                    if not created_dataset_uri:
                        LOGGER.error("The Dataset's URI already exists. This should never happen.")


class MetadataIngester(Ingester):
    """
    Base class for ingester which rely on normalized metadata. Such ingesters should inherit from
    this class and implement the _get_normalized_attributes() method.
    """

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
        self._metadata_handler = GeospatialMetadataHandler(self.DATASET_PARAMETER_NAMES)

    def _get_normalized_attributes(self, url):
        """Returns a dictionary of normalized attribute which characterize a Dataset"""
        raise NotImplementedError('The _ingest_dataset() method was not implemented')

    def _ingest_dataset(self, url, *args, **kwargs):
        """
        Ingests one dataset from a URL by getting the parameters needed to create a geospaas catalog
        dataset from its metadata
        """
        normalized_attributes = self._get_normalized_attributes(url)

        # Create the objects with which the dataset has relationships
        # (or get them if they already exist)
        data_center, _ = DataCenter.objects.get_or_create(
            normalized_attributes.pop('provider'))

        geographic_location, _ = GeographicLocation.objects.get_or_create(
            geometry=normalized_attributes.pop('location_geometry'))

        location, _ = Location.objects.get_or_create(normalized_attributes.pop('gcmd_location'))

        iso_topic_category, _ = ISOTopicCategory.objects.get_or_create(
            normalized_attributes.pop('iso_topic_category'))

        platform, _ = Platform.objects.get_or_create(normalized_attributes.pop('platform'))

        instrument, _ = Instrument.objects.get_or_create(normalized_attributes.pop('instrument'))

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

        return (created_dataset, created_dataset_uri)


class DDXIngester(MetadataIngester):
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

    def _get_normalized_attributes(self, url):
        """Get normalized metadata from the DDX info of the dataset located at the provided URL"""

        ddx_url = url if url.endswith('.ddx') else url + '.ddx'

        # Get the metadata from the dataset as an XML tree
        stream = io.BytesIO(requests.get(ddx_url, stream=True).content)

        # Get all the global attributes of the Dataset into a dictionary
        dataset_global_attributes = self._extract_global_attributes(
            ET.parse(stream).getroot())

        # Get the parameters needed to create a geospaas catalog dataset from the
        # global attributes
        return self._metadata_handler.get_parameters(dataset_global_attributes)


class CopernicusODataIngester(MetadataIngester):
    """Ingest datasets from the metadata returned by calls to the Copernicus OData API"""

    def __init__(self, username=None, password=None):
        super().__init__()
        self._credentials = (username, password)
        self._url_regex = re.compile(r'^(\S+)/\$value$')

    def _build_metadata_url(self, url):
        """Returns the URL to query to get the metadata"""
        matches = self._url_regex.match(url)
        if matches:
            return matches.group(1) + '?$format=json&$expand=Attributes'
        else:
            raise ValueError('The URL does not match the expected pattern')

    def _get_raw_metadata(self, url):
        """Opens a stream """
        try:
            metadata_url = self._build_metadata_url(url)
            stream = requests.get(metadata_url, auth=self._credentials, stream=True).content
        except (requests.exceptions.RequestException, ValueError):
            LOGGER.error("Could not get metadata for the dataset located at '%s'", url,
                         exc_info=True)
        else:
            return json.load(io.BytesIO(stream))

    def _get_normalized_attributes(self, url):
        """Get attributes from the Copernicus OData API"""

        raw_metadata = self._get_raw_metadata(url)
        attributes = {a['Name']: a['Value'] for a in raw_metadata['d']['Attributes']['results']}

        return self._metadata_handler.get_parameters(attributes)


class NansatIngester(Ingester):
    """Ingester class using Nansat to open files or streams"""

    def _ingest_dataset(self, url, *args, **kwargs):
        """Ingest one dataset using nansat"""
        n_points = int(kwargs.get('n_points', 10))
        nansat_options = kwargs.get('nansat_options', {})

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

        return (created_dataset, created_dataset_uri)
