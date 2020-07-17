"""
Ingesters which, given a set of (possibly remote) files, add those files' metadata
in the GeoSPaaS catalog
"""
import concurrent.futures
import io
import json
import logging
import queue
import re
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

import dateutil.parser
import django.db
import django.db.utils
import requests
from django.contrib.gis.geos import GEOSGeometry

import pythesint as pti
from geospaas.catalog.managers import (DAP_SERVICE_NAME, FILE_SERVICE_NAME,
                                       HTTP_SERVICE, HTTP_SERVICE_NAME,
                                       LOCAL_FILE_SERVICE, OPENDAP_SERVICE)
from geospaas.catalog.models import (Dataset, DatasetURI, GeographicLocation,
                                     Source, DatasetParameter)
from geospaas.utils.utils import nansat_filename
from geospaas.vocabularies.models import (DataCenter, Instrument,
                                          ISOTopicCategory, Location, Parameter, Platform)
from metanorm.handlers import GeospatialMetadataHandler
from nansat import Nansat

logging.getLogger(__name__).addHandler(logging.NullHandler())


class Ingester():
    """
    Base class for ingesters. Takes care of orchestrating the ingestion of datasets using the
    attributes gathered using the _get_normalized_attributes() method, which needs to be implemented
    in child classes.
    Fetching these attributes and writing to the database are multi-threaded tasks. See the
    documentation of the ingest() method for more detail.
    """

    LOGGER = logging.getLogger(__name__ + '.Ingester')
    QUEUE_SIZE = 500

    def __init__(self, max_fetcher_threads=1, max_db_threads=1):
        self.max_fetcher_threads = max_fetcher_threads
        self.max_db_threads = max_db_threads
        self._to_ingest = queue.Queue(self.QUEUE_SIZE)

    def __getstate__(self):
        """
        Defines the state to be serialized when using pickle.
        The queue cannot be pickled, so its contents are saved as a list.
        """
        state = dict(self.__dict__)
        state['_to_ingest_state'] = list(state.pop('_to_ingest').queue)
        return state

    def __setstate__(self, state):
        """Instantiation from a pickled state"""
        state['_to_ingest'] = queue.Queue(self.QUEUE_SIZE)
        state['_to_ingest'].queue.extend(state.pop('_to_ingest_state'))
        self.__dict__.update(state)

    @staticmethod
    def _uri_exists(uri):
        """Checks if the given URI already exists in the database"""
        return bool(DatasetURI.objects.filter(uri=uri))

    def _get_normalized_attributes(self, url, *args, **kwargs):
        """
        Returns a dictionary of normalized attribute which characterize a Dataset. It should
        contain the following extra entries: `geospaas_service` and `geospaas_service_name`, which
        should respectively contain the `service` and `service_name` values necessary to create a
        DatasetURI object.
        """
        raise NotImplementedError('The _get_normalized_attributes() method was not implemented')

    def _ingest_dataset(self, url, normalized_attributes):
        """Writes a dataset to the database based on its attributes and URL"""
        try:
            #Extract service information
            service = normalized_attributes.pop('geospaas_service', 'UNKNOWN')
            service_name = normalized_attributes.pop('geospaas_service_name', 'UNKNOWN')

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

            instrument, _ = Instrument.objects.get_or_create(
                normalized_attributes.pop('instrument'))

            source, _ = Source.objects.get_or_create(
                platform=platform,
                instrument=instrument,
                specs=normalized_attributes.pop('specs', ''))
            dataset_parameters_list = normalized_attributes.pop('dataset_parameters')
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
            _ , created_dataset_uri = DatasetURI.objects.get_or_create(
                name=service_name,
                service=service,
                uri=url,
                dataset=dataset)

            for dataset_parameter_info in dataset_parameters_list:
                standard_name = dataset_parameter_info.get('standard_name', None)
                short_name = dataset_parameter_info.get('short_name', None)
                units = dataset_parameter_info.get('units', None)
                if standard_name in ['latitude', 'longitude', None]:
                    continue
                params = Parameter.objects.filter(standard_name=standard_name)
                if params.count() > 1 and short_name is not None:
                    params = params.filter(short_name=short_name)
                if params.count() > 1 and units is not None:
                    params = params.filter(units=units)
                if params.count() >= 1:
                    dsp, dsp_created = DatasetParameter.objects.get_or_create(
                            dataset=dataset, parameter=params[0])
                    dataset.parameters.add(params[0])

        except django.db.utils.OperationalError:
            self.LOGGER.error('Database insertion failed', exc_info=True)
            return (created_dataset if 'created_dataset' in locals() else False,
                    created_dataset_uri if 'created_dataset_uri' in locals() else False)

        return (created_dataset, created_dataset_uri)

    def _thread_get_normalized_attributes(self, url, *args, **kwargs):
        """
        Gets the attributes needed to insert a dataset into the database from its URL, and puts a
        dictionary containing these attribtues in the queue to be written in the database.
        This method is meant to be run in a thread.
        """
        self.LOGGER.debug("Getting metadata from '%s'", url)
        try:
            self._to_ingest.put((url, self._get_normalized_attributes(url, *args, **kwargs)))
        except Exception: #pylint: disable=broad-except
            self.LOGGER.error("Could not get metadata from '%s'", url, exc_info=True)

    def _thread_ingest_dataset(self):
        """
        Reads datasets attributes from the queue and write them to the database.
        This method is meant to be run in a thread.
        """
        while True:
            self.LOGGER.debug('Waiting on the queue for a dataset to ingest...')
            self.LOGGER.debug('Queue size: %d', self._to_ingest.qsize())

            item = self._to_ingest.get()
            self.LOGGER.debug('Got "%s" from queue', item)

            if item is None:
                self._to_ingest.task_done()
                break

            url = item[0]
            dataset_attributes = item[1]

            try:
                self.LOGGER.debug("Ingesting '%s'", url)
                (created_dataset, created_dataset_uri) = self._ingest_dataset(
                    url, dataset_attributes)
            except Exception:  # pylint: disable=broad-except
                self.LOGGER.error("Ingestion of the dataset at '%s' failed", url, exc_info=True)
            else:
                if created_dataset:
                    self.LOGGER.info("Successfully created dataset from url: '%s'", url)
                else:
                    self.LOGGER.info("Dataset at '%s' already exists in the database.", url)
                if not created_dataset_uri:
                    self.LOGGER.error("The Dataset's URI already exists. This should never happen.")
            self._to_ingest.task_done()
        # It's important to close the database connection after the thread has done its work
        django.db.connection.close()

    def ingest(self, urls, *args, **kwargs):
        """
        `urls` should be an iterable. This method iterates over it and ingests the datasets at these
        URLs into the database.
        To be efficient, the tasks of getting the datasets' attributes from their URLs and inserting
        them in the database are multi-threaded.

        Two thread pools are used: one for fetching the attributes, and one for writing in the
        database. The number of threads in each pool is configurable.
        For the database writer, one thread should be enough. Please keep in mind that each thread
        with database access uses its own connection, and those connections are limited
        (e.g. 100 by default for postgresql).
        For the attribute fetchers, the optimal number of threads depends on each ingester

        How this works: the Ingester's `_to_ingest` attribute is a thread-safe queue. The threads
        which get the datasets' attributes put these attributes in the queue
        (see `_thread_get_normalized_attributes()`). The threads which handle database writing
        read from the queue and insert each dataset in the database.

        If a KeyboardInterrupt exception occurs (which might mean that a SIGINT or SIGTERM was
        received by the process), all scheduled metadata fetching threads are cancelled. We wait for
        the currently running fetching threads to finish, then for the database threads to finish
        processing the queue before exiting.
        """
        # Launch threads which read from the queue and create datasets in the database
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_db_threads,
                thread_name_prefix=self.__class__.__name__ + '.db') as db_executor:
            for _ in range(self.max_db_threads):
                db_executor.submit(self._thread_ingest_dataset)

            # Launch threads which fetch datasets attributes and put them in the queue
            try:
                with concurrent.futures.ThreadPoolExecutor(
                        max_workers=self.max_fetcher_threads,
                        thread_name_prefix=self.__class__.__name__ + '.attr') as attr_executor:
                    attr_futures = []
                    for url in urls:
                        if self._uri_exists(url):
                            self.LOGGER.info("'%s' is already present in the database", url)
                        else:
                            attr_futures.append(attr_executor.submit(
                                self._thread_get_normalized_attributes, url, *args, *kwargs))
            except KeyboardInterrupt:
                for future in attr_futures:
                    future.cancel()
                self.LOGGER.debug(
                    'Cancelled future fetching threads, waiting for the running threads to finish')
                concurrent.futures.wait(attr_futures)
                raise
            finally:
                # Wait for all queue elements to be processed and stop database access threads
                self.LOGGER.debug('Waiting for all the datasets in the queue to be ingested...')
                self._to_ingest.join()
                self.LOGGER.debug('Stopping all database access threads')
                for _ in range(self.max_db_threads):
                    self._to_ingest.put(None)


class MetanormIngester(Ingester):
    """
    Base class for ingester which rely on normalized metadata. Such ingesters should inherit from
    this class and implement the _get_normalized_attributes() method.
    """

    LOGGER = logging.getLogger(__name__ + '.MetanormIngester')

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
        'gcmd_location',
        'dataset_parameters'
    ]

    def __init__(self, max_fetcher_threads=1, max_db_threads=1):
        super().__init__(max_fetcher_threads, max_db_threads)
        self._metadata_handler = GeospatialMetadataHandler(self.DATASET_PARAMETER_NAMES)

    def _get_normalized_attributes(self, url, *args, **kwargs):
        """Returns a dictionary of normalized attribute which characterize a Dataset"""
        raise NotImplementedError('The _ingest_dataset() method was not implemented')

    @staticmethod
    def add_url(url, raw_attributes):
        """Utility method to add the dataset's URL to the raw attributes in case it is not there"""
        if 'url' not in raw_attributes:
            raw_attributes['url'] = url


class DDXIngester(MetanormIngester):
    """Ingests metadata in DDX format from an OpenDAP server"""

    LOGGER = logging.getLogger(__name__ + '.DDXIngester')
    GLOBAL_ATTRIBUTES_NAME = 'NC_GLOBAL'
    NAMESPACE_REGEX = r'^\{(\S+)\}Dataset$'

    def _get_xml_namespace(self, root):
        """Try to get the namespace for the XML tag in the document from the root tag"""
        try:
            namespace_prefix = re.match(self.NAMESPACE_REGEX, root.tag)[1]  # first matched group
        except TypeError:
            namespace_prefix = ''
            self.LOGGER.warning('Could not find XML namespace while reading DDX metadata')
        return namespace_prefix

    def _extract_global_attributes(self, root):
        """Extracts the global attributes of a dataset from a DDX document"""
        self.LOGGER.debug("Getting the dataset's global attributes.")
        namespaces = {'default': self._get_xml_namespace(root)}
        global_attributes = {}
        for attribute in root.findall(
                f"./default:Attribute[@name='{self.GLOBAL_ATTRIBUTES_NAME}']/default:Attribute",
                namespaces):

            global_attributes[attribute.get('name')] = attribute.find(
                "./default:value", namespaces).text

        return global_attributes
    def prepare_url(self, url):
        return url

    def _get_normalized_attributes(self, url, *args, **kwargs):
        """Get normalized metadata from the DDX info of the dataset located at the provided URL"""

        prepared_url = url if url.endswith('.ddx') else url + '.ddx'
        prepared_url = self.prepare_url(prepared_url)
        # Get the metadata from the dataset as an XML tree
        stream = io.BytesIO(requests.get(prepared_url, stream=True).content)

        # Get all the global attributes of the Dataset into a dictionary
        dataset_global_attributes = self._extract_global_attributes(
            ET.parse(stream).getroot())

        # Get the parameters needed to create a geospaas catalog dataset from the
        # global attributes
        normalized_attributes = self._metadata_handler.get_parameters(dataset_global_attributes)
        normalized_attributes['geospaas_service'] = OPENDAP_SERVICE
        normalized_attributes['geospaas_service_name'] = DAP_SERVICE_NAME

        return normalized_attributes

class DDXOSISAFIngester(DDXIngester):
    def prepare_url(self, prepared_url):
        return prepared_url.replace(prepared_url[prepared_url.find("catalog/"):prepared_url.find("?dataset=")+9],"dodsC/")

class CopernicusODataIngester(MetanormIngester):
    """Ingest datasets from the metadata returned by calls to the Copernicus OData API"""

    LOGGER = logging.getLogger(__name__ + '.CopernicusODataIngester')

    def __init__(self, username=None, password=None, max_fetcher_threads=1, max_db_threads=1):
        super().__init__(max_fetcher_threads, max_db_threads)
        self._credentials = (username, password) if username and password else None
        self._url_regex = re.compile(r'^(\S+)/\$value$')

    def _build_metadata_url(self, url):
        """Returns the URL to query to get the metadata"""
        matches = self._url_regex.match(url)
        if matches:
            return matches.group(1) + '?$format=json&$expand=Attributes'
        else:
            raise ValueError('The URL does not match the expected pattern')

    def _get_raw_metadata(self, url):
        """Get the raw JSON metadata from a Copernicus OData URL"""
        try:
            metadata_url = self._build_metadata_url(url)
            stream = requests.get(metadata_url, auth=self._credentials, stream=True).content
        except (requests.exceptions.RequestException, ValueError):
            self.LOGGER.error("Could not get metadata for the dataset located at '%s'", url,
                              exc_info=True)
        else:
            return json.load(io.BytesIO(stream))

    def _get_normalized_attributes(self, url, *args, **kwargs):
        """Get attributes from the Copernicus OData API"""

        raw_metadata = self._get_raw_metadata(url)
        attributes = {a['Name']: a['Value'] for a in raw_metadata['d']['Attributes']['results']}

        self.add_url(url, attributes)

        normalized_attributes = self._metadata_handler.get_parameters(attributes)
        normalized_attributes['geospaas_service'] = HTTP_SERVICE
        normalized_attributes['geospaas_service_name'] = HTTP_SERVICE_NAME

        return normalized_attributes


class NansatIngester(Ingester):
    """Ingester class using Nansat to open files or streams"""

    LOGGER = logging.getLogger(__name__ + '.NansatIngester')

    def _get_normalized_attributes(self, url, *args, **kwargs):
        """Gets dataset attributes using nansat"""
        normalized_attributes = {}
        n_points = int(kwargs.get('n_points', 10))
        nansat_options = kwargs.get('nansat_options', {})

        # Open file with Nansat
        nansat_object = Nansat(nansat_filename(url), **nansat_options)

        # get metadata from Nansat and get objects from vocabularies
        n_metadata = nansat_object.get_metadata()

        # set service info attributes
        url_scheme = urlparse(url).scheme
        if 'http' in url_scheme:
            normalized_attributes['geospaas_service_name'] = DAP_SERVICE_NAME
            normalized_attributes['geospaas_service'] = OPENDAP_SERVICE
        else:
            normalized_attributes['geospaas_service_name'] = FILE_SERVICE_NAME
            normalized_attributes['geospaas_service'] = LOCAL_FILE_SERVICE

        # set compulsory metadata (source)
        normalized_attributes['entry_title'] = n_metadata.get('entry_title', 'NONE')
        normalized_attributes['summary'] = n_metadata.get('summary', 'NONE')
        normalized_attributes['time_coverage_start'] = dateutil.parser.parse(
            n_metadata['time_coverage_start'])
        normalized_attributes['time_coverage_end'] = dateutil.parser.parse(
            n_metadata['time_coverage_end'])
        normalized_attributes['platform'] = json.loads(n_metadata['platform'])
        normalized_attributes['instrument'] = json.loads(n_metadata['instrument'])
        normalized_attributes['specs'] = n_metadata.get('specs', '')
        normalized_attributes['entry_id'] = n_metadata.get('entry_id', 'NERSC_' + str(uuid.uuid4()))

        # set optional ForeignKey metadata from Nansat or from defaults
        normalized_attributes['gcmd_location'] = n_metadata.get(
            'gcmd_location', pti.get_gcmd_location('SEA SURFACE'))
        normalized_attributes['provider'] = n_metadata.get(
            'data_center', pti.get_gcmd_provider('NERSC'))
        normalized_attributes['iso_topic_category'] = n_metadata.get(
            'ISO_topic_category', pti.get_iso19115_topic_category('Oceans'))

        # Find coverage to set number of points in the geolocation
        if len(nansat_object.vrt.dataset.GetGCPs()) > 0:
            nansat_object.reproject_gcps()
        normalized_attributes['location_geometry'] = GEOSGeometry(
            nansat_object.get_border_wkt(nPoints=n_points), srid=4326)

        return normalized_attributes
