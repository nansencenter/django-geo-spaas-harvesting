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
import netCDF4
import numpy as np
import requests
from dateutil.tz import tzutc
from django.contrib.gis.geos import GEOSGeometry, LineString, MultiPoint
from django.contrib.gis.geos.point import Point

import pythesint as pti
import geospaas_harvesting.utils as utils
from geospaas.catalog.managers import (DAP_SERVICE_NAME, FILE_SERVICE_NAME,
                                       HTTP_SERVICE, HTTP_SERVICE_NAME,
                                       LOCAL_FILE_SERVICE, OPENDAP_SERVICE)
from geospaas.catalog.models import (Dataset, DatasetURI, GeographicLocation,
                                     Source)
from geospaas.utils.utils import nansat_filename
from geospaas.vocabularies.models import (DataCenter, Instrument,
                                          ISOTopicCategory, Location, Parameter, Platform)
from nansat import Nansat
from metanorm.handlers import GeospatialMetadataHandler
from metanorm.utils import get_cf_or_wkv_standard_name
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
        if not (isinstance(max_fetcher_threads, int) and isinstance(max_db_threads, int)):
            raise TypeError
        self.max_fetcher_threads = max_fetcher_threads
        self.max_db_threads = max_db_threads
        self._to_ingest = queue.Queue(self.QUEUE_SIZE)
        # safety check in order to prevent harvesting process with an empty list of parameters
        if Parameter.objects.count() < 1:
            raise RuntimeError((
                "Parameters must be updated (with the 'update_vocabularies' command "
                "of django-geospaas) before the harvesting process"
            ))

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
        return DatasetURI.objects.filter(uri=uri).exists()

    @staticmethod
    def get_download_url(dataset_info):
        """Get the download URL from the information returned by a
        crawler. The default behavior is for crawlers to return
        download URLs, so here it just returns its argument.
        """
        return dataset_info

    def _get_normalized_attributes(self, dataset_info, *args, **kwargs):
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
            # Extract service information
            service = normalized_attributes.pop('geospaas_service', 'UNKNOWN')
            service_name = normalized_attributes.pop('geospaas_service_name', 'UNKNOWN')

            # Create the objects with which the dataset has relationships
            # (or get them if they already exist)
            data_center, _ = DataCenter.objects.get_or_create(
                normalized_attributes.pop('provider'))

            location_geometry = normalized_attributes.pop('location_geometry')
            if isinstance(location_geometry, GEOSGeometry):
                # backward compatibility. can be removed later
                geometry = location_geometry
            else:
                geometry = GEOSGeometry(location_geometry)
            geographic_location, _ = GeographicLocation.objects.get_or_create(geometry=geometry)

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

            # Create the URI for the created Dataset in the database
            _, created_dataset_uri = DatasetURI.objects.get_or_create(
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
                    dataset.parameters.add(params[0])

        except django.db.utils.OperationalError:
            self.LOGGER.error('Database insertion failed', exc_info=True)
            return (created_dataset if 'created_dataset' in locals() else False,
                    created_dataset_uri if 'created_dataset_uri' in locals() else False)

        return (created_dataset, created_dataset_uri)

    def _thread_get_normalized_attributes(self, download_url, dataset_info, *args, **kwargs):
        """
        Gets the attributes needed to insert a dataset into the database from its URL, and puts a
        dictionary containing these attribtues in the queue to be written in the database.
        This method is meant to be run in a thread.
        """
        self.LOGGER.debug("Getting metadata for '%s'", download_url)
        try:
            normalized_attributes = self._get_normalized_attributes(dataset_info, *args, **kwargs)
        except Exception:  # pylint: disable=broad-except
            self.LOGGER.error("Could not get metadata for '%s'", download_url, exc_info=True)
        else:
            self._to_ingest.put((download_url, normalized_attributes))

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
                    # This can happen if the dataset was already
                    # present, or if a database problem occurred in
                    # _ingest_dataset(). Note that this problem might
                    # not happen during the dataset creation.
                    self.LOGGER.info("Dataset at '%s' was not created.", url)
                if not created_dataset_uri:
                    # This should only happen if a database problem
                    # occurred in _ingest_dataset(), because the
                    # presence of the URI in the database is checked
                    # before attempting to ingest.
                    self.LOGGER.error("The Dataset URI '%s' was not created.", url)
            self._to_ingest.task_done()
        # It's important to close the database connection after the thread has done its work
        django.db.connection.close()

    def ingest(self, datasets_to_ingest, *args, **kwargs):
        """
        `datasets_to_ingest` should be an iterable containing information about the datasets to
        ingest. The nature of this information depends on the crawler and the ingester, but it
        usually is the URL where the dataset can be downloaded.
        This method iterates over datasets_to_ingest and ingests the datasets into the database.
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
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.max_fetcher_threads,
                    thread_name_prefix=self.__class__.__name__ + '.attr') as attr_executor:
                try:
                    attr_futures = []
                    for dataset_info in datasets_to_ingest:
                        download_url = self.get_download_url(dataset_info)
                        if self._uri_exists(download_url):
                            self.LOGGER.info(
                                "'%s' is already present in the database", download_url)
                        else:
                            attr_futures.append(attr_executor.submit(
                                self._thread_get_normalized_attributes,
                                download_url,
                                dataset_info,
                                *args, **kwargs
                            ))
                except KeyboardInterrupt:
                    for future in reversed(attr_futures):
                        future.cancel()
                    self.LOGGER.debug(
                        'Cancelled future fetching threads, '
                        'waiting for the running threads to finish')
                    raise
                finally:
                    # Wait for running fetching threads to finish
                    concurrent.futures.wait(attr_futures)
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
        'entry_id',
        'summary',
        'time_coverage_start',
        'time_coverage_end',
        'platform',
        'instrument',
        'location_geometry',
        'provider',
        'iso_topic_category',
        'gcmd_location',
    ]
    DATASET_CUMULATIVE_PARAMETER_NAMES = [
        'dataset_parameters',
    ]

    def __init__(self, max_fetcher_threads=1, max_db_threads=1):
        super().__init__(max_fetcher_threads, max_db_threads)
        self._metadata_handler = GeospatialMetadataHandler(
            self.DATASET_PARAMETER_NAMES, self.DATASET_CUMULATIVE_PARAMETER_NAMES)

    def _get_normalized_attributes(self, dataset_info, *args, **kwargs):
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

    def _extract_attributes(self, root):
        """
        Extracts the global or specific attributes of a dataset or specific ones from a DDX document

        "x_path_global" is pointing to the 'NC_GLOBAL' part of response of the DDX document to
        obtain general information.
        "x_path_specific" is used to extract the dataset parameter names from the DDX document.
        """
        self.LOGGER.debug("Getting the dataset's global attributes.")
        namespaces = {'default': self._get_xml_namespace(root)}
        extracted_attributes = {}
        x_path_global = "./default:Attribute[@name='NC_GLOBAL']/default:Attribute"
        x_path_specific = "./default:Grid/default:Attribute[@name='standard_name']"
        # finding the global metadata
        for attribute in root.findall(x_path_global, namespaces):
            extracted_attributes[attribute.get('name')] = attribute.find(
                "./default:value", namespaces).text
        # finding the parameters of the dataset that are declared in
        # the online source (specific metadata)
        # The specific ones are stored in 'raw_dataset_parameters' part of
        # the returned dictionary("extracted_attributes")
        extracted_attributes['raw_dataset_parameters'] = list()
        for attribute in root.findall(x_path_specific, namespaces):
            extracted_attributes['raw_dataset_parameters'].append(
                attribute.find("./default:value", namespaces).text)
        # removing the "latitude" and "longitude" from
        # the 'raw_dataset_parameters' part of the dictionary
        if 'latitude' in extracted_attributes['raw_dataset_parameters']:
            extracted_attributes['raw_dataset_parameters'].remove('latitude')
        if 'longitude' in extracted_attributes['raw_dataset_parameters']:
            extracted_attributes['raw_dataset_parameters'].remove('longitude')
        return extracted_attributes

    @classmethod
    def prepare_url(cls, url):
        """
        Converts the downloadable link into the link for reading meta data. In all cases,
        this method results in a url that ends with '.ddx' which will be used in further steps
        of ingestion.
        """
        if url.endswith('.ddx'):
            return url
        elif url.endswith('.dods'):
            return url[:-4]+'ddx'
        else:
            return url + '.ddx'

    def _get_normalized_attributes(self, dataset_info, *args, **kwargs):
        """Get normalized metadata from the DDX info of the dataset located at the provided URL"""
        prepared_url = self.prepare_url(dataset_info)
        # Get the metadata from the dataset as an XML tree
        stream = io.BytesIO(utils.http_request('GET', prepared_url, stream=True).content)
        # Get all the global attributes of the Dataset into a dictionary
        extracted_attributes = self._extract_attributes(
            ET.parse(stream).getroot())
        self.add_url(dataset_info, extracted_attributes)
        # Get the parameters needed to create a geospaas catalog dataset from the global attributes
        normalized_attributes = self._metadata_handler.get_parameters(extracted_attributes)
        normalized_attributes['geospaas_service'] = OPENDAP_SERVICE
        normalized_attributes['geospaas_service_name'] = DAP_SERVICE_NAME

        return normalized_attributes


class ThreddsIngester(DDXIngester):
    """Ingest datasets from the DDX metadata of the OpenDAP service of a Thredds server"""

    url_matcher = re.compile(r'^(.*)/(fileServer)/(.*)$')

    @classmethod
    def prepare_url(cls, url):
        url_match = cls.url_matcher.match(url)
        if url_match:
            return f"{url_match[1]}/dodsC/{url_match[3]}.ddx"
        else:
            raise ValueError(f"{url} is not a Thredds HTTPServer URL")


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
            stream = utils.http_request(
                'GET', metadata_url, auth=self._credentials, stream=True).content
        except (requests.exceptions.RequestException, ValueError):
            self.LOGGER.error("Could not get metadata for the dataset located at '%s'", url,
                              exc_info=True)
        else:
            return json.load(io.BytesIO(stream))

    def _get_normalized_attributes(self, dataset_info, *args, **kwargs):
        """Get attributes from the Copernicus OData API"""

        raw_metadata = self._get_raw_metadata(dataset_info)
        attributes = {a['Name']: a['Value'] for a in raw_metadata['d']['Attributes']['results']}

        self.add_url(dataset_info, attributes)

        normalized_attributes = self._metadata_handler.get_parameters(attributes)
        normalized_attributes['geospaas_service'] = HTTP_SERVICE
        normalized_attributes['geospaas_service_name'] = HTTP_SERVICE_NAME

        return normalized_attributes


class CreodiasEOFinderIngester(MetanormIngester):
    """Ingest datasets from the metadata returned by calls to the Creodias finder API"""

    LOGGER = logging.getLogger(__name__ + '.CreodiasEOFinderIngester')

    def _get_normalized_attributes(self, dataset_info, *args, **kwargs):
        """Get attributes from the Creodias finder API"""
        self.add_url(self.get_download_url(dataset_info), dataset_info)

        normalized_attributes = self._metadata_handler.get_parameters(dataset_info)
        normalized_attributes['geospaas_service'] = HTTP_SERVICE
        normalized_attributes['geospaas_service_name'] = HTTP_SERVICE_NAME

        return normalized_attributes

    @staticmethod
    def get_download_url(dataset_info):
        """Checks if the dataset's URI already exists in the database"""
        return dataset_info['services']['download']['url']


class URLNameIngester(MetanormIngester):
    """Ingester class which associates hard-coded data to known URLs"""
    LOGGER = logging.getLogger(__name__ + '.URLNameIngester')

    def _get_normalized_attributes(self, dataset_info, *args, **kwargs):
        """Gets dataset attributes using ftp"""
        raw_attributes = {}
        self.add_url(dataset_info, raw_attributes)
        normalized_attributes = self._metadata_handler.get_parameters(raw_attributes)
        # TODO: add FTP_SERVICE_NAME and FTP_SERVICE in django-geo-spaas
        normalized_attributes['geospaas_service_name'] = 'ftp'
        normalized_attributes['geospaas_service'] = 'ftp'
        return normalized_attributes


class NetCDFIngester(MetanormIngester):
    """Ingests metadata from NetCDF files. The files can be either
    local or remote (if the remote repository supports it).
    """

    def __init__(self, max_fetcher_threads=1, max_db_threads=1,
                 longitude_attribute='LONGITUDE', latitude_attribute='LATITUDE'):
        super().__init__(max_fetcher_threads, max_db_threads)
        self.longitude_attribute = longitude_attribute
        self.latitude_attribute = latitude_attribute

    def _get_geometry_wkt(self, dataset):
        longitudes = dataset.variables[self.longitude_attribute]
        latitudes = dataset.variables[self.latitude_attribute]

        lonlat_dependent_data = False
        for nc_variable_name, nc_variable_value in dataset.variables.items():
            if (nc_variable_name not in dataset.dimensions
                    and self.longitude_attribute in nc_variable_value.dimensions
                    and self.latitude_attribute in nc_variable_value.dimensions):
                lonlat_dependent_data = True
                break

        # If at least a variable is dependent on latitude and
        # longitude, the longitude and latitude arrays are combined to
        # find all the data points
        if lonlat_dependent_data:
            points = []
            for lon in longitudes:
                for lat in latitudes:
                    points.append(Point(float(lon), float(lat), srid=4326))
            geometry = MultiPoint(points, srid=4326).convex_hull
        # If the longitude and latitude variables have the same shape,
        # we assume that they contain the coordinates for each data
        # point
        elif longitudes.shape == latitudes.shape:
            points = []
            # in this case numpy.nditer() works like zip() for
            # multi-dimensional arrays
            for lon, lat in np.nditer((longitudes, latitudes), flags=['buffered']):
                new_point = Point(float(lon), float(lat), srid=4326)
                # Don't add duplicate points in trajectories
                if not points or new_point != points[-1]:
                    points.append(new_point)

            if len(longitudes.shape) == 1:
                if len(points) == 1:
                    geometry = points[0]
                else:
                    geometry = LineString(points, srid=4326)
            else:
                geometry = MultiPoint(points, srid=4326).convex_hull
        else:
            raise ValueError("Could not determine the spatial coverage")

        return geometry.wkt

    def _get_raw_attributes(self, dataset_path):
        """Get the raw metadata from the NetCDF file"""
        dataset = netCDF4.Dataset(dataset_path)
        raw_attributes = dataset.__dict__
        self.add_url(dataset_path, raw_attributes)
        raw_attributes['geometry'] = self._get_geometry_wkt(dataset)
        raw_attributes['raw_dataset_parameters'] = self._get_parameter_names(dataset)
        return raw_attributes

    def _get_parameter_names(self, dataset):
        """Get the names of the dataset's variables"""
        return [
            variable.standard_name
            for variable in dataset.variables.values()
            if hasattr(variable, 'standard_name')
        ]

    def _get_normalized_attributes(self, dataset_info, *args, **kwargs):
        raw_attributes = self._get_raw_attributes(dataset_info)
        normalized_attributes = self._metadata_handler.get_parameters(raw_attributes)

        if dataset_info.startswith('http'):
            normalized_attributes['geospaas_service'] = HTTP_SERVICE
            normalized_attributes['geospaas_service_name'] = HTTP_SERVICE_NAME
        else:
            normalized_attributes['geospaas_service'] = LOCAL_FILE_SERVICE
            normalized_attributes['geospaas_service_name'] = FILE_SERVICE_NAME

        return normalized_attributes


class NansatIngester(Ingester):
    """Ingester class using Nansat to open files or streams"""

    LOGGER = logging.getLogger(__name__ + '.NansatIngester')

    def _get_normalized_attributes(self, dataset_info, *args, **kwargs):
        """Gets dataset attributes using nansat"""
        normalized_attributes = {}
        n_points = int(kwargs.get('n_points', 10))
        nansat_options = kwargs.get('nansat_options', {})
        url_scheme = urlparse(dataset_info).scheme
        if not 'http' in url_scheme and not 'ftp' in url_scheme:
            normalized_attributes['geospaas_service_name'] = FILE_SERVICE_NAME
            normalized_attributes['geospaas_service'] = LOCAL_FILE_SERVICE
        elif 'http' in url_scheme and not 'ftp' in url_scheme:
            normalized_attributes['geospaas_service_name'] = DAP_SERVICE_NAME
            normalized_attributes['geospaas_service'] = OPENDAP_SERVICE
        elif 'ftp' in url_scheme:
            raise ValueError(f"Can't ingest '{dataset_info}': nansat can't open remote ftp files")

        # Open file with Nansat
        nansat_object = Nansat(nansat_filename(dataset_info),
                               log_level=self.LOGGER.getEffectiveLevel(),
                               **nansat_options)

        # get metadata from Nansat and get objects from vocabularies
        n_metadata = nansat_object.get_metadata()

        # set compulsory metadata (source)
        normalized_attributes['entry_title'] = n_metadata.get('entry_title', 'NONE')
        normalized_attributes['summary'] = n_metadata.get('summary', 'NONE')
        normalized_attributes['time_coverage_start'] = dateutil.parser.parse(
            n_metadata['time_coverage_start']).replace(tzinfo=tzutc())
        normalized_attributes['time_coverage_end'] = dateutil.parser.parse(
            n_metadata['time_coverage_end']).replace(tzinfo=tzutc())
        normalized_attributes['platform'] = json.loads(n_metadata['platform'])
        normalized_attributes['instrument'] = json.loads(n_metadata['instrument'])
        normalized_attributes['specs'] = n_metadata.get('specs', '')
        normalized_attributes['entry_id'] = n_metadata.get('entry_id', 'NERSC_' + str(uuid.uuid4()))

        # set optional ForeignKey metadata from Nansat or from defaults
        normalized_attributes['gcmd_location'] = n_metadata.get(
            'gcmd_location', pti.get_gcmd_location('SEA SURFACE'))
        normalized_attributes['provider'] = pti.get_gcmd_provider(
            n_metadata.get('provider', 'NERSC'))
        normalized_attributes['iso_topic_category'] = n_metadata.get(
            'ISO_topic_category', pti.get_iso19115_topic_category('Oceans'))

        # Find coverage to set number of points in the geolocation
        if nansat_object.vrt.dataset.GetGCPs():
            nansat_object.reproject_gcps()
        normalized_attributes['location_geometry'] = GEOSGeometry(
            nansat_object.get_border_wkt(n_points=n_points), srid=4326)

        json_dumped_dataset_parameters = n_metadata.get('dataset_parameters', None)
        if json_dumped_dataset_parameters:
            json_loads_result = json.loads(json_dumped_dataset_parameters)
            if isinstance(json_loads_result, list):
                normalized_attributes['dataset_parameters'] = [
                        get_cf_or_wkv_standard_name(dataset_param)
                        for dataset_param in json_loads_result
                    ]
            else:
                raise TypeError(
                    f"Can't ingest '{dataset_info}': the 'dataset_parameters' section of the "
                    "metadata returned by nansat is not a JSON list")

        return normalized_attributes
