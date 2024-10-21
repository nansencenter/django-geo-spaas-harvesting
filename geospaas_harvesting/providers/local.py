"""Code for searching local files"""
import itertools
import json
import logging
import uuid
from urllib.parse import urlparse

import dateutil.parser
import netCDF4
import numpy as np
import shapely.wkt
from dateutil.tz import tzutc
from shapely.geometry import MultiPoint

import geospaas.catalog.managers as catalog_managers
import pythesint as pti
from geospaas.utils.utils import nansat_filename
from metanorm.utils import get_cf_or_wkv_standard_name
from nansat import Nansat
from .base import Provider, TimeFilterMixin
from ..arguments import PathArgument, StringArgument
from ..crawlers import LocalDirectoryCrawler


class NansatProvider(TimeFilterMixin, Provider):
    """Provider for local files with metadata provided by Nansat
    """

    type = 'nansat'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_parameters_parser.add_arguments([
            PathArgument('directory', default='.'),
            StringArgument('include', default='.'),
        ])

    def _make_crawler(self, parameters):
        return NansatCrawler(
            parameters['directory'],
            time_range=(parameters['start_time'], parameters['end_time']),
            include=parameters['include'],
        )


class NetCDFProvider(TimeFilterMixin, Provider):
    """Provider for local files with metadata extracted directly using
    """

    type = 'netcdf'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.longitude_attribute = kwargs.get('longitude_attribute', 'LONGITUDE')
        self.latitude_attribute = kwargs.get('latitude_attribute', 'LATITUDE')
        self.search_parameters_parser.add_arguments([
            PathArgument('directory', default='.'),
            StringArgument('include', default=r'\.nc$'),
        ])

    def _make_crawler(self, parameters):
        return NetCDFCrawler(
            parameters['directory'],
            time_range=(parameters['start_time'], parameters['end_time']),
            include=parameters['include'],
            longitude_attribute=self.longitude_attribute,
            latitude_attribute=self.latitude_attribute,
        )


class NansatCrawler(LocalDirectoryCrawler):
    """Crawler for local files, using Nansat to get metadata"""

    logger = logging.getLogger(__name__ + '.NansatCrawler')

    # --------- get metadata ---------
    def get_normalized_attributes(self, dataset_info, **kwargs):
        """Gets dataset attributes using nansat"""
        normalized_attributes = {}
        n_points = int(kwargs.get('n_points', 10))
        nansat_options = kwargs.get('nansat_options', {})
        url_scheme = urlparse(dataset_info.url).scheme
        if not 'http' in url_scheme and not 'ftp' in url_scheme:
            normalized_attributes['geospaas_service_name'] = catalog_managers.FILE_SERVICE_NAME
            normalized_attributes['geospaas_service'] = catalog_managers.LOCAL_FILE_SERVICE
        elif 'http' in url_scheme and not 'ftp' in url_scheme:
            normalized_attributes['geospaas_service_name'] = catalog_managers.DAP_SERVICE_NAME
            normalized_attributes['geospaas_service'] = catalog_managers.OPENDAP_SERVICE
        elif 'ftp' in url_scheme:
            raise ValueError(
                f"Can't ingest '{dataset_info.url}': nansat can't open remote ftp files")

        # Open file with Nansat
        nansat_object = Nansat(nansat_filename(dataset_info.url),
                               log_level=self.logger.getEffectiveLevel(),
                               mapper='mapper_sentinel1_l1',
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
        normalized_attributes['location_geometry'] = shapely.wkt.loads(
            nansat_object.get_border_wkt(n_points=n_points))

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
                    f"Can't ingest '{dataset_info.url}': the 'dataset_parameters' section of the "
                    "metadata returned by nansat is not a JSON list")
        else:
            normalized_attributes['dataset_parameters'] = []

        return normalized_attributes


class NetCDFCrawler(LocalDirectoryCrawler):
    """Crawler for local NetCDF files"""

    logger = logging.getLogger(__name__ + '.NetCDFCrawler')

    def __init__(self, *args, **kwargs):
        self.longitude_attribute = kwargs.pop('longitude_attribute')
        self.latitude_attribute = kwargs.pop('latitude_attribute')
        super().__init__(*args, **kwargs)

    # --------- get metadata ---------
    def _get_geometry_wkt(self, dataset):
        longitudes = dataset.variables[self.longitude_attribute][:]
        latitudes = dataset.variables[self.latitude_attribute][:]

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
            valid_lon = longitudes.compressed() if np.ma.isMaskedArray(longitudes) else longitudes
            valid_lat = latitudes.compressed() if np.ma.isMaskedArray(latitudes) else latitudes
            points = list(itertools.product(valid_lon, valid_lat))
        # If the longitude and latitude variables have the same shape,
        # we assume that they contain the coordinates for each data
        # point
        elif longitudes.shape == latitudes.shape:
            masks = []
            for l in (longitudes, latitudes):
                if np.ma.isMaskedArray(l):
                    masks.append(l.mask)
                else:
                    masks.append(np.full(l.shape, False))
            combined_mask = np.logical_or(*masks)
            points = np.array(np.nditer((longitudes[~combined_mask],
                                         latitudes[~combined_mask]),
                                        flags=['buffered']))
        else:
            raise ValueError("Could not determine the spatial coverage")
        geometry = MultiPoint(points).convex_hull
        return geometry.wkt

    def _get_raw_attributes(self, dataset_path):
        """Get the raw metadata from the NetCDF file"""
        dataset = netCDF4.Dataset(dataset_path)
        raw_attributes = dataset.__dict__
        self.add_url(dataset_path, raw_attributes)
        raw_attributes['raw_dataset_parameters'] = self._get_parameter_names(dataset)
        return raw_attributes

    def _get_parameter_names(self, dataset):
        """Get the names of the dataset's variables"""
        return [
            variable.standard_name
            for variable in dataset.variables.values()
            if hasattr(variable, 'standard_name')
        ]

    def get_normalized_attributes(self, dataset_info, **kwargs):
        raw_attributes = self._get_raw_attributes(dataset_info.url)
        normalized_attributes = self._metadata_handler.get_parameters(raw_attributes)

        if not normalized_attributes.get('location_geometry'):
            normalized_attributes['location_geometry'] = self._get_geometry_wkt(
                netCDF4.Dataset(dataset_info.url))

        if dataset_info.url.startswith('http'):
            normalized_attributes['geospaas_service'] = catalog_managers.HTTP_SERVICE
            normalized_attributes['geospaas_service_name'] = catalog_managers.HTTP_SERVICE_NAME
        else:
            normalized_attributes['geospaas_service'] = catalog_managers.LOCAL_FILE_SERVICE
            normalized_attributes['geospaas_service_name'] = catalog_managers.FILE_SERVICE_NAME

        return normalized_attributes
