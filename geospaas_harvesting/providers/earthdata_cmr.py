"""Code for searching EarthData CMR (https://www.earthdata.nasa.gov/)"""
import json

from shapely.geometry import LineString, Point, Polygon

import geospaas.catalog.managers as catalog_managers
from geospaas_harvesting.crawlers import DatasetInfo, HTTPPaginatedAPICrawler
from .base import Provider
from ..arguments import ChoiceArgument, StringArgument, WKTArgument


class EarthDataCMRProvider(Provider):
    """Provider for the EarthData CMR API. The arguments are not
    properly validated because of the massive amount of collections
    available through this API. This needs to be refined.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_url = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json'
        self.search_parameters_parser.add_arguments([
            WKTArgument('location', required=False, geometry_types=(LineString, Point, Polygon)),
            StringArgument('short_name', required=True, description='Short name of the collection'),
            ChoiceArgument('downloadable', valid_options=['true', 'false'], default='true'),
            StringArgument('platform'),
            StringArgument('instrument'),
            StringArgument('sensor'),
        ])

    def _make_crawler(self, parameters):
        time_range = (parameters.pop('start_time'), parameters.pop('end_time'))
        location = parameters.pop('location')
        parameters.update(self._make_spatial_parameter(location))
        return EarthDataCMRCrawler(
            self.search_url,
            search_terms=parameters,
            time_range=time_range,
            username=self.username,
            password=self.password,
        )

    def _make_spatial_parameter(self, geometry):
        if isinstance(geometry, Polygon):
            # the API takes a sequence of points to define a polygon:
            # lon0,lat0,lon1,lat1,lon2,lat2,...,lon0,lat0
            points = zip(*geometry.exterior.coords.xy)
            result = {'polygon': ','.join([f"{lon},{lat}" for lon, lat in points])}
        elif isinstance(geometry, LineString):
            points = zip(*geometry.xy)
            result = {'line': ','.join([f"{lon},{lat}" for lon, lat in points])}
        elif isinstance(geometry, Point):
            result = {'point': f"{geometry.xy[0][0]},{geometry.xy[1][0]}"}
        else:
            raise ValueError(f"Unsupported geometry type {type(geometry)}")
        return result


class EarthDataCMRCrawler(HTTPPaginatedAPICrawler):
    """Crawler for the CMR Earthdata search API"""

    PAGE_OFFSET_NAME = 'page_num'
    PAGE_SIZE_NAME = 'page_size'
    MIN_OFFSET = 1

    # ------------- crawl ------------
    def _build_request_parameters(self, search_terms=None, time_range=(None, None),
                                  username=None, password=None, page_size=100):
        request_parameters = super()._build_request_parameters(
            search_terms, time_range, username, password, page_size)

        if search_terms:
            request_parameters['params'].update(**search_terms)

        # sort by start date, ascending
        request_parameters['params']['sort_key'] = '+start_date'

        if time_range[0] or time_range[1]:
            request_parameters['params']['temporal'] = ','.join(
                date.isoformat() if date else ''
                for date in time_range)

        return request_parameters

    def _get_datasets_info(self, page):
        """Get dataset attributes from the current page and
        adds them to self._results.
        Returns True if attributes were found, False otherwise"""
        entries = json.loads(page)['items']

        for entry in entries:
            url = entry['umm']['RelatedUrls'][0]['URL']
            self.logger.debug("Adding '%s' to the list of resources.", url)
            self._results.append(DatasetInfo(url, entry))

        return bool(entries)

    # --------- get metadata ---------
    def get_normalized_attributes(self, dataset_info, **kwargs):
        """Get attributes from an API crawler"""
        # metanorm expects a 'url' key in the raw attributes
        self.add_url(dataset_info.url, dataset_info.metadata)

        normalized_attributes = self._metadata_handler.get_parameters(dataset_info.metadata)
        normalized_attributes['geospaas_service'] = catalog_managers.HTTP_SERVICE
        normalized_attributes['geospaas_service_name'] = catalog_managers.HTTP_SERVICE_NAME

        return normalized_attributes
