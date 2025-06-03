"""Code for searching EarthData CMR (https://www.earthdata.nasa.gov/)"""
import json
import logging
import math

import shapely.errors
from shapely.geometry import LineString, Point, Polygon

from geospaas_harvesting.crawlers import DatasetInfo, HTTPPaginatedAPICrawler
from .base import Provider
from ..arguments import ChoiceArgument, StringArgument, WKTArgument


class EarthDataCMRProvider(Provider):
    """Provider for the EarthData CMR API. The arguments are not
    properly validated because of the massive amount of collections
    available through this API. This needs to be refined.
    """

    type = 'earthdata_cmr'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_url = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json'
        self.search_parameters_parser.add_arguments([
            WKTArgument('location', required=False, geometry_types=(LineString, Point, Polygon)),
            StringArgument('bounding_box', required=False),
            StringArgument('short_name', required=True, description='Short name of the collection'),
            ChoiceArgument('downloadable', valid_options=['true', 'false'], default='true'),
            StringArgument('platform'),
            StringArgument('instrument'),
            StringArgument('sensor'),
        ])

    def make_crawler(self, parameters):
        time_range = (parameters.pop('start_time'), parameters.pop('end_time'))
        username = parameters.pop('username')
        password = parameters.pop('password')
        return EarthDataCMRCrawler(
            self.search_url,
            search_terms=parameters,
            time_range=time_range,
            username=username,
            password=password,
        )


class EarthDataCMRCrawler(HTTPPaginatedAPICrawler):
    """Crawler for the CMR Earthdata search API"""

    PAGE_OFFSET_NAME = 'page_num'
    PAGE_SIZE_NAME = 'page_size'
    MIN_OFFSET = 1

    # ------------- crawl ------------
    def _build_request_parameters(self, search_terms=None, time_range=(None, None),
                                  username=None, password=None, page_size=100):
        if 'location' in search_terms:
            geometry = search_terms.pop('location')
            search_terms.update(self._make_spatial_parameter(geometry))

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

    def _exclude_edges(self, raw_points):
        """Slightly move points on the edges inward for compatibility with the
        Earthdata CMR interface
        """
        points = []
        for lon, lat in raw_points:
            abs_lon = abs(lon)
            abs_lat = abs(lat)
            if abs_lon == 180:
                lon = math.copysign(abs_lon - .01, lon)
            if abs_lat == 90:
                lat = math.copysign(abs_lat - .01, lat)
            points.append((lon, lat))
        return points

    def _make_spatial_parameter(self, geometry):
        if isinstance(geometry, Polygon):
            # the API takes a sequence of points to define a polygon:
            # lon0,lat0,lon1,lat1,lon2,lat2,...,lon0,lat0
            points= self._exclude_edges(zip(*geometry.exterior.coords.xy))
            result = {'polygon[]': ','.join([f"{lon},{lat}" for lon, lat in points])}
        elif isinstance(geometry, LineString):
            points = self._exclude_edges(zip(*geometry.xy))
            result = {'line[]': ','.join([f"{lon},{lat}" for lon, lat in points])}
        elif isinstance(geometry, Point):
            result = {'point': f"{geometry.xy[0][0]},{geometry.xy[1][0]}"}
        elif isinstance(geometry, str):
            name, value = geometry.split('=')
            result = {name: value}
        else:
            raise ValueError(f"Unsupported geometry type {type(geometry)}")
        return result

    def _find_download_url(self, entry):
        """Return the first URL whose type is 'GET DATA'"""
        urls = entry['umm']['RelatedUrls']
        for url in urls:
            if url.get('Type', '').lower() == 'get data':
                return url['URL']
        return urls[0]['URL']

    def _get_datasets_info(self, page):
        """Get dataset attributes from the current page and
        adds them to self._results.
        Returns True if attributes were found, False otherwise"""
        entries = json.loads(page)['items']

        for entry in entries:
            url = self._find_download_url(entry)
            self.logger.debug("Adding '%s' to the list of resources.", url)
            self._results.append(DatasetInfo(url, entry))

        return bool(entries)
