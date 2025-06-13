"""Crawlers for paginated APIs"""
import json
import logging
import math

from shapely.geometry import LineString, Point, Polygon

import geospaas_harvesting.arguments as arguments
import geospaas_harvesting.utils as utils
from .base import Crawler, DatasetInfo


class HTTPPaginatedAPICrawler(Crawler):
    """Base class for crawlers used on repositories exposing a paginated API over HTTP"""
    name = None
    argument_parser = arguments.ArgumentParser([
        *Crawler.argument_parser.arguments.values(),
        arguments.StringArgument('url', required=True),
        arguments.DictArgument('search_terms', default=None),
        arguments.IntegerArgument('page_size', default=100),
        arguments.IntegerArgument('initial_offset', default=None),
        arguments.WKTArgument('location', default=None),
    ])

    PAGE_OFFSET_NAME = ''
    PAGE_SIZE_NAME = ''
    MIN_OFFSET = 0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.url = kwargs['url']
        self._results = None
        self.initial_offset = kwargs['initial_offset'] or self.MIN_OFFSET
        self.request_parameters = self._build_request_parameters(
            kwargs['search_terms'], kwargs['time_range'], kwargs['location'],
            kwargs['username'], kwargs['password'],
            kwargs['page_size'])

    def __eq__(self, other):
        return (
            self.url == other.url and
            self.initial_offset == other.initial_offset and
            self.request_parameters == other.request_parameters
        )

    # ------------- crawl ------------
    @property
    def page_size(self):
        """Getter for the page size"""
        return self.request_parameters['params'][self.PAGE_SIZE_NAME]

    @property
    def page_offset(self):
        """Getter for the page offset"""
        return self.request_parameters['params'][self.PAGE_OFFSET_NAME]

    @page_offset.setter
    def page_offset(self, offset):
        """Setter for the page offset"""
        self.request_parameters['params'][self.PAGE_OFFSET_NAME] = offset

    def increment_offset(self):
        self.page_offset += 1

    def _build_request_parameters(self, search_terms=None, time_range=(None, None), location=None,
                                  username=None, password=None, page_size=100):
        """Build a dict containing the parameters used to query the API.
        This dict will be unpacked to provide the arguments to `requests.get()`.
        """
        return {
            'params': {
                self.PAGE_OFFSET_NAME: self.initial_offset,
                self.PAGE_SIZE_NAME: page_size,
            }
        }

    def set_initial_state(self):
        self.page_offset = self.initial_offset
        self._results = []

    def crawl(self):
        self.set_initial_state()
        while True:
            entries = self._get_entries(self._get_next_page())
            if not entries:
                break
            for dataset_info in self._get_datasets_info(entries):
                yield dataset_info

    def _get_next_page(self):
        """Get the next page of search results"""
        self.logger.debug("Looking for resources at '%s', matching '%s'",
                         self.url, self.request_parameters['params'])
        current_page = self._http_get(self.url, self.request_parameters).text
        self.increment_offset()
        return current_page

    def _get_entries(self, page):
        """Get entries about datasets from the pages returned by the
        API
        """
        raise NotImplementedError()

    def _get_datasets_info(self, page):
        """Get datasets information from raw entries and yield
        DatasetInfo objects.
        """
        raise NotImplementedError()

    # --------- get metadata ---------
    def get_normalized_attributes(self, dataset_info, **kwargs):
        raise NotImplementedError()


class EarthDataCMRCrawler(HTTPPaginatedAPICrawler):
    """Crawler for the CMR Earthdata search API"""
    name = 'earthdata_cmr'
    argument_parser = arguments.ArgumentParser([
        *HTTPPaginatedAPICrawler.argument_parser.arguments.values(),
        arguments.ArgumentParser(name='search_terms', arguments=[
            arguments.StringArgument('bounding_box', required=False),
            arguments.StringArgument(
                'short_name', required=True, description='Short name of the collection'),
            arguments.ChoiceArgument(
                'downloadable', valid_options=['true', 'false'], default='true'),
            arguments.StringArgument('platform'),
            arguments.StringArgument('instrument'),
            arguments.StringArgument('sensor'),
        ]),
    ])
    PAGE_OFFSET_NAME = 'page_num'
    PAGE_SIZE_NAME = 'page_size'
    MIN_OFFSET = 1

    # ------------- crawl ------------
    def _build_request_parameters(self, search_terms=None, time_range=(None, None), location=None,
                                  username=None, password=None, page_size=100):
        if location is not None:
            search_terms.update(self._make_spatial_parameter(location))

        request_parameters = super()._build_request_parameters(
            search_terms, time_range, location, username, password, page_size)

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

    def _get_entries(self, page):
        return json.loads(page)['items']

    def _get_datasets_info(self, entries):
        """Get dataset attributes from the current page and
        adds them to self._results.
        Returns True if attributes were found, False otherwise"""
        for entry in entries:
            url = self._find_download_url(entry)
            yield DatasetInfo(url, entry)


class RestoCrawler(HTTPPaginatedAPICrawler):
    """Crawler for the Creodias EO finder API"""
    name = 'resto'
    argument_parser = arguments.ArgumentParser([
        *HTTPPaginatedAPICrawler.argument_parser.arguments.values(),
        arguments.StringArgument('collection', required=True),
        arguments.ArgumentParser(name='search_terms', strict=False, arguments=[
            arguments.StringArgument('status', default='all'),
            arguments.StringArgument('dataset', default='ESA-DATASET'),
            arguments.StringArgument('productIdentifier', required=False),
        ]),
    ])
    logger = logging.getLogger(__name__ + '.RestoCrawler')

    PAGE_OFFSET_NAME = 'page'
    PAGE_SIZE_NAME = 'maxRecords'
    MIN_OFFSET = 1

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.url = f"{self.url}/resto/api/collections/{kwargs['collection']}/search.json"

    # ------------- crawl ------------
    def _build_request_parameters(self, search_terms=None, time_range=(None, None), location=None,
                                  username=None, password=None, page_size=100, max_threads=1,):
        """Build a dict containing the parameters used to query
        the Creodias EO finder API.
        search_terms should be a dictionary containing the search
        parameters and their values.
        Results are sorted ascending, which avoids missing some
        if products are added while the harvesting is happening
        (it will generally be the case)
        """
        request_parameters = super()._build_request_parameters(
            search_terms, time_range, location, username, password, page_size)

        if search_terms is not None:
            if location is not None:
                search_terms['geometry'] = location.wkt
            request_parameters['params'].update(**search_terms)

        request_parameters['params']['sortParam'] = 'published'
        request_parameters['params']['sortOrder'] = 'ascending'

        api_date_format = '%Y-%m-%dT%H:%M:%SZ'
        if time_range[0]:
            request_parameters['params']['startDate'] = time_range[0].strftime(api_date_format)
        if time_range[1]:
            request_parameters['params']['completionDate'] = time_range[1].strftime(api_date_format)

        return request_parameters

    def _get_entries(self, page):
        return json.loads(page)['features']

    def _get_datasets_info(self, entries):
        """Get dataset attributes from the current page and
        adds them to self._results.
        Returns True if attributes were found, False otherwise"""
        for entry in entries:
            metadata = entry['properties']
            metadata['geometry'] = json.dumps(entry['geometry'])
            url = metadata['services']['download']['url']
            yield DatasetInfo(url, metadata)
