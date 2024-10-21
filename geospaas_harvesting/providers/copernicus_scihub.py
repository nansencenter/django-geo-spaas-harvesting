"""Code for searching Copernicus Scihub (https://scihub.copernicus.eu/)"""
import io
import json
import logging
import re
from datetime import datetime

import feedparser
from shapely.geometry.polygon import LineString, Point, Polygon

import geospaas.catalog.managers as catalog_managers
import geospaas_harvesting.utils as utils
from geospaas_harvesting.crawlers import DatasetInfo, HTTPPaginatedAPICrawler
from .base import Provider
from ..arguments import ChoiceArgument, StringArgument, WKTArgument


class CopernicusScihubProvider(Provider):
    """Provider for the Copernicus Scihub APIs"""

    type = 'copernicus_scihub'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_url = 'https://apihub.copernicus.eu/apihub/search'
        # TODO: precise argument validation
        self.search_parameters_parser.add_arguments([
            WKTArgument('location', geometry_types=(LineString, Point, Polygon)),
            ChoiceArgument('level', valid_options=('L0', 'L1', 'L2')),
            StringArgument('platformname'),
            StringArgument('ingestiondate'),
            StringArgument('collection'),
            StringArgument('filename'),
            StringArgument('orbitnumber'),
            StringArgument('lastorbitnumber'),
            StringArgument('relativeorbitnumber'),
            StringArgument('lastrelativeorbitnumber'),
            StringArgument('orbitdirection'),
            StringArgument('polarisationmode'),
            StringArgument('producttype'),
            StringArgument('sensoroperationalmode'),
            StringArgument('swathidentifier'),
            StringArgument('cloudcoverpercentage'),
            StringArgument('timeliness'),
            StringArgument('raw_query', description='Full text query appended to the query '
                                                    'generated using the other fields'),
        ])

    def _make_crawler(self, parameters):
        time_range = (parameters.pop('start_time'), parameters.pop('end_time'))
        self._replace_location(parameters)
        self._replace_level(parameters)

        return CopernicusScihubCrawler(
            self.search_url,
            time_range=time_range,
            username=self.username,
            password=self.password,
            search_terms=parameters,
        )

    def _replace_location(self, parameters):
        """Replaces the location parameter with the footprint parameter
        accepted by scihub
        """
        location = parameters.pop('location', None)
        if location is not None:
            parameters['footprint'] = f'"intersects({location.wkt})"'

    def _replace_level(self, parameters):
        """Adds the level to the raw_query
        """
        level = parameters.pop('level', '')
        if level:
            if 'raw_query' in parameters:
                parameters['raw_query'] += f" AND {level}"
            else:
                parameters['raw_query'] = level


class CopernicusScihubCrawler(HTTPPaginatedAPICrawler):
    """Crawler for Copernicus Scihub. Uses the OpenSearch API to look for
    datasets, then uses the OData API to get the metatada about those datasets.
    """
    logger = logging.getLogger(__name__ + '.CopernicusScihubCrawler')
    MIN_DATETIME = datetime(1000, 1, 1)

    PAGE_OFFSET_NAME = 'start'
    PAGE_SIZE_NAME = 'rows'
    MIN_OFFSET = 0

    def __init__(self, *args, **kwargs):
        self._url_regex = re.compile(r'^(\S+)/\$value$')
        super().__init__(*args, **kwargs)

    def increment_offset(self):
        self.page_offset += self.page_size

    # ------------- crawl ------------
    def _build_request_parameters(self, search_terms=None, time_range=(None, None),
                                  username=None, password=None, page_size=100):
        """Build a dict containing the parameters used to query the Copernicus API.
        Results are sorted ascending, which avoids missing some
        if products are added while the harvesting is happening
        (it will generally be the case)
        """
        request_parameters = super()._build_request_parameters(
            search_terms, time_range, username, password, page_size)

        if search_terms:
            request_parameters['params']['q'] = self._make_query(search_terms)

        time_condition = self._make_time_condition(time_range)

        if time_condition:
            request_parameters['params']['q'] += f" AND ({time_condition})"

        request_parameters['params']['orderby'] = 'ingestiondate asc'
        if username and password:
            request_parameters['auth'] = (username, password)

        return request_parameters

    def _make_query(self, search_terms):
        """Generates the string of search terms to be included in the request
        """
        raw_query = search_terms.pop('raw_query', None)
        query = ' AND '.join((f"{k}:{v}" for k, v in search_terms.items()))
        query_to_append = f" AND ({query})" if query else ''
        if raw_query is not None:
            query = f"({raw_query}){query_to_append}"
        return query

    def _make_time_condition(self, time_range):
        """Make a time condition for the API from a time range"""
        # build the time condition equivalent to:
        # start_date <= time_range[1] and end_date >= time_range[0]
        api_date_format = '%Y-%m-%dT%H:%M:%SZ'
        time_condition = ''
        if time_range[1]:
            min_date = self.MIN_DATETIME.strftime(api_date_format)
            end_date = time_range[1].strftime(api_date_format)
            time_condition += f"beginposition:[{min_date} TO {end_date}]"
        if time_range[0]:
            start_date = time_range[0].strftime(api_date_format)
            if time_condition:
                time_condition += ' AND '
            time_condition += f"endposition:[{start_date} TO NOW]"
        return time_condition

    def _get_datasets_info(self, page):
        """Get links from the current page and adds them to self._results.
        Returns True if links were found, False otherwise"""
        entries = feedparser.parse(page)['entries']

        for entry in entries:
            self.logger.debug("Adding '%s' to the list of resources.", entry['link'])
            self._results.append(DatasetInfo(entry['link']))

        return bool(entries)

    # --------- get metadata ---------
    def _build_metadata_url(self, url):
        """Returns the URL to query to get the metadata"""
        matches = self._url_regex.match(url)
        if matches:
            return matches.group(1) + '?$format=json&$expand=Attributes'
        else:
            raise ValueError('The URL does not match the expected pattern')

    def _get_raw_metadata(self, url):
        """Get the raw JSON metadata from a Copernicus OData URL"""
        metadata_url = self._build_metadata_url(url)
        stream = utils.http_request(
            'GET', metadata_url, auth=self.request_parameters.get('auth'), stream=True).content
        return json.load(io.BytesIO(stream))

    def get_normalized_attributes(self, dataset_info, **kwargs):
        """Get attributes from the Copernicus OData API"""

        raw_metadata = self._get_raw_metadata(dataset_info.url)
        attributes = {a['Name']: a['Value'] for a in raw_metadata['d']['Attributes']['results']}

        self.add_url(dataset_info.url, attributes)

        normalized_attributes = self._metadata_handler.get_parameters(attributes)
        normalized_attributes['geospaas_service'] = catalog_managers.HTTP_SERVICE
        normalized_attributes['geospaas_service_name'] = catalog_managers.HTTP_SERVICE_NAME

        return normalized_attributes
