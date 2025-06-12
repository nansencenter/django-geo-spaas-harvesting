"""Crawlers for ERDDAP APIs"""
import logging
import re

import requests
import shapely.geometry

import geospaas_harvesting.arguments as arguments
from .base import Crawler, DatasetInfo


class ERDDAPTableCrawler(Crawler):
    """Crawler for ERDDAP tabledap APIs"""
    name = 'erddap'
    argument_parser = arguments.ArgumentParser([
        *Crawler.argument_parser.arguments.values(),
        arguments.StringArgument('url', required=True),
        arguments.StringArgument('id_attrs', required=True),
        arguments.StringArgument('entry_id_prefix', default=''),
        arguments.StringArgument('longitude_attr', default='longitude'),
        arguments.StringArgument('latitude_attr', default='latitude'),
        arguments.StringArgument('time_attr', default='time'),
        arguments.StringArgument('position_qc_attr', default=''),
        arguments.StringArgument('time_qc_attr', default=''),
        arguments.SequenceArgument('valid_qc_codes',
                                   contents_type=arguments.IntegerArgument,
                                   default=None),
        arguments.DictArgument('search_terms', default=None),
        arguments.SequenceArgument('variables', default=None),
    ])
    logger = logging.getLogger(__name__ + '.ERDDAPTableCrawler')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        url = kwargs['url']
        if url.rstrip('/').endswith('.json'):
            self.url = url
        else:
            raise ValueError("The URL should end with .json")
        self.id_attrs = kwargs['id_attrs']
        self.entry_id_prefix = kwargs['entry_id_prefix']
        self.longitude_attr = kwargs['longitude_attr']
        self.latitude_attr = kwargs['latitude_attr']
        self.time_attr = kwargs['time_attr']
        self.position_qc_attr = kwargs['position_qc_attr']
        self.time_qc_attr = kwargs['time_qc_attr']
        self.valid_qc_codes = kwargs['valid_qc_codes']
        self.search_terms = kwargs['search_terms'] if kwargs['search_terms'] is not None else []
        self.variables = kwargs['variables'] if kwargs['variables'] else []

    def __eq__(self, other):
        return (
            self.url == other.url and
            self.id_attrs == other.id_attrs and
            self.longitude_attr == other.longitude_attr and
            self.latitude_attr == other.latitude_attr and
            self.time_attr == other.time_attr and
            self.position_qc_attr == other.position_qc_attr and
            self.time_qc_attr == other.time_qc_attr and
            self.valid_qc_codes == other.valid_qc_codes and
            self.search_terms == other.search_terms and
            self.variables == other.variables
        )

    def get_ids(self):
        """Fetch identifiers matching the search terms"""
        url = f"{self.url}?{','.join(self.id_attrs)}&distinct()"
        kwargs = {}
        url = '&'.join([url] + self.search_terms)
        try:
            response = self._http_get(url, max_tries=1, **kwargs)
        except requests.HTTPError as error:
            self.logger.error("Could not list dataset identifiers at %s: %s",
                              url, error.response.content, exc_info=True)
            raise
        for row in response.json()['table']['rows']:
            yield row[:len(self.id_attrs)]

    def _make_condition_parameters(self, parameters):
        """Prepare the parameters to filter a query using the id
        attributes. Necessary because the API requires different
        formats depending on the type of parameter
        """
        params = {}
        for key, value in parameters.items():
            if isinstance(value, str):
                params[key] = f'"{value}"'
            else:
                params[key] = value
        return params

    def crawl(self):
        attributes = [self.time_attr, self.longitude_attr, self.latitude_attr]
        for qc_attr in (self.time_qc_attr, self.position_qc_attr):
            if qc_attr:
                attributes.append(qc_attr)
        attributes.extend(self.variables)
        for id_values in self.get_ids():
            id_attrs = dict(zip(self.id_attrs, id_values))
            id_condition = '&'.join(
                f"{id_attr}={id_value}"
                for id_attr, id_value in self._make_condition_parameters(id_attrs).items()
            )
            yield DatasetInfo(
                f'{self.url}?{",".join(attributes)}&{id_condition}',
                {'id_attributes': id_attrs})

    def _check_qc(self, qc_value):
        """Return True if the QC value indicates valid data or the
        valid codes are unknown
        """
        return not self.valid_qc_codes or qc_value in self.valid_qc_codes

    def _make_coverage_url(self):
        """"""
        qc_attributes = ','.join(c for c in (self.time_qc_attr, self.position_qc_attr) if c)
        if qc_attributes:
            qc_attributes = f",{qc_attributes}"
        return (f'{self.url}?{self.time_attr},{self.longitude_attr},{self.latitude_attr}' +
                qc_attributes +
                f'&distinct()&orderBy("{self.time_attr}")')

    def get_coverage(self, id_attributes):
        """Get the temporal and spatial coverage for a specific dataset
        """
        try:
            response = self._http_get(self._make_coverage_url(), request_parameters={
                'params': self._make_condition_parameters(id_attributes)
            })
        except requests.HTTPError as error:
            self.logger.error("Could not get coverage for dataset %s: %s",
                              id_attributes, error.response.content)
            raise
        rows = response.json()['table']['rows']

        # build the trajectory and get the first time with valid QC
        # (the query results are sorted by time)
        time_coverage_start = None
        trajectory = []
        for row in rows:
            if time_coverage_start is None and self._check_qc(row[3]):
                time_coverage_start = row[0]
            point = (row[1], row[2])
            if point not in trajectory and self._check_qc(row[4]):
                trajectory.append(point)

        # get the last time with valid QC
        time_coverage_end = None
        for row in rows[::-1]:
            if self._check_qc(row[3]):
                time_coverage_end = row[0]
                break

        if time_coverage_start is None or time_coverage_end is None or not trajectory:
            raise RuntimeError(f"Could not determine coverage for dataset {id_attributes}")

        return ((time_coverage_start, time_coverage_end), trajectory)

    def _make_product_metadata_url(self):
        """Generate the product metadata URL from the base data URL"""
        match = re.match(r'^(https?://.*)/tabledap/(.*)\.json$', self.url)
        if match:
            return f"{match.group(1)}/info/{match.group(2)}/index.json"
        else:
            raise RuntimeError(f"Unable to get product metadata URL from {self.url}")

    def get_product_metadata(self):
        """Get the product's metadata"""
        url = self._make_product_metadata_url()
        try:
            response = self._http_get(url)
        except requests.HTTPError:
            self.logger.info("Could not get product metadata from %s", url)
            raise
        return response.json()

    def get_normalized_attributes(self, dataset_info, **kwargs):
        """Use metanorm to normalize a DatasetInfo's raw attributes"""
        raw_attributes = dataset_info.metadata
        self.add_url(dataset_info.url, raw_attributes)
        coverage = self.get_coverage(dataset_info.metadata['id_attributes'])
        raw_attributes['entry_id'] = (
            self.entry_id_prefix +
            '_'.join(map(str, dataset_info.metadata['id_attributes'].values()))
        )
        raw_attributes['temporal_coverage'] = coverage[0]
        raw_attributes['trajectory'] = shapely.geometry.MultiPoint(coverage[1]).wkt
        raw_attributes['product_metadata'] = self.get_product_metadata()

        normalized_attributes = self._metadata_handler.get_parameters(raw_attributes)
        return normalized_attributes
