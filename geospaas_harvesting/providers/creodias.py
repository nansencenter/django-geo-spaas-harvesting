"""Code for searching Creodias data (https://creodias.eu/)"""
import logging
import json
from urllib.parse import urljoin

from shapely.geometry.polygon import Polygon

import geospaas.catalog.managers as catalog_managers
import geospaas_harvesting.utils as utils
from geospaas_harvesting.crawlers import DatasetInfo, HTTPPaginatedAPICrawler
from .base import Provider
from ..arguments import Argument, ChoiceArgument, IntegerArgument, StringArgument, WKTArgument


class CreodiasProvider(Provider):
    """Entry point to search for data hosted by CREODIAS.
    The one mandatory search parameter is 'collection'.
    The list of available collections and the corresponding search
    parameters are fetched from the Creodias API.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_url = 'https://datahub.creodias.eu'
        self.search_url = f"{self.base_url}/resto/api/collections/{{collection}}/search.json"
        self._collections = None
        self.search_parameters_parser.add_arguments([
            WKTArgument('location', geometry_types=(Polygon,)),
            CollectionArgument('collection', required=True, valid_options=self.collections),
            StringArgument('status', default='all'),
            StringArgument('dataset', default='ESA-DATASET'),
            ProductIdentifierArgument('productIdentifier', required=False),
        ])

    def _make_crawler(self, parameters):
        collection_url = self.search_url.format(collection=parameters.pop('collection'))
        location = parameters.pop('location', None)  # shapely geometry or None
        if location is not None:
            parameters['geometry'] = location.wkt
        time_range = (parameters.pop('start_time'), parameters.pop('end_time'))

        return CreodiasEOFinderCrawler(
            collection_url,
            search_terms=parameters,
            time_range=time_range,
            username=self.username,
            password=self.password,
        )

    @property
    def collections(self):
        """Fetches the list of available collections and their
        associated fields from the Creodias API and builds a dictionary
        with the following format:
        {
            "collection_1": {
                'field_1': {
                    "id": "field_1",
                    "fieldType": "select",
                    "options": [{"name": "option1", "value": "option1"}]
                },
                'field_2': {
                    "id": "field_2",
                    "fieldType": "input",
                    "inputType": "number",
                    "min": 1, "max": 10, "step": 1
                }
            }
        }
        """
        if self._collections is None:
            response = utils.http_request('GET', urljoin(self.base_url, 'collections.json'))
            response.raise_for_status()

            self._collections = {
                collection['id']: {
                    field['id']: field
                    for field in collection['formFields']
                }
                for collection in response.json()['collections']
            }

        return self._collections


class ProductIdentifierArgument(StringArgument):
    """Product identifiers need to be put between '%' characters
    """
    def parse(self, value):
        return f"%{super().parse(value)}%"


class CollectionArgument(ChoiceArgument):
    """Argument representing a Creodias collection.
    It populates child parameters at parsing time according to the
    collection being searched.
    """

    def __str__(self):
        return (Argument.__str__(self) +
                f", valid options={list(self.valid_options.keys())}")

    def _get_collection_parameters(self, collection):
        """Makes argument objects from the data returned by the API
        endpoint defining collections and fields
        """
        for field_name, field_attributes in self.valid_options[collection].items():
            if field_attributes['fieldType'] == 'select':
                self.add_child(ChoiceArgument(
                    field_name,
                    required=field_attributes['required'],
                    valid_options=[
                        opt['value'] for opt in field_attributes['options']
                    ]
                ))
            elif field_attributes['fieldType'] == 'input':
                if field_attributes['inputType'] == 'text':
                    self.add_child(StringArgument(
                        field_name, required=field_attributes['required'],))
                elif field_attributes['inputType'] == 'number':
                    self.add_child(IntegerArgument(
                        field_name,
                        required=field_attributes['required'],
                        min_value=field_attributes['min'],
                        max_value=field_attributes['max']))
                else:
                    raise ValueError(f"Unknown input type {field_attributes['inputType']}")
            else:
                raise ValueError(f"Unknown field type {field_attributes['fieldType']}")

    def parse(self, value):
        collection = super().parse(value)
        self._get_collection_parameters(collection)
        return collection


class CreodiasEOFinderCrawler(HTTPPaginatedAPICrawler):
    """Crawler for the Creodias EO finder API"""

    logger = logging.getLogger(__name__ + '.CreodiasEOFinderCrawler')

    PAGE_OFFSET_NAME = 'page'
    PAGE_SIZE_NAME = 'maxRecords'
    MIN_OFFSET = 1

    # ------------- crawl ------------
    def _build_request_parameters(self, search_terms=None, time_range=(None, None),
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
            search_terms, time_range, username, password, page_size)

        if search_terms:
            request_parameters['params'].update(**search_terms)

        request_parameters['params']['sortParam'] = 'published'
        request_parameters['params']['sortOrder'] = 'ascending'

        api_date_format = '%Y-%m-%dT%H:%M:%SZ'
        if time_range[0]:
            request_parameters['params']['startDate'] = time_range[0].strftime(api_date_format)
        if time_range[1]:
            request_parameters['params']['completionDate'] = time_range[1].strftime(api_date_format)

        return request_parameters

    def _get_datasets_info(self, page):
        """Get dataset attributes from the current page and
        adds them to self._results.
        Returns True if attributes were found, False otherwise"""
        entries = json.loads(page)['features']

        for entry in entries:
            metadata = entry['properties']
            metadata['geometry'] = json.dumps(entry['geometry'])
            url = metadata['services']['download']['url']
            self.logger.debug("Adding '%s' to the list of resources.", url)
            self._results.append(DatasetInfo(url, metadata))

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
