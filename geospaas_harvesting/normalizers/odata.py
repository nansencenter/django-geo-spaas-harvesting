"""Normalizer for the metadata used in a OData API
"""
import dateutil
import dateutil.parser
import json
import re

import shapely
import shapely.geometry

import geospaas_harvesting.normalizers.utils as utils
from .base import MetadataNormalizer


class ODataMetadataNormalizer(MetadataNormalizer):
    """Generate the properties of a GeoSPaaS Dataset using OData
    attributes
    """
    name = 'odata'

    @utils.raises(KeyError)
    def get_entry_title(self, dataset_info):
        return dataset_info.metadata['Name']

    @utils.raises(KeyError)
    def get_entry_id(self, dataset_info):
        return dataset_info.metadata['Name']

    @utils.raises(KeyError)
    def get_summary(self, dataset_info):
        return ''

    @utils.raises(KeyError)
    def get_time_coverage_start(self, dataset_info):
        return dateutil.parser.parse(dataset_info.metadata['ContentDate']['Start'])

    @utils.raises(KeyError)
    def get_time_coverage_end(self, dataset_info):
        return dateutil.parser.parse(dataset_info.metadata['ContentDate']['End'])

    def get_keywords(self, dataset_info):
        lookups = []
        for attribute in dataset_info.metadata['Attributes']:
            if attribute['Name'] == 'platformShortName':
                lookups.append({'kind': 'gcmd_platform', 'data__icontains': attribute['Value']})
            elif attribute['Name'] == 'instrumentShortName':
                lookups.append({'kind': 'gcmd_instrument', 'data__icontains': attribute['Value']})
        return utils.find_keywords(lookups)

    @utils.raises(KeyError)
    def get_location_geometry(self, dataset_info):
        return shapely.to_wkt(shapely.geometry.shape(dataset_info.metadata['GeoFootprint']))
