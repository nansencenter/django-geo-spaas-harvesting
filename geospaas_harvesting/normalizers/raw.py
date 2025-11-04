""""""
from geospaas.catalog.models import Dataset, DatasetURI, Tag

from .base import MetadataNormalizer
from .utils import NC_H5_FILENAME_MATCHER


class RawMetadataNormalizer(MetadataNormalizer):
    """No guess work, just add the raw metadata as a tag
    """

    name = 'raw'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.save_raw_metadata = kwargs.get('save_raw_metadata', True)

    def get_entry_id(self, dataset_info):
        filename_match = NC_H5_FILENAME_MATCHER.search(dataset_info.url)
        if filename_match:
            return filename_match.group(1)
        else:
            return None

    def get_time_coverage_start(self, dataset_info):
        return None

    def get_time_coverage_end(self, dataset_info):
        return None

    def get_location_geometry(self, dataset_info):
        return None

    def get_tags(self, dataset_info):
        tags_kwargs = []
        if self.save_raw_metadata:
            tags_kwargs = [
                {'name': key, 'value': str(value)}
                for key, value in dataset_info.metadata.items()
            ]
        return tags_kwargs
