""""""
from geospaas.catalog.models import Dataset, DatasetURI, Tag

from .base import MetadataNormalizer
from .utils import NC_H5_FILENAME_MATCHER


class RawMetadataNormalizer(MetadataNormalizer):
    """No guess work, just add the raw metadata as a tag
    """

    name = 'raw'

    def __init__(self, **kwargs):
        self.save_raw_metadata = kwargs.get('save_raw_metadata', True)
        super().__init__(**kwargs)

    def normalize(self, dataset_info):
        dataset_kwargs = {}
        entry_id = self.get_entry_id(dataset_info)
        if entry_id:
            dataset_kwargs['entry_id'] = entry_id
        dataset = Dataset(**dataset_kwargs)
        dataset_uri = DatasetURI(uri=dataset_info.url, dataset=dataset)
        if self.save_raw_metadata:
            tags = self.get_tags(dataset_info)
        else:
            tags = []
        return (dataset, dataset_uri, [], [], tags)

    def get_entry_id(self, dataset_info):
        filename_match = NC_H5_FILENAME_MATCHER.search(dataset_info.url)
        if filename_match:
            return filename_match.group(1)
        else:
            return None

    def get_tags(self, dataset_info):
        tags = []
        for key, value in dataset_info.metadata.items():
            tags.append(Tag(name=key, value=str(value)))
        return tags
