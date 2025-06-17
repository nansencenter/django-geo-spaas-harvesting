"""Base classes for use by providers"""
import logging

import django.db.models as models
from django.core.exceptions import ValidationError
from shapely.geometry.polygon import Polygon

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.ingesters as ingesters
import geospaas_harvesting.normalizers as normalizers
from ..arguments import ArgumentParser, DatetimeArgument, DictArgument, StringArgument, WKTArgument


logger = logging.getLogger(__name__)


class FilterMixin():
    """Base class for filter mixins. These are used to easily add
    filtering capabilities to providers.
    This filtering is applied to the output of the crawler, after
    the normalization step. So it is far less costly to narrow the
    search down at the crawler level whenever possible rather than
    using these filters (for example when the provider exposes an API
    with search capabilities).
    """

    def make_filters(self, parsed_parameters):  # pylint: disable=unused-argument
        """No filters by default"""
        return []

    def _filter(self, dataset_info):
        """Apply all the filters to the DatasetInfo object and returns
        False if any filter returns False
        """
        for filter_ in self.filters:
            if not filter_(dataset_info):
                return False
        return True

    def filter(self, filters, dataset_infos):
        """Apply filters to an iterator of DatasetInfo and yield the
        valid ones
        """
        for dataset_info in dataset_infos:
            valid = True
            for filter_ in filters:
                if not filter_(dataset_info):
                    valid = False
            if valid:
                yield dataset_info


class TimeFilterMixin(FilterMixin):
    """Adaptation for directory crawlers. Since the precision of
    time filtering is at the folder level, we need to filter more
    finely.
    """

    def _time_coverage_end_gt(self, dataset_info):
        """Compares a DatasetInfo's time coverage to the stored value"""
        return dataset_info.metadata['time_coverage_end'] > self._mixin_start_time

    def _time_coverage_start_lte(self, dataset_info):
        """Compares a DatasetInfo's time coverage to the stored value"""
        return dataset_info.metadata['time_coverage_start'] <= self._mixin_end_time

    def make_filters(self, parsed_parameters):
        """Check that the search parameters' time range and the
        dataset's time range intersect.
        """
        filters = []
        self._mixin_start_time = parsed_parameters.get('start_time')
        if self._mixin_start_time is not None:
            filters.append(self._time_coverage_end_gt)
        self._mixin_end_time = parsed_parameters.get('end_time')
        if self._mixin_end_time is not None:
            filters.append(self._time_coverage_start_lte)
        return filters


# class Provider(models.Model, FilterMixin):
class Provider(FilterMixin):
    """Base class for Providers. Child classes should add their
    specific parameters to the 'search_parameters' attribute in the
    form of Argument objects.
    They should also implement the make_crawler() method.
    """
    name = models.CharField(max_length=100)
    normalizer_name = models.CharField(max_length=100)
    crawler_name = models.CharField(max_length=100)

    @staticmethod
    def validate_config(value):
        if not (isinstance(value, dict) and set(('crawler', 'ingester')).issubset(value.keys())):
            raise ValidationError

    config = models.JSONField(validators=[validate_config])

    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        # super(models.Model).__init__(*args, **kwargs)
        self.search_parameters_parser = ArgumentParser([
            DictArgument('crawler', default={}),
            DictArgument('ingester', default={}),
        ])

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"

    def __str__(self):
        return (f"Provider: {self.name} ("
                f"normalizer: {self.normalizer_name}, "
                f"crawler: {self.crawler_name}"
                ")")

    def __eq__(self, other):
        return (
            type(self) is type(other) and
            self.name == other.name and
            self.normalizer_name == other.normalizer_name and
            self.crawler_name == other.crawler_name and
            self.config == other.config)

    @property
    def crawler_class(self):
        return crawlers.index[self.crawler_name]

    @property
    def normalizer_class(self):
        return normalizers.index[self.normalizer_name]

    def search(self, **parameters):
        """Returns a Search object which can be used to explore the
        search results returned by the crawler
        """
        crawler = self.make_crawler(parameters)
        normalizer = self.make_normalizer()

        return SearchResults(
            normalizer.normalize_stream(crawler),
            ingesters.Ingester(**self.config.get('ingester', {})),
        )

    def make_crawler(self, parameters):
        """Create a crawler from the search parameters"""
        try:
            return self.crawler_class.from_config({
                **self.config.get('crawler', {}),
                **parameters
            })
        except KeyError:
            raise ValueError(f"Unknown crawler {self.crawler_name}")

    def make_normalizer(self):
        """Get MetadataNormalizer class from index and instantiate it
        """
        try:
            return self.normalizer_class()
        except KeyError:
            raise ValueError(f"Unknown normalizer {self.normalizer_name}")


class SearchResults():
    """Facilitates navigation in the results returned by a crawler and
    enables ingestion of the results in the database.
    Provides only basic functionality for now. To be extended when
    integrating the search and harvesting process in the web UI.
    """
    def __init__(self, results_iterable, ingester=None):
        self.results_iterable = results_iterable
        self.ingester = ingester
        self._cached_results = []

    # def __repr__(self):
    #     return f"SearchResults for crawler: {self.crawler}"

    def __iter__(self):
        return iter(self.results_iterable)

    def save(self, **kwargs):
        """Save the datasets matching the search to the database"""
        logger.info("%s starting ingestion", self)
        self.ingester.ingest(self, **kwargs)
