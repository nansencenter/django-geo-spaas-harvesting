"""Base classes for use by providers"""
import logging

from shapely.geometry.polygon import Polygon

import geospaas_harvesting.ingesters as ingesters
from ..arguments import ArgumentParser, DatetimeArgument, WKTArgument


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

    def _make_filters(self, parsed_parameters):  # pylint: disable=unused-argument
        """No filters by default"""
        return []


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

    def _make_filters(self, parsed_parameters):
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


class Provider(FilterMixin):
    """Base class for Providers. Child classes should add their
    specific parameters to the 'search_parameters' attribute in the
    form of Argument objects.
    They should also implement the _make_crawler() method.
    """

    def __init__(self, *args, **kwargs):
        self.name = kwargs.get('name', 'unknown')
        self.username = kwargs.get('username')
        self.password = kwargs.get('password')

        self.search_parameters_parser = ArgumentParser([
            DatetimeArgument('start_time', default=None),
            DatetimeArgument('end_time', default=None),
            WKTArgument('location', geometry_types=(Polygon,)),
        ])

    def __repr__(self):
        return f"[{self.__class__.__name__}, name: {self.name}]"

    def search(self, **parameters):
        """Returns a Search object which can be used to explore the
        search results returned by the crawler
        """
        parsed_parameters = self.search_parameters_parser.parse(parameters)
        filters = self._make_filters(parsed_parameters)
        return SearchResults(self._make_crawler(parsed_parameters), filters=filters)

    def _make_crawler(self, parameters):
        """Create a crawler from the search parameters"""
        raise NotImplementedError()


class SearchResults():
    """Facilitates navigation in the results returned by a crawler and
    enables ingestion of the results in the database.
    Provides only basic functionality for now. To be extended when
    integrating the search and harvesting process in the web UI.
    """
    def __init__(self, crawler, filters=None):
        self.crawler = crawler
        self.crawler_iterator = None
        self.filters = filters if filters is not None else []

    def __repr__(self):
        return f"SearchResults for crawler: {self.crawler}"

    def __eq__(self, other):
        return self.crawler == other.crawler and self.filters == other.filters

    def __iter__(self):
        self.crawler.set_initial_state()
        self.crawler_iterator = iter(self.crawler)
        return self

    def __next__(self):
        """Look for the next dataset_info returned by the crawler which
        matches the filters
        """
        # will be interrupted by the StopIteration when arriving at the
        # end of the crawler
        while True:
            next_dataset_info = next(self.crawler_iterator)
            if self._filter(next_dataset_info):
                return next_dataset_info

    def _filter(self, dataset_info):
        """Apply all the filters to the DatasetInfo object and returns
        False if any filter returns False
        """
        for filter_ in self.filters:
            if not filter_(dataset_info):
                return False
        return True

    def save(self):
        """Save the datasets matching the search to the database"""
        logger.info("%s starting ingestion", self)
        ingesters.Ingester().ingest(self)
