"""Base classes for use by providers"""
import logging

import geospaas_harvesting.ingesters as ingesters
from ..arguments import ArgumentParser, DatetimeArgument


logger = logging.getLogger(__name__)


class FilterMixin():
    """Base class for filter mixins. These are used to easily add
    filtering capabilities to providers.
    """

    def _make_filters(self, parsed_parameters):  # pylint: disable=unused-argument
        """No filters by default"""
        return []


class TimeFilterMixin(FilterMixin):
    """Adaptation for directory crawlers. Since the precision of
    time filtering is at the folder level, we need to filter more
    finely.
    """

    def _make_filters(self, parsed_parameters):
        """Check that the search parameters' time range and the
        dataset's time range intersect.
        """
        filters = []
        start_time = parsed_parameters.get('start_time')
        if start_time is not None:
            # di is a DatasetInfo object
            filters.append(lambda di: di.metadata['time_coverage_end'] >= start_time)
        end_time = parsed_parameters.get('end_time')
        if end_time is not None:
            filters.append(lambda di: di.metadata['time_coverage_start'] <= end_time)
        return filters


class Provider(FilterMixin):
    """Base class for Providers. Child classes should add their
    specific parameters to the 'search_parameters' attribute in the
    form of Argument objects.
    They should also implement the _make_crawler() method.
    """

    def __init__(self, *args, **kwargs):
        self.name = kwargs['name']
        self.username = kwargs.get('username')
        self.password = kwargs.get('password')

        self.search_parameters_parser = ArgumentParser([
            DatetimeArgument('start_time', required=False, default=None),
            DatetimeArgument('end_time', required=False, default=None),
        ])

    def __repr__(self):
        return f"{self.__class__.__name__}, name: {self.name}"

    def search(self, **parameters):
        """Returns a Search object which can be used to explore the
        search results returned by the crawler
        """
        parsed_parameters = self.search_parameters_parser.parse(parameters)
        time_filters = self._make_filters(parsed_parameters)
        return SearchResults(self._make_crawler(parsed_parameters), filters=time_filters)

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