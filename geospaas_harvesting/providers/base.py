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


class Provider(models.Model):
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
        valid_keys = set(('crawler', 'ingester', 'normalizer'))
        if not (isinstance(value, dict) and valid_keys.issubset(value.keys())):
            raise ValidationError

    config = models.JSONField(validators=[validate_config])

    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        super(models.Model).__init__(*args, **kwargs)
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
            return self.normalizer_class(**self.config.get('normalizer', {}))
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
