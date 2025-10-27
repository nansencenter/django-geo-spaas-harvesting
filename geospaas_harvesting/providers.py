"""Base classes for use by providers"""
import logging

import django.db.models as models
from django.core.exceptions import ValidationError
from shapely.geometry.polygon import Polygon

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.ingesters as ingesters
import geospaas_harvesting.normalizers as normalizers
from .arguments import ArgumentParser, DatetimeArgument, DictArgument, StringArgument, WKTArgument


logger = logging.getLogger(__name__)


def validate_provider_config(value):
    valid_keys = set(('crawler', 'ingester', 'normalizer'))
    if not (isinstance(value, dict) and valid_keys.issubset(value.keys())):
        raise ValidationError

class Provider(models.Model):
    """Base class for Providers. Child classes should add their
    specific parameters to the 'search_parameters' attribute in the
    form of Argument objects.
    They should also implement the make_crawler() method.
    """
    name = models.CharField(max_length=100, unique=True, null=False, blank=False)
    normalizer_name = models.CharField(max_length=100, null=False, blank=False)
    crawler_name = models.CharField(max_length=100, null=False, blank=False)
    config = models.JSONField(validators=[validate_provider_config])

    class Meta:
        app_label = 'geospaas_harvesting'

    def __repr__(self):
        return (f"{self.__class__.__name__}(name='{self.name}', "
                f"normalizer_name='{self.normalizer_name}', crawler_name='{self.crawler_name}', "
                f"config={self.get_config_repr()})")

    def get_config_repr(self):
        """Returns a representation of the provider's configuration
        with masked passwords
        """
        config = self.config.copy()
        if config.get('crawler', {}).get('password') is not None:
            config['crawler']['password'] = '*****'
        return repr(config)

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
        try:
            return normalizers.index[self.normalizer_name]
        except KeyError:
            raise ValueError(f"Unknown normalizer {self.normalizer_name}")

    def search(self, **search_parameters):
        """Returns a Search object which can be used to explore the
        search results returned by the crawler
        """
        crawler = self.make_crawler(search_parameters)
        normalizer, max_threads = self.make_normalizer()

        return SearchResults(
            repr(self),
            normalizer.normalize_stream(crawler, max_threads),
            ingesters.Ingester(**self.config.get('ingester', {})),
        )

    def make_crawler(self, search_parameters):
        """Create a crawler from the search parameters and the stored configuration
        """
        try:
            return self.crawler_class.from_config({
                **self.config.get('crawler', {}),
                **search_parameters
            })
        except KeyError:
            raise ValueError(f"Unknown crawler {self.crawler_name}")

    def make_normalizer(self):
        """Get MetadataNormalizer class from index and instantiate it.
        Also retrieve the max_threads parameter from configuration
        """
        normalizer_config = self.config.get('normalizer', {})
        max_threads = normalizer_config.pop('max_threads', 1)
        return (self.normalizer_class(**normalizer_config), max_threads)


class SearchResults():
    """Facilitates navigation in the results returned by a crawler and
    enables ingestion of the results in the database.
    Provides only basic functionality for now. To be extended when
    integrating the search and harvesting process in the web UI.
    """
    def __init__(self, provider_info, results_iterable, ingester=None):
        self.provider_info = provider_info
        self.results_iterable = results_iterable
        self.ingester = ingester
        self._cached_results = []

    def __str__(self):
        return f"SearchResults for {self.provider_info}"

    def __iter__(self):
        return iter(self.results_iterable)

    def save(self, **kwargs):
        """Save the datasets matching the search to the database"""
        logger.info("%s starting ingestion", self)
        self.ingester.ingest(self, **kwargs)
