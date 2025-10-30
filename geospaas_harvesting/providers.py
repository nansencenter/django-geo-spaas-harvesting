"""Base classes for use by providers"""
import logging

import django.db.models as models
from django.core.exceptions import ValidationError
from shapely.geometry.polygon import Polygon

import geospaas_harvesting.arguments as arguments
import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.ingesters as ingesters
import geospaas_harvesting.normalizers as normalizers


logger = logging.getLogger(__name__)


class Provider(models.Model):
    """TODO
    """
    name = models.CharField(max_length=100, unique=True, null=False, blank=False)
    config = models.JSONField(null=False)

    class Meta:
        app_label = 'geospaas_harvesting'

    config_parser = arguments.ArgumentParser([
        arguments.DictArgument('crawler', required=True),
        arguments.DictArgument('normalizer', default={'name': 'raw'}),
        arguments.DictArgument('ingester', default=dict),
        arguments.IntegerArgument('max_normalizer_threads', default=1),
    ])

    @classmethod
    def from_config(cls, name, config):
        """Instantiate a provider from a config dictionary"""
        parsed_config = cls.config_parser.parse(config)

        # check that the crawler configuration is valid
        crawler_config = parsed_config['crawler'].copy()
        crawler_name = crawler_config.pop('name')
        crawlers.index[crawler_name].argument_parser.parse(crawler_config, allow_missing=True)

        return cls(name=name, config=parsed_config)

    def __repr__(self):
        return (f"{self.__class__.__name__}(name='{self.name}', "
                f"normalizer_name='{self.normalizer_name}', "
                f"crawler_name='{self.crawler_name}', "
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
            self.config == other.config)

    @property
    def crawler_name(self):
        return self.config['crawler']['name']

    @property
    def normalizer_name(self):
        return self.config['normalizer']['name']

    @property
    def crawler_class(self):
        try:
            return crawlers.index[self.crawler_name]
        except KeyError:
            raise ValueError(f"Unknown crawler {self.crawler_name}")

    @property
    def normalizer_class(self):
        try:
            return normalizers.index[self.normalizer_name]
        except KeyError:
            raise ValueError(f"Unknown normalizer {self.normalizer_name}")

    def get_config_section(self, key):
        """Returns a config section without the 'name' attribute"""
        config_ = self.config.get(key, {}).copy()
        config_.pop('name', None)
        return config_

    def search(self, **search_parameters):
        """Returns a Search object which can be used to explore the
        search results returned by the crawler
        """
        crawler = self._make_component(self.crawler_class.from_kwargs, 'crawler', search_parameters)
        normalizer = self._make_component(self.normalizer_class, 'normalizer', search_parameters)
        ingester = self._make_component(ingesters.Ingester, 'ingester', search_parameters)
        max_threads = self.config['max_normalizer_threads']

        return SearchResults(
            repr(self),
            normalizer.normalize_stream(crawler, max_threads),
            ingester)

    def _make_component(self, class_, config_key, search_parameters):
        """Instantiate a component from a class given search parameters
        which override the default configuration
        """
        return class_(**{
            **self.get_config_section(config_key),
            **search_parameters.get(config_key, {})
        })


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
