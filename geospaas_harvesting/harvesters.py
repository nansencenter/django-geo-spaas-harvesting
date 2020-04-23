"""Harvesters which use crawlers and ingesters to get data from providers' websites"""

import logging
import os

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.ingesters as ingesters

logging.getLogger(__name__).addHandler(logging.NullHandler())


class HarvesterConfigurationError(Exception):
    """
    Exception which occurs when the configuration for a harvester is not correctly retrieved or is
    missing mandatory sections
    """

class Harvester():
    """
    Base Harvester class. Implements the basic behavior but is not meant to be used directly.
    It should be subclassed, and child classes should implement the following methods:
        - _create_crawlers()
        - _create_ingester()
    """

    def _create_crawlers(self):
        """Should return a list of crawlers. Needs to be implemented in child classes"""
        raise NotImplementedError('')

    def _create_ingester(self):
        """Should return an ingester. Needs to be implemented in child classes"""
        raise NotImplementedError('')

    def __init__(self, **config):
        self.config = config

        try:
            self._crawlers = self._create_crawlers()
            self._ingester = self._create_ingester()
        except (KeyError, TypeError) as error:
            raise HarvesterConfigurationError("Missing configuration key") from error

        self._crawlers_iterator = iter(self._crawlers)
        self._current_crawler = next(self._crawlers_iterator)

    def harvest(self):
        """
        Loop through the crawlers and ingest files for each one.
        Looping by using the iterator explicitly enables to resume after a deserialization
        """
        while True:
            try:
                self._ingester.ingest(self._current_crawler)
                # When the crawler is done iterating, reset it so that it can be reused
                self._current_crawler.set_initial_state()
                self._current_crawler = next(self._crawlers_iterator)
            except StopIteration:
                break


class PODAACHarvester(Harvester):
    """Harvester class for PODAAC data (NASA)"""
    def _create_crawlers(self):
        return [crawlers.OpenDAPCrawler(url) for url in self.config['urls']]

    def _create_ingester(self):
        parameters = {}
        for parameter_name in ['max_fetcher_threads', 'max_db_threads']:
            if parameter_name in self.config:
                parameters[parameter_name] = self.config[parameter_name]
        return ingesters.DDXIngester(**parameters)


class CopernicusSentinelHarvester(Harvester):
    """Harvester class for Copernicus Sentinel data"""
    def _create_crawlers(self):
        return [
            crawlers.CopernicusOpenSearchAPICrawler(
                self.config['url'],
                search_terms=search,
                username=self.config.get('username', None),
                password=os.getenv(self.config.get('password', ''), None))
            for search in self.config['search_terms']
        ]

    def _create_ingester(self):
        parameters = {}
        for parameter_name in ['username', 'max_fetcher_threads', 'max_db_threads']:
            if parameter_name in self.config:
                parameters[parameter_name] = self.config[parameter_name]
        if 'password' in self.config:
            parameters['password'] = os.getenv(self.config['password'])
        return ingesters.CopernicusODataIngester(**parameters)
