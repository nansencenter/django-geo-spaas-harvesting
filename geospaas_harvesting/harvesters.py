"""Harvesters which use crawlers and ingesters to get data from """

import logging
import os

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.ingesters as ingesters

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class HarvesterConfigurationError(Exception):
    """
    Exception which occurs when the configuration for a harvester is not correctly retrieved or is
    missing mandatory sections
    """

class Harvester():
    """
    Base Harvester class. Implements the basic behavior but is not meant to be used directly.
    It should be subclassed, and child classes should have the following elements:
        - CRAWLER_CLASS class attribute
        - INGESTER_CLASS class attribute
        - _create_crawlers() method
        - _create_ingester() methods
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
        Crawl through the URLs and ingest files
        Looping by using the iterator explicitly enables to resume after a deserialization
        """
        while True:
            try:
                self._ingester.ingest(self._current_crawler)
                self._current_crawler = next(self._crawlers_iterator)
            except StopIteration:
                break


class EndlessHarvesterIterator():
    """Iterator which enables to endlessly iterate over a list of harvesters"""

    def __init__(self, harvesters_list):
        """Initialize internal list and current index"""
        self._current_index = 0
        self._harvesters_list = harvesters_list


    def __next__(self):
        try:
            result = self._harvesters_list[self._current_index]
        except IndexError:
            if self._current_index == 0:
                # Stop the recursion if the list is empty
                raise StopIteration()
            else:
                self._current_index = 0
                return self.__next__()
        self._current_index += 1
        return result


class HarvesterList():
    """Iterable class containing Harvester instances"""

    def __init__(self, config=None):
        """
        If present, config must be a dict containing the harvesters configurations, with the
        following structure:
            harvester_name:
                class: 'HarvesterClass'
                urls: ['url1', 'url2', ...]
            harvester_name_2:
            ...
        """
        self._harvesters = []
        if config:
            try:
                for harvester_name, harvester_config in config.items():
                    try:
                        harvester_class = globals()[harvester_config['class']]
                        self.append(harvester_class(
                            **{key: value
                               for (key, value) in harvester_config.items() if key != 'class'}
                        ))
                    except KeyError:
                        LOGGER.error("Wrong harvester class name for '%s'", harvester_name,
                                     exc_info=True)
            except AttributeError:
                LOGGER.error("'config' must be a dict", exc_info=True)

    def append(self, harvester):
        """Appends a harvester in the """
        if isinstance(harvester, Harvester):
            self._harvesters.append(harvester)
        else:
            raise TypeError('Attempt to insert a non-Harvester object')

    def __getitem__(self, index):
        return self._harvesters[index]


    def __len__(self):
        return len(self._harvesters)

    def __iter__(self):
        return EndlessHarvesterIterator(self)


class PODAACHarvester(Harvester):
    """Harvester class for PODAAC data (NASA)"""
    CRAWLER_CLASS = crawlers.OpenDAPCrawler
    INGESTER_CLASS = ingesters.DDXIngester

    def _create_crawlers(self):
        return [self.CRAWLER_CLASS(url) for url in self.config['urls']]

    def _create_ingester(self):
        return self.INGESTER_CLASS()


class CopernicusSentinelHarvester(Harvester):
    """Harvester class for Copernicus Sentinel data"""
    CRAWLER_CLASS = crawlers.CopernicusOpenSearchAPICrawler
    INGESTER_CLASS = ingesters.CopernicusODataIngester

    def _create_crawlers(self):
        return [
            self.CRAWLER_CLASS(self.config['url'],
                               search_terms=search,
                               username=self.config.get('username', None),
                               password=os.getenv(self.config.get('password', ''), None))
            for search in self.config['search_terms']
        ]

    def _create_ingester(self):
        return self.INGESTER_CLASS(
            username=self.config.get('username', None),
            password=os.getenv(self.config.get('password', ''), None))
