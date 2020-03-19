"""Harvesters which use crawlers and ingesters to get data from """

import logging

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
    """Base Harvester class"""

    CRAWLER_CLASS = crawlers.Crawler
    INGESTER_CLASS = ingesters.Ingester

    def __init__(self, **config):
        """Load the configuration for the harvester from the file and the constructor arguments"""

        if self.__class__.__name__ == 'Harvester':
            raise NotImplementedError('Harvester is a base class, it should not be instanciated')

        try:
            root_urls = config.pop('urls')
        except (KeyError, TypeError):
            raise HarvesterConfigurationError(
                "The 'urls' configuration for the PODAACHarvester was found neither in the " +
                "configuration file, nor in the constructor arguments.")

        self._crawlers = [self.CRAWLER_CLASS(url, **config) for url in root_urls]
        self._crawlers_iterator = iter(self._crawlers)
        self._current_crawler = next(self._crawlers_iterator)

        self._ingester = self.INGESTER_CLASS(**config)

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
        if harvesters_list:
            self._current_index = 0
            self._harvesters_list = harvesters_list
        else:
            raise ValueError('Harvesters list is empty')

    def __next__(self):
        try:
            result = self._harvesters_list[self._current_index]
        except IndexError:
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
        if isinstance(index, int):
            return self._harvesters[index]
        else:
            raise TypeError('Index must be an integer')

    def __len__(self):
        return len(self._harvesters)

    def __iter__(self):
        return EndlessHarvesterIterator(self)


class PODAACHarvester(Harvester):
    """Harvester class for PODAAC data (NASA)"""
    CRAWLER_CLASS = crawlers.OpenDAPCrawler
    INGESTER_CLASS = ingesters.DDXIngester

class CopernicusSentinelHarvester(Harvester):
    """Harvester class for Copernicus Sentinel data"""
    CRAWLER_CLASS = crawlers.CopernicusOpenSearchAPICrawler
    INGESTER_CLASS = ingesters.CopernicusODataIngester
