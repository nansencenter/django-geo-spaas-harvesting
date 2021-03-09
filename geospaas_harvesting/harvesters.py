"""Harvesters which use crawlers and ingesters to get data from providers' websites"""

import logging

import dateutil.parser

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

    def get_time_range(self):
        """
        Build a couple representing the time coverage of the harvester based on its configuration
        Time zones are ignored because we generally don't get the information. The time range must
        be defined according to the format of the dates in the remote repository.
        """
        time_range = (None, None)
        try:
            if len(self.config['time_range']) == 2:
                time_range = tuple(dateutil.parser.parse(date, ignoretz=True) if date else None
                                   for date in self.config['time_range'])
            else:
                raise ValueError("time_range must have two elements")
        except dateutil.parser.ParserError as error:
            raise ValueError("dateutil can't parse the dates in time_range") from error
        except (KeyError, TypeError):
            pass

        if time_range and all(time_range) and time_range[0] > time_range[1]:
            raise ValueError("The first value of the time range must be inferior to the second")

        return time_range

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


class WebDirectoryHarvester(Harvester):
    """
    class for harvesting online data sources that rely on webpages (and most of the time on opendap)

    Child classes should also assign values to the ingester and crawler class attributes.
    These values should be the crawler and ingester classes to be used in the _create_crawlers()
    and _create_ingester() methods.
    """
    ingester = None
    crawler = None

    def __init__(self, **config):
        super().__init__(**config)
        if 'include' in config:
            if not isinstance(config['include'], str):
                raise HarvesterConfigurationError(
                    "The 'include' field must be fed with a regex matching URLs to include")

    def _create_crawlers(self):
        if self.crawler is None:
            raise HarvesterConfigurationError(
                "The class of crawler has not been specified properly")
        try:
            return [
                self.crawler(url, time_range=(self.get_time_range()),
                             include=self.config.get('include', None))
                for url in self.config['urls']
            ]
        except TypeError as error:
            raise HarvesterConfigurationError(
                "crawler must be created properly with correct configuration file") from error

    def _create_ingester(self):
        parameters = {}
        if self.ingester is None:
            raise HarvesterConfigurationError(
                "The class of ingester has not been specified properly")
        try:
            for parameter_name in ['max_fetcher_threads', 'max_db_threads']:
                if parameter_name in self.config:
                    parameters[parameter_name] = self.config[parameter_name]
            return self.ingester(**parameters)
        except TypeError as error:
            raise HarvesterConfigurationError(
                "ingester must be created properly with correct configuration file") from error


class PODAACHarvester(WebDirectoryHarvester):
    """Harvester class for PODAAC data (NASA)"""
    ingester = ingesters.DDXIngester
    crawler = crawlers.OpenDAPCrawler


class OSISAFHarvester(WebDirectoryHarvester):
    """Harvester class for OSISAF project"""
    ingester = ingesters.ThreddsIngester
    crawler = crawlers.ThreddsCrawler


class FTPHarvester(WebDirectoryHarvester):
    """Harvester class for some specific FTP protecol"""
    ingester = ingesters.URLNameIngester
    def _create_crawlers(self):
        return [
            crawlers.FTPCrawler(
                root_url=url,
                username=self.config.get('username', None),
                password=self.config.get('password'),
                time_range=(self.get_time_range()),
                include=self.config.get('include', None)
            )
            for url in self.config['urls']
        ]


class CopernicusSentinelHarvester(Harvester):
    """Harvester class for Copernicus Sentinel data"""

    def _create_crawlers(self):
        return [
            crawlers.CopernicusOpenSearchAPICrawler(
                url=self.config['url'],
                search_terms=search,
                username=self.config.get('username', None),
                password=self.config.get('password'),
                time_range=(self.get_time_range()))
            for search in self.config['search_terms']
        ]

    def _create_ingester(self):
        parameters = {}
        for parameter_name in ['username', 'max_fetcher_threads', 'max_db_threads']:
            if parameter_name in self.config:
                parameters[parameter_name] = self.config[parameter_name]
        if 'password' in self.config:
            parameters['password'] = self.config.get('password')
        return ingesters.CopernicusODataIngester(**parameters)


class CreodiasEOFinderHarvester(Harvester):
    """Harvester class for Creodias data"""

    def _create_crawlers(self):
        return [
            crawlers.CreodiasEOFinderCrawler(
                url=self.config['url'],
                search_terms=search,
                time_range=self.get_time_range())
            for search in self.config['search_terms']
        ]

    def _create_ingester(self):
        parameters = {}
        for parameter_name in ['max_fetcher_threads', 'max_db_threads']:
            if parameter_name in self.config:
                parameters[parameter_name] = self.config[parameter_name]
        return ingesters.CreodiasEOFinderIngester(**parameters)


class LOCALHarvester(WebDirectoryHarvester):
    """ Harvester class for some specific local files """
    def _create_crawlers(self):
        return [
            crawlers.LocalDirectoryCrawler(
                url,
                include = self.config.get('include', None),
                time_range = self.get_time_range()
                )
            for url in self.config['paths']
        ]
    ingester = ingesters.NansatIngester


class OneDimensionNetCDFLocalHarvester(LOCALHarvester):
    """Harvester class for one-dimensional NetCDF file hosted locally"""
    ingester = ingesters.OneDimensionNetCDFIngester
