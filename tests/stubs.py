import logging

import geospaas_harvesting.crawlers as crawlers
import geospaas_harvesting.ingesters as ingesters
import geospaas_harvesting.harvesters as harvesters

class StubCrawler(crawlers.Crawler):
    """Stub crawler class which iterates over a defined set of URLs"""

    TEST_DATA = {
        'https://random1.url': ['ressource_1', 'ressource_2', 'ressource_3'],
        'https://random2.url': ['ressource_a', 'ressource_b', 'ressource_c']
    }

    def __init__(self, root_url):
        """Build a list of URLs which will be returned by the iterator"""
        self.root_url = root_url
        self.data = []
        try:
            for uri in self.TEST_DATA[root_url]:
                self.data.append(f'{root_url}/{uri}')
        except KeyError:
            pass

        self.current_index = 0

    def set_initial_state(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        try:
            result = self.data[self.current_index]
        except IndexError as error:
            raise StopIteration from error
        else:
            self.current_index += 1
        return result


class StubIngester(ingesters.Ingester):
    """Stub ingester class """
    def __init__(self):
        super().__init__()
        self.ingested_urls = []

    def ingest(self, urls):
        """Appends the URLs in the 'urls' iterable to the list of ingested URLs"""
        for url in urls:
            logging.getLogger(ingesters.__name__).info(url)
            self.ingested_urls.append(url)


class StubInterruptIngester(ingesters.Ingester):
    """Stub ingester class"""
    def __init__(self):
        super().__init__()
        self.countdown = 1

    def ingest(self, urls):
        """
        Simulates the following behavior:
            - ingests one URL
            - raises a KeyboardInterrupt exception
            - resumes ingesting URLs after deserialization
        """
        for url in urls:
            # The ingestion works if the countdown is negative, so that deserialization is testable
            if self.countdown > 0 or self.countdown < 0:
                logging.getLogger(ingesters.__name__).info(url)
                self.countdown -= 1
            else:
                self.countdown -= 1
                raise KeyboardInterrupt


class StubExceptionIngester(ingesters.Ingester):
    """Stub ingester class which raises an exception"""

    def ingest(self, urls):
        raise IndexError


class StubHarvester(harvesters.Harvester):
    """Stub harvester class using the previously defined mock crawler and ingester"""
    def _create_crawlers(self):
        return [StubCrawler(url) for url in self.config['urls']]

    def _create_ingester(self):
        return StubIngester()


class StubInterruptHarvester(harvesters.Harvester):
    """Stub harvester class using the previously defined mock crawler and ingester"""
    def _create_crawlers(self):
        return [StubCrawler(url) for url in self.config['urls']]

    def _create_ingester(self):
        return StubInterruptIngester()


class StubExceptionHarvester(harvesters.Harvester):
    """Stub harvester class using the previously defined mock crawler and ingester"""
    def _create_crawlers(self):
        return [StubCrawler(url) for url in self.config['urls']]

    def _create_ingester(self):
        return StubExceptionIngester()


class StubInterruptHarvesterList(harvesters.HarvesterList):
    """HarvesterList class for testing interruptions"""

    def __init__(self, *args):
        self._harvesters = [StubInterruptHarvester(urls=['https://random1.url']),
                            StubHarvester(urls=['https://random2.url'])]
    def __iter__(self):
        return iter(self._harvesters)


class StubExceptionHarvesterList(harvesters.HarvesterList):
    """HarvesterList class for testing exceptions"""
    def __init__(self, *args):
        self._harvesters = [StubExceptionHarvester(urls=['https://random1.url'])]
