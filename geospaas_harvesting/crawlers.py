"""A set of crawlers used to explore data provider interfaces and get resources URLs"""

import logging
import re
from html.parser import HTMLParser

import feedparser
import requests

logging.getLogger(__name__).addHandler(logging.NullHandler())


class Crawler():
    """Base Crawler class"""

    LOGGER = logging.getLogger(__name__ + '.Crawler')

    def __iter__(self):
        raise NotImplementedError('The __iter__() method was not implemented')

    @classmethod
    def _http_get(cls, url, request_parameters=None):
        """Returns the contents of a Web page as a string"""
        html_page = ''
        cls.LOGGER.debug("Getting page: '%s'", url)
        try:
            response = requests.get(url, **request_parameters or {})
            response.raise_for_status()
            html_page = response.text
        except requests.exceptions.RequestException:
            cls.LOGGER.error('Could not get page', exc_info=True)
        return html_page


class OpenDAPCrawler(Crawler):
    """Crawler for OpenDAP resources"""

    LOGGER = logging.getLogger(__name__ + '.OpenDAPCrawler')
    FOLDERS_SUFFIXES = ('/contents.html')
    FILES_SUFFIXES = ('.nc', '.nc.gz')
    EXCLUDE = ('?')

    def __init__(self, root_url):
        """
        The _urls attribute contains URLs to the resources which will be returned by the crawler
        The _to_process attribute contains URLs to pages which need to be searched for resources
        """
        self._urls = []
        self._to_process = [root_url.rstrip('/')]

    def __iter__(self):
        """Make the crawler iterable"""
        return self

    def __next__(self):
        """Make the crawler an iterator"""
        try:
            # Return all resource URLs from the previously processed folder
            result = self._urls.pop()
        except IndexError:
            # If no more URLs from the previously processed folder are available,
            # process the next one
            try:
                self._explore_page(self._to_process.pop())
                result = self.__next__()
            except IndexError:
                raise StopIteration
        return result

    def _explore_page(self, folder_url):
        """Gets all relevant links from a page and feeds the _urls and _to_process attributes"""
        self.LOGGER.info("Looking for resources in '%s'...", folder_url)

        current_location = re.sub(r'/\w+\.\w+$', '', folder_url)
        links = self._get_links(self._http_get(folder_url))
        for link in links:
            # Select links which do not contain any of the self.EXCLUDE strings
            if all(map(lambda s, l=link: s not in l, self.EXCLUDE)):
                if link.endswith(self.FOLDERS_SUFFIXES):
                    self.LOGGER.debug("Adding '%s' to the list of pages to process.", link)
                    self._to_process.append(f"{current_location}/{link}")
                elif link.endswith(self.FILES_SUFFIXES):
                    self.LOGGER.debug("Adding '%s' to the list of resources.", link)
                    self._urls.append(f"{current_location}/{link}")

    @classmethod
    def _get_links(cls, html):
        """Returns the list of links contained in an HTML page, passed as a string"""

        parser = LinkExtractor()
        cls.LOGGER.debug("Parsing HTML data.")
        parser.feed(html)

        return parser.links


class LinkExtractor(HTMLParser):
    """
    HTML parser which extracts links from an HTML page
    """

    # TODO: remove useless convert_charrefs which already defaults to True in the parent constructor
    def __init__(self, convert_charrefs=True):
        """Constructor with extra attribute definition"""
        super().__init__(convert_charrefs=convert_charrefs)
        self._links = []

    def error(self, message):
        """Error behavior"""
        print(message)

    def feed(self, data):
        """Reset links lists when new data is fed to the parser"""
        self._links = []
        super().feed(data)

    @property
    def links(self):
        """Getter for the links attribute"""
        return self._links

    def handle_starttag(self, tag, attrs):
        """Extracts links from the HTML data"""
        if tag == 'a':
            for attr in attrs:
                if attr[0] == 'href':
                    self._links.append(attr[1])


class CopernicusOpenSearchAPICrawler(Crawler):
    """
    Crawler which returns the search results of an Opensearch API, given the URL and search
    terms
    """

    LOGGER = logging.getLogger(__name__ + '.CopernicusOpenSearchAPICrawler')

    def __init__(self, url, search_terms='*', username=None, password=None,
                 page_size=100, offset=0):
        self.url = url
        self.search_terms = search_terms
        self._credentials = (username, password) if username and password else None
        self.page_size = page_size
        self.offset = offset
        self._urls = []

    def __iter__(self):
        """Makes the crawler iterable"""
        return self

    def __next__(self):
        """Makes the crawler an iterator"""
        try:
            # Return all resource URLs from the previously processed page
            result = self._urls.pop()
        except IndexError:
            # If no more URLs from the previously processed page are available, process the next one
            if not self._get_resources_urls(self._get_next_page()):
                self.LOGGER.debug("No more entries found at '%s' matching '%s'",
                                  self.url, self.search_terms)
                raise StopIteration
            try:
                result = self.__next__()
            except IndexError:
                raise StopIteration
        return result

    def _get_next_page(self):
        """
        Get the next page of search results. Results are sorted ascending, which avoids missing some
        if products are added while the harvesting is happening (it will generally be the case)
        """
        self.LOGGER.info("Looking for ressources at '%s', matching '%s' with an offset of %s",
                         self.url, self.search_terms, self.offset)

        request_parameters = {
            'params': {
                'q': self.search_terms,
                'start': self.offset,
                'rows': self.page_size,
                'orderby': 'ingestiondate asc'
            }
        }
        if self._credentials:
            request_parameters['auth'] = self._credentials

        current_page = self._http_get(self.url, request_parameters)
        self.offset += self.page_size

        return current_page

    def _get_resources_urls(self, xml):
        """Get links from the current page. Returns True if links were found, False otherwise"""

        entries = feedparser.parse(xml)['entries']

        for entry in entries:
            self.LOGGER.debug("Adding '%s' to the list of resources.", entry['link'])
            self._urls.append(entry['link'])

        return bool(entries)
