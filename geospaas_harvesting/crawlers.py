"""A set of crawlers used to explore data provider interfaces and get resources URLs"""
import logging
from html.parser import HTMLParser
import re
import requests


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class Crawler():
    """Base Crawler class"""
    def __init__(self, root_url):
        self.root_url = root_url.rstrip('/')

    def __iter__(self):
        raise NotImplementedError('The __iter__() method was not implemented')

    def __next__(self):
        raise NotImplementedError('The __next__() method was not implemented')


class OpenDAPCrawler(Crawler):
    """Crawler for OpenDAP resources"""

    FOLDERS_SUFFIXES = ('/contents.html')
    FILES_SUFFIXES = ('.nc', '.nc.gz')
    EXCLUDE = ('?')

    def __init__(self, root_url):
        super().__init__(root_url)
        # The _urls attribute contains URLs to the resources which will be returned by the crawler
        self._urls = set()
        # The _to_process attribute contains URLs to pages which need to be searched for resources
        self._to_process = set()

    def __iter__(self):
        """Make the crawler iterable"""
        self._explore_page(self.root_url)
        return self

    def __next__(self):
        """Make the crawler an iterator"""
        try:
            # Return all resource URLs from the previously processed folder
            result = self._urls.pop()
        except KeyError:
            # If no more URLs from the previously processed folder are available,
            # process the next one
            try:
                self._explore_page(self._to_process.pop())
                result = self.__next__()
            except KeyError:
                raise StopIteration
        return result

    def _explore_page(self, folder_url):
        """Gets all relevant links from a page and feeds the _urls and _to_process attributes"""
        LOGGER.info("Looking for resources in '%s'...", folder_url)

        current_location = re.sub(r'/\w+\.\w+$', '', folder_url)
        links = self._get_links(self._get_html_page(folder_url))
        for link in links:
            # Select links which do not contain any of the self.EXCLUDE strings
            if all(map(lambda s, l=link: s not in l, self.EXCLUDE)):
                if link.endswith(self.FOLDERS_SUFFIXES):
                    LOGGER.debug("Adding '%s' to the list of pages to process.", link)
                    self._to_process.add(f"{current_location}/{link}")
                elif link.endswith(self.FILES_SUFFIXES):
                    LOGGER.debug("Adding '%s' to the list of resources.", link)
                    self._urls.add(f"{current_location}/{link}")

    @staticmethod
    def _get_html_page(url):
        """Returns the contents of a Web page as a string"""
        html_page = ''
        LOGGER.debug("Getting page: '%s'", url)
        try:
            response = requests.get(url)
            response.raise_for_status()
            html_page = response.text
        except requests.exceptions.RequestException as exception:
            LOGGER.error('Could not get page due to the following error: %s', str(exception))
        return html_page

    @classmethod
    def _get_links(cls, html):
        """Returns the list of links contained in an HTML page, passed as a string"""

        parser = LinkExtractor()
        LOGGER.debug("Parsing HTML data.")
        parser.feed(html)

        return parser.links


class LinkExtractor(HTMLParser):
    """
    HTML parser which extracts links from an HTML page
    """

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
