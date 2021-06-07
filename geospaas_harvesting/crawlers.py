"""
A set of crawlers used to explore data provider interfaces and get resources URLs. Each crawler
should inherit from the Crawler class and implement the abstract methods defined in Crawler.
"""
import calendar
import ftplib
import functools
import json
import logging
import os
import os.path
import re
from datetime import datetime, timedelta
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import feedparser
import requests

import geospaas_harvesting.utils as utils

logging.getLogger(__name__).addHandler(logging.NullHandler())


class Crawler():
    """Base Crawler class"""

    LOGGER = logging.getLogger(__name__ + '.Crawler')

    def __iter__(self):
        raise NotImplementedError('The __iter__() method was not implemented')

    def set_initial_state(self):
        """
        This method should set the crawler's attributes which are used for iterating in their
        initial state. Child classes must implement this method so that the crawler can be reused
        """
        raise NotImplementedError('The set_initial_state() method was not implemented')

    @classmethod
    def _http_get(cls, url, request_parameters=None):
        """Returns the contents of a Web page as a string"""
        html_page = ''
        cls.LOGGER.debug("Getting page: '%s'", url)
        try:
            response = utils.http_request('GET', url, **request_parameters or {})
            response.raise_for_status()
            html_page = response.text
        except requests.exceptions.RequestException:
            cls.LOGGER.error('Could not get page', exc_info=True)
        return html_page


class LinkExtractor(HTMLParser):
    """
    HTML parser which extracts links from an HTML page
    """

    LOGGER = logging.getLogger(__name__ + '.LinkExtractor')

    def __init__(self):
        """Constructor with extra attribute definition"""
        super().__init__()
        self._links = []

    def error(self, message):
        """Error behavior"""
        self.LOGGER.error(message)

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


class WebDirectoryCrawler(Crawler):
    """Parent class for crawlers used on repositories which expose a directory-like structure"""
    LOGGER = None
    EXCLUDE = None

    YEAR_PATTERN = r'y?(\d{4})'
    MONTH_PATTERN = r'm?(1[0-2]|0[1-9]|[1-9])'
    DAY_OF_MONTH_PATTERN = r'(3[0-1]|[1-2]\d|0[1-9]|[1-9]| [1-9])'
    DAY_OF_YEAR_PATTERN = r'(36[0-6]|3[0-5]\d|[1-2]\d\d|0[1-9]\d|00[1-9]|[1-9]\d|0[1-9]|[1-9])'

    YEAR_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}(/.*)?$')
    MONTH_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}/{MONTH_PATTERN}(/.*)?$')
    DAY_OF_MONTH_MATCHER = re.compile(
        f'^.*/{YEAR_PATTERN}/{MONTH_PATTERN}/{DAY_OF_MONTH_PATTERN}/.*$')
    DAY_OF_YEAR_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}/{DAY_OF_YEAR_PATTERN}(/.*)?$')

    def __init__(self, root_url, time_range=(None, None), include=None):
        """
        `root_url` is the URL of the data repository to explore.
        `time_range` is a 2-tuple of datetime.datetime objects defining the time range
        of the datasets returned by the crawler.
        `include` is a regular expression string used to filter the crawler's output.
        Only URLs matching it are returned.
        """
        self.root_url = urlparse(root_url)
        self.time_range = time_range
        self.include = re.compile(include) if include else None
        self.set_initial_state()

    @property
    def base_url(self):
        """Get the root URL without the path"""
        return f"{self.root_url.scheme}://{self.root_url.netloc}"

    def set_initial_state(self):
        """
        The `_urls` attribute contains URLs to the resources which
        will be returned by the crawler.
        The `_to_process` attribute contains URLs to pages which
        need to be searched for resources.
        """
        self._urls = []
        self._to_process = [self.root_url.path.rstrip('/')]

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
                self._process_folder(self._to_process.pop())
                result = self.__next__()
            except IndexError:
                raise StopIteration
        return result

    @classmethod
    def _folder_coverage(cls, folder_path):
        """
        Find out if the folder has date info in its path. The resolution is one day.
        For now, it supports the following structures:
          - .../year/...
          - .../year/month/...
          - .../year/month/day/...
          - .../year/day_of_year/...
        It will need to be updated to support new structures.
        """
        match_year = cls.YEAR_MATCHER.search(folder_path)
        if match_year:
            match_month = cls.MONTH_MATCHER.search(folder_path)
            if match_month:
                match_day = cls.DAY_OF_MONTH_MATCHER.search(folder_path)
                if match_day:
                    folder_coverage_start = datetime(
                        int(match_year[1]), int(match_month[2]), int(match_day[3]), 0, 0, 0)
                    folder_coverage_stop = datetime(
                        int(match_year[1]), int(match_month[2]), int(match_day[3]), 23, 59, 59)
                else:
                    last_day_of_month = calendar.monthrange(
                        int(match_year[1]), int(match_month[2]))[1]
                    folder_coverage_start = datetime(
                        int(match_year[1]), int(match_month[2]), 1, 0, 0, 0)
                    folder_coverage_stop = datetime(
                        int(match_year[1]), int(match_month[2]), last_day_of_month, 23, 59, 59)
            else:
                match_day_of_year = cls.DAY_OF_YEAR_MATCHER.search(folder_path)
                if match_day_of_year:
                    offset = timedelta(int(match_day_of_year[2]) - 1)
                    folder_coverage_start = (datetime(int(match_year[1]), 1, 1, 0, 0, 0)
                                             + offset)
                    folder_coverage_stop = (datetime(int(match_year[1]), 1, 1, 23, 59, 59)
                                            + offset)
                else:
                    folder_coverage_start = datetime(int(match_year[1]), 1, 1, 0, 0, 0)
                    folder_coverage_stop = datetime(int(match_year[1]), 12, 31, 23, 59, 59)
        else:
            folder_coverage_start = folder_coverage_stop = None

        return (folder_coverage_start, folder_coverage_stop)

    def _intersects_time_range(self, start_time=None, stop_time=None):
        """
        Return True if either of these conditions is met:
          - a time coverage was extracted from the folder's path or a timestamp from the dataset's
            name, and this time coverage intersects with the Crawler's time range
          - no time range was defined when instantiating the crawler
          - no time coverage was extracted from the folder's url or dataset's name
        """
        return ((not start_time or not self.time_range[1] or start_time <= self.time_range[1]) and
                (not stop_time or not self.time_range[0] or stop_time >= self.time_range[0]))

    def _list_folder_contents(self, folder_path):
        """Lists the contents of a folder. Should return absolute paths"""
        raise NotImplementedError("_list_folder_contents is abstract in WebDirectoryCrawler")

    def _is_folder(self, path):
        """Returns True if path points to a folder"""
        raise NotImplementedError("_is_folder is abstract in WebDirectoryCrawler")

    def _add_url_to_return(self, path):
        """
        Add a URL to the list of URLs returned by the crawler after
        checking that it fits inside the crawler's time range.
        """
        resource_url = urljoin(self.base_url, path)
        download_url = self.get_download_url(resource_url)
        if download_url is not None:
            if download_url not in self._urls:
                self.LOGGER.debug("Adding '%s' to the list of resources.", download_url)
                self._urls.append(download_url)

    def _add_folder_to_process(self, path):
        """Add a folder to the list of folder which will be explored later"""
        if self._intersects_time_range(*self._folder_coverage(path)):
            if path not in self._to_process:
                self.LOGGER.debug("Adding '%s' to the list of pages to process.", path)
                self._to_process.append(path)

    def _process_folder(self, folder_path):
        """
        Get the contents of a folder and feed the _urls (based on includes) and _to_process
        attributes
        """
        self.LOGGER.info("Looking for resources in '%s'...", folder_path)
        for path in self._list_folder_contents(folder_path):
            # deselect paths which contains any of the excludes strings
            if self.EXCLUDE and self.EXCLUDE.search(path):
                continue
            if self._is_folder(path):
                self._add_folder_to_process(path)
            # select paths which are matched based on input config file
            if self.include and self.include.search(path):
                self._add_url_to_return(path)

    def get_download_url(self, resource_url):
        """
        Get a download link from a resource URL, in case the URL found
        by the crawler is not a direct download link.
        By default, it just returns the ressource URL and can be
        overridden in the child classes if necessary.
        """
        return resource_url


class LocalDirectoryCrawler(WebDirectoryCrawler):
    """Crawl through the contents of a local folder"""

    LOGGER = logging.getLogger(__name__ + '.LocalDirectoryCrawler')

    def _list_folder_contents(self, folder_path):
        return [os.path.join(folder_path, file_path) for file_path in os.listdir(folder_path)]

    def _is_folder(self, path):
        return os.path.isdir(path)


class HTMLDirectoryCrawler(WebDirectoryCrawler):
    """Implementation of WebDirectoryCrawler for repositories exposed as HTML pages."""

    FOLDERS_SUFFIXES = None

    @staticmethod
    def _strip_folder_page(folder_path):
        """
        Remove the index page of a folder path.
        For example: /foo/bar/contents.html becomes /foo/bar.
        """
        return re.sub(r'/\w+\.html?$', r'', folder_path)

    def _is_folder(self, path):
        return path.endswith(self.FOLDERS_SUFFIXES)

    @classmethod
    def _get_links(cls, html):
        """Returns the list of links contained in an HTML page, passed as a string"""
        parser = LinkExtractor()
        cls.LOGGER.debug("Parsing HTML data.")
        parser.feed(html)
        return parser.links

    @staticmethod
    def _prepend_parent_path(parent_path, paths):
        """
        Prepend the parent_path to each path contained in paths,
        except if the path already starts with the parent_path.
        """
        result = []
        if not parent_path.endswith('/'):
            parent_path += '/'
        for path in paths:
            if path.startswith(parent_path):
                result.append(path)
            else:
                result.append(urljoin(parent_path, path))
        return result

    def _list_folder_contents(self, folder_path):
        html = self._http_get(f"{self.base_url}{folder_path}")
        stripped_folder_path = self._strip_folder_page(folder_path)
        return self._prepend_parent_path(stripped_folder_path, self._get_links(html))


class OpenDAPCrawler(HTMLDirectoryCrawler):
    """
    Crawler for harvesting the data of OpenDAP
    """
    LOGGER = logging.getLogger(__name__ + '.OpenDAPCrawler')
    FOLDERS_SUFFIXES = ('/contents.html',)
    EXCLUDE = re.compile(r'\?')


class ThreddsCrawler(HTMLDirectoryCrawler):
    """
    Crawler for harvesting the data which are provided by Thredds
    """
    LOGGER = logging.getLogger(__name__ + '.ThreddsCrawler')
    FOLDERS_SUFFIXES = ('/catalog.html',)
    FILES_SUFFIXES = ('.nc',)
    EXCLUDE = re.compile(r'/thredds/catalog.html$')

    def get_download_url(self, resource_url):
        result = None
        links = self._get_links(self._http_get(resource_url))
        for link in links:
            if "fileServer" in link and link.endswith(self.FILES_SUFFIXES):
                result = f"{self.base_url}{link}"
                break
        return result


class FTPCrawler(WebDirectoryCrawler):
    """
    Crawler which returns the search results of an FTP, given the URL and search
    terms
    """
    LOGGER = logging.getLogger(__name__ + '.FTPCrawler')

    def __init__(self, root_url, time_range=(None, None), include=None,
                 username='anonymous', password='anonymous'):

        if not root_url.startswith('ftp://'):
            raise ValueError("The root url must start with 'ftp://'")

        self.username = username
        self.password = password
        self.ftp = None

        super().__init__(root_url, time_range, include)

    def set_initial_state(self):
        """
        The `_urls` attribute contains URLs to the resources which will be returned by the crawler.
        The `_to_process` attribute contains URLs to pages which need to be searched for resources.
        """
        self._urls = []
        self._to_process = [self.root_url.path or '/']
        self.connect()

    def connect(self):
        """Creates an FTP connection and logs in"""
        self.ftp = ftplib.FTP(self.root_url.netloc, user=self.username, passwd=self.password)
        try:
            self.ftp.login(self.username, self.password)
        except ftplib.error_perm as err_content:
            # these errors happen when we try to log in twice, so they can be ignored
            if not (err_content.args[0].startswith('503') or err_content.args[0].startswith('230')):
                raise

    class Decorators():
        """Decorators for the FTPCrawler"""
        @staticmethod
        def retry_on_timeout(tries=2):
            """Wrapper around the retry decorator which
            enables to pass the number of tries"""
            def decorator_retry(method):
                """Decorator which re-creates the FTP connection
                if a timeout error occurs"""
                @functools.wraps(method)
                def wrapper_reconnect(crawler_instance, *args, **kwargs):
                    """Try to execute the decorated method.
                    If a FTP 421 error or a ConnectionError
                    (a network issue) occurs, re-create the connection
                    """
                    countdown = tries
                    last_error = None
                    while countdown > 0:
                        try:
                            return method(crawler_instance, *args, **kwargs)
                        except (ftplib.error_temp, ConnectionError) as error:
                            last_error = error
                            if isinstance(error, ftplib.error_temp) and '421' not in error.args[0]:
                                raise
                            else:
                                crawler_instance.LOGGER.info("Re-initializing the FTP connection")
                                crawler_instance.connect()
                            countdown -= 1
                    if last_error:
                        raise last_error
                return wrapper_reconnect
            return decorator_retry

    @Decorators.retry_on_timeout(tries=5)
    def _list_folder_contents(self, folder_path):
        return self.ftp.nlst(folder_path)

    @Decorators.retry_on_timeout(tries=5)
    def _is_folder(self, path):
        """Determine if path is a folder by trying to change the working directory to path."""
        try:
            self.ftp.cwd(path)
        except ftplib.error_perm:
            return False
        else:
            return True


class HTTPPaginatedAPICrawler(Crawler):
    """Base class for crawlers used on repositories exposing a paginated API over HTTP"""

    PAGE_OFFSET_NAME = ''
    PAGE_SIZE_NAME = ''
    MIN_OFFSET = 0

    def __init__(self, url, search_terms=None, time_range=(None, None),
                 username=None, password=None,
                 page_size=100, initial_offset=None):
        self.url = url
        self._results = None
        self.initial_offset = initial_offset or self.MIN_OFFSET
        self.request_parameters = self._build_request_parameters(
            search_terms, time_range, username, password, page_size)
        self.set_initial_state()

    def set_initial_state(self):
        self.page_offset = self.initial_offset
        self._results = []

    @property
    def page_size(self):
        """Getter for the page size"""
        return self.request_parameters['params'][self.PAGE_SIZE_NAME]

    @property
    def page_offset(self):
        """Getter for the page offset"""
        return self.request_parameters['params'][self.PAGE_OFFSET_NAME]

    @page_offset.setter
    def page_offset(self, offset):
        """Setter for the page offset"""
        self.request_parameters['params'][self.PAGE_OFFSET_NAME] = offset

    def increment_offset(self):
        self.page_offset += 1

    def _build_request_parameters(self, search_terms=None, time_range=(None, None),
                                  username=None, password=None, page_size=100):
        """Build a dict containing the parameters used to query the API.
        This dict will be unpacked to provide the arguments to `requests.get()`.
        """
        return {
            'params': {
                self.PAGE_OFFSET_NAME: self.initial_offset,
                self.PAGE_SIZE_NAME: page_size,
            }
        }

    def __iter__(self):
        """Makes the crawler iterable"""
        return self

    def __next__(self):
        """Makes the crawler an iterator"""
        try:
            # Return all resource URLs from the previously processed page
            result = self._results.pop()
        except IndexError:
            # If no more URLs from the previously processed page are available, process the next one
            if not self._get_datasets_info(self._get_next_page()):
                self.LOGGER.debug("No more entries found at '%s' matching '%s'",
                                  self.url, self.request_parameters['params'])
                raise StopIteration
            result = self.__next__()
        return result

    def _get_next_page(self):
        """Get the next page of search results"""
        self.LOGGER.info("Looking for ressources at '%s', matching '%s'",
                         self.url, self.request_parameters['params'])
        current_page = self._http_get(self.url, self.request_parameters)
        self.increment_offset()
        return current_page

    def _get_datasets_info(self, page):
        """Get dataset information from the current page and add it
        to self._results. It can be a download URL or a dictionary
        of dataset metadata.
        Returns True if information was found, False otherwise"""
        raise NotImplementedError()


class CopernicusOpenSearchAPICrawler(HTTPPaginatedAPICrawler):
    """
    Crawler which returns the search results of an Opensearch API,
    given the URL and search terms.
    """
    LOGGER = logging.getLogger(__name__ + '.CopernicusOpenSearchAPICrawler')
    MIN_DATETIME = datetime(1000, 1, 1)

    PAGE_OFFSET_NAME = 'start'
    PAGE_SIZE_NAME = 'rows'
    MIN_OFFSET = 0

    def increment_offset(self):
        self.page_offset += self.page_size

    def _build_request_parameters(self, search_terms=None, time_range=(None, None),
                                  username=None, password=None, page_size=100):
        """Build a dict containing the parameters used to query the Copernicus API.
        Results are sorted ascending, which avoids missing some
        if products are added while the harvesting is happening
        (it will generally be the case)
        """
        request_parameters = super()._build_request_parameters(
            search_terms, time_range, username, password, page_size)

        if search_terms:
            request_parameters['params']['q'] = f"({search_terms})"

        request_parameters['params']['orderby'] = 'ingestiondate asc'

        if time_range[0] or time_range[1]:
            api_date_format = '%Y-%m-%dT%H:%M:%SZ'
            start = (time_range[0] or self.MIN_DATETIME).strftime(api_date_format)
            end = time_range[1].strftime(api_date_format) if time_range[1] else 'NOW'
            time_condition = f"beginposition:[{start} TO {end}]"
            request_parameters['params']['q'] += f" AND ({time_condition})"

        if username and password:
            request_parameters['auth'] = (username, password)

        return request_parameters

    def _get_datasets_info(self, page):
        """Get links from the current page and adds them to self._results.
        Returns True if links were found, False otherwise"""
        entries = feedparser.parse(page)['entries']

        for entry in entries:
            self.LOGGER.debug("Adding '%s' to the list of resources.", entry['link'])
            self._results.append(entry['link'])

        return bool(entries)


class CreodiasEOFinderCrawler(HTTPPaginatedAPICrawler):
    """Crawler for the Creodias EO finder API"""

    LOGGER = logging.getLogger(__name__ + '.CreodiasEOFinderCrawler')

    PAGE_OFFSET_NAME = 'page'
    PAGE_SIZE_NAME = 'maxRecords'
    MIN_OFFSET = 1

    def _build_request_parameters(self, search_terms=None, time_range=(None, None),
                                  username=None, password=None, page_size=100):
        """Build a dict containing the parameters used to query
        the Creodias EO finder API.
        search_terms should be a dictionary containing the search
        parameters and their values.
        Results are sorted ascending, which avoids missing some
        if products are added while the harvesting is happening
        (it will generally be the case)
        """
        request_parameters = super()._build_request_parameters(
            search_terms, time_range, username, password, page_size)

        if search_terms:
            request_parameters['params'].update(**search_terms)

        request_parameters['params']['sortParam'] = 'published'
        request_parameters['params']['sortOrder'] = 'ascending'

        api_date_format = '%Y-%m-%dT%H:%M:%SZ'
        if time_range[0]:
            request_parameters['params']['startDate'] = time_range[0].strftime(api_date_format)
        if time_range[1]:
            request_parameters['params']['completionDate'] = time_range[1].strftime(api_date_format)

        return request_parameters

    def _get_datasets_info(self, page):
        """Get dataset attributes from the current page and
        adds them to self._results.
        Returns True if attributes were found, False otherwise"""
        entries = json.loads(page)['features']

        for entry in entries:
            url = entry['properties']
            url['geometry'] = json.dumps(entry['geometry'])
            self.LOGGER.debug("Adding '%s' to the list of resources.",
                              url['services']['download']['url'])
            self._results.append(url)

        return bool(entries)
