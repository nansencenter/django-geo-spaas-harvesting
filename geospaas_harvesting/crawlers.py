"""
A set of crawlers used to explore data provider interfaces and get resources URLs. Each crawler
should inherit from the Crawler class and implement the abstract methods defined in Crawler.
"""
import calendar
import ftplib
import logging
import re
from datetime import datetime, timedelta
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import feedparser
import requests

logging.getLogger(__name__).addHandler(logging.NullHandler())

MIN_DATETIME = datetime(1, 1, 1)


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
            response = requests.get(url, **request_parameters or {})
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
    """
    Parent class for crawlers used on repositories which expose a directory-like structure
    in the form of HTML pages
    """
    LOGGER = None
    EXCLUDE = None

    YEAR_PATTERN = r'(\d{4})'
    MONTH_PATTERN = r'(1[0-2]|0[1-9]|[1-9])'
    DAY_OF_MONTH_PATTERN = r'(3[0-1]|[1-2]\d|0[1-9]|[1-9]| [1-9])'
    DAY_OF_YEAR_PATTERN = r'(36[0-6]|3[0-5]\d|[1-2]\d\d|0[1-9]\d|00[1-9]|[1-9]\d|0[1-9]|[1-9])'

    YEAR_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}(/.*)?$')
    MONTH_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}/{MONTH_PATTERN}(/.*)?$')
    DAY_OF_MONTH_MATCHER = re.compile(
        f'^.*/{YEAR_PATTERN}/{MONTH_PATTERN}/{DAY_OF_MONTH_PATTERN}/.*$')
    DAY_OF_YEAR_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}/{DAY_OF_YEAR_PATTERN}(/.*)?$')

    TIMESTAMP_MATCHER = re.compile(
        r'(?P<date>(\d{4})(1[0-2]|0[1-9]|[1-9])(3[0-1]|[1-2]\d|0[1-9]|[1-9]| [1-9]))_?'
        r'(?P<time>(2[0-3]|[0-1]\d|\d)([0-5]\d|\d)(6[0-1]|[0-5]\d|\d))')

    def __init__(self, root_url, time_range=(None, None), excludes=None):
        """
        `root_url` is the URL of the data repository to explore.
        `time_range` is a tuple of datetime.datetime objects defining the time range of the datasets
        returned the crawler.
        `excludes` is the list of string that are the associated url is ignored during
        the harvesting process if these strings are found in the crawled url.
        """
        self.root_url = urlparse(root_url)
        self.time_range = time_range
        self.excludes = (self.EXCLUDE or []) + (excludes or [])
        self.set_initial_state()

    @property
    def base_url(self):
        """Get the root URL without the path"""
        return f"{self.root_url.scheme}://{self.root_url.netloc}"

    def set_initial_state(self):
        """
        The `_urls` attribute contains URLs to the resources which will be returned by the crawler.
        The `_to_process` attribute contains URLs to pages which need to be searched for resources.
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

    @classmethod
    def _dataset_timestamp(cls, dataset_name):
        """Tries to find a timestamp in the dataset's name"""
        timestamp_match = cls.TIMESTAMP_MATCHER.search(dataset_name)
        if timestamp_match:
            return datetime.strptime(
                timestamp_match['date'] + timestamp_match['time'],
                '%Y%m%d%H%M%S'
            )
        else:
            return None

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
        """"""
        raise NotImplementedError("_list_folder_contents is abstract in WebDirectoryCrawler")

    def _is_folder(self, path):
        """"""
        raise NotImplementedError("_is_folder is abstract in WebDirectoryCrawler")

    def _is_file(self, path):
        """"""
        raise NotImplementedError("_is_file is abstract in WebDirectoryCrawler")

    def _add_url_to_return(self, path):
        """"""
        if self._intersects_time_range(*(self._dataset_timestamp(path),) * 2):
            resource_url = urljoin(self.base_url, path)
            download_url = self.get_download_url(resource_url)
            if download_url is not None:
                if download_url not in self._urls:
                    self.LOGGER.debug("Adding '%s' to the list of resources.", download_url)
                    self._urls.append(download_url)

    def _add_folder_to_process(self, path):
        """"""
        if self._intersects_time_range(*self._folder_coverage(path)):
            if path not in self._to_process:
                self.LOGGER.debug("Adding '%s' to the list of pages to process.", path)
                self._to_process.append(path)

    def _process_folder(self, folder_path):
        """Get all relevant links from a page and feeds the _urls and _to_process attributes"""
        self.LOGGER.info("Looking for resources in '%s'...", folder_path)
        for path in self._list_folder_contents(folder_path):
            # Select paths which do not contain any of the self.excludes strings
            if all(map(lambda s, p=path: s not in p, self.excludes)):
                if self._is_folder(path):
                    self._add_folder_to_process(path)
                elif self._is_file(path):
                    self._add_url_to_return(path)

    def get_download_url(self, resource_url):
        """
        This method should return the downloadable form of the crawled link. It means providing a
        direct download link.

        The philosophy of this method is to turn the link inside the "explore_pages" method into
        the link that is downloadable by the geospaas user.

        This method is only used in the "_explore_page" method.
        Thus, if any class defined its "_explore_page" method in a way that there is no need to
        modify the link (i.e. both downloadable link and metadata provider link are the identical),
        then there is no need to define this method.
        """
        return resource_url


class HTMLDirectoryCrawler(WebDirectoryCrawler):
    """"""

    FOLDERS_SUFFIXES = None
    FILES_SUFFIXES = None

    @staticmethod
    def _strip_folder_page(folder_path):
        """"""
        return re.sub(r'/\w+\.html?$', r'', folder_path)

    def _is_folder(self, path):
        return path.endswith(self.FOLDERS_SUFFIXES)

    def _is_file(self, path):
        return path.endswith(self.FILES_SUFFIXES)

    @classmethod
    def _get_links(cls, html):
        """Returns the list of links contained in an HTML page, passed as a string"""
        parser = LinkExtractor()
        cls.LOGGER.debug("Parsing HTML data.")
        parser.feed(html)
        return parser.links

    @staticmethod
    def _prepend_parent_path(parent_path, paths):
        """"""
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
        """"""
        html = self._http_get(f"{self.base_url}{folder_path}")
        stripped_folder_path = self._strip_folder_page(folder_path)
        return self._prepend_parent_path(stripped_folder_path, self._get_links(html))


class OpenDAPCrawler(HTMLDirectoryCrawler):
    """
    Crawler for harvesting the data of OpenDAP
    """
    LOGGER = logging.getLogger(__name__ + '.OpenDAPCrawler')
    FOLDERS_SUFFIXES = ('/contents.html',)
    FILES_SUFFIXES = ('.nc', '.nc.gz')
    EXCLUDE = ['?']


class ThreddsCrawler(HTMLDirectoryCrawler):
    """
    Crawler for harvesting the data which are provided by Thredds
    """
    LOGGER = logging.getLogger(__name__ + '.ThreddsCrawler')
    FOLDERS_SUFFIXES = ('/catalog.html',)
    FILES_SUFFIXES = ('.nc',)
    EXCLUDE = ['/thredds/catalog.html']

    def get_download_url(self, resource_url):
        result = None
        links = self._get_links(self._http_get(resource_url))
        for link in links:
            if "fileServer" in link and link.endswith(self.FILES_SUFFIXES):
                result = f"{self.base_url}{link}"
                break
        return result


class CopernicusOpenSearchAPICrawler(Crawler):
    """
    Crawler which returns the search results of an Opensearch API, given the URL and search
    terms
    """
    LOGGER = logging.getLogger(__name__ + '.CopernicusOpenSearchAPICrawler')

    def __init__(self, url, search_terms='*', time_range=(None, None),
                 username=None, password=None,
                 page_size=100, initial_offset=0):
        self.url = url
        self.initial_offset = initial_offset
        self.request_parameters = self._build_request_parameters(
            search_terms, time_range, username, password, page_size, initial_offset)
        self.set_initial_state()

    def set_initial_state(self):
        self.request_parameters['params']['start'] = self.initial_offset
        self._urls = []

    @staticmethod
    def _build_request_parameters(search_terms, time_range, username, password, page_size,
                                  initial_offset):
        """Build a dict containing the parameters used to query the Copernicus API"""
        if time_range:
            api_date_format = '%Y-%m-%dT%H:%M:%SZ'
            start = (time_range[0] or MIN_DATETIME).strftime(api_date_format)
            end = time_range[1].strftime(api_date_format) if time_range[1] else 'NOW'
            time_condition = f"beginposition:[{start} TO {end}]"

        request_parameters = {
            'params': {
                'q': f"({search_terms}) AND ({time_condition})",
                'start': initial_offset,
                'rows': page_size,
                'orderby': 'beginposition asc'
            }
        }

        if username and password:
            request_parameters['auth'] = (username, password)
        return request_parameters

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
                                  self.url, self.request_parameters['params']['q'])
                raise StopIteration
            result = self.__next__()
        return result

    def _get_next_page(self):
        """
        Get the next page of search results. Results are sorted ascending, which avoids missing some
        if products are added while the harvesting is happening (it will generally be the case)
        """
        self.LOGGER.info("Looking for ressources at '%s', matching '%s' with an offset of %s",
                         self.url, self.request_parameters['params']['q'],
                         self.request_parameters['params']['start'])

        current_page = self._http_get(self.url, self.request_parameters)
        self.request_parameters['params']['start'] += self.request_parameters['params']['rows']

        return current_page

    def _get_resources_urls(self, xml):
        """Get links from the current page. Returns True if links were found, False otherwise"""

        entries = feedparser.parse(xml)['entries']

        for entry in entries:
            self.LOGGER.debug("Adding '%s' to the list of resources.", entry['link'])
            self._urls.append(entry['link'])

        return bool(entries)


class FTPCrawler(WebDirectoryCrawler):
    """
    Crawler which returns the search results of an FTP, given the URL and search
    terms
    """
    LOGGER = logging.getLogger(__name__ + '.FTPCrawler')

    def __init__(self, root_url, time_range=(None, None), excludes=None,
                 username='anonymous', password='anonymous', files_suffixes=''):

        if not root_url.startswith('ftp://'):
            raise ValueError("The root url must start with 'ftp://'")

        self.username = username
        self.password = password
        self.files_suffixes = files_suffixes

        super().__init__(root_url, time_range, excludes)

    def set_initial_state(self):
        """
        The `_urls` attribute contains URLs to the resources which will be returned by the crawler.
        The `_to_process` attribute contains URLs to pages which need to be searched for resources.
        """
        self._urls = []
        self._to_process = [self.root_url.path or '/']
        self.ftp = ftplib.FTP(self.root_url.netloc, user=self.username, passwd=self.password)
        try:
            self.ftp.login(self.username, self.password)
        except ftplib.error_perm as err_content:
            # these two cases are in the mentioned FTP servers that deals with "login once again"
            if not (err_content.args[0].startswith('503') or err_content.args[0].startswith('230')):
                raise

    def _list_folder_contents(self, folder_path):
        return self.ftp.nlst(folder_path)

    def _is_folder(self, path):
        try:
            self.ftp.cwd(path)
        except ftplib.error_perm:
            return False
        else:
            return True

    def _is_file(self, path):
        return path.endswith(self.files_suffixes)
