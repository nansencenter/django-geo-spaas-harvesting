"""
A set of crawlers used to explore data provider interfaces and get resources URLs. Each crawler
should inherit from the Crawler class and implement the abstract methods defined in Crawler.
"""
import calendar
import ftplib
import functools
import io
import logging
import os
import os.path
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests
import shapely.geometry

import geospaas_harvesting.arguments as arguments
import geospaas_harvesting.utils as utils


logging.getLogger(__name__).addHandler(logging.NullHandler())


class Stop():
    """Class used in normalizing queues to signal that processing
    should stop
    """


class DatasetInfo():
    """Class used to store dataset information coming from crawled repositories
    url is a string, metadata is a dict
    """
    def __init__(self, url, metadata=None):
        self.url = url
        if metadata is None:
            self.metadata = {}
        else:
            self.metadata = metadata

    def __repr__(self):
        return f"DatasetInfo(url='{self.url}', metadata={self.metadata})"

    def __eq__(self, other):
        return self.url == other.url and self.metadata == other.metadata


class Crawler():
    """Base Crawler class"""

    logger = logging.getLogger(__name__ + '.Crawler')
    argument_parser = arguments.ArgumentParser([
        arguments.IntegerArgument('max_threads', default=1),
    ])

    def __init__(self, **kwargs):
        self.max_threads = kwargs.get('max_threads', 1)

    @classmethod
    def from_config(cls, config: dict):
        return cls(**cls.argument_parser.parse(config))

    # ------------- crawl ------------
    def __iter__(self):
        return iter(self.crawl())

    def crawl(self):
        """Generator which crawls through a dataset repository and yields
        DatasetInfo objects
        """
        raise NotImplementedError()

    def _http_get(self, url, request_parameters=None, max_tries=5, wait_time=5):
        """Sends an HTTP GET request, retry in case of failure"""
        self.logger.debug("Getting page: '%s'", url)

        last_error = None
        for try_index in range(max_tries):
            try:
                response = utils.http_request('GET', url, **request_parameters or {})
                response.raise_for_status()
                return response
            except (requests.ConnectionError, requests.HTTPError, requests.Timeout) as error:
                # retry only for connection errors and HTTP errors 5**
                if (isinstance(error, requests.HTTPError) and
                        (error.response.status_code < 500 or error.response.status_code > 599)):
                    raise
                else:
                    last_error = error
                    self.logger.warning('Error while sending request to %s, %d retries left',
                                        url, max_tries - try_index - 1, exc_info=True)
            time.sleep(wait_time)
            wait_time *= 2
        raise RuntimeError(f"Max retries reached trying to get {url}") from last_error


class LinkExtractor(HTMLParser):
    """
    HTML parser which extracts links from an HTML page
    """

    logger = logging.getLogger(__name__ + '.LinkExtractor')

    def __init__(self):
        """Constructor with extra attribute definition"""
        super().__init__()
        self._links = []

    def error(self, message):
        """Error behavior"""
        self.logger.error(message)

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


class DirectoryCrawler(Crawler):
    """Parent class for crawlers used on repositories which expose a directory-like structure"""
    argument_parser = arguments.ArgumentParser([
        *Crawler.argument_parser.arguments,
        arguments.StringArgument('root_url', required=True),
        arguments.SequenceArgument('time_range',
                                   contents_type=arguments.DatetimeArgument,
                                   length=2,
                                   default=(None, None)),
        arguments.StringArgument('include', default=None),
        arguments.StringArgument('username', default=None),
        arguments.StringArgument('password', default=None),
    ])

    EXCLUDE = None

    YEAR_PATTERN = r'y?(?P<year>\d{4})'
    MONTH_PATTERN = r'm?(?P<month>1[0-2]|0[1-9])'
    DAY_OF_MONTH_PATTERN = r'(?P<day>3[0-1]|[1-2]\d|0[1-9])'
    DAY_OF_YEAR_PATTERN = (r'(?P<day>36[0-6]|3[0-5]\d|[1-2]\d\d|0[1-9]\d|00[1-9])')

    YEAR_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}(/.*)?$')
    MONTH_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}/?{MONTH_PATTERN}(/.*)?$')
    DAY_OF_MONTH_MATCHER = re.compile(
        f'^.*/{YEAR_PATTERN}/?{MONTH_PATTERN}/?{DAY_OF_MONTH_PATTERN}(/.*)?$')
    DAY_OF_YEAR_MATCHER = re.compile(f'^.*/{YEAR_PATTERN}/{DAY_OF_YEAR_PATTERN}(/.*)?$')

    def __init__(self, **kwargs):
        """
        `root_url` is the URL of the data repository to explore.
        `time_range` is a 2-tuple of datetime.datetime objects defining the time range
        of the datasets returned by the crawler.
        `include` is a regular expression string used to filter the crawler's output.
        Only URLs matching it are returned.
        """
        super().__init__(**kwargs)
        self.root_url = urlparse(kwargs['root_url'])
        self.time_range = kwargs['time_range']
        include = kwargs.get('include')
        self.include = re.compile(include) if include else None
        self.username = kwargs['username']
        self.password = kwargs['password']
        self._results = None
        self._to_process = None

    def __eq__(self, other):
        return (
            self.root_url == other.root_url and
            self.time_range == other.time_range and
            self.include == other.include and
            self.username == other.username and
            self.password == other.password)

    def _http_get(self, url, request_parameters=None, max_tries=5, wait_time=5):
        if self.username is not None and self.password is not None:
           if request_parameters is None:
               request_parameters = {}
           request_parameters['auth'] = (self.username, self.password)
        return super()._http_get(url, request_parameters=request_parameters,
                                 max_tries=max_tries, wait_time=wait_time)

    @property
    def base_url(self):
        """Get the root URL without the path"""
        return f"{self.root_url.scheme}://{self.root_url.netloc}"

    # ------------- crawl ------------
    def set_initial_state(self):
        """
        The `_urls` attribute contains URLs to the resources which
        will be returned by the crawler.
        The `_to_process` attribute contains URLs to pages which
        need to be searched for resources.
        """
        self._results = []
        self._to_process = [self.root_url.path.rstrip('/')]

    def crawl(self):
        self.set_initial_state()
        while True:
            try:
                # Return all resource URLs from the previously processed folder
                yield self._results.pop()
            except IndexError:
                # If no more URLs from the previously processed folder are available,
                # process the next one
                try:
                    self._process_folder(self._to_process.pop())
                except IndexError:
                    break

    @classmethod
    def _folder_coverage(cls, folder_path, time_zone=timezone.utc):
        """
        Find out if the folder has date info in its path.
        The maximum resolution is one day.
        For now, it supports the following structures:
          - .../yyyy/...
          - .../yyyy/mm/...
          - .../yyyymm/...
          - .../yyyy/mm/dd/...
          - .../yyyymmdd/...
          - .../yyyy/ddd/... (day of year)
        It will need to be updated to support new structures.
        """
        folder_coverage_start = folder_coverage_stop = None

        match_day = cls.DAY_OF_MONTH_MATCHER.search(folder_path)
        if match_day:
            folder_coverage_start = datetime(
                int(match_day.group('year')),
                int(match_day.group('month')),
                int(match_day.group('day')),
                tzinfo=time_zone)
            folder_coverage_stop = folder_coverage_start + timedelta(days=1)
            return (folder_coverage_start, folder_coverage_stop)

        match_day_of_year = cls.DAY_OF_YEAR_MATCHER.search(folder_path)
        if match_day_of_year:
            offset = timedelta(int(match_day_of_year.group('day')) - 1)
            folder_coverage_start = datetime(
                int(match_day_of_year.group('year')), 1, 1,
                tzinfo=time_zone) + offset
            folder_coverage_stop = folder_coverage_start + timedelta(days=1)
            return (folder_coverage_start, folder_coverage_stop)

        match_month = cls.MONTH_MATCHER.search(folder_path)
        if match_month:
            last_day_of_month = calendar.monthrange(
                int(match_month.group('year')), int(match_month.group('month')))[1]
            folder_coverage_start = datetime(
                int(match_month.group('year')),
                int(match_month.group('month')),
                1,
                tzinfo=time_zone)
            folder_coverage_stop = datetime(
                int(match_month.group('year')),
                int(match_month.group('month')),
                last_day_of_month,
                tzinfo=time_zone) + timedelta(days=1)
            return (folder_coverage_start, folder_coverage_stop)

        match_year = cls.YEAR_MATCHER.search(folder_path)
        if match_year:
            folder_coverage_start = datetime(int(match_year.group('year')), 1, 1,
                                             tzinfo=time_zone)
            folder_coverage_stop = datetime(int(match_year.group('year')) + 1, 1, 1,
                                            tzinfo=time_zone)
            return (folder_coverage_start, folder_coverage_stop)

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
        raise NotImplementedError()

    def _is_folder(self, path):
        """Returns True if path points to a folder"""
        raise NotImplementedError()

    def get_download_url(self, path):
        """Get the download URL from a path in the repository
        """
        return urljoin(self.base_url, path)

    def _add_url_to_return(self, path):
        """
        Add a URL to the list of URLs returned by the crawler after
        checking that it fits inside the crawler's time range.
        """
        download_url = self.get_download_url(path)
        if download_url is not None:
            dataset_info = DatasetInfo(download_url, self.get_raw_attributes(download_url))
            if dataset_info not in self._results:
                self.logger.debug("Adding '%s' to the list of resources.", dataset_info)
                self._results.append(dataset_info)

    def _add_folder_to_process(self, path):
        """Add a folder to the list of folder which will be explored later"""
        if self._intersects_time_range(*self._folder_coverage(path)):
            if path not in self._to_process:
                self.logger.debug("Adding '%s' to the list of pages to process.", path)
                self._to_process.append(path)

    def _process_folder(self, folder_path):
        """
        Get the contents of a folder and feed the _urls (based on includes) and _to_process
        attributes
        """
        self.logger.debug("Looking for resources in '%s'...", folder_path)
        for path in self._list_folder_contents(folder_path):
            # deselect paths which contains any of the excludes strings
            if ((self.EXCLUDE and self.EXCLUDE.search(path)) or
                    self.root_url.path.startswith(path.rstrip(f"{os.sep}/"))):
                continue
            if self._is_folder(path):
                self._add_folder_to_process(path)
            # select paths which are matched based on input config file
            if self.include and self.include.search(path):
                self._add_url_to_return(path)

    def get_raw_attributes(self, download_url):
        """Gets raw attributes in the cases where they need to be
        fetched. For example, the y can sometimes be fetched from
        a different URL.
        """
        return {}


class LocalDirectoryCrawler(DirectoryCrawler):
    """Crawl through the contents of a local folder"""

    logger = logging.getLogger(__name__ + '.LocalDirectoryCrawler')

    # ------------- crawl ------------
    def _list_folder_contents(self, folder_path):
        if self._is_folder(folder_path):
            return [os.path.join(folder_path, file_path) for file_path in os.listdir(folder_path)]
        else:
            # if the given path points to a file, just return it
            return [folder_path]

    def _is_folder(self, path):
        return os.path.isdir(path)


class HTMLDirectoryCrawler(DirectoryCrawler):
    """Implementation of DirectoryCrawler for repositories exposed as HTML pages."""

    logger = logging.getLogger(__name__ + '.HTMLDirectoryCrawler')

    FOLDERS_SUFFIXES = ('/',)

    # ------------- crawl ------------
    @staticmethod
    def _strip_folder_page(folder_path):
        """
        Remove the index page of a folder path.
        For example: /foo/bar/contents.html becomes /foo/bar.
        """
        return re.sub(r'/(\w+\.html)?$', r'', folder_path)

    def _is_folder(self, path):
        return path.endswith(self.FOLDERS_SUFFIXES)

    @classmethod
    def _get_links(cls, html):
        """Returns the list of links contained in an HTML page, passed as a string"""
        parser = LinkExtractor()
        cls.logger.debug("Parsing HTML data.")
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
            if urlparse(path).scheme != '':
                continue
            if path.startswith(parent_path):
                result.append(path)
            else:
                result.append(urljoin(parent_path, path))
        return result

    def _list_folder_contents(self, folder_path):
        request_parameters = {}
        html = self._http_get(f"{self.base_url}{folder_path}", request_parameters).text
        stripped_folder_path = self._strip_folder_page(folder_path)
        return self._prepend_parent_path(stripped_folder_path, self._get_links(html))


class OpenDAPCrawler(HTMLDirectoryCrawler):
    """
    Crawler for harvesting the data of OpenDAP
    """
    logger = logging.getLogger(__name__ + '.OpenDAPCrawler')
    FOLDERS_SUFFIXES = ('/contents.html',)
    EXCLUDE = re.compile(r'\?')
    GLOBAL_ATTRIBUTES_NAME = 'NC_GLOBAL'
    NAMESPACE_REGEX = r'^\{(\S+)\}Dataset$'

    # --------- get metadata ---------
    def _get_xml_namespace(self, root):
        """Try to get the namespace for the XML tag in the document from the root tag"""
        try:
            namespace_prefix = re.match(self.NAMESPACE_REGEX, root.tag)[1]  # first matched group
        except TypeError:
            namespace_prefix = ''
            self.logger.warning('Could not find XML namespace while reading DDX metadata')
        return namespace_prefix

    def _extract_attributes(self, root):
        """
        Extracts the global or specific attributes of a dataset or specific ones from a DDX document

        "x_path_global" is pointing to the 'NC_GLOBAL' part of response of the DDX document to
        obtain general information.
        "x_path_specific" is used to extract the dataset parameter names from the DDX document.
        """
        self.logger.debug("Getting the dataset's global attributes.")
        namespaces = {'default': self._get_xml_namespace(root)}
        extracted_attributes = {}
        x_path_global = "./default:Attribute[@name='NC_GLOBAL']/default:Attribute"
        x_path_specific = "./*/default:Attribute[@name='standard_name']"
        # finding the global metadata
        for attribute in root.findall(x_path_global, namespaces):
            extracted_attributes[attribute.get('name')] = attribute.find(
                "./default:value", namespaces).text
        # finding the parameters of the dataset that are declared in
        # the online source (specific metadata)
        # The specific ones are stored in 'raw_dataset_parameters' part of
        # the returned dictionary("extracted_attributes")
        extracted_attributes['raw_dataset_parameters'] = set()
        for attribute in root.findall(x_path_specific, namespaces):
            extracted_attributes['raw_dataset_parameters'].add(
                attribute.find("./default:value", namespaces).text)
        return extracted_attributes

    @classmethod
    def get_ddx_url(cls, url):
        """
        Converts the downloadable link into the link for reading meta data. In all cases,
        this method results in a url that ends with '.ddx' which will be used in further steps
        of ingestion.
        """
        if url.endswith('.ddx'):
            return url
        elif url.endswith('.dods'):
            return url[:-4]+'ddx'
        else:
            return url + '.ddx'

    def get_raw_attributes(self, url, **kwargs):
        """Get normalized metadata from the DDX info of the dataset located at
        the provided URL
        """
        ddx_url = self.get_ddx_url(url)
        # Get the metadata from the dataset as an XML tree
        stream = io.BytesIO(self._http_get(ddx_url, request_parameters={'stream': True}).content)
        # Add the metadata to the DatasetInfo
        return self._extract_attributes(ET.parse(stream).getroot())


class ThreddsCrawler(OpenDAPCrawler):
    """
    Crawler for harvesting the data which are provided by Thredds
    """
    logger = logging.getLogger(__name__ + '.ThreddsCrawler')
    FOLDERS_SUFFIXES = ('/catalog.html',)
    FILES_SUFFIXES = ('.nc',)
    EXCLUDE = re.compile(r'/thredds/catalog.html$')
    url_matcher = re.compile(r'^(.*)/(fileServer)/(.*)$')

    # --------- get metadata ---------
    @classmethod
    def get_ddx_url(cls, url):
        url_match = cls.url_matcher.match(url)
        if url_match:
            return f"{url_match[1]}/dodsC/{url_match[3]}.ddx"
        else:
            raise ValueError(f"{url} is not a Thredds HTTPServer URL")

    def get_download_url(self, path):
        result = None
        links = self._get_links(self._http_get(urljoin(self.base_url, path)).text)
        for link in links:
            if "fileServer" in link and link.endswith(self.FILES_SUFFIXES):
                result = f"{self.base_url}{link}"
                break
        return result


class FTPCrawler(DirectoryCrawler):
    """
    Crawler which returns the search results of an FTP, given the URL and search
    terms
    """
    logger = logging.getLogger(__name__ + '.FTPCrawler')

    def __init__(self, root_url, time_range=(None, None), include=None,
                 username=None, password=None, max_threads=1):
        if not root_url.startswith('ftp://'):
            raise ValueError("The root url must start with 'ftp://'")

        if username is None:
            username = 'anonymous'
        if password is None:
            password = 'anonymous'
        self.ftp = None

        super().__init__(root_url, time_range, include, max_threads=1,
                         username=username, password=password)

    # ------------- crawl ------------
    def set_initial_state(self):
        """
        The `_urls` attribute contains URLs to the resources which will be returned by the crawler.
        The `_to_process` attribute contains URLs to pages which need to be searched for resources.
        """
        self._results = []
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
                                crawler_instance.logger.info("Re-initializing the FTP connection")
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

    # --------- get metadata ---------
    def get_normalized_attributes(self, dataset_info, **kwargs):
        """Gets dataset attributes using ftp"""
        raw_attributes = {}
        self.add_url(dataset_info.url, raw_attributes)
        normalized_attributes = self._metadata_handler.get_parameters(raw_attributes)
        # TODO: add FTP_SERVICE_NAME and FTP_SERVICE in django-geo-spaas
        normalized_attributes['geospaas_service_name'] = 'ftp'
        normalized_attributes['geospaas_service'] = 'ftp'
        return normalized_attributes


class HTTPPaginatedAPICrawler(Crawler):
    """Base class for crawlers used on repositories exposing a paginated API over HTTP"""

    argument_parser = arguments.ArgumentParser([
        *Crawler.argument_parser.arguments,
        arguments.StringArgument('url', required=True),
        arguments.DictArgument('search_terms', default=None),
        arguments.SequenceArgument('time_range',
                                   contents_type=arguments.DatetimeArgument,
                                   length=2,
                                   default=(None, None)),
        arguments.StringArgument('username', default=None),
        arguments.StringArgument('password', default=None),
        arguments.IntegerArgument('page_size', default=100),
        arguments.IntegerArgument('initial_offset', default=None),
    ])

    PAGE_OFFSET_NAME = ''
    PAGE_SIZE_NAME = ''
    MIN_OFFSET = 0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.url = kwargs['url']
        self._results = None
        self.initial_offset = kwargs['initial_offset'] or self.MIN_OFFSET
        self.request_parameters = self._build_request_parameters(
            kwargs['search_terms'], kwargs['time_range'],
            kwargs['username'], kwargs['password'],
            kwargs['page_size'])

    def __eq__(self, other):
        return (
            self.url == other.url and
            self.initial_offset == other.initial_offset and
            self.request_parameters == other.request_parameters
        )

    # ------------- crawl ------------
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

    def set_initial_state(self):
        self.page_offset = self.initial_offset
        self._results = []

    def crawl(self):
        self.set_initial_state()
        while True:
            try:
                # Return all resource URLs from the previously processed page
                yield self._results.pop()
            except IndexError:
                # If no more URLs from the previously processed page are available,
                # process the next one
                if not self._get_datasets_info(self._get_next_page()):
                    self.logger.debug("No more entries found at '%s' matching '%s'",
                                    self.url, self.request_parameters['params'])
                    break

    def _get_next_page(self):
        """Get the next page of search results"""
        self.logger.debug("Looking for resources at '%s', matching '%s'",
                         self.url, self.request_parameters['params'])
        current_page = self._http_get(self.url, self.request_parameters).text
        self.increment_offset()
        return current_page

    def _get_datasets_info(self, page):
        """Get datasets information from the current page and add it
        to self._results. It should be a DatasetInfo object.
        Returns True if information was found, False otherwise"""
        raise NotImplementedError()

    # --------- get metadata ---------
    def get_normalized_attributes(self, dataset_info, **kwargs):
        raise NotImplementedError()


class ERDDAPTableCrawler(Crawler):
    """Crawler for ERDDAP tabledap APIs"""

    argument_parser = arguments.ArgumentParser([
        *Crawler.argument_parser.arguments,
        arguments.StringArgument('url', required=True),
        arguments.StringArgument('id_attrs', required=True),
        arguments.StringArgument('entry_id_prefix', default=''),
        arguments.StringArgument('longitude_attr', default='longitude'),
        arguments.StringArgument('latitude_attr', default='latitude'),
        arguments.StringArgument('time_attr', default='time'),
        arguments.StringArgument('position_qc_attr', default=''),
        arguments.StringArgument('time_qc_attr', default=''),
        arguments.SequenceArgument('valid_qc_codes',
                                   contents_type=arguments.IntegerArgument,
                                   default=None),
        arguments.DictArgument('search_terms', default=None),
        arguments.SequenceArgument('variables', default=None),
    ])
    logger = logging.getLogger(__name__ + '.ERDDAPTableCrawler')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        url = kwargs['url']
        if url.rstrip('/').endswith('.json'):
            self.url = url
        else:
            raise ValueError("The URL should end with .json")
        self.id_attrs = kwargs['id_attrs']
        self.entry_id_prefix = kwargs['entry_id_prefix']
        self.longitude_attr = kwargs['longitude_attr']
        self.latitude_attr = kwargs['latitude_attr']
        self.time_attr = kwargs['time_attr']
        self.position_qc_attr = kwargs['position_qc_attr']
        self.time_qc_attr = kwargs['time_qc_attr']
        self.valid_qc_codes = kwargs['valid_qc_codes']
        self.search_terms = kwargs['search_terms'] if kwargs['search_terms'] is not None else []
        self.variables = kwargs['variables'] if kwargs['variables'] else []

    def __eq__(self, other):
        return (
            self.url == other.url and
            self.id_attrs == other.id_attrs and
            self.longitude_attr == other.longitude_attr and
            self.latitude_attr == other.latitude_attr and
            self.time_attr == other.time_attr and
            self.position_qc_attr == other.position_qc_attr and
            self.time_qc_attr == other.time_qc_attr and
            self.valid_qc_codes == other.valid_qc_codes and
            self.search_terms == other.search_terms and
            self.variables == other.variables
        )

    def get_ids(self):
        """Fetch identifiers matching the search terms"""
        url = f"{self.url}?{','.join(self.id_attrs)}&distinct()"
        kwargs = {}
        url = '&'.join([url] + self.search_terms)
        try:
            response = self._http_get(url, max_tries=1, **kwargs)
        except requests.HTTPError as error:
            self.logger.error("Could not list dataset identifiers at %s: %s",
                              url, error.response.content, exc_info=True)
            raise
        for row in response.json()['table']['rows']:
            yield row[:len(self.id_attrs)]

    def _make_condition_parameters(self, parameters):
        """Prepare the parameters to filter a query using the id
        attributes. Necessary because the API requires different
        formats depending on the type of parameter
        """
        params = {}
        for key, value in parameters.items():
            if isinstance(value, str):
                params[key] = f'"{value}"'
            else:
                params[key] = value
        return params

    def crawl(self):
        attributes = [self.time_attr, self.longitude_attr, self.latitude_attr]
        for qc_attr in (self.time_qc_attr, self.position_qc_attr):
            if qc_attr:
                attributes.append(qc_attr)
        attributes.extend(self.variables)
        for id_values in self.get_ids():
            id_attrs = dict(zip(self.id_attrs, id_values))
            id_condition = '&'.join(
                f"{id_attr}={id_value}"
                for id_attr, id_value in self._make_condition_parameters(id_attrs).items()
            )
            yield DatasetInfo(
                f'{self.url}?{",".join(attributes)}&{id_condition}',
                {'id_attributes': id_attrs})

    def _check_qc(self, qc_value):
        """Return True if the QC value indicates valid data or the
        valid codes are unknown
        """
        return not self.valid_qc_codes or qc_value in self.valid_qc_codes

    def _make_coverage_url(self):
        """"""
        qc_attributes = ','.join(c for c in (self.time_qc_attr, self.position_qc_attr) if c)
        if qc_attributes:
            qc_attributes = f",{qc_attributes}"
        return (f'{self.url}?{self.time_attr},{self.longitude_attr},{self.latitude_attr}' +
                qc_attributes +
                f'&distinct()&orderBy("{self.time_attr}")')

    def get_coverage(self, id_attributes):
        """Get the temporal and spatial coverage for a specific dataset
        """
        try:
            response = self._http_get(self._make_coverage_url(), request_parameters={
                'params': self._make_condition_parameters(id_attributes)
            })
        except requests.HTTPError as error:
            self.logger.error("Could not get coverage for dataset %s: %s",
                              id_attributes, error.response.content)
            raise
        rows = response.json()['table']['rows']

        # build the trajectory and get the first time with valid QC
        # (the query results are sorted by time)
        time_coverage_start = None
        trajectory = []
        for row in rows:
            if time_coverage_start is None and self._check_qc(row[3]):
                time_coverage_start = row[0]
            point = (row[1], row[2])
            if point not in trajectory and self._check_qc(row[4]):
                trajectory.append(point)

        # get the last time with valid QC
        time_coverage_end = None
        for row in rows[::-1]:
            if self._check_qc(row[3]):
                time_coverage_end = row[0]
                break

        if time_coverage_start is None or time_coverage_end is None or not trajectory:
            raise RuntimeError(f"Could not determine coverage for dataset {id_attributes}")

        return ((time_coverage_start, time_coverage_end), trajectory)

    def _make_product_metadata_url(self):
        """Generate the product metadata URL from the base data URL"""
        match = re.match(r'^(https?://.*)/tabledap/(.*)\.json$', self.url)
        if match:
            return f"{match.group(1)}/info/{match.group(2)}/index.json"
        else:
            raise RuntimeError(f"Unable to get product metadata URL from {self.url}")

    def get_product_metadata(self):
        """Get the product's metadata"""
        url = self._make_product_metadata_url()
        try:
            response = self._http_get(url)
        except requests.HTTPError:
            self.logger.info("Could not get product metadata from %s", url)
            raise
        return response.json()

    def get_normalized_attributes(self, dataset_info, **kwargs):
        """Use metanorm to normalize a DatasetInfo's raw attributes"""
        raw_attributes = dataset_info.metadata
        self.add_url(dataset_info.url, raw_attributes)
        coverage = self.get_coverage(dataset_info.metadata['id_attributes'])
        raw_attributes['entry_id'] = (
            self.entry_id_prefix +
            '_'.join(map(str, dataset_info.metadata['id_attributes'].values()))
        )
        raw_attributes['temporal_coverage'] = coverage[0]
        raw_attributes['trajectory'] = shapely.geometry.MultiPoint(coverage[1]).wkt
        raw_attributes['product_metadata'] = self.get_product_metadata()

        normalized_attributes = self._metadata_handler.get_parameters(raw_attributes)
        return normalized_attributes
