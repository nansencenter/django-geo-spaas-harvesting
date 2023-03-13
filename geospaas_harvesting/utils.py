"""Utilities module for geospaas_harvesting"""
import os

import requests
import yaml
from urllib.parse import urlparse


class TrustDomainSession(requests.Session):
    """Session class which allows keeping authentication headers in
    case of redirection to the same domain
    """

    def should_strip_auth(self, old_url, new_url):
        """Keep the authentication header when redirecting to a
        different host in the same domain, for example from
        "scihub.copernicus.eu" to "apihub.copernicus.eu".
        If not in this case, defer to the parent class.
        """
        old_split_hostname = urlparse(old_url).hostname.split('.')
        new_split_hostname = urlparse(new_url).hostname.split('.')
        if (len(old_split_hostname) == len(new_split_hostname) > 2
                and old_split_hostname[1:] == new_split_hostname[1:]):
            return False
        else:
            return super().should_strip_auth(old_url, new_url)


def http_request(http_method, *args, **kwargs):
    """Wrapper around requests.request() which runs the HTTP request
    inside a TrustDomainSession if authentication is provided. This
    makes it possible to follow redirections inside the same domain.
    """
    auth = kwargs.pop('auth', None)
    if auth:
        with TrustDomainSession() as session:
            session.auth = auth
            return session.request(http_method, *args, **kwargs)
    else:
        return requests.request(http_method, *args, **kwargs)


class EnvTag(yaml.YAMLObject):
    """class for reading the tags of yml file for finding the value of
    environment variables
    """
    yaml_tag = u'!ENV'

    @classmethod
    def from_yaml(cls, loader, node):
        return os.getenv(node.value)


def read_yaml_file(config_path):
    """Loads the harvesting configuration from a file"""
    yaml.SafeLoader.add_constructor('!ENV', EnvTag.from_yaml)
    data = None
    with open(config_path, 'rb') as config_stream:
        data = yaml.safe_load(config_stream)
    return data
