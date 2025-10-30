"""Utilities module for geospaas_harvesting"""
import importlib
import os
import pkgutil
import sys
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

import requests
import yaml


def get_all_subclasses(base_class):
    """Recursively get all subclasses of `base_class`.
    Returns a set to ensure uniqueness
    """
    subclasses = set()
    for subclass in base_class.__subclasses__():
        subclasses.add(subclass)
        subclasses = subclasses.union(get_all_subclasses(subclass))
    return subclasses


def export_subclasses(package__all__, package_name, package_dir, base_class):
    """Append `base_class` and all of its subclasses declared in
    modules in `package_dir` to `all`. This is meant to be used in
    __init__.py files to make normalizer classes easily importable.
    """
    package__all__.append(base_class.__name__)

    # Import the modules in the package
    for (_, name, _) in pkgutil.iter_modules([package_dir]):
        importlib.import_module('.' + name, package_name)

    # Make the base_class subclasses available
    # in the 'package' namespace
    for cls in get_all_subclasses(base_class):
        setattr(sys.modules[package_name], cls.__name__, cls)
        package__all__.append(cls.__name__)

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

    # TODO: temporary fix until implementation of proper config management
    ca_bundle_path = os.getenv('GEOSPAAS_HARVESTING_CA_BUNDLE')
    if ca_bundle_path:
        kwargs['verify'] = ca_bundle_path

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


def parse_xml_get_ns(file):
    """Parse an XML document and return an ElementTree and a dict of
    namespaces
    """
    events = "start", "start-ns"
    root = None
    namespaces = {}
    for event, elem in ET.iterparse(file, events):
        if event == "start-ns":
            if elem[0] in namespaces and namespaces[elem[0]] != elem[1]:
                raise KeyError("Duplicate prefix with different URI found.")
            namespaces[elem[0] if elem[0] else 'default'] = str(elem[1])
        elif event == "start":
            if root is None:
                root = elem
    return ET.ElementTree(root), namespaces


def mask_secrets(dictionary, secret_keys=('password',)):
    """Returns a copy of the dictionary, replacing the values of secret
    keys with '******'
    """
    masked = dictionary.copy()
    for key in secret_keys:
        if key in dictionary:
            masked[key] = '******'
    return masked
