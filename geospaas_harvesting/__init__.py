"""This module provides means to gather metadata about various datasets
into the GeoSPaaS catalog
"""
import logging.config
import os
import os.path
import sys
import yaml

DEFAULT_LOGGING_CONF_FILE = os.path.join(os.path.dirname(__file__), 'logging.yml')
LOGGING_CONF_FILE = os.getenv('GEOSPAAS_HARVESTING_LOG_CONF_PATH', DEFAULT_LOGGING_CONF_FILE)

try:
    with open(LOGGING_CONF_FILE, 'rb') as stream:
        logging_configuration = yaml.safe_load(stream)  # pylint: disable=invalid-name
except FileNotFoundError:  # pragma: no cover
    print(f"'{LOGGING_CONF_FILE}' does not exist, logging can't be configured.", file=sys.stderr)
    logging_configuration = None  # pylint: disable=invalid-name

if logging_configuration:
    logging.config.dictConfig(logging_configuration)
    logging.captureWarnings(True)
