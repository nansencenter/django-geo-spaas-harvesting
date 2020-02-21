"""
This module provides means to gather metadata about various Datasets into the GeoSPaaS catalog
"""
#TODO: review docstrings in the whole package

import logging
import logging.handlers

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# HANDLER = logging.StreamHandler()
HANDLER = logging.handlers.RotatingFileHandler(
    '/var/log/geospaas/harvesting.log',
    maxBytes=100000000,
    backupCount=10)

HANDLER.setFormatter(FORMATTER)

LOGGER.addHandler(HANDLER)
