""" Verification module. """
import os
import sys
from datetime import datetime

import django
import requests

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_harvesting.settings')
django.setup()

from geospaas.catalog.models import DatasetURI

def main():
    """ Verifies the datasets based on their dataseturi. If the download link does not provide a
    download availability and returns a response that does not start with '2' in its status code,
    then the dataset uri is written into a file named "filename"."""
    try:
        filename = sys.argv[1]
    except IndexError:
        filename = f"unverified_datasets_at_{datetime.now().strftime('%Y-%m-%d___%H_%M_%S')}"
    with open(filename + ".txt", 'w') as f:
        for dsuri in DatasetURI.objects.iterator():
            response = requests.head(dsuri.uri, allow_redirects=True)
            if response.status_code < 200 or response.status_code > 299:
                f.write(dsuri.uri + os.linesep)


if __name__ == '__main__':
    main()
