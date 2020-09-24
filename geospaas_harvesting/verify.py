""" Verification module. """
import os
import sys
from datetime import datetime

import django
import requests

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_harvesting.settings')
django.setup()

from geospaas.catalog.models import DatasetURI

def main(filename):
    """ Verifies the datasets based on their dataseturi. If the download link does not provide a
    download availability and returns a text or html response, then the dataset uri is written into
    a file named "filename"."""
    if filename=='':
        filename=f"unverified_dataset_at_{datetime.now().strftime('%Y-%m-%d___%H_%M_%S')}"
    with open(filename+".txt", 'w') as f:
        for dsuri in DatasetURI.objects.iterator():
            if not str(requests.head(dsuri.uri, allow_redirects=True).status_code).startswith('2'):
                f.write(dsuri.uri + os.linesep)


if __name__ == '__main__':
    main(filename=sys.argv[1] if len(sys.argv) == 2 else '')
