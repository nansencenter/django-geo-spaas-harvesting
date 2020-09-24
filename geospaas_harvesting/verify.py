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
    download availability and returns a text or html response, then the dataset uri is removed. If
    there is no other "dataseturi" remains for the dataset, then the dataset is also removed in
    order to be harvested again in the future with a correct and healthy "dataseturi".
    Since the number of datasets in the database might be enormous, the datasets are retrieved
    into with a variable named retrieved_dataset_uris with an specific length number
    for memory management. """
    if filename=='':
        filename=f"unverified_dataset_at_{datetime.now().strftime('%Y-%m-%d___%H_%M_%S')}"
    with open(filename+".txt", 'w') as f:
        for dsuri in DatasetURI.objects.iterator(chunk_size=1000).__iter__():
            if requests.head(dsuri.uri, allow_redirects=True).status_code==200:
                content_type = requests.head(dsuri.uri, allow_redirects=True).headers.get('content-type')
                if 'html' in content_type.lower() or 'text' in content_type.lower():
                    f.write(dsuri.uri + os.linesep)
            else:
                f.write(dsuri.uri + os.linesep)


if __name__ == '__main__':
    main(filename=sys.argv[1] if len(sys.argv) == 2 else '')
