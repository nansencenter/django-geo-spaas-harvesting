""" Verification module. """
import os
from datetime import datetime

import django
import requests

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_harvesting.settings')
django.setup()

from geospaas.catalog.models import DatasetURI

def main():
    """ Verifies the datasets based on their dataseturi. If the download link does not provide a
    download availability and returns a text or html response, then the dataset uri is removed. If
    there is no other "dataseturi" remains for the dataset, then the dataset is also removed in
    order to be harvested again in the future with a correct and healthy "dataseturi".
    Since the number of datasets in the database might be enormous, the datasets are retrieved
    into with a variable named retrieved_dataset_uris with an specific length number
    for memory management. """
    corrupted_url_set = set()
    id_range = range(DatasetURI.objects.earliest('id').id,
                     DatasetURI.objects.latest('id').id, 1000)# <=number for the length of retrieved
    for i in range(len(id_range)):
        try:
            retrieved_dataset_uris = DatasetURI.objects.filter(
                id__gte=id_range[i], id__lt=id_range[i+1])
        except IndexError:
            retrieved_dataset_uris = DatasetURI.objects.filter(
                id__gte=id_range[i], id__lte=DatasetURI.objects.latest('id').id)
        for dsuri in retrieved_dataset_uris:
            content_type = requests.head(dsuri.uri, allow_redirects=True).headers.get('content-type')
            if 'html' in content_type.lower() or 'text' in content_type.lower():
                corrupted_url_set.add(dsuri.uri)

    with open(f"unverified_ones_at_{datetime.now().strftime('%Y-%m-%d|%H_%M_%S')}.txt", 'w') as f:
        for url in corrupted_url_set:
            # Write down the urls on unverified_ones_at_blablabla.txt
            f.write(url + '\n')
            # If that url is the only url of the dataset, then delete the dataset. Otherwise, delete
            # the url only. python assert is used to make sure that only exact number (exactly one
            # or exactly two,i.e. one dataset and one dataseturi) of records are being removed
            # from the database.
            if DatasetURI.objects.get(uri=url).dataset.dataseturi_set.count() == 1:
                assert DatasetURI.objects.get(uri=url).dataset.delete()[0] == 2
            else:
                assert DatasetURI.objects.get(uri=url).delete()[0] == 1


if __name__ == '__main__':
    main()
