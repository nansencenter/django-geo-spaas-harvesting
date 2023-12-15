import os
import os.path
import setuptools

with open(os.path.join(os.path.dirname(__file__), "README.md"), "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="geospaas_harvesting",
    version=os.getenv('GEOSPAAS_HARVESTING_RELEASE', '0.0.0dev'),
    author="Adrien Perrin",
    author_email="adrien.perrin@nersc.no",
    description="Metadata harvesting tool for GeoSPaaS",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nansencenter/django-geo-spaas-harvesting",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires='>=3.7',
    install_requires=[
        'django-geo-spaas',
        'django',
        'feedparser',
        'graypy',
        'metanorm',
        'nansat',
        'netCDF4',
        'numpy',
        'oauthlib',
        'pythesint',
        'python-dateutil',
        'PyYAML',
        'requests_oauthlib',
        'requests',
        'shapely',
    ],
    package_data={'': ['*.yml']},
)
