"""This package contains normalizers which extract the metadata
necessary to instantiate GeoSPaaS Datasets.
All normalizers in this package should inherit from
MetadataNormalizer.
"""
import pkgutil
import importlib
from pathlib import Path

from .base import MetadataNormalizer
from .utils import export_subclasses, get_all_subclasses

# __all__ = []
# export_subclasses(__all__, __package__, os.path.dirname(__file__), MetadataNormalizer)

for (_, name, _) in pkgutil.iter_modules([Path(__file__).parent]):
        importlib.import_module('.' + name, __package__)

# index that allows to retrieve a normalizer class using its name
index = {
    cls.name: cls
    for cls in get_all_subclasses(MetadataNormalizer)
    if cls.name is not None
}
