"""This package contains the code to search and get metadata from
various data providers
"""
import importlib
import pkgutil

# import all submodules automatically
for loader, module_name, is_pkg in pkgutil.walk_packages(__path__):
    importlib.import_module(f"{__package__}.{module_name}")
