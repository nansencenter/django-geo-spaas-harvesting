"""This module defines classes used to parse and validate arguments.
"""
import re
from datetime import timezone

import dateutil.parser
import shapely.wkt


class NoDefault:
    """Special class used when no default value is specified"""

class ArgumentParser():
    """Class capable of validating if a dictionary of parameters
    matches a list of argument definitions
    """
    def __init__(self, arguments, strict=True):
        """Set the list of valid arguments.
        If `strict` is True, only the defined arguments must be present
        in the parameters being validated. Otherwise, extra parameters
        are allowed
        """
        self.arguments = {}
        self.add_arguments(arguments)
        self.strict = strict

    def __str__(self):
        result = ['available arguments:']
        result.extend([str(arg) for arg in self.arguments.values()])
        return '\n\t'.join(result)

    def add_arguments(self, arguments):
        """Adds or updates Arguments in the valid arguments"""
        for arg in arguments:
            if not isinstance(arg, Argument):
                raise ValueError(f"{arg} should be an Argument object")
            self.arguments[arg.name] = arg

    def parse(self, parameters):
        """Makes sure the right arguments are passed and parses them.
        `parameters` should be a dictionary of parameters to be
        validated.
        """
        parsed_parameters = {}
        recursion_stack = list(self.arguments.values())
        max_stack_size = 10000

        # Loop through the argument definitions and check that the
        # parameters match the definitions.
        # If an argument has children, they will be checked too
        while recursion_stack and len(recursion_stack) <= max_stack_size:
            # if the name of the argument is found in the parameters,
            # the value is parsed and adde to the final results.
            argument = recursion_stack.pop()
            if argument.name in parameters:
                parsed_parameters[argument.name] = argument.parse(
                    parameters.pop(argument.name))
                # add the child arguments to the stack so that they are
                # processed
                for child in argument.children:
                    recursion_stack.append(child)
            elif argument.required:
                raise ValueError(f"Argument {argument.name} not provided")
            else:
                if argument.default is not NoDefault:
                    parsed_parameters[argument.name] = argument.default

        if self.strict and parameters:
            raise ValueError(f"Unknown argument(s) {parameters}")

        return parsed_parameters


class Argument():
    """Base class for arguments. Each argument has at least a name and
    a 'required' attribute.
    In case there are arguments depending on another one, they can be
    listed as children. In that case, their 'parent' attribute is set
    """
    type = 'unknown type'

    def __init__(self, name, **kwargs):
        self.name = name
        self.required = kwargs.get('required', False)
        self.default = kwargs.get('default', NoDefault)
        self.description = kwargs.get('description', '')
        self.parent = None
        self.children = []

    def __eq__(self, other):
        return (
            self.name == other.name and
            self.required == other.required and
            self.default == other.default and
            self.description == other.description
        )

    def __str__(self):
        return ', '.join(filter(None, (
            f"{self.name}",
            f"type={self.type}",
            'required' if self.required else 'not required',
            f"default={self.default}" if self.default is not NoDefault else '',
            f"description={self.description}" if self.description else '',
        )))

    def _set_parent(self, parent):
        """Define the parent of the current argument"""
        self.parent = parent

    def add_child(self, child):
        """Add a child argument"""
        child._set_parent(self)
        self.children.append(child)

    def parse(self, value):
        """Return a properly formatted value for the argument.
        If the input is not correct, should raise an exception
        """
        raise NotImplementedError()


class AnyArgument(Argument):
    """Passthrough argument with no validation"""
    type = 'any type'

    def parse(self, value):
        return value


class BooleanArgument(Argument):
    """Boolean argument. Should be an explicit boolean"""
    type = 'boolean'

    def parse(self, value):
        if isinstance(value, bool):
            return value
        else:
            raise ValueError(f"{self.name} should be a boolean")


class ChoiceArgument(Argument):
    """Validates that the value of the argument is included in a list
    of valid options
    """
    type = 'multiple choices'

    def __init__(self, name, **kwargs):
        self.valid_options = kwargs.pop('valid_options', [])
        super().__init__(name, **kwargs)
        if self.default is not NoDefault:
            self.validate(self.default)

    def __eq__(self, other):
        return super().__eq__(other) and self.valid_options == other.valid_options

    def __str__(self):
        return super().__str__() + f", valid options={self.valid_options}"

    def validate(self, value):
        """Check if the value is valid. Returns a boolean"""
        if self.valid_options and value not in self.valid_options:
            raise ValueError(f"{value} is not a valid option for {self.name}")

    def parse(self, value):
        self.validate(value)
        return value


class DatetimeArgument(Argument):
    """Creates a Datetime from a string. If no timezone is specified,
    it is set as UTC.
    """
    type = 'datetime'

    def parse(self, value):
        if value is None:
            return None
        _datetime = dateutil.parser.parse(value)
        if _datetime.tzinfo is None:
            _datetime = _datetime.replace(tzinfo=timezone.utc)
        return _datetime


class DictArgument(Argument):
    """Dictionary argument"""
    type = 'dictionary'

    def __init__(self, name, **kwargs):
        self.valid_keys = set(kwargs.pop('valid_keys', []))
        super().__init__(name, **kwargs)

    def __eq__(self, other):
        return super().__eq__(other) and self.valid_keys == other.valid_keys

    def __str__(self):
        return (super().__str__() +
                (f", valid keys={self.valid_keys}" if self.valid_keys else ''))

    def parse(self, value):
        if not isinstance(value, dict):
            raise ValueError(f"{self.name} should be a dictionary")
        keys = set(value.keys())
        if self.valid_keys and not keys.issubset(self.valid_keys):
            raise ValueError(f"Invalid keys {keys.difference(self.valid_keys)}")
        return value


class IntegerArgument(Argument):
    """Validates that the argument value is an integer, optionally
    comprised between a minimum and a maximum value
    """
    type = 'integer'

    def __init__(self, name, **kwargs):
        self.min_value = kwargs.pop('min_value', None)
        self.max_value = kwargs.pop('max_value', None)
        super().__init__(name, **kwargs)

    def __eq__(self, other):
        return (
            super().__eq__(other) and
            self.min_value == other.min_value and
            self.max_value == other.max_value
        )

    def __str__(self):
        return (super().__str__() +
                (f", minimum value={self.min_value}" if self.min_value else '') +
                (f", maximum value={self.max_value}" if self.max_value else ''))

    def parse(self, value):
        if not isinstance(value, int):
            raise ValueError(f"{self.name} should be an integer")
        if (self.min_value is not None and value < self.min_value or
                self.max_value is not None and value > self.max_value):
            raise ValueError(
                f"{value} outside of allowed range: [{self.min_value}, {self.max_value}]")
        return value


class ListArgument(Argument):
    """Check that the value is a list"""
    type = 'list'

    def parse(self, value):
        if not isinstance(value, list):
            raise ValueError(f"{self.name} should be a list")
        return value


class PathArgument(ChoiceArgument):
    """Path argument with format validation and optional valid options.
    Only absolute paths are accepted.
    Subdirectories of the valid options are still valid.
    """
    type = 'path'
    SEP = '/'
    path_re = re.compile(rf'^\.{{,2}}({SEP}[^{SEP}]*)*{SEP}?$')

    def is_path(self, path):
        """Returns True if the value is a valid path"""
        return self.path_re.match(path)

    def validate(self, value):
        # check path format
        if not self.is_path(value):
            raise ValueError(f"{value} is not a valid path")

        # check valid options
        if self.valid_options:
            found = False
            for valid_path in self.valid_options:
                if value.startswith(valid_path):
                    found = True
                    break
            if not found:
                raise ValueError(
                    f"{value} is not an accepted path :{self.valid_options}")


class StringArgument(Argument):
    """String argument with optional regex validation"""
    type = 'string'

    def __init__(self, name, **kwargs):
        self.regex = kwargs.pop('regex', None)
        super().__init__(name, **kwargs)

    def __eq__(self, other):
        return super().__eq__(other) and self.regex == other.regex

    def __str__(self):
        return (super().__str__() +
                (f", validation regex={self.regex}" if self.regex else ''))

    def parse(self, value):
        if not isinstance(value, str):
            raise ValueError(f"{self.name} should be a string")
        if self.regex is not None and not re.match(self.regex, value):
            raise ValueError(f"{value} does not match the validation pattern {self.regex}")
        return value


class WKTArgument(Argument):
    """Creates a shapely geometry object from a WKT string"""
    type = 'WKT'

    def __init__(self, name, **kwargs):
        self.geometry_types = kwargs.pop('geometry_types', None)
        super().__init__(name, **kwargs)

    def __eq__(self, other):
        return super().__eq__(other) and self.geometry_types == other.geometry_types

    def __str__(self):
        accepted_geometries = [g.__name__ for g in self.geometry_types]
        return (super().__str__() +
                (f", accepted geometries={accepted_geometries}" if accepted_geometries else ''))

    def parse(self, value):
        geometry = shapely.wkt.loads(value)
        geometry_type = type(geometry)
        if self.geometry_types is None or geometry_type in self.geometry_types:
            return geometry
        else:
            raise ValueError(f"{geometry_type} is not supported for argument {self.name}")
