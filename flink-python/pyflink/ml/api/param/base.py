################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import array
from typing import TypeVar, Generic

V = TypeVar('V')


class WithParams(Generic[V]):
    """
    Parameters are widely used in machine learning realm. This class defines a common
    interface to interact with classes with parameters.
    """

    def get_params(self) -> 'Params':
        """
        Returns all the parameters.

        :return: all the parameters.
        """
        pass

    def set(self, info: 'ParamInfo', value: V) -> 'WithParams':
        """
        Set the value of a specific parameter.

        :param info: the info of the specific param to set.
        :param value: the value to be set to the specific param.
        :return: the WithParams itself.
        """
        self.get_params().set(info, value)
        return self

    def get(self, info: 'ParamInfo') -> V:
        """
        Returns the value of the specific param.

        :param info: the info of the specific param, usually with default value.
        :return: the value of the specific param, or default value defined in the \
        ParamInfo if the inner Params doesn't contains this param.
        """
        return self.get_params().get(info)

    def _set(self, **kwargs):
        """
        Sets user-supplied params.
        """
        for param, value in kwargs.items():
            p = getattr(self, param)
            if value is not None:
                try:
                    value = p.type_converter(value)
                except TypeError as e:
                    raise TypeError('Invalid param value given for param "%s". %s' % (p.name, e))
            self.get_params().set(p, value)
        return self


class Params(Generic[V]):
    """
    The map-like container class for parameter. This class is provided to unify
    the interaction with parameters.
    """

    def __init__(self):
        self._param_map = {}

    def set(self, info: 'ParamInfo', value: V) -> 'Params':
        """
        Return the number of params.

        :param info: the info of the specific parameter to set.
        :param value: the value to be set to the specific parameter.
        :return: return the current Params.
        """
        self._param_map[info] = value
        return self

    def get(self, info: 'ParamInfo') -> V:
        """
        Returns the value of the specific parameter, or default value defined in the
        info if this Params doesn't have a value set for the parameter. An exception
        will be thrown in the following cases because no value could be found for the
        specified parameter.

        :param info: the info of the specific parameter to set.
        :return: the value of the specific param, or default value defined in the \
        info if this Params doesn't contain the parameter.
        """
        if info not in self._param_map:
            if not info.is_optional:
                raise ValueError("Missing non-optional parameter %s" % info.name)
            elif not info.has_default_value:
                raise ValueError("Cannot find default value for optional parameter %s" % info.name)
            else:
                return info.default_value
        else:
            return self._param_map[info]

    def remove(self, info: 'ParamInfo') -> V:
        """
        Removes the specific parameter from this Params.

        :param info: the info of the specific parameter to remove.
        :return: the type of the specific parameter.
        """
        self._param_map.pop(info)

    def contains(self, info: 'ParamInfo') -> bool:
        """
        Check whether this params has a value set for the given `info`.

        :param info: the info of the specific parameter to check.
        :return: `True` if this params has a value set for the specified `info`, false otherwise.
        """
        return info in self._param_map

    def size(self) -> int:
        """
        Return the number of params.

        :return: Return the number of params.
        """
        return len(self._param_map)

    def clear(self) -> None:
        """
        Removes all of the params. The params will be empty after this call returns.

        :return: None.
        """
        self._param_map.clear()

    def is_empty(self) -> bool:
        """
        Returns `true` if this params contains no mappings.

        :return: `true` if this params contains no mappings.
        """
        return len(self._param_map) == 0

    def to_json(self) -> str:
        """
        Returns a json containing all parameters in this Params. The json should be
        human-readable if possible.

        :return: a json containing all parameters in this Params.
        """
        import jsonpickle
        return str(jsonpickle.encode(self._param_map, keys=True))

    def load_json(self, json: str) -> None:
        """
        Restores the parameters from the given json. The parameters should be exactly
        the same with the one who was serialized to the input json after the restoration.

        :param json: the json String to restore from.
        :return: None.
        """
        import jsonpickle
        self._param_map.update(jsonpickle.decode(json, keys=True))

    @staticmethod
    def from_json(json) -> 'Params':
        """
        Factory method for constructing params.

        :param json: the json string to load.
        :return: the `Params` loaded from the json string.
        """
        ret = Params()
        ret.load_json(json)
        return ret

    def merge(self, other_params: 'Params') -> 'Params':
        """
        Merge other params into this.

        :param other_params: other params.
        :return: return this Params.
        """
        if other_params is not None:
            self._param_map.update(other_params._param_map)
        return self

    def clone(self) -> 'Params':
        """
        Creates and returns a deep clone of this Params.

        :return: a clone of this Params.
        """
        new_params = Params()
        new_params._param_map.update(self._param_map)
        return new_params


class ParamInfo(object):
    """
    Definition of a parameter, including name, description, type_converter and so on.
    """

    def __init__(self, name, description, is_optional=True,
                 has_default_value=False, default_value=None,
                 type_converter=None):
        self.name = str(name)
        self.description = str(description)
        self.is_optional = is_optional
        self.has_default_value = has_default_value
        self.default_value = default_value
        self.type_converter = TypeConverters.identity if type_converter is None else type_converter

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Param(name=%r, description=%r)" % (self.name, self.description)

    def __hash__(self):
        return hash(str(self.name))

    def __eq__(self, other):
        if isinstance(other, ParamInfo):
            return self.name == other.name
        else:
            return False


class TypeConverters(object):
    """
    Factory methods for common type conversion functions for `Param.typeConverter`.
    The TypeConverter makes PyFlink ML pipeline support more types of parameters. For example,
    a list could be a list, a range or an array. Validation can also be done in the converters.
    """

    @staticmethod
    def _is_numeric(value):
        vtype = type(value)
        return vtype in [int, float] or vtype.__name__ == 'long'

    @staticmethod
    def _is_integer(value):
        return TypeConverters._is_numeric(value) and float(value).is_integer()

    @staticmethod
    def _can_convert_to_string(value):
        return isinstance(value, str)

    @staticmethod
    def identity(value):
        """
        Dummy converter that just returns value.
        """
        return value

    @staticmethod
    def to_list(value):
        """
        Convert a value to a list, if possible.
        """
        if isinstance(value, list):
            return value
        elif type(value) in [tuple, range, array.array]:
            return list(value)
        else:
            raise TypeError("Could not convert %s to list" % value)

    @staticmethod
    def to_list_float(value):
        """
        Convert a value to list of floats, if possible.
        """
        value = TypeConverters.to_list(value)
        if all(map(lambda v: TypeConverters._is_numeric(v), value)):
            return [float(v) for v in value]
        raise TypeError("Could not convert %s to list of floats" % value)

    @staticmethod
    def to_list_int(value):
        """
        Convert a value to list of ints, if possible.
        """
        value = TypeConverters.to_list(value)
        if all(map(lambda v: TypeConverters._is_integer(v), value)):
            return [int(v) for v in value]
        raise TypeError("Could not convert %s to list of ints" % value)

    @staticmethod
    def to_list_string(value):
        """
        Convert a value to list of strings, if possible.
        """
        value = TypeConverters.to_list(value)
        if all(map(lambda v: TypeConverters._can_convert_to_string(v), value)):
            return [TypeConverters.to_string(v) for v in value]
        raise TypeError("Could not convert %s to list of strings" % value)

    @staticmethod
    def to_float(value: float) -> float:
        """
        Convert a value to a float, if possible.
        """
        if TypeConverters._is_numeric(value):
            return float(value)
        else:
            raise TypeError("Could not convert %s to float" % value)

    @staticmethod
    def to_int(value: int) -> int:
        """
        Convert a value to an int, if possible.
        """
        if TypeConverters._is_integer(value):
            return int(value)
        else:
            raise TypeError("Could not convert %s to int" % value)

    @staticmethod
    def to_string(value: str) -> str:
        """
        Convert a value to a string, if possible.
        """
        if isinstance(value, str):
            return value
        else:
            raise TypeError("Could not convert %s to string type" % type(value))

    @staticmethod
    def to_boolean(value: bool) -> bool:
        """
        Convert a value to a boolean, if possible.
        """
        if isinstance(value, bool):
            return value
        else:
            raise TypeError("Boolean Param requires value of type bool. Found %s." % type(value))
