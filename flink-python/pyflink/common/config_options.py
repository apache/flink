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
from typing import TypeVar, Generic

from pyflink.java_gateway import get_gateway

T = TypeVar('T')

__all__ = ['ConfigOptions', 'ConfigOption']


class ConfigOptions(object):
    """
    {@code ConfigOptions} are used to build a :class:`~pyflink.common.ConfigOption`. The option is
    typically built in one of the following patterns:

    Example:
    ::

        # simple string-valued option with a default value
        >>> ConfigOptions.key("tmp.dir").string_type().default_value("/tmp")
        # simple integer-valued option with a default value
        >>> ConfigOptions.key("application.parallelism").int_type().default_value(100)
        # option with no default value
        >>> ConfigOptions.key("user.name").string_type().no_default_value()
    """

    def __init__(self, j_config_options):
        self._j_config_options = j_config_options

    @staticmethod
    def key(key: str):
        """
        Starts building a new ConfigOption.

        :param key: The key for the config option.
        :return: The builder for the config option with the given key.
        """
        gateway = get_gateway()
        j_option_builder = gateway.jvm.org.apache.flink.configuration.ConfigOptions.key(key)
        return ConfigOptions.OptionBuilder(j_option_builder)

    class OptionBuilder(object):
        def __init__(self, j_option_builder):
            self._j_option_builder = j_option_builder

        def boolean_type(self) -> 'ConfigOptions.TypedConfigOptionBuilder[bool]':
            """
            Defines that the value of the option should be of bool type.
            """
            return ConfigOptions.TypedConfigOptionBuilder(self._j_option_builder.booleanType())

        def int_type(self) -> 'ConfigOptions.TypedConfigOptionBuilder[int]':
            """
            Defines that the value of the option should be of int type
            (from -2,147,483,648 to 2,147,483,647).
            """
            return ConfigOptions.TypedConfigOptionBuilder(self._j_option_builder.intType())

        def long_type(self) -> 'ConfigOptions.TypedConfigOptionBuilder[int]':
            """
            Defines that the value of the option should be of int type
            (from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807).
            """
            return ConfigOptions.TypedConfigOptionBuilder(self._j_option_builder.longType())

        def float_type(self) -> 'ConfigOptions.TypedConfigOptionBuilder[float]':
            """
            Defines that the value of the option should be of float type
            (4-byte single precision floating point number).
            """
            return ConfigOptions.TypedConfigOptionBuilder(self._j_option_builder.floatType())

        def double_type(self) -> 'ConfigOptions.TypedConfigOptionBuilder[float]':
            """
            Defines that the value of the option should be of float Double} type
            (8-byte double precision floating point number).
            """
            return ConfigOptions.TypedConfigOptionBuilder(self._j_option_builder.doubleType())

        def string_type(self) -> 'ConfigOptions.TypedConfigOptionBuilder[str]':
            """
            Defines that the value of the option should be of str type.
            """
            return ConfigOptions.TypedConfigOptionBuilder(self._j_option_builder.stringType())

    class TypedConfigOptionBuilder(Generic[T]):
        def __init__(self, j_typed_config_option_builder):
            self._j_typed_config_option_builder = j_typed_config_option_builder

        def default_value(self, value: T) -> 'ConfigOption[T]':
            return ConfigOption(self._j_typed_config_option_builder.defaultValue(value))

        def no_default_value(self) -> 'ConfigOption[str]':
            return ConfigOption(self._j_typed_config_option_builder.noDefaultValue())


class ConfigOption(Generic[T]):
    """
    A {@code ConfigOption} describes a configuration parameter. It encapsulates the configuration
    key, deprecated older versions of the key, and an optional default value for the configuration
    parameter.

    {@code ConfigOptions} are built via the ConfigOptions class. Once created, a config
    option is immutable.
    """

    def __init__(self, j_config_option):
        self._j_config_option = j_config_option
