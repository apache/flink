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
from typing import Set, Dict

from py4j.java_gateway import JavaObject

from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import add_jars_to_context_class_loader


class Configuration:
    """
    Lightweight configuration object which stores key/value pairs.
    """

    def __init__(self, other: 'Configuration' = None, j_configuration: JavaObject = None):
        """
        Creates a new configuration.

        :param other: Optional, if this parameter exists, creates a new configuration with a
                      copy of the given configuration.
        :param j_configuration: Optional, the py4j java configuration object, if this parameter
                                exists, creates a wrapper for it.
        """
        if j_configuration is not None:
            self._j_configuration = j_configuration
        else:
            gateway = get_gateway()
            JConfiguration = gateway.jvm.org.apache.flink.configuration.Configuration
            if other is not None:
                self._j_configuration = JConfiguration(other._j_configuration)
            else:
                self._j_configuration = JConfiguration()

    def get_string(self, key: str, default_value: str) -> str:
        """
        Returns the value associated with the given key as a string.

        :param key: The key pointing to the associated value.
        :param default_value: The default value which is returned in case there is no value
                              associated with the given key.
        :return: The (default) value associated with the given key.
        """
        return self._j_configuration.getString(key, default_value)

    def set_string(self, key: str, value: str) -> 'Configuration':
        """
        Adds the given key/value pair to the configuration object.

        :param key: The key of the key/value pair to be added.
        :param value: The value of the key/value pair to be added.
        """
        jvm = get_gateway().jvm
        jars_key = jvm.org.apache.flink.configuration.PipelineOptions.JARS.key()
        classpaths_key = jvm.org.apache.flink.configuration.PipelineOptions.CLASSPATHS.key()
        if key in [jars_key, classpaths_key]:
            add_jars_to_context_class_loader(value.split(";"))
        self._j_configuration.setString(key, value)
        return self

    def get_integer(self, key: str, default_value: int) -> int:
        """
        Returns the value associated with the given key as an integer.

        :param key: The key pointing to the associated value.
        :param default_value: The default value which is returned in case there is no value
                              associated with the given key.
        :return: The (default) value associated with the given key.
        """
        return self._j_configuration.getLong(key, default_value)

    def set_integer(self, key: str, value: int) -> 'Configuration':
        """
        Adds the given key/value pair to the configuration object.

        :param key: The key of the key/value pair to be added.
        :param value: The value of the key/value pair to be added.
        """
        self._j_configuration.setLong(key, value)
        return self

    def get_boolean(self, key: str, default_value: bool) -> bool:
        """
        Returns the value associated with the given key as a boolean.

        :param key: The key pointing to the associated value.
        :param default_value: The default value which is returned in case there is no value
                              associated with the given key.
        :return: The (default) value associated with the given key.
        """
        return self._j_configuration.getBoolean(key, default_value)

    def set_boolean(self, key: str, value: bool) -> 'Configuration':
        """
        Adds the given key/value pair to the configuration object.

        :param key: The key of the key/value pair to be added.
        :param value: The value of the key/value pair to be added.
        """
        self._j_configuration.setBoolean(key, value)
        return self

    def get_float(self, key: str, default_value: float) -> float:
        """
        Returns the value associated with the given key as a float.

        :param key: The key pointing to the associated value.
        :param default_value: The default value which is returned in case there is no value
                              associated with the given key.
        :return: The (default) value associated with the given key.
        """
        return self._j_configuration.getDouble(key, float(default_value))

    def set_float(self, key: str, value: float) -> 'Configuration':
        """
        Adds the given key/value pair to the configuration object.

        :param key: The key of the key/value pair to be added.
        :param value: The value of the key/value pair to be added.
        """
        self._j_configuration.setDouble(key, float(value))
        return self

    def get_bytearray(self, key: str, default_value: bytearray) -> bytearray:
        """
        Returns the value associated with the given key as a byte array.

        :param key: The key pointing to the associated value.
        :param default_value: The default value which is returned in case there is no value
                              associated with the given key.
        :return: The (default) value associated with the given key.
        """
        return bytearray(self._j_configuration.getBytes(key, default_value))

    def set_bytearray(self, key: str, value: bytearray) -> 'Configuration':
        """
        Adds the given byte array to the configuration object.

        :param key: The key under which the bytes are added.
        :param value: The byte array to be added.
        """
        self._j_configuration.setBytes(key, value)
        return self

    def key_set(self) -> Set[str]:
        """
        Returns the keys of all key/value pairs stored inside this configuration object.

        :return: The keys of all key/value pairs stored inside this configuration object.
        """
        return set(self._j_configuration.keySet())

    def add_all_to_dict(self, target_dict: Dict):
        """
        Adds all entries in this configuration to the given dict.

        :param target_dict: The dict to be updated.
        """
        properties = get_gateway().jvm.java.util.Properties()
        self._j_configuration.addAllToProperties(properties)
        target_dict.update(properties)

    def add_all(self, other: 'Configuration', prefix: str = None) -> 'Configuration':
        """
        Adds all entries from the given configuration into this configuration. The keys are
        prepended with the given prefix if exist.

        :param other: The configuration whose entries are added to this configuration.
        :param prefix: Optional, the prefix to prepend.
        """
        if prefix is None:
            self._j_configuration.addAll(other._j_configuration)
        else:
            self._j_configuration.addAll(other._j_configuration, prefix)
        return self

    def contains_key(self, key: str) -> bool:
        """
        Checks whether there is an entry with the specified key.

        :param key: Key of entry.
        :return: True if the key is stored, false otherwise.
        """
        return self._j_configuration.containsKey(key)

    def to_dict(self) -> Dict[str, str]:
        """
        Converts the configuration into a dict representation of string key-pair.

        :return: Dict representation of the configuration.
        """
        return dict(self._j_configuration.toMap())

    def remove_config(self, key: str) -> bool:
        """
        Removes given config key from the configuration.

        :param key: The config key to remove.
        :return: True if config has been removed, false otherwise.
        """
        gateway = get_gateway()
        JConfigOptions = gateway.jvm.org.apache.flink.configuration.ConfigOptions
        config_option = JConfigOptions.key(key).noDefaultValue()
        return self._j_configuration.removeConfig(config_option)

    def __deepcopy__(self, memodict=None):
        return Configuration(j_configuration=self._j_configuration.clone())

    def __hash__(self):
        return self._j_configuration.hashCode()

    def __eq__(self, other):
        if isinstance(other, Configuration):
            return self._j_configuration.equals(other._j_configuration)
        else:
            return False

    def __str__(self):
        return self._j_configuration.toString()
