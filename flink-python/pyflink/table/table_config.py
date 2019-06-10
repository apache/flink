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
import sys

from pyflink.java_gateway import get_gateway

__all__ = ['TableConfig']

if sys.version > '3':
    unicode = str


class TableConfig(object):
    """
    A config to define the runtime behavior of the Table API.
    """

    def __init__(self):
        self._jvm = get_gateway().jvm
        self._j_table_config = self._jvm.TableConfig()
        self._is_stream = None  # type: bool
        self._parallelism = None  # type: int

    def get_timezone(self):
        """
        Returns the timezone id, either an abbreviation such as "PST", a full name such as
        "America/Los_Angeles", or a custom timezone_id such as "GMT-8:00".
        """
        return self._j_table_config.getTimeZone().getID()

    def set_timezone(self, timezone_id):
        """
        Sets the timezone id for date/time/timestamp conversions.

        :param timezone_id: The timezone id, either an abbreviation such as "PST", a full name
                            such as "America/Los_Angeles", or a custom timezone_id such as
                            "GMT-8:00".
        """
        if timezone_id is not None and isinstance(timezone_id, (str, unicode)):
            j_timezone = self._jvm.java.util.TimeZone.getTimeZone(timezone_id)
            self._j_table_config.setTimeZone(j_timezone)
        else:
            raise Exception("TableConfig.timezone should be a string!")

    def get_null_check(self):
        """
        A boolean value, "True" enables NULL check and "False" disables NULL check.
        """
        return self._j_table_config.getNullCheck()

    def set_null_check(self, null_check):
        """
        Sets the NULL check. If enabled, all fields need to be checked for NULL first.
        """
        if null_check is not None and isinstance(null_check, bool):
            self._j_table_config.setNullCheck(null_check)
        else:
            raise Exception("TableConfig.null_check should be a bool value!")

    def get_max_generated_code_length(self):
        """
        The current threshold where generated code will be split into sub-function calls. Java has
        a maximum method length of 64 KB. This setting allows for finer granularity if necessary.
        Default is 64000.
        """
        return self._j_table_config.getMaxGeneratedCodeLength()

    def set_max_generated_code_length(self, max_generated_code_length):
        """
        Returns the current threshold where generated code will be split into sub-function calls.
        Java has a maximum method length of 64 KB. This setting allows for finer granularity if
        necessary. Default is 64000.
        """
        if max_generated_code_length is not None and isinstance(max_generated_code_length, int):
            self._j_table_config.setMaxGeneratedCodeLength(max_generated_code_length)
        else:
            raise Exception("TableConfig.max_generated_code_length should be a int value!")

    def get_built_in_catalog_name(self):
        """
        Gets the specified name of the initial catalog to be created when instantiating
        :class:`TableEnvironment`.
        """
        return self._j_table_config.getBuiltInCatalogName()

    def set_built_in_catalog_name(self, built_in_catalog_name):
        """
        Specifies the name of the initial catalog to be created when instantiating
        :class:`TableEnvironment`. This method has no effect if called on the
        :func:`~pyflink.table.TableEnvironment.get_config`.
        """
        if built_in_catalog_name is not None and isinstance(built_in_catalog_name, str):
            self._j_table_config.setBuiltInCatalogName(built_in_catalog_name)
        else:
            raise Exception("TableConfig.built_in_catalog_name should be a string value!")

    def get_built_in_database_name(self):
        """
        Gets the specified name of the default database in the initial catalog to be created when
        instantiating :class:`TableEnvironment`.
        """
        return self._j_table_config.getBuiltInDatabaseName()

    def set_built_in_database_name(self, built_in_database_name):
        """
        Specifies the name of the default database in the initial catalog to be created when
        instantiating :class:`TableEnvironment`. This method has no effect if called on the
        :func:`~pyflink.table.TableEnvironment.get_config`.
        """
        if built_in_database_name is not None and isinstance(built_in_database_name, str):
            self._j_table_config.setBuiltInDatabaseName(built_in_database_name)
        else:
            raise Exception("TableConfig.built_in_database_name should be a string value!")
