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

    def is_stream(self):
        """
        Configures execution mode, "true" for streaming and "false" for batch.
        """
        return self._is_stream

    def _set_stream(self, is_stream):
        self._is_stream = is_stream

    def parallelism(self):
        """
        The parallelism for all operations.
        """
        return self._parallelism

    def _set_parallelism(self, parallelism):
        self._parallelism = parallelism

    def timezone(self):
        """
        Returns the timezone id, either an abbreviation such as "PST", a full name such as
        "America/Los_Angeles", or a custom timezone_id such as "GMT-8:00".
        """
        return self._j_table_config.getTimeZone().getID()

    def _set_timezone(self, timezone_id):
        if timezone_id is not None and isinstance(timezone_id, (str, unicode)):
            j_timezone = self._jvm.java.util.TimeZone.getTimeZone(timezone_id)
            self._j_table_config.setTimeZone(j_timezone)
        else:
            raise Exception("TableConfig.timezone should be a string!")

    def null_check(self):
        """
        A boolean value, "True" enables NULL check and "False" disables NULL check.
        """
        return self._j_table_config.getNullCheck()

    def _set_null_check(self, null_check):
        if null_check is not None and isinstance(null_check, bool):
            self._j_table_config.setNullCheck(null_check)
        else:
            raise Exception("TableConfig.null_check should be a bool value!")

    def max_generated_code_length(self):
        """
        The current threshold where generated code will be split into sub-function calls. Java has
        a maximum method length of 64 KB. This setting allows for finer granularity if necessary.
        Default is 64000.
        """
        return self._j_table_config.getMaxGeneratedCodeLength()

    def _set_max_generated_code_length(self, max_generated_code_length):
        if max_generated_code_length is not None and isinstance(max_generated_code_length, int):
            self._j_table_config.setMaxGeneratedCodeLength(max_generated_code_length)
        else:
            raise Exception("TableConfig.max_generated_code_length should be a int value!")

    class Builder(object):

        def __init__(self):
            self._is_stream = None  # type: bool
            self._parallelism = None  # type: int
            self._timezone_id = None  # type: str
            self._null_check = None  # type: bool
            self._max_generated_code_length = None  # type: int

        def as_streaming_execution(self):
            """
            Configures streaming execution mode.
            If this method is called, :class:`StreamTableEnvironment` will be created.

            :return: :class:`TableConfig.Builder`
            """
            self._is_stream = True
            return self

        def as_batch_execution(self):
            """
            Configures batch execution mode.
            If this method is called, :class:`BatchTableEnvironment` will be created.

            :return: :class:`TableConfig.Builder`
            """
            self._is_stream = False
            return self

        def set_parallelism(self, parallelism):
            """
            Sets the parallelism for all operations.

            :param parallelism: The parallelism.
            :return: :class:`TableConfig.Builder`
            """
            self._parallelism = parallelism
            return self

        def set_timezone(self, time_zone_id):
            """
            Sets the timezone for date/time/timestamp conversions.

            :param time_zone_id: The time zone ID in string format, either an abbreviation such as
                                 "PST", a full name such as "America/Los_Angeles", or a custom ID
                                 such as "GMT-8:00".
            :return: :class:`TableConfig.Builder`
            """
            self._timezone_id = time_zone_id
            return self

        def set_null_check(self, null_check):
            """
            Sets the NULL check. If enabled, all fields need to be checked for NULL first.

            :param null_check: A boolean value, "True" enables NULL check and "False" disables
                               NULL check.
            :return: :class:`TableConfig.Builder`
            """
            self._null_check = null_check
            return self

        def set_max_generated_code_length(self, max_length):
            """
            Sets the current threshold where generated code will be split into sub-function calls.
            Java has a maximum method length of 64 KB. This setting allows for finer granularity if
            necessary. Default is 64000.

            :param max_length: The maximum method length of generated java code.
            :return: :class:`TableConfig.Builder`
            """
            self._max_generated_code_length = max_length
            return self

        def build(self):
            """
            Builds :class:`TableConfig` object.

            :return: TableConfig
            """
            config = TableConfig()
            if self._parallelism is not None:
                config._set_parallelism(self._parallelism)
            if self._is_stream is not None:
                config._set_stream(self._is_stream)
            if self._timezone_id is not None:
                config._set_timezone(self._timezone_id)
            if self._null_check is not None:
                config._set_null_check(self._null_check)
            if self._max_generated_code_length is not None:
                config._set_max_generated_code_length(self._max_generated_code_length)
            return config
