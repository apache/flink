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
import datetime

from py4j.compat import long

from pyflink.common.configuration import Configuration
from pyflink.java_gateway import get_gateway
from pyflink.table.sql_dialect import SqlDialect

__all__ = ['TableConfig']

from pyflink.util.java_utils import add_jars_to_context_class_loader


class TableConfig(object):
    """
    Configuration for the current :class:`TableEnvironment` session to adjust Table & SQL API
    programs.

    This class is a pure API class that abstracts configuration from various sources. Currently,
    configuration can be set in any of the following layers (in the given order):

    - flink-conf.yaml
    - CLI parameters
    - :class:`~pyflink.datastream.StreamExecutionEnvironment` when bridging to DataStream API
    - :func:`~EnvironmentSettings.Builder.with_configuration`
    - :func:`~TableConfig.set`

    The latter two represent the application-specific part of the configuration. They initialize
    and directly modify :func:`~TableConfig.get_configuration`. Other layers represent the
    configuration of the execution context and are immutable.

    The getter :func:`~TableConfig.get` gives read-only access to the full configuration. However,
    application-specific configuration has precedence. Configuration of outer layers is used for
    defaults and fallbacks. The setter :func:`~TableConfig.set` will only affect
    application-specific configuration.

    For common or important configuration options, this class provides getters and setters methods
    with detailed inline documentation.

    For more advanced configuration, users can directly access the underlying key-value map via
    :func:`~pyflink.table.TableConfig.get_configuration`.

    Example:
    ::

        >>> table_config = t_env.get_config()
        >>> config = Configuration()
        >>> config.set_string("parallelism.default", "128") \\
        ...       .set_string("pipeline.auto-watermark-interval", "800ms") \\
        ...       .set_string("execution.checkpointing.interval", "30s")
        >>> table_config.add_configuration(config)

    .. note::

        Because options are read at different point in time when performing operations, it is
        recommended to set configuration options early after instantiating a table environment.
    """

    def __init__(self, j_table_config=None):
        gateway = get_gateway()
        if j_table_config is None:
            self._j_table_config = gateway.jvm.TableConfig.getDefault()
        else:
            self._j_table_config = j_table_config

    def get(self, key: str, default_value: str) -> str:
        """
        Returns the value associated with the given key as a string.

        :param key: The key pointing to the associated value.
        :param default_value: The default value which is returned in case there is no value
                              associated with the given key.
        :return: The (default) value associated with the given key.
        """
        if self.get_configuration().contains_key(key):
            return self.get_configuration().get_string(key, default_value)
        else:
            return self._j_table_config.getRootConfiguration().getString(key, default_value)

    def set(self, key: str, value: str) -> 'TableConfig':
        """
        Sets a string-based value for the given string-based key.

        The value will be parsed by the framework on access.
        """
        self._j_table_config.set(key, value)

        jvm = get_gateway().jvm
        jars_key = jvm.org.apache.flink.configuration.PipelineOptions.JARS.key()
        classpaths_key = jvm.org.apache.flink.configuration.PipelineOptions.CLASSPATHS.key()
        if key in [jars_key, classpaths_key]:
            add_jars_to_context_class_loader(value.split(";"))

        return self

    def get_local_timezone(self) -> str:
        """
        Returns the local timezone id for timestamp with local time zone, either an abbreviation
        such as "PST", a full name such as "America/Los_Angeles", or a custom timezone_id such
        as "GMT-08:00".
        """
        return self._j_table_config.getLocalTimeZone().getId()

    def set_local_timezone(self, timezone_id: str):
        """
        Sets the local timezone id for timestamp with local time zone.

        :param timezone_id: The timezone id, either an abbreviation such as "PST", a full name
                            such as "America/Los_Angeles", or a custom timezone_id such as
                            "GMT-08:00".
        """
        if timezone_id is not None and isinstance(timezone_id, str):
            j_timezone = get_gateway().jvm.java.time.ZoneId.of(timezone_id)
            self._j_table_config.setLocalTimeZone(j_timezone)
        else:
            raise Exception("TableConfig.timezone should be a string!")

    def get_max_generated_code_length(self) -> int:
        """
        The current threshold where generated code will be split into sub-function calls. Java has
        a maximum method length of 64 KB. This setting allows for finer granularity if necessary.
        Default is 64000.
        """
        return self._j_table_config.getMaxGeneratedCodeLength()

    def set_max_generated_code_length(self, max_generated_code_length: int):
        """
        Returns the current threshold where generated code will be split into sub-function calls.
        Java has a maximum method length of 64 KB. This setting allows for finer granularity if
        necessary. Default is 64000.
        """
        if max_generated_code_length is not None and isinstance(max_generated_code_length, int):
            self._j_table_config.setMaxGeneratedCodeLength(max_generated_code_length)
        else:
            raise Exception("TableConfig.max_generated_code_length should be a int value!")

    def set_idle_state_retention_time(self,
                                      min_time: datetime.timedelta,
                                      max_time: datetime.timedelta):
        """
        Specifies a minimum and a maximum time interval for how long idle state, i.e., state which
        was not updated, will be retained.

        State will never be cleared until it was idle for less than the minimum time and will never
        be kept if it was idle for more than the maximum time.

        When new data arrives for previously cleaned-up state, the new data will be handled as if it
        was the first data. This can result in previous results being overwritten.

        Set to 0 (zero) to never clean-up the state.

        Example:
        ::

            >>> table_config = TableConfig() \\
            ...     .set_idle_state_retention_time(datetime.timedelta(days=1),
            ...                                    datetime.timedelta(days=3))

        .. note::

            Cleaning up state requires additional bookkeeping which becomes less expensive for
            larger differences of minTime and maxTime. The difference between minTime and maxTime
            must be at least 5 minutes.

            Method set_idle_state_retention_time is deprecated now. The suggested way to set idle
            state retention time is :func:`~pyflink.table.TableConfig.set_idle_state_retention`
            Currently, setting max_time will not work and the max_time is directly derived from the
            min_time as 1.5 x min_time.

        :param min_time: The minimum time interval for which idle state is retained. Set to
                         0 (zero) to never clean-up the state.
        :param max_time: The maximum time interval for which idle state is retained. Must be at
                         least 5 minutes greater than minTime. Set to
                         0 (zero) to never clean-up the state.
        """
        j_time_class = get_gateway().jvm.org.apache.flink.api.common.time.Time
        j_min_time = j_time_class.milliseconds(long(round(min_time.total_seconds() * 1000)))
        j_max_time = j_time_class.milliseconds(long(round(max_time.total_seconds() * 1000)))
        self._j_table_config.setIdleStateRetentionTime(j_min_time, j_max_time)

    def set_idle_state_retention(self, duration: datetime.timedelta):
        """
        Specifies a retention time interval for how long idle state, i.e., state which
        was not updated, will be retained.

        State will never be cleared until it was idle for less than the duration and will never
        be kept if it was idle for more than the 1.5 x duration.

        When new data arrives for previously cleaned-up state, the new data will be handled as if it
        was the first data. This can result in previous results being overwritten.

        Set to 0 (zero) to never clean-up the state.

        Example:
        ::

            >>> table_config.set_idle_state_retention(datetime.timedelta(days=1))

        .. note::

            Cleaning up state requires additional bookkeeping which becomes less expensive for
            larger differences of minTime and maxTime. The difference between minTime and maxTime
            must be at least 5 minutes.

        :param duration: The retention time interval for which idle state is retained. Set to
                         0 (zero) to never clean-up the state.
        """
        j_duration_class = get_gateway().jvm.java.time.Duration
        j_duration = j_duration_class.ofMillis(long(round(duration.total_seconds() * 1000)))
        self._j_table_config.setIdleStateRetention(j_duration)

    def get_min_idle_state_retention_time(self) -> int:
        """
        State might be cleared and removed if it was not updated for the defined period of time.

        .. note::

            Currently the concept of min/max idle state retention has been deprecated and only
            idle state retention time is supported. The min idle state retention is regarded as
            idle state retention and the max idle state retention is derived from idle state
            retention as 1.5 x idle state retention.

        :return: The minimum time until state which was not updated will be retained.
        """
        return self._j_table_config.getMinIdleStateRetentionTime()

    def get_max_idle_state_retention_time(self) -> int:
        """
        State will be cleared and removed if it was not updated for the defined period of time.

        .. note::

            Currently the concept of min/max idle state retention has been deprecated and only
            idle state retention time is supported. The min idle state retention is regarded as
            idle state retention and the max idle state retention is derived from idle state
            retention as 1.5 x idle state retention.

        :return: The maximum time until state which was not updated will be retained.
        """
        return self._j_table_config.getMaxIdleStateRetentionTime()

    def get_idle_state_retention(self) -> datetime.timedelta:
        """

        :return: The duration until state which was not updated will be retained.
        """
        return datetime.timedelta(
            milliseconds=self._j_table_config.getIdleStateRetention().toMillis())

    def get_configuration(self) -> Configuration:
        """
        Gives direct access to the underlying key-value map for advanced configuration.

        :return: Entire key-value configuration.
        """
        return Configuration(j_configuration=self._j_table_config.getConfiguration())

    def add_configuration(self, configuration: Configuration):
        """
        Adds the given key-value configuration to the underlying configuration. It overwrites
        existing keys.

        :param configuration: Key-value configuration to be added.
        """
        self._j_table_config.addConfiguration(configuration._j_configuration)

    def get_sql_dialect(self) -> SqlDialect:
        """
        Returns the current SQL dialect.

        """
        return SqlDialect._from_j_sql_dialect(self._j_table_config.getSqlDialect())

    def set_sql_dialect(self, sql_dialect: SqlDialect):
        """
        Sets the current SQL dialect to parse a SQL query. Flink's SQL behavior by default.

        :param sql_dialect: The given SQL dialect.
        """
        self._j_table_config.setSqlDialect(SqlDialect._to_j_sql_dialect(sql_dialect))

    def set_python_executable(self, python_exec: str):
        """
        Sets the path of the python interpreter which is used to execute the python udf workers.

        e.g. "/usr/local/bin/python3".

        If python UDF depends on a specific python version which does not exist in the cluster,
        the method :func:`pyflink.table.TableEnvironment.add_python_archive` can be used to upload
        a virtual environment. The path of the python interpreter contained in the uploaded
        environment can be specified via this method.

        Example:
        ::

            # command executed in shell
            # assume that the relative path of python interpreter is py_env/bin/python
            $ zip -r py_env.zip py_env

            # python code
            >>> table_env.add_python_archive("py_env.zip")
            >>> table_env.get_config().set_python_executable("py_env.zip/py_env/bin/python")

        .. note::

            Please make sure the uploaded python environment matches the platform that the cluster
            is running on and that the python version must be 3.7 or higher.

        .. note::

            The python udf worker depends on Apache Beam (version == 2.43.0).
            Please ensure that the specified environment meets the above requirements.

        :param python_exec: The path of python interpreter.

        .. versionadded:: 1.10.0
        """
        jvm = get_gateway().jvm
        self.set(jvm.PythonOptions.PYTHON_EXECUTABLE.key(), python_exec)

    def get_python_executable(self) -> str:
        """
        Gets the path of the python interpreter which is used to execute the python udf workers.
        If no path is specified before, it will return a None value.

        :return: The path of the python interpreter which is used to execute the python udf workers.

        .. versionadded:: 1.10.0
        """
        jvm = get_gateway().jvm
        return self.get_configuration().get_string(jvm.PythonOptions.PYTHON_EXECUTABLE.key(), None)

    @staticmethod
    def get_default() -> 'TableConfig':
        """
        :return: A TableConfig object with default settings.
        """
        return TableConfig(get_gateway().jvm.TableConfig.getDefault())
