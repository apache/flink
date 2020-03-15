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

from py4j.compat import long

from pyflink.common import Configuration
from pyflink.common.dependency_manager import DependencyManager
from pyflink.java_gateway import get_gateway
from pyflink.table import SqlDialect

__all__ = ['TableConfig']


class TableConfig(object):
    """
    Configuration for the current :class:`TableEnvironment` session to adjust Table & SQL API
    programs.

    For common or important configuration options, this class provides getters and setters methods
    with detailed inline documentation.

    For more advanced configuration, users can directly access the underlying key-value map via
    :func:`~pyflink.table.TableConfig.get_configuration`. Currently, key-value options are only
    supported for the Blink planner.

    .. note::

        Because options are read at different point in time when performing operations, it is
        recommended to set configuration options early after instantiating a table environment.
    """

    def __init__(self, j_table_config=None):
        gateway = get_gateway()
        if j_table_config is None:
            self._j_table_config = gateway.jvm.TableConfig()
        else:
            self._j_table_config = j_table_config

    def get_local_timezone(self):
        """
        Returns the local timezone id for timestamp with local time zone, either an abbreviation
        such as "PST", a full name such as "America/Los_Angeles", or a custom timezone_id such
        as "GMT-8:00".
        """
        return self._j_table_config.getLocalTimeZone().getId()

    def set_local_timezone(self, timezone_id):
        """
        Sets the local timezone id for timestamp with local time zone.

        :param timezone_id: The timezone id, either an abbreviation such as "PST", a full name
                            such as "America/Los_Angeles", or a custom timezone_id such as
                            "GMT-8:00".
        """
        if timezone_id is not None and isinstance(timezone_id, str):
            j_timezone = get_gateway().jvm.java.time.ZoneId.of(timezone_id)
            self._j_table_config.setLocalTimeZone(j_timezone)
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

    def set_idle_state_retention_time(self, min_time, max_time):
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

        :param min_time: The minimum time interval for which idle state is retained. Set to
                         0 (zero) to never clean-up the state.
        :type min_time: datetime.timedelta
        :param max_time: The maximum time interval for which idle state is retained. Must be at
                         least 5 minutes greater than minTime. Set to
                         0 (zero) to never clean-up the state.
        :type max_time: datetime.timedelta
        """
        j_time_class = get_gateway().jvm.org.apache.flink.api.common.time.Time
        j_min_time = j_time_class.milliseconds(long(round(min_time.total_seconds() * 1000)))
        j_max_time = j_time_class.milliseconds(long(round(max_time.total_seconds() * 1000)))
        self._j_table_config.setIdleStateRetentionTime(j_min_time, j_max_time)

    def get_min_idle_state_retention_time(self):
        """
        State might be cleared and removed if it was not updated for the defined period of time.

        :return: The minimum time until state which was not updated will be retained.
        :rtype: int
        """
        return self._j_table_config.getMinIdleStateRetentionTime()

    def get_max_idle_state_retention_time(self):
        """
        State will be cleared and removed if it was not updated for the defined period of time.

        :return: The maximum time until state which was not updated will be retained.
        :rtype: int
        """
        return self._j_table_config.getMaxIdleStateRetentionTime()

    def set_decimal_context(self, precision, rounding_mode):
        """
        Sets the default context for decimal division calculation.
        (precision=34, rounding_mode=HALF_EVEN) by default.

        The precision is the number of digits to be used for an operation. A value of 0 indicates
        that unlimited precision (as many digits as are required) will be used. Note that leading
        zeros (in the coefficient of a number) are never significant.

        The rounding mode is the rounding algorithm to be used for an operation. It could be:

        **UP**, **DOWN**, **CEILING**, **FLOOR**, **HALF_UP**, **HALF_DOWN**, **HALF_EVEN**,
        **UNNECESSARY**

        The table below shows the results of rounding input to one digit with the given rounding
        mode:

        +-------+----+------+---------+-------+---------+-----------+-----------+-------------+
        | Input | UP | DOWN | CEILING | FLOOR | HALF_UP | HALF_DOWN | HALF_EVEN | UNNECESSARY |
        +=======+====+======+=========+=======+=========+===========+===========+=============+
        | 5.5   |  6 |   5  |    6    |   5   |    6    |     5     |     6     |  Exception  |
        +-------+----+------+---------+-------+---------+-----------+-----------+-------------+
        | 2.5   |  3 |   2  |    3    |   2   |    3    |     2     |     2     |  Exception  |
        +-------+----+------+---------+-------+---------+-----------+-----------+-------------+
        | 1.6   |  2 |   1  |    2    |   1   |    2    |     2     |     2     |  Exception  |
        +-------+----+------+---------+-------+---------+-----------+-----------+-------------+
        | 1.1   |  2 |   1  |    2    |   1   |    1    |     1     |     1     |  Exception  |
        +-------+----+------+---------+-------+---------+-----------+-----------+-------------+
        | 1.0   |  1 |   1  |    1    |   1   |    1    |     1     |     1     |      1      |
        +-------+----+------+---------+-------+---------+-----------+-----------+-------------+
        | -1.0  | -1 |  -1  |   -1    |  -1   |   -1    |    -1     |    -1     |     -1      |
        +-------+----+------+---------+-------+---------+-----------+-----------+-------------+
        | -1.1  | -2 |  -1  |   -1    |  -2   |   -1    |    -1     |    -1     |  Exception  |
        +-------+----+------+---------+-------+---------+-----------+-----------+-------------+
        | -1.6  | -2 |  -1  |   -1    |  -2   |   -2    |    -2     |    -2     |  Exception  |
        +-------+----+------+---------+-------+---------+-----------+-----------+-------------+
        | 2.5   | -3 |  -2  |   -2    |  -3   |   -3    |    -2     |    -2     |  Exception  |
        +-------+----+------+---------+-------+---------+-----------+-----------+-------------+
        | 5.5   | -6 |  -5  |   -5    |  -6   |   -6    |    -5     |    -6     |  Exception  |
        +-------+----+------+---------+-------+---------+-----------+-----------+-------------+

        :param precision: The precision of the decimal context.
        :type precision: int
        :param rounding_mode: The rounding mode of the decimal context.
        :type rounding_mode: str
        """
        if rounding_mode not in (
                "UP",
                "DOWN",
                "CEILING",
                "FLOOR",
                "HALF_UP",
                "HALF_DOWN",
                "HALF_EVEN",
                "UNNECESSARY"):
            raise ValueError("Unsupported rounding_mode: %s" % rounding_mode)
        gateway = get_gateway()
        j_rounding_mode = getattr(gateway.jvm.java.math.RoundingMode, rounding_mode)
        j_math_context = gateway.jvm.java.math.MathContext(precision, j_rounding_mode)
        self._j_table_config.setDecimalContext(j_math_context)

    def get_decimal_context(self):
        """
        Returns current context for decimal division calculation,
        (precision=34, rounding_mode=HALF_EVEN) by default.

        .. seealso:: :func:`set_decimal_context`

        :return: the current context for decimal division calculation.
        :rtype: (int, str)
        """
        j_math_context = self._j_table_config.getDecimalContext()
        precision = j_math_context.getPrecision()
        rounding_mode = j_math_context.getRoundingMode().name()
        return precision, rounding_mode

    def get_configuration(self):
        """
        Gives direct access to the underlying key-value map for advanced configuration.

        :return: Entire key-value configuration.
        :rtype: Configuration
        """
        return Configuration(j_configuration=self._j_table_config.getConfiguration())

    def add_configuration(self, configuration):
        """
        Adds the given key-value configuration to the underlying configuration. It overwrites
        existing keys.

        :param configuration: Key-value configuration to be added.
        :type configuration: Configuration
        """
        self._j_table_config.addConfiguration(configuration._j_configuration)

    def get_sql_dialect(self):
        """
        Returns the current SQL dialect.

        :rtype: SqlDialect
        """
        return SqlDialect._from_j_sql_dialect(self._j_table_config.getSqlDialect())

    def set_sql_dialect(self, sql_dialect):
        """
        Sets the current SQL dialect to parse a SQL query. Flink's SQL behavior by default.

        :param sql_dialect: The given SQL dialect.
        :type sql_dialect: SqlDialect
        """
        self._j_table_config.setSqlDialect(SqlDialect._to_j_sql_dialect(sql_dialect))

    def set_python_executable(self, python_exec):
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
            is running on and that the python version must be 3.5 or higher.

        .. note::

            The python udf worker depends on Apache Beam (version == 2.15.0).
            Please ensure that the specified environment meets the above requirements.

        :param python_exec: The path of python interpreter.
        :type python_exec: str

        .. versionadded:: 1.10.0
        """
        self.get_configuration().set_string(DependencyManager.PYTHON_EXEC, python_exec)

    def get_python_executable(self):
        """
        Gets the path of the python interpreter which is used to execute the python udf workers.
        If no path is specified before, it will return a None value.

        :return: The path of the python interpreter which is used to execute the python udf workers.
        :rtype: str

        .. versionadded:: 1.10.0
        """
        return self.get_configuration().get_string(DependencyManager.PYTHON_EXEC, None)

    @staticmethod
    def get_default():
        """
        :return: A TableConfig object with default settings.
        :rtype: TableConfig
        """
        return TableConfig(get_gateway().jvm.TableConfig.getDefault())
