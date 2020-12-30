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
from pyflink.java_gateway import get_gateway

__all__ = ['EnvironmentSettings']


class EnvironmentSettings(object):
    """
    Defines all parameters that initialize a table environment. Those parameters are used only
    during instantiation of a :class:`~pyflink.table.TableEnvironment` and cannot be changed
    afterwards.

    Example:
    ::

        >>> EnvironmentSettings.new_instance() \\
        ...     .use_old_planner() \\
        ...     .in_streaming_mode() \\
        ...     .with_built_in_catalog_name("my_catalog") \\
        ...     .with_built_in_database_name("my_database") \\
        ...     .build()
    """

    class Builder(object):
        """
        A builder for :class:`EnvironmentSettings`.
        """

        def __init__(self):
            gateway = get_gateway()
            self._j_builder = gateway.jvm.EnvironmentSettings.Builder()

        def use_old_planner(self) -> 'EnvironmentSettings.Builder':
            """
            Sets the old Flink planner as the required module.

            This is the default behavior.

            :return: This object.
            """
            self._j_builder = self._j_builder.useOldPlanner()
            return self

        def use_blink_planner(self) -> 'EnvironmentSettings.Builder':
            """
            Sets the Blink planner as the required module. By default, :func:`use_old_planner` is
            enabled.

            :return: This object.
            """
            self._j_builder = self._j_builder.useBlinkPlanner()
            return self

        def use_any_planner(self) -> 'EnvironmentSettings.Builder':
            """
            Does not set a planner requirement explicitly.

            A planner will be discovered automatically, if there is only one planner available.

            By default, :func:`use_old_planner` is enabled.

            :return: This object.
            """
            self._j_builder = self._j_builder.useAnyPlanner()
            return self

        def in_batch_mode(self) -> 'EnvironmentSettings.Builder':
            """
            Sets that the components should work in a batch mode. Streaming mode by default.

            :return: This object.
            """
            self._j_builder = self._j_builder.inBatchMode()
            return self

        def in_streaming_mode(self) -> 'EnvironmentSettings.Builder':
            """
            Sets that the components should work in a streaming mode. Enabled by default.

            :return: This object.
            """
            self._j_builder = self._j_builder.inStreamingMode()
            return self

        def with_built_in_catalog_name(self, built_in_catalog_name: str) \
                -> 'EnvironmentSettings.Builder':
            """
            Specifies the name of the initial catalog to be created when instantiating
            a :class:`~pyflink.table.TableEnvironment`. This catalog will be used to store all
            non-serializable objects such as tables and functions registered via e.g.
            :func:`~pyflink.table.TableEnvironment.register_table_sink` or
            :func:`~pyflink.table.TableEnvironment.register_java_function`. It will also be the
            initial value for the current catalog which can be altered via
            :func:`~pyflink.table.TableEnvironment.use_catalog`.

            Default: "default_catalog".

            :param built_in_catalog_name: The specified built-in catalog name.
            :return: This object.
            """
            self._j_builder = self._j_builder.withBuiltInCatalogName(built_in_catalog_name)
            return self

        def with_built_in_database_name(self, built_in_database_name: str) \
                -> 'EnvironmentSettings.Builder':
            """
            Specifies the name of the default database in the initial catalog to be
            created when instantiating a :class:`~pyflink.table.TableEnvironment`. The database
            will be used to store all non-serializable objects such as tables and functions
            registered via e.g. :func:`~pyflink.table.TableEnvironment.register_table_sink` or
            :func:`~pyflink.table.TableEnvironment.register_java_function`. It will also be the
            initial value for the current database which can be altered via
            :func:`~pyflink.table.TableEnvironment.use_database`.

            Default: "default_database".

            :param built_in_database_name: The specified built-in database name.
            :return: This object.
            """
            self._j_builder = self._j_builder.withBuiltInDatabaseName(built_in_database_name)
            return self

        def build(self) -> 'EnvironmentSettings':
            """
            Returns an immutable instance of EnvironmentSettings.

            :return: an immutable instance of EnvironmentSettings.
            """
            return EnvironmentSettings(self._j_builder.build())

    def __init__(self, j_environment_settings):
        self._j_environment_settings = j_environment_settings

    def get_built_in_catalog_name(self) -> str:
        """
        Gets the specified name of the initial catalog to be created when instantiating a
        :class:`~pyflink.table.TableEnvironment`.

        :return: The specified name of the initial catalog to be created.
        """
        return self._j_environment_settings.getBuiltInCatalogName()

    def get_built_in_database_name(self) -> str:
        """
        Gets the specified name of the default database in the initial catalog to be created when
        instantiating a :class:`~pyflink.table.TableEnvironment`.

        :return: The specified name of the default database in the initial catalog to be created.
        """
        return self._j_environment_settings.getBuiltInDatabaseName()

    def is_streaming_mode(self) -> bool:
        """
        Tells if the :class:`~pyflink.table.TableEnvironment` should work in a batch or streaming
        mode.

        :return: True if the TableEnvironment should work in a streaming mode, false otherwise.
        """
        return self._j_environment_settings.isStreamingMode()

    @staticmethod
    def new_instance() -> 'EnvironmentSettings.Builder':
        """
        Creates a builder for creating an instance of EnvironmentSettings.

        By default, it does not specify a required planner and will use the one that is available
        on the classpath via discovery.

        :return: A builder of EnvironmentSettings.
        """
        return EnvironmentSettings.Builder()
