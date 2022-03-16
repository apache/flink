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

from pyflink.common import Configuration

__all__ = ['EnvironmentSettings']


class EnvironmentSettings(object):
    """
    Defines all parameters that initialize a table environment. Those parameters are used only
    during instantiation of a :class:`~pyflink.table.TableEnvironment` and cannot be changed
    afterwards.

    Example:
    ::

        >>> EnvironmentSettings.new_instance() \\
        ...     .in_streaming_mode() \\
        ...     .with_built_in_catalog_name("my_catalog") \\
        ...     .with_built_in_database_name("my_database") \\
        ...     .build()

    :func:`EnvironmentSettings.in_streaming_mode` or :func:`EnvironmentSettings.in_batch_mode`
    might be convenient as shortcuts.
    """

    class Builder(object):
        """
        A builder for :class:`EnvironmentSettings`.
        """

        def __init__(self):
            gateway = get_gateway()
            self._j_builder = gateway.jvm.EnvironmentSettings.Builder()

        def with_configuration(self, config: Configuration) -> 'EnvironmentSettings.Builder':
            """
            Creates the EnvironmentSetting with specified Configuration.

            :return: EnvironmentSettings.
            """
            self._j_builder = self._j_builder.withConfiguration(config._j_configuration)
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
            a :class:`~pyflink.table.TableEnvironment`.

            This catalog is an in-memory catalog that will be used to store all temporary objects
            (e.g. from :func:`~pyflink.table.TableEnvironment.create_temporary_view` or
            :func:`~pyflink.table.TableEnvironment.create_temporary_system_function`) that cannot
            be persisted because they have no serializable representation.

            It will also be the initial value for the current catalog which can be altered via
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
            created when instantiating a :class:`~pyflink.table.TableEnvironment`.

            This database is an in-memory database that will be used to store all temporary
            objects (e.g. from :func:`~pyflink.table.TableEnvironment.create_temporary_view` or
            :func:`~pyflink.table.TableEnvironment.create_temporary_system_function`) that cannot
            be persisted because they have no serializable representation.

            It will also be the initial value for the current catalog which can be altered via
            :func:`~pyflink.table.TableEnvironment.use_catalog`.

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

    def to_configuration(self) -> Configuration:
        """
        Convert to `pyflink.common.Configuration`.

        :return: Configuration with specified value.

        .. note:: Deprecated in 1.15. Please use
                :func:`EnvironmentSettings.get_configuration` instead.
        """
        return Configuration(j_configuration=self._j_environment_settings.toConfiguration())

    def get_configuration(self) -> Configuration:
        """
        Get the underlying `pyflink.common.Configuration`.

        :return: Configuration with specified value.
        """
        return Configuration(j_configuration=self._j_environment_settings.getConfiguration())

    @staticmethod
    def new_instance() -> 'EnvironmentSettings.Builder':
        """
        Creates a builder for creating an instance of EnvironmentSettings.

        :return: A builder of EnvironmentSettings.
        """
        return EnvironmentSettings.Builder()

    @staticmethod
    def from_configuration(config: Configuration) -> 'EnvironmentSettings':
        """
        Creates the EnvironmentSetting with specified Configuration.

        :return: EnvironmentSettings.

        .. note:: Deprecated in 1.15. Please use
                :func:`EnvironmentSettings.Builder.with_configuration` instead.
        """
        return EnvironmentSettings(
            get_gateway().jvm.EnvironmentSettings.fromConfiguration(config._j_configuration))

    @staticmethod
    def in_streaming_mode() -> 'EnvironmentSettings':
        """
        Creates a default instance of EnvironmentSettings in streaming execution mode.

        In this mode, both bounded and unbounded data streams can be processed.

        This method is a shortcut for creating a :class:`~pyflink.table.TableEnvironment` with
        little code. Use the builder provided in :func:`EnvironmentSettings.new_instance` for
        advanced settings.

        :return: EnvironmentSettings.
        """
        return EnvironmentSettings(
            get_gateway().jvm.EnvironmentSettings.inStreamingMode())

    @staticmethod
    def in_batch_mode() -> 'EnvironmentSettings':
        """
        Creates a default instance of EnvironmentSettings in batch execution mode.

        This mode is highly optimized for batch scenarios. Only bounded data streams can be
        processed in this mode.

        This method is a shortcut for creating a :class:`~pyflink.table.TableEnvironment` with
        little code. Use the builder provided in :func:`EnvironmentSettings.new_instance` for
        advanced settings.

        :return: EnvironmentSettings.
        """
        return EnvironmentSettings(
            get_gateway().jvm.EnvironmentSettings.inBatchMode())
