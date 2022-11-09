.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################

================
TableEnvironment
================

A table environment is the base class, entry point, and central context for creating Table
and SQL API programs.

EnvironmentSettings
-------------------

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

:func:`~EnvironmentSettings.in_streaming_mode` or :func:`~EnvironmentSettings.in_batch_mode`
might be convenient as shortcuts.

.. currentmodule:: pyflink.table.environment_settings

.. autosummary::
    :toctree: api/

    EnvironmentSettings.new_instance
    EnvironmentSettings.from_configuration
    EnvironmentSettings.in_streaming_mode
    EnvironmentSettings.in_batch_mode
    EnvironmentSettings.get_built_in_catalog_name
    EnvironmentSettings.get_built_in_database_name
    EnvironmentSettings.is_streaming_mode
    EnvironmentSettings.to_configuration
    EnvironmentSettings.get_configuration
    EnvironmentSettings.Builder.with_configuration
    EnvironmentSettings.Builder.in_batch_mode
    EnvironmentSettings.Builder.in_streaming_mode
    EnvironmentSettings.Builder.with_built_in_catalog_name
    EnvironmentSettings.Builder.with_built_in_database_name
    EnvironmentSettings.Builder.build

TableConfig
-----------

.. currentmodule:: pyflink.table.table_config

.. autosummary::
    :toctree: api/

    TableConfig

TableEnvironment
----------------

A table environment is the base class, entry point, and central context for creating Table
and SQL API programs.

It is unified for bounded and unbounded data processing.

A table environment is responsible for:

    - Connecting to external systems.
    - Registering and retrieving :class:`~pyflink.table.Table` and other meta objects from a
      catalog.
    - Executing SQL statements.
    - Offering further configuration options.

The path in methods such as :func:`create_temporary_view`
should be a proper SQL identifier. The syntax is following
[[catalog-name.]database-name.]object-name, where the catalog name and database are optional.
For path resolution see :func:`use_catalog` and :func:`use_database`. All keywords or other
special characters need to be escaped.

Example: `cat.1`.`db`.`Table` resolves to an object named 'Table' (table is a reserved
keyword, thus must be escaped) in a catalog named 'cat.1' and database named 'db'.

.. note::

    This environment is meant for pure table programs. If you would like to convert from or to
    other Flink APIs, it might be necessary to use one of the available language-specific table
    environments in the corresponding bridging modules.

.. currentmodule:: pyflink.table.table_environment

.. autosummary::
    :toctree: api/

    TableEnvironment.add_python_archive
    TableEnvironment.add_python_file
    TableEnvironment.create
    TableEnvironment.create_java_function
    TableEnvironment.create_java_temporary_function
    TableEnvironment.create_java_temporary_system_function
    TableEnvironment.create_statement_set
    TableEnvironment.create_table
    TableEnvironment.create_temporary_function
    TableEnvironment.create_temporary_system_function
    TableEnvironment.create_temporary_table
    TableEnvironment.create_temporary_view
    TableEnvironment.drop_function
    TableEnvironment.drop_temporary_function
    TableEnvironment.drop_temporary_system_function
    TableEnvironment.drop_temporary_table
    TableEnvironment.drop_temporary_view
    TableEnvironment.execute_sql
    TableEnvironment.explain_sql
    TableEnvironment.from_descriptor
    TableEnvironment.from_elements
    TableEnvironment.from_pandas
    TableEnvironment.from_path
    TableEnvironment.from_table_source
    TableEnvironment.get_catalog
    TableEnvironment.get_config
    TableEnvironment.get_current_catalog
    TableEnvironment.get_current_database
    TableEnvironment.list_catalogs
    TableEnvironment.list_databases
    TableEnvironment.list_full_modules
    TableEnvironment.list_functions
    TableEnvironment.list_modules
    TableEnvironment.list_tables
    TableEnvironment.list_temporary_tables
    TableEnvironment.list_temporary_views
    TableEnvironment.list_user_defined_functions
    TableEnvironment.list_views
    TableEnvironment.load_module
    TableEnvironment.register_catalog
    TableEnvironment.register_function
    TableEnvironment.register_java_function
    TableEnvironment.register_table
    TableEnvironment.register_table_sink
    TableEnvironment.register_table_source
    TableEnvironment.scan
    TableEnvironment.set_python_requirements
    TableEnvironment.sql_query
    TableEnvironment.unload_module
    TableEnvironment.use_catalog
    TableEnvironment.use_database
    TableEnvironment.use_modules

StreamTableEnvironment
----------------------

.. currentmodule:: pyflink.table.table_environment

.. autosummary::
    :toctree: api/

    StreamTableEnvironment.add_python_archive
    StreamTableEnvironment.add_python_file
    StreamTableEnvironment.create
    StreamTableEnvironment.create_java_function
    StreamTableEnvironment.create_java_temporary_function
    StreamTableEnvironment.create_java_temporary_system_function
    StreamTableEnvironment.create_statement_set
    StreamTableEnvironment.create_table
    StreamTableEnvironment.create_temporary_function
    StreamTableEnvironment.create_temporary_system_function
    StreamTableEnvironment.create_temporary_table
    StreamTableEnvironment.create_temporary_view
    StreamTableEnvironment.drop_function
    StreamTableEnvironment.drop_temporary_function
    StreamTableEnvironment.drop_temporary_system_function
    StreamTableEnvironment.drop_temporary_table
    StreamTableEnvironment.drop_temporary_view
    StreamTableEnvironment.execute_sql
    StreamTableEnvironment.explain_sql
    StreamTableEnvironment.from_descriptor
    StreamTableEnvironment.from_elements
    StreamTableEnvironment.from_pandas
    StreamTableEnvironment.from_path
    StreamTableEnvironment.from_table_source
    StreamTableEnvironment.from_data_stream
    StreamTableEnvironment.from_changelog_stream
    StreamTableEnvironment.get_catalog
    StreamTableEnvironment.get_config
    StreamTableEnvironment.get_current_catalog
    StreamTableEnvironment.get_current_database
    StreamTableEnvironment.list_catalogs
    StreamTableEnvironment.list_databases
    StreamTableEnvironment.list_full_modules
    StreamTableEnvironment.list_functions
    StreamTableEnvironment.list_modules
    StreamTableEnvironment.list_tables
    StreamTableEnvironment.list_temporary_tables
    StreamTableEnvironment.list_temporary_views
    StreamTableEnvironment.list_user_defined_functions
    StreamTableEnvironment.list_views
    StreamTableEnvironment.load_module
    StreamTableEnvironment.register_catalog
    StreamTableEnvironment.register_function
    StreamTableEnvironment.register_java_function
    StreamTableEnvironment.register_table
    StreamTableEnvironment.register_table_sink
    StreamTableEnvironment.register_table_source
    StreamTableEnvironment.scan
    StreamTableEnvironment.set_python_requirements
    StreamTableEnvironment.sql_query
    StreamTableEnvironment.to_data_stream
    StreamTableEnvironment.to_changelog_stream
    StreamTableEnvironment.to_append_stream
    StreamTableEnvironment.to_retract_stream
    StreamTableEnvironment.unload_module
    StreamTableEnvironment.use_catalog
    StreamTableEnvironment.use_database
    StreamTableEnvironment.use_modules
