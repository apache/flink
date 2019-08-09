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
import os
import tempfile
from abc import ABCMeta, abstractmethod

from py4j.java_gateway import get_java_class

from pyflink.serializers import BatchedSerializer, PickleSerializer
from pyflink.table.catalog import Catalog
from pyflink.table.table_config import TableConfig
from pyflink.table.descriptors import (StreamTableDescriptor, ConnectorDescriptor,
                                       BatchTableDescriptor)

from pyflink.java_gateway import get_gateway
from pyflink.table import Table
from pyflink.table.types import _to_java_type, _create_type_verifier, RowType, DataType, \
    _infer_schema_from_data, _create_converter
from pyflink.util import utils

__all__ = [
    'BatchTableEnvironment',
    'StreamTableEnvironment',
    'TableEnvironment'
]


class TableEnvironment(object):
    """
    The abstract base class for batch and stream TableEnvironments.
    """

    __metaclass__ = ABCMeta

    def __init__(self, j_tenv, serializer=PickleSerializer()):
        self._j_tenv = j_tenv
        self._serializer = serializer

    def from_table_source(self, table_source):
        """
        Creates a table from a table source.

        Example:
        ::

            >>> csv_table_source = CsvTableSource(
            ...     csv_file_path, ['a', 'b'], [DataTypes.STRING(), DataTypes.BIGINT()])
            >>> table_env.from_table_source(csv_table_source)

        :param table_source: The table source used as table.
        :return: The result :class:`Table`.
        """
        return Table(self._j_tenv.fromTableSource(table_source._j_table_source))

    def register_catalog(self, catalog_name, catalog):
        """
        Registers a :class:`pyflink.table.catalog.Catalog` under a unique name.
        All tables registered in the :class:`pyflink.table.catalog.Catalog` can be accessed.

        :param catalog_name: The name under which the catalog will be registered.
        :param catalog: The :class:`pyflink.table.catalog.Catalog` to register.
        """
        self._j_tenv.registerCatalog(catalog_name, catalog._j_catalog)

    def get_catalog(self, catalog_name):
        """
        Gets a registered :class:`pyflink.table.catalog.Catalog` by name.

        :param catalog_name: The name to look up the :class:`pyflink.table.catalog.Catalog`.
        :return: The requested :class:`pyflink.table.catalog.Catalog`, None if there is no
                 registered catalog with given name.
        """
        catalog = self._j_tenv.getCatalog(catalog_name)
        if catalog.isPresent():
            return Catalog._get(catalog.get())
        else:
            return None

    def register_table(self, name, table):
        """
        Registers a :class:`Table` under a unique name in the TableEnvironment's catalog.
        Registered tables can be referenced in SQL queries.

        Example:
        ::

            >>> tab = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['a', 'b'])
            >>> table_env.register_table("source", tab)

        :param name: The name under which the table will be registered.
        :param table: The table to register.
        """
        self._j_tenv.registerTable(name, table._j_table)

    def register_table_source(self, name, table_source):
        """
        Registers an external :class:`TableSource` in this :class:`TableEnvironment`'s catalog.
        Registered tables can be referenced in SQL queries.

        Example:
        ::

            >>> table_env.register_table_source("source",
            ...                                 CsvTableSource("./1.csv",
            ...                                                ["a", "b"],
            ...                                                [DataTypes.INT(),
            ...                                                 DataTypes.STRING()]))

        :param name: The name under which the :class:`TableSource` is registered.
        :param table_source: The :class:`TableSource` to register.
        """
        self._j_tenv.registerTableSource(name, table_source._j_table_source)

    def register_table_sink(self, name, table_sink):
        """
        Registers an external :class:`TableSink` with given field names and types in this
        :class:`TableEnvironment`'s catalog.
        Registered sink tables can be referenced in SQL DML statements.

        Example:
        ::

            >>> table_env.register_table_sink("sink",
            ...                               CsvTableSink(["a", "b"],
            ...                                            [DataTypes.INT(),
            ...                                             DataTypes.STRING()],
            ...                                            "./2.csv"))

        :param name: The name under which the :class:`TableSink` is registered.
        :param table_sink: The :class:`TableSink` to register.
        """
        self._j_tenv.registerTableSink(name, table_sink._j_table_sink)

    def scan(self, *table_path):
        """
        Scans a registered table and returns the resulting :class:`Table`.
        A table to scan must be registered in the TableEnvironment. It can be either directly
        registered or be an external member of a :class:`pyflink.table.catalog.Catalog`.

        See the documentation of :func:`~pyflink.table.TableEnvironment.use_database` or
        :func:`~pyflink.table.TableEnvironment.use_catalog` for the rules on the path resolution.

        Examples:

        Scanning a directly registered table
        ::

            >>> tab = table_env.scan("tableName")

        Scanning a table from a registered catalog
        ::

            >>> tab = table_env.scan("catalogName", "dbName", "tableName")

        :param table_path: The path of the table to scan.
        :throws: Exception if no table is found using the given table path.
        :return: The resulting :class:`Table`
        """
        gateway = get_gateway()
        j_table_paths = utils.to_jarray(gateway.jvm.String, table_path)
        j_table = self._j_tenv.scan(j_table_paths)
        return Table(j_table)

    def insert_into(self, table, table_path, *table_path_continued):
        """
        Writes the :class:`Table` to a :class:`TableSink` that was registered under
        the specified name. For the path resolution algorithm see
        :func:`~TableEnvironment.use_database`.

        Example:
        ::

            >>> tab = table_env.scan("tableName")
            >>> table_env.insert_into(tab, "sink")

        :param table: :class:`Table` to write to the sink.
        :param table_path: The first part of the path of the registered :class:`TableSink` to which
               the :class:`Table` is written. This is to ensure at least the name of the
               :class:`Table` is provided.
        :param table_path_continued: The remaining part of the path of the registered
                :class:`TableSink` to which the :class:`Table`  is written.
        """
        gateway = get_gateway()
        j_table_path = utils.to_jarray(gateway.jvm.String, table_path_continued)
        self._j_tenv.insertInto(table._j_table, table_path, j_table_path)

    def list_catalogs(self):
        """
        Gets the names of all catalogs registered in this environment.

        :return: List of catalog names.
        """
        j_catalog_name_array = self._j_tenv.listCatalogs()
        return [item for item in j_catalog_name_array]

    def list_databases(self):
        """
        Gets the names of all databases in the current catalog.

        :return: List of database names in the current catalog.
        """
        j_database_name_array = self._j_tenv.listDatabases()
        return [item for item in j_database_name_array]

    def list_tables(self):
        """
        Gets the names of all tables in the current database of the current catalog.

        :return: List of table names in the current database of the current catalog.
        """
        j_table_name_array = self._j_tenv.listTables()
        return [item for item in j_table_name_array]

    def list_user_defined_functions(self):
        """
        Gets the names of all user defined functions registered in this environment.

        :return: List of the names of all user defined functions registered in this environment.
        :rtype: list[str]
        """
        j_udf_name_array = self._j_tenv.listUserDefinedFunctions()
        return [item for item in j_udf_name_array]

    def explain(self, table=None, extended=False):
        """
        Returns the AST of the specified Table API and SQL queries and the execution plan to compute
        the result of the given :class:`Table` or multi-sinks plan.

        :param table: The table to be explained. If table is None, explain for multi-sinks plan,
                      else for given table.
        :param extended: If the plan should contain additional properties.
                         e.g. estimated cost, traits
        :return: The table for which the AST and execution plan will be returned.
        """
        if table is None:
            return self._j_tenv.explain(extended)
        else:
            return self._j_tenv.explain(table._j_table, extended)

    def sql_query(self, query):
        """
        Evaluates a SQL query on registered tables and retrieves the result as a :class:`Table`.

        All tables referenced by the query must be registered in the TableEnvironment.

        A :class:`Table` is automatically registered when its :func:`~Table.__str__` method is
        called, for example when it is embedded into a String.

        Hence, SQL queries can directly reference a :class:`Table` as follows:
        ::

            >>> table = ...
            # the table is not registered to the table environment
            >>> table_env.sql_query("SELECT * FROM %s" % table)

        :param query: The sql query string.
        :return: The result :class:`Table`.
        """
        j_table = self._j_tenv.sqlQuery(query)
        return Table(j_table)

    def sql_update(self, stmt):
        """
        Evaluates a SQL statement such as INSERT, UPDATE or DELETE or a DDL statement

        .. note::

            Currently only SQL INSERT statements and CREATE TABLE statements are supported.

        All tables referenced by the query must be registered in the TableEnvironment.
        A :class:`Table` is automatically registered when its :func:`~Table.__str__` method is
        called, for example when it is embedded into a String.
        Hence, SQL queries can directly reference a :class:`Table` as follows:
        ::

            # register the table sink into which the result is inserted.
            >>> table_env.register_table_sink("sink_table", table_sink)
            >>> source_table = ...
            # source_table is not registered to the table environment
            >>> table_env.sql_update("INSERT INTO sink_table SELECT * FROM %s" % source_table)

        A DDL statement can also be executed to create/drop a table:
        For example, the below DDL statement would create a CSV table named `tbl1`
        into the current catalog::

            create table tbl1(
                a int,
                b bigint,
                c varchar
            ) with (
                'connector.type' = 'filesystem',
                'format.type' = 'csv',
                'connector.path' = 'xxx'
            )

        SQL queries can directly execute as follows:
        ::

            >>> source_ddl = \\
            ... '''
            ... create table sourceTable(
            ...     a int,
            ...     b varchar
            ... ) with (
            ...     'connector.type' = 'kafka',
            ...     'update-mode' = 'append',
            ...     'connector.topic' = 'xxx',
            ...     'connector.properties.0.key' = 'k0',
            ...     'connector.properties.0.value' = 'v0'
            ... )
            ... '''

            >>> sink_ddl = \\
            ... '''
            ... create table sinkTable(
            ...     a int,
            ...     b varchar
            ... ) with (
            ...     'connector.type' = 'filesystem',
            ...     'format.type' = 'csv',
            ...     'connector.path' = 'xxx'
            ... )
            ... '''

            >>> query = "INSERT INTO sinkTable SELECT FROM sourceTable"
            >>> table_env.sql(source_ddl)
            >>> table_env.sql(sink_ddl)
            >>> table_env.sql(query)
            >>> table_env.execute("MyJob")

        :param stmt: The SQL statement to evaluate.
        :param query_config: The :class:`QueryConfig` to use.
        """
        # type: (str) -> None
        self._j_tenv.sqlUpdate(stmt)

    def get_current_catalog(self):
        """
        Gets the current default catalog name of the current session.

        :return: The current default catalog name that is used for the path resolution.

        .. seealso:: :func:`~pyflink.table.TableEnvironment.use_catalog`
        """
        return self._j_tenv.getCurrentCatalog()

    def use_catalog(self, catalog_name):
        """
        Sets the current catalog to the given value. It also sets the default
        database to the catalog's default one.
        See also :func:`~TableEnvironment.use_database`.

        This is used during the resolution of object paths. Both the catalog and database are
        optional when referencing catalog objects such as tables, views etc. The algorithm looks for
        requested objects in following paths in that order:

        * ``[current-catalog].[current-database].[requested-path]``
        * ``[current-catalog].[requested-path]``
        * ``[requested-path]``

        Example:

        Given structure with default catalog set to ``default_catalog`` and default database set to
        ``default_database``. ::

            root:
              |- default_catalog
                  |- default_database
                      |- tab1
                  |- db1
                      |- tab1
              |- cat1
                  |- db1
                      |- tab1

        The following table describes resolved paths:

        +----------------+-----------------------------------------+
        | Requested path |             Resolved path               |
        +================+=========================================+
        | tab1           | default_catalog.default_database.tab1   |
        +----------------+-----------------------------------------+
        | db1.tab1       | default_catalog.db1.tab1                |
        +----------------+-----------------------------------------+
        | cat1.db1.tab1  | cat1.db1.tab1                           |
        +----------------+-----------------------------------------+

        :param: catalog_name: The name of the catalog to set as the current default catalog.
        :throws: :class:`pyflink.util.exceptions.CatalogException` thrown if a catalog with given
                 name could not be set as the default one.

        .. seealso:: :func:`~pyflink.table.TableEnvironment.use_database`
        """
        self._j_tenv.useCatalog(catalog_name)

    def get_current_database(self):
        """
        Gets the current default database name of the running session.

        :return: The name of the current database of the current catalog.

        .. seealso:: :func:`~pyflink.table.TableEnvironment.use_database`
        """
        return self._j_tenv.getCurrentDatabase()

    def use_database(self, database_name):
        """
        Sets the current default database. It has to exist in the current catalog. That path will
        be used as the default one when looking for unqualified object names.

        This is used during the resolution of object paths. Both the catalog and database are
        optional when referencing catalog objects such as tables, views etc. The algorithm looks for
        requested objects in following paths in that order:

        * ``[current-catalog].[current-database].[requested-path]``
        * ``[current-catalog].[requested-path]``
        * ``[requested-path]``

        Example:

        Given structure with default catalog set to ``default_catalog`` and default database set to
        ``default_database``. ::

            root:
              |- default_catalog
                  |- default_database
                      |- tab1
                  |- db1
                      |- tab1
              |- cat1
                  |- db1
                      |- tab1

        The following table describes resolved paths:

        +----------------+-----------------------------------------+
        | Requested path |             Resolved path               |
        +================+=========================================+
        | tab1           | default_catalog.default_database.tab1   |
        +----------------+-----------------------------------------+
        | db1.tab1       | default_catalog.db1.tab1                |
        +----------------+-----------------------------------------+
        | cat1.db1.tab1  | cat1.db1.tab1                           |
        +----------------+-----------------------------------------+

        :throws: :class:`pyflink.util.exceptions.CatalogException` thrown if the given catalog and
                 database could not be set as the default ones.

        .. seealso:: :func:`~pyflink.table.TableEnvironment.use_catalog`

        :param: database_name: The name of the database to set as the current database.
        """
        self._j_tenv.useDatabase(database_name)

    @abstractmethod
    def get_config(self):
        """
        Returns the table config to define the runtime behavior of the Table API.

        :return: Current :class:`TableConfig`.
        """
        pass

    @abstractmethod
    def connect(self, connector_descriptor):
        """
        Creates a table source and/or table sink from a descriptor.

        Descriptors allow for declaring the communication to external systems in an
        implementation-agnostic way. The classpath is scanned for suitable table factories that
        match the desired configuration.

        The following example shows how to read from a connector using a JSON format and
        registering a table source as "MyTable":

        Example:
        ::

            >>> table_env \\
            ...     .connect(ExternalSystemXYZ()
            ...              .version("0.11")) \\
            ...     .with_format(Json()
            ...                  .json_schema("{...}")
            ...                  .fail_on_missing_field(False)) \\
            ...     .with_schema(Schema()
            ...                  .field("user-name", "VARCHAR")
            ...                  .from_origin_field("u_name")
            ...                  .field("count", "DECIMAL")) \\
            ...     .register_table_source("MyTable")

        :param connector_descriptor: Connector descriptor describing the external system.
        :return: A :class:`pyflink.table.descriptors.ConnectTableDescriptor` used to build the
                 table source/sink.
        """
        pass

    def register_java_function(self, name, function_class_name):
        """
        Registers a java user defined function under a unique name. Replaces already existing
        user-defined functions under this name. The acceptable function type contains
        **ScalarFunction**, **TableFunction** and **AggregateFunction**.

        Example:
        ::

            >>> table_env.register_java_function("func1", "java.user.defined.function.class.name")

        :param name: The name under which the function is registered.
        :type name: str
        :param function_class_name: The java full qualified class name of the function to register.
                                    The function must have a public no-argument constructor and can
                                    be founded in current Java classloader.
        :type function_class_name: str
        """
        gateway = get_gateway()
        java_function = gateway.jvm.Thread.currentThread().getContextClassLoader()\
            .loadClass(function_class_name).newInstance()
        self._j_tenv.registerFunction(name, java_function)

    def execute(self, job_name):
        """
        Triggers the program execution. The environment will execute all parts of
        the program.

        The program execution will be logged and displayed with the provided name.

        .. note::

            It is highly advised to set all parameters in the :class:`TableConfig`
            on the very beginning of the program. It is undefined what configurations values will
            be used for the execution if queries are mixed with config changes. It depends on
            the characteristic of the particular parameter. For some of them the value from the
            point in time of query construction (e.g. the current catalog) will be used. On the
            other hand some values might be evaluated according to the state from the time when
            this method is called (e.g. timezone).

        :param job_name: Desired name of the job.
        :type job_name: str
        """
        self._j_tenv.execute(job_name)

    def from_elements(self, elements, schema=None, verify_schema=True):
        """
        Creates a table from a collection of elements.
        The elements types must be acceptable atomic types or acceptable composite types.
        All elements must be of the same type.
        If the elements types are composite types, the composite types must be strictly equal,
        and its subtypes must also be acceptable types.
        e.g. if the elements are tuples, the length of the tuples must be equal, the element types
        of the tuples must be equal in order.

        The built-in acceptable atomic element types contains:

        **int**, **long**, **str**, **unicode**, **bool**,
        **float**, **bytearray**, **datetime.date**, **datetime.time**, **datetime.datetime**,
        **datetime.timedelta**, **decimal.Decimal**

        The built-in acceptable composite element types contains:

        **list**, **tuple**, **dict**, **array**, :class:`pyflink.table.Row`

        If the element type is a composite type, it will be unboxed.
        e.g. table_env.from_elements([(1, 'Hi'), (2, 'Hello')]) will return a table like:

        +----+-------+
        | _1 |  _2   |
        +====+=======+
        | 1  |  Hi   |
        +----+-------+
        | 2  | Hello |
        +----+-------+

        "_1" and "_2" are generated field names.

        Example:
        ::

            # use the second parameter to specify custom field names
            >>> table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['a', 'b'])
            # use the second parameter to specify custom table schema
            >>> table_env.from_elements([(1, 'Hi'), (2, 'Hello')],
            ...                         DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT()),
            ...                                        DataTypes.FIELD("b", DataTypes.STRING())]))
            # use the thrid parameter to switch whether to verify the elements against the schema
            >>> table_env.from_elements([(1, 'Hi'), (2, 'Hello')],
            ...                         DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT()),
            ...                                        DataTypes.FIELD("b", DataTypes.STRING())]),
            ...                         False)

        :param elements: The elements to create a table from.
        :param schema: The schema of the table.
        :param verify_schema: Whether to verify the elements against the schema.
        :return: The result :class:`Table`.
        """

        # verifies the elements against the specified schema
        if isinstance(schema, RowType):
            verify_func = _create_type_verifier(schema) if verify_schema else lambda _: True

            def verify_obj(obj):
                verify_func(obj)
                return obj
        elif isinstance(schema, DataType):
            data_type = schema
            schema = RowType().add("value", schema)

            verify_func = _create_type_verifier(
                data_type, name="field value") if verify_schema else lambda _: True

            def verify_obj(obj):
                verify_func(obj)
                return obj
        else:
            def verify_obj(obj):
                return obj

        if "__len__" not in dir(elements):
            elements = list(elements)

        # infers the schema if not specified
        if schema is None or isinstance(schema, (list, tuple)):
            schema = _infer_schema_from_data(elements, names=schema)
            converter = _create_converter(schema)
            elements = map(converter, elements)
            if isinstance(schema, (list, tuple)):
                for i, name in enumerate(schema):
                    schema.fields[i].name = name
                    schema.names[i] = name

        elif not isinstance(schema, RowType):
            raise TypeError(
                "schema should be RowType, list, tuple or None, but got: %s" % schema)

        # verifies the elements against the specified schema
        elements = map(verify_obj, elements)
        # converts python data to sql data
        elements = [schema.to_sql_type(element) for element in elements]
        return self._from_elements(elements, schema)

    def _from_elements(self, elements, schema):
        """
        Creates a table from a collection of elements.

        :param elements: The elements to create a table from.
        :return: The result :class:`Table`.
        """

        # serializes to a file, and we read the file in java
        temp_file = tempfile.NamedTemporaryFile(delete=False, dir=tempfile.mkdtemp())
        serializer = BatchedSerializer(self._serializer)
        try:
            try:
                serializer.dump_to_stream(elements, temp_file)
            finally:
                temp_file.close()
            row_type_info = _to_java_type(schema)
            execution_config = self._get_execution_config(temp_file.name, schema)
            gateway = get_gateway()
            j_objs = gateway.jvm.PythonBridgeUtils.readPythonObjects(temp_file.name, True)
            j_input_format = gateway.jvm.PythonTableUtils.getInputFormat(
                j_objs, row_type_info, execution_config)
            j_table_source = gateway.jvm.PythonInputFormatTableSource(
                j_input_format, row_type_info)

            return Table(self._j_tenv.fromTableSource(j_table_source))
        finally:
            os.unlink(temp_file.name)

    @abstractmethod
    def _get_execution_config(self, filename, schema):
        pass


class StreamTableEnvironment(TableEnvironment):

    def __init__(self, j_tenv):
        self._j_tenv = j_tenv
        super(StreamTableEnvironment, self).__init__(j_tenv)

    def _get_execution_config(self, filename, schema):
        return self._j_tenv.execEnv().getConfig()

    def get_config(self):
        """
        Returns the table config to define the runtime behavior of the Table API.

        :return: Current :class:`TableConfig`.
        """
        table_config = TableConfig()
        table_config._j_table_config = self._j_tenv.getConfig()
        return table_config

    def connect(self, connector_descriptor):
        """
        Creates a table source and/or table sink from a descriptor.

        Descriptors allow for declaring the communication to external systems in an
        implementation-agnostic way. The classpath is scanned for suitable table factories that
        match the desired configuration.

        The following example shows how to read from a connector using a JSON format and
        registering a table source as "MyTable":
        ::

            >>> table_env \\
            ...     .connect(ExternalSystemXYZ()
            ...              .version("0.11")) \\
            ...     .with_format(Json()
            ...                  .json_schema("{...}")
            ...                  .fail_on_missing_field(False)) \\
            ...     .with_schema(Schema()
            ...                  .field("user-name", "VARCHAR")
            ...                  .from_origin_field("u_name")
            ...                  .field("count", "DECIMAL")) \\
            ...     .register_table_source("MyTable")

        :param connector_descriptor: Connector descriptor describing the external system.
        :return: A :class:`StreamTableDescriptor` used to build the table source/sink.
        """
        # type: (ConnectorDescriptor) -> StreamTableDescriptor
        return StreamTableDescriptor(
            self._j_tenv.connect(connector_descriptor._j_connector_descriptor))

    @staticmethod
    def create(stream_execution_environment, table_config=None, environment_settings=None):
        """
        Creates a :class:`TableEnvironment` for a
        :class:`~pyflink.datastream.StreamExecutionEnvironment`.

        Example:
        ::

            >>> env = StreamExecutionEnvironment.get_execution_environment()
            # create without optional parameters.
            >>> table_env = StreamTableEnvironment.create(env)
            # create with TableConfig
            >>> table_config = TableConfig()
            >>> table_config.set_null_check(False)
            >>> table_env = StreamTableEnvironment.create(env, table_config)
            # create with EnvrionmentSettings
            >>> environment_settings = EnvironmentSettings.new_instance().use_blink_planner() \\
            ...     .build()
            >>> table_env = StreamTableEnvironment.create(
            ...     env, environment_settings=environment_settings)


        :param stream_execution_environment: The
                                             :class:`~pyflink.datastream.StreamExecutionEnvironment`
                                             of the TableEnvironment.
        :type stream_execution_environment: pyflink.datastream.StreamExecutionEnvironment
        :param table_config: The configuration of the TableEnvironment, optional.
        :type table_config: TableConfig
        :param environment_settings: The environment settings used to instantiate the
                                     TableEnvironment. It provides the interfaces about planner
                                     selection(flink or blink), optional.
        :type environment_settings: pyflink.table.EnvironmentSettings
        :return: The :class:`StreamTableEnvironment` created from given StreamExecutionEnvironment
                 and configuration.
        :rtype: StreamTableEnvironment
        """
        if table_config is not None and environment_settings is not None:
            raise ValueError("The param 'table_config' and "
                             "'environment_settings' cannot be used at the same time")

        gateway = get_gateway()
        if table_config is not None:
            j_tenv = gateway.jvm.StreamTableEnvironment.create(
                stream_execution_environment._j_stream_execution_environment,
                table_config._j_table_config)
        elif environment_settings is not None:
            if not environment_settings.is_streaming_mode():
                raise ValueError("The environment settings for StreamTableEnvironment must be "
                                 "set to streaming mode.")
            j_tenv = gateway.jvm.StreamTableEnvironment.create(
                stream_execution_environment._j_stream_execution_environment,
                environment_settings._j_environment_settings)
        else:
            j_tenv = gateway.jvm.StreamTableEnvironment.create(
                stream_execution_environment._j_stream_execution_environment)
        return StreamTableEnvironment(j_tenv)


class BatchTableEnvironment(TableEnvironment):

    def __init__(self, j_tenv):
        self._j_tenv = j_tenv
        super(BatchTableEnvironment, self).__init__(j_tenv)

    def _get_execution_config(self, filename, schema):
        gateway = get_gateway()
        blink_t_env_class = get_java_class(
            gateway.jvm.org.apache.flink.table.api.internal.TableEnvironmentImpl)
        is_blink = (blink_t_env_class == self._j_tenv.getClass())
        if is_blink:
            # we can not get ExecutionConfig object from the TableEnvironmentImpl
            # for the moment, just create a new ExecutionConfig.
            execution_config = gateway.jvm.org.apache.flink.api.common.ExecutionConfig()
        else:
            execution_config = self._j_tenv.execEnv().getConfig()

        return execution_config

    def get_config(self):
        """
        Returns the table config to define the runtime behavior of the Table API.

        :return: Current :class:`TableConfig`.
        """
        table_config = TableConfig()
        table_config._j_table_config = self._j_tenv.getConfig()
        return table_config

    def connect(self, connector_descriptor):
        """
        Creates a table source and/or table sink from a descriptor.

        Descriptors allow for declaring the communication to external systems in an
        implementation-agnostic way. The classpath is scanned for suitable table factories that
        match the desired configuration.

        The following example shows how to read from a connector using a JSON format and
        registering a table source as "MyTable":
        ::

            >>> table_env \\
            ...     .connect(ExternalSystemXYZ()
            ...              .version("0.11")) \\
            ...     .with_format(Json()
            ...                  .json_schema("{...}")
            ...                  .fail_on_missing_field(False)) \\
            ...     .with_schema(Schema()
            ...                  .field("user-name", "VARCHAR")
            ...                  .from_origin_field("u_name")
            ...                  .field("count", "DECIMAL")) \\
            ...     .register_table_source("MyTable")

        :param connector_descriptor: Connector descriptor describing the external system.
        :type connector_descriptor: ConnectorDescriptor
        :return: A :class:`BatchTableDescriptor` or a :class:`StreamTableDescriptor`
                 (for blink planner) used to build the table source/sink.
        :rtype: BatchTableDescriptor or StreamTableDescriptor
        """
        gateway = get_gateway()
        blink_t_env_class = get_java_class(
            gateway.jvm.org.apache.flink.table.api.internal.TableEnvironmentImpl)
        if blink_t_env_class == self._j_tenv.getClass():
            return StreamTableDescriptor(
                self._j_tenv.connect(connector_descriptor._j_connector_descriptor))
        else:
            return BatchTableDescriptor(
                self._j_tenv.connect(connector_descriptor._j_connector_descriptor))

    @staticmethod
    def create(execution_environment=None, table_config=None, environment_settings=None):
        """
        Creates a :class:`BatchTableEnvironment`.

        Example:
        ::

            # create with ExecutionEnvironment.
            >>> env = ExecutionEnvironment.get_execution_environment()
            >>> table_env = BatchTableEnvironment.create(env)
            # create with ExecutionEnvironment and TableConfig.
            >>> table_config = TableConfig()
            >>> table_config.set_null_check(False)
            >>> table_env = BatchTableEnvironment.create(env, table_config)
            # create with EnvironmentSettings.
            >>> environment_settings = EnvironmentSettings.new_instance().in_batch_mode() \\
            ...     .use_blink_planner().build()
            >>> table_env = BatchTableEnvironment.create(environment_settings=environment_settings)

        :param execution_environment: The batch :class:`pyflink.dataset.ExecutionEnvironment` of
                                      the TableEnvironment.
        :type execution_environment: pyflink.dataset.ExecutionEnvironment
        :param table_config: The configuration of the TableEnvironment, optional.
        :type table_config: TableConfig
        :param environment_settings: The environment settings used to instantiate the
                                     TableEnvironment. It provides the interfaces about planner
                                     selection(flink or blink), optional.
        :type environment_settings: pyflink.table.EnvironmentSettings
        :return: The BatchTableEnvironment created from given ExecutionEnvironment and
                 configuration.
        :rtype: BatchTableEnvironment
        """
        if execution_environment is None and \
                table_config is None and \
                environment_settings is None:
            raise ValueError("No argument found, the param 'execution_environment' "
                             "or 'environment_settings' is required.")
        elif execution_environment is None and \
                table_config is not None and \
                environment_settings is None:
            raise ValueError("Only the param 'table_config' is found, "
                             "the param 'execution_environment' is also required.")
        elif execution_environment is not None and \
                environment_settings is not None:
            raise ValueError("The param 'execution_environment' and "
                             "'environment_settings' cannot be used at the same time")
        elif table_config is not None and \
                environment_settings is not None:
            raise ValueError("The param 'table_config' and "
                             "'environment_settings' cannot be used at the same time")

        gateway = get_gateway()
        if execution_environment is not None and environment_settings is None:
            if table_config is not None:
                j_tenv = gateway.jvm.BatchTableEnvironment.create(
                    execution_environment._j_execution_environment,
                    table_config._j_table_config)
            else:
                j_tenv = gateway.jvm.BatchTableEnvironment.create(
                    execution_environment._j_execution_environment)
            return BatchTableEnvironment(j_tenv)
        elif environment_settings is not None and \
                execution_environment is None and \
                table_config is None:
            if environment_settings.is_streaming_mode():
                raise ValueError("The environment settings for BatchTableEnvironment must be "
                                 "set to batch mode.")
            j_tenv = gateway.jvm.TableEnvironment.create(
                environment_settings._j_environment_settings)
            return BatchTableEnvironment(j_tenv)
