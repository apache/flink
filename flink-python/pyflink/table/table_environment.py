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

from pyflink.serializers import BatchedSerializer, PickleSerializer
from pyflink.table.query_config import StreamQueryConfig, BatchQueryConfig, QueryConfig
from pyflink.table.table_config import TableConfig
from pyflink.table.table_descriptor import (StreamTableDescriptor, ConnectorDescriptor,
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

        :param table_source: The table source used as table.
        :return: The result table.
        """
        return Table(self._j_tenv.fromTableSource(table_source._j_table_source))

    def register_table(self, name, table):
        """
        Registers a :class:`Table` under a unique name in the TableEnvironment's catalog.
        Registered tables can be referenced in SQL queries.

        :param name: The name under which the table will be registered.
        :param table: The table to register.
        """
        self._j_tenv.registerTable(name, table._j_table)

    def register_table_source(self, name, table_source):
        """
        Registers an external :class:`TableSource` in this :class:`TableEnvironment`'s catalog.
        Registered tables can be referenced in SQL queries.

        :param name: The name under which the :class:`TableSource` is registered.
        :param table_source: The :class:`TableSource` to register.
        """
        self._j_tenv.registerTableSource(name, table_source._j_table_source)

    def register_table_sink(self, name, field_names, field_types, table_sink):
        """
        Registers an external :class:`TableSink` with given field names and types in this
        :class:`TableEnvironment`'s catalog.
        Registered sink tables can be referenced in SQL DML statements.

        :param name: The name under which the :class:`TableSink` is registered.
        :param field_names: The field names to register with the :class:`TableSink`.
        :param field_types: The field types to register with the :class:`TableSink`.
        :param table_sink: The :class:`TableSink` to register.
        """
        gateway = get_gateway()
        j_field_names = utils.to_jarray(gateway.jvm.String, field_names)
        j_field_types = utils.to_jarray(
            gateway.jvm.TypeInformation,
            [_to_java_type(field_type) for field_type in field_types])
        self._j_tenv.registerTableSink(name, j_field_names, j_field_types, table_sink._j_table_sink)

    def scan(self, *table_path):
        """
        Scans a registered table and returns the resulting :class:`Table`.
        A table to scan must be registered in the TableEnvironment. It can be either directly
        registered as TableSource or Table.

        Examples:

        Scanning a directly registered table
        ::
            >>> tab = t_env.scan("tableName")

        Scanning a table from a registered catalog
        ::
            >>> tab = t_env.scan("catalogName", "dbName", "tableName")

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
        :func:`~TableEnvironment.useDatabase`.

        Example:
        ::
            >>> tab = t_env.scan("tableName")
            >>> t_env.insert_into(tab, "print")

        :param table The :class:`Table` to write to the sink.
        :param table_path: The first part of the path of the registered :class:`TableSink` to which
               the :class:`Table` is written. This is to ensure at least the name of the
               :class:`Table` is provided.
        :param table_path_continued: The remaining part of the path of the registered
                :class:`TableSink` to which the :class:`Table`  is written.
        """
        gateway = get_gateway()
        j_table_path = utils.to_jarray(gateway.jvm.String, table_path_continued)
        self._j_tenv.insertInto(table._j_table, table_path, j_table_path)

    def list_tables(self):
        """
        Gets the names of all tables registered in this environment.

        :return: List of table names.
        """
        j_table_name_array = self._j_tenv.listTables()
        return [item for item in j_table_name_array]

    def explain(self, table):
        """
        Returns the AST of the specified Table API and SQL queries and the execution plan to compute
        the result of the given :class:`Table`.

        :param table: The table to be explained.
        :return: The table for which the AST and execution plan will be returned.
        """
        return self._j_tenv.explain(table._j_table)

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
            >>> t_env.sql_query("SELECT * FROM %s" % table)

        :param query: The sql query string.
        :return: The result :class:`Table`.
        """
        j_table = self._j_tenv.sqlQuery(query)
        return Table(j_table)

    def sql_update(self, stmt, query_config=None):
        """
        Evaluates a SQL statement such as INSERT, UPDATE or DELETE or a DDL statement

        ..note::
            Currently only SQL INSERT statements are supported.

        All tables referenced by the query must be registered in the TableEnvironment.
        A :class:`Table` is automatically registered when its :func:`~Table.__str__` method is
        called, for example when it is embedded into a String.
        Hence, SQL queries can directly reference a :class:`Table` as follows:

        ::
            # register the table sink into which the result is inserted.
            >>> t_env.register_table_sink("sink_table", field_names, fields_types, table_sink)
            >>> source_table = ...
            # source_table is not registered to the table environment
            >>> tEnv.sql_update(s"INSERT INTO sink_table SELECT * FROM %s" % source_table)

        :param stmt: The SQL statement to evaluate.
        :param query_config: The :class:`QueryConfig` to use.
        """
        # type: (str, QueryConfig) -> None
        if query_config is not None:
            self._j_tenv.sqlUpdate(stmt, query_config._j_query_config)
        else:
            self._j_tenv.sqlUpdate(stmt)

    def get_current_catalog(self):
        """
        Gets the current default catalog name of the current session.

        :return The current default catalog name that is used for the path resolution.
        .. seealso:: :func:`~pyflink.table.TableEnvironment.use_catalog`
        """
        self._j_tenv.getCurrentCatalog()

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
        :throws: CatalogException thrown if a catalog with given name could not be set as the
                 default one
        .. seealso:: :func:`~pyflink.table.TableEnvironment.use_database`
        """
        self._j_tenv.useCatalog(catalog_name)

    def get_current_database(self):
        """
        Gets the current default database name of the running session.

        :return The name of the current database of the current catalog.
        .. seealso:: :func:`~pyflink.table.TableEnvironment.use_database`
        """
        self._j_tenv.getCurrentCatalog()

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

        :throws: CatalogException thrown if the given catalog and database could not be set as
                the default ones
        .. seealso:: :func:`~pyflink.table.TableEnvironment.use_catalog`

        :param: database_name: The name of the database to set as the current database.
        """
        self._j_tenv.useDatabase(database_name)

    def execute(self, job_name=None):
        """
        Triggers the program execution.

        :param job_name: Optional, desired name of the job.
        """
        if job_name is not None:
            self._j_tenv.execEnv().execute(job_name)
        else:
            self._j_tenv.execEnv().execute()

    @abstractmethod
    def get_config(self):
        """
        Returns the table config to define the runtime behavior of the Table API.

        :return: Current :class:`TableConfig`.
        """
        pass

    @abstractmethod
    def query_config(self):
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
        ::
            >>> table_env\
            ...     .connect(ExternalSystemXYZ()
            ...              .version("0.11"))\
            ...     .with_format(Json()
            ...                  .json_schema("{...}")
            ...                 .fail_on_missing_field(False))\
            ...     .with_schema(Schema()
            ...                 .field("user-name", "VARCHAR")
            ...                 .from_origin_field("u_name")
            ...                 .field("count", "DECIMAL"))\
            ...     .register_table_source("MyTable")

        :param connector_descriptor: Connector descriptor describing the external system.
        :return: A :class:`ConnectTableDescriptor` used to build the table source/sink.
        """
        pass

    @classmethod
    def create(cls, table_config):
        """
        Returns a :class:`StreamTableEnvironment` or a :class:`BatchTableEnvironment`
        which matches the :class:`TableConfig`'s content.

        :type table_config: The TableConfig for the new TableEnvironment.
        :return: Desired :class:`TableEnvironment`.
        """
        gateway = get_gateway()
        if table_config.is_stream():
            j_execution_env = gateway.jvm.StreamExecutionEnvironment.getExecutionEnvironment()
            j_tenv = gateway.jvm.StreamTableEnvironment.create(
                j_execution_env, table_config._j_table_config)
            t_env = StreamTableEnvironment(j_tenv)
        else:
            j_execution_env = gateway.jvm.ExecutionEnvironment.getExecutionEnvironment()
            j_tenv = gateway.jvm.BatchTableEnvironment.create(
                j_execution_env, table_config._j_table_config)
            t_env = BatchTableEnvironment(j_tenv)

        if table_config.parallelism() is not None:
            t_env._j_tenv.execEnv().setParallelism(table_config.parallelism())

        return t_env

    def from_elements(self, elements, schema=None, verify_schema=True):
        """
        Creates a table from a collection of elements.

        :param elements: The elements to create a table from.
        :param schema: The schema of the table.
        :param verify_schema: Whether to verify the elements against the schema.
        :return: A Table.
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

        # converts python data to sql data
        elements = [schema.to_sql_type(element) for element in elements]
        return self._from_elements(map(verify_obj, elements), schema)

    def _from_elements(self, elements, schema):
        """
        Creates a table from a collection of elements.

        :param elements: The elements to create a table from.
        :return: A table.
        """

        # serializes to a file, and we read the file in java
        temp_file = tempfile.NamedTemporaryFile(delete=False, dir=tempfile.mkdtemp())
        serializer = BatchedSerializer(self._serializer)
        try:
            try:
                serializer.dump_to_stream(elements, temp_file)
            finally:
                temp_file.close()
            return self._from_file(temp_file.name, schema)
        finally:
            os.unlink(temp_file.name)

    @abstractmethod
    def _from_file(self, filename, schema):
        pass


class StreamTableEnvironment(TableEnvironment):

    def __init__(self, j_tenv):
        self._j_tenv = j_tenv
        super(StreamTableEnvironment, self).__init__(j_tenv)

    def _from_file(self, filename, schema):
        gateway = get_gateway()
        jds = gateway.jvm.PythonBridgeUtils.createDataStreamFromFile(
            self._j_tenv.execEnv(), filename, True)
        return Table(gateway.jvm.PythonTableUtils.fromDataStream(
            self._j_tenv, jds, _to_java_type(schema)))

    def get_config(self):
        """
        Returns the table config to define the runtime behavior of the Table API.

        :return: Current :class:`TableConfig`.
        """
        table_config = TableConfig()
        table_config._j_table_config = self._j_tenv.getConfig()
        table_config._set_stream(True)
        table_config._set_parallelism(self._j_tenv.execEnv().getParallelism())
        return table_config

    def query_config(self):
        """
        Returns a :class:`StreamQueryConfig` that holds parameters to configure the behavior of
        streaming queries.

        :return: A new :class:`StreamQueryConfig`.
        """
        return StreamQueryConfig(self._j_tenv.queryConfig())

    def connect(self, connector_descriptor):
        """
        Creates a table source and/or table sink from a descriptor.

        Descriptors allow for declaring the communication to external systems in an
        implementation-agnostic way. The classpath is scanned for suitable table factories that
        match the desired configuration.

        The following example shows how to read from a connector using a JSON format and
        registering a table source as "MyTable":
        ::
            >>> table_env\
            ...     .connect(ExternalSystemXYZ()
            ...              .version("0.11"))\
            ...     .with_format(Json()
            ...                  .json_schema("{...}")
            ...                 .fail_on_missing_field(False))\
            ...     .with_schema(Schema()
            ...                 .field("user-name", "VARCHAR")
            ...                 .from_origin_field("u_name")
            ...                 .field("count", "DECIMAL"))\
            ...     .register_table_source("MyTable")

        :param connector_descriptor: Connector descriptor describing the external system.
        :return: A :class:`StreamTableDescriptor` used to build the table source/sink.
        """
        # type: (ConnectorDescriptor) -> StreamTableDescriptor
        return StreamTableDescriptor(
            self._j_tenv.connect(connector_descriptor._j_connector_descriptor))


class BatchTableEnvironment(TableEnvironment):

    def __init__(self, j_tenv):
        self._j_tenv = j_tenv
        super(BatchTableEnvironment, self).__init__(j_tenv)

    def _from_file(self, filename, schema):
        gateway = get_gateway()
        jds = gateway.jvm.PythonBridgeUtils.createDataSetFromFile(
            self._j_tenv.execEnv(), filename, True)
        return Table(gateway.jvm.PythonTableUtils.fromDataSet(
            self._j_tenv, jds, _to_java_type(schema)))

    def get_config(self):
        """
        Returns the table config to define the runtime behavior of the Table API.

        :return: Current :class:`TableConfig`.
        """
        table_config = TableConfig()
        table_config._j_table_config = self._j_tenv.getConfig()
        table_config._set_stream(False)
        table_config._set_parallelism(self._j_tenv.execEnv().getParallelism())
        return table_config

    def query_config(self):
        """
        Returns the :class:`BatchQueryConfig` that holds parameters to configure the behavior of
        batch queries.

        :return: A new :class:`BatchQueryConfig`.
        """
        return BatchQueryConfig(self._j_tenv.queryConfig())

    def connect(self, connector_descriptor):
        """
        Creates a table source and/or table sink from a descriptor.

        Descriptors allow for declaring the communication to external systems in an
        implementation-agnostic way. The classpath is scanned for suitable table factories that
        match the desired configuration.

        The following example shows how to read from a connector using a JSON format and
        registering a table source as "MyTable":
        ::
            >>> table_env\
            ...     .connect(ExternalSystemXYZ()
            ...              .version("0.11"))\
            ...     .with_format(Json()
            ...                  .json_schema("{...}")
            ...                 .fail_on_missing_field(False))\
            ...     .with_schema(Schema()
            ...                 .field("user-name", "VARCHAR")
            ...                 .from_origin_field("u_name")
            ...                 .field("count", "DECIMAL"))\
            ...     .register_table_source("MyTable")

        :param connector_descriptor: Connector descriptor describing the external system.
        :return: A :class:`BatchTableDescriptor` used to build the table source/sink.
        """
        # type: (ConnectorDescriptor) -> BatchTableDescriptor
        return BatchTableDescriptor(
            self._j_tenv.connect(connector_descriptor._j_connector_descriptor))
