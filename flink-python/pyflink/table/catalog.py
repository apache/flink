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
from enum import Enum

from py4j.java_gateway import java_import, get_java_class, JavaObject

from pyflink.common.configuration import Configuration
from pyflink.java_gateway import get_gateway
from pyflink.table.resolved_expression import ResolvedExpression
from pyflink.table.schema import Schema
from pyflink.table.table_schema import TableSchema
from pyflink.table.types import DataType, _to_java_data_type, _from_java_data_type
from pyflink.util.java_utils import to_jarray
from typing import Dict, List, Optional, Union
from abc import ABCMeta, abstractmethod

__all__ = ['Catalog', 'CatalogDatabase', 'CatalogBaseTable', 'CatalogPartition', 'CatalogFunction',
           'Procedure', 'ObjectPath', 'CatalogPartitionSpec', 'CatalogTableStatistics',
           'CatalogColumnStatistics', 'HiveCatalog', 'CatalogDescriptor', 'ObjectIdentifier',
           'Column', 'PhysicalColumn', 'ComputedColumn', 'MetadataColumn', 'WatermarkSpec',
           'Constraint', 'UniqueConstraint', 'ResolvedSchema']


class Catalog(object):
    """
    Catalog is responsible for reading and writing metadata such as database/table/views/UDFs
    from a registered catalog. It connects a registered catalog and Flink's Table API.
    """

    def __init__(self, j_catalog):
        self._j_catalog = j_catalog

    def get_default_database(self) -> str:
        """
        Get the name of the default database for this catalog. The default database will be the
        current database for the catalog when user's session doesn't specify a current database.
        The value probably comes from configuration, will not change for the life time of the
        catalog instance.

        :return: The name of the current database.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.getDefaultDatabase()

    def list_databases(self) -> List[str]:
        """
        Get the names of all databases in this catalog.

        :return: A list of the names of all databases.
        :raise: CatalogException in case of any runtime exception.
        """
        return list(self._j_catalog.listDatabases())

    def get_database(self, database_name: str) -> 'CatalogDatabase':
        """
        Get a database from this catalog.

        :param database_name: Name of the database.
        :return: The requested database :class:`CatalogDatabase`.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return CatalogDatabase._get(self._j_catalog.getDatabase(database_name))

    def database_exists(self, database_name: str) -> bool:
        """
        Check if a database exists in this catalog.

        :param database_name: Name of the database.
        :return: true if the given database exists in the catalog false otherwise.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.databaseExists(database_name)

    def create_database(self, name: str, database: 'CatalogDatabase', ignore_if_exists: bool):
        """
        Create a database.

        :param name: Name of the database to be created.
        :param database: The :class:`CatalogDatabase` database definition.
        :param ignore_if_exists: Flag to specify behavior when a database with the given name
                                 already exists:
                                 if set to false, throw a DatabaseAlreadyExistException,
                                 if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                DatabaseAlreadyExistException if the given database already exists and
                ignoreIfExists is false.
        """
        self._j_catalog.createDatabase(name, database._j_catalog_database, ignore_if_exists)

    def drop_database(self, name: str, ignore_if_exists: bool):
        """
        Drop a database.

        :param name: Name of the database to be dropped.
        :param ignore_if_exists: Flag to specify behavior when the database does not exist:
                                 if set to false, throw an exception,
                                 if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the given database does not exist.
        """
        self._j_catalog.dropDatabase(name, ignore_if_exists)

    def alter_database(self, name: str, new_database: 'CatalogDatabase',
                       ignore_if_not_exists: bool):
        """
        Modify an existing database.

        :param name: Name of the database to be modified.
        :param new_database: The new database :class:`CatalogDatabase` definition.
        :param ignore_if_not_exists: Flag to specify behavior when the given database does not
                                     exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the given database does not exist.
        """
        self._j_catalog.alterDatabase(name, new_database._j_catalog_database, ignore_if_not_exists)

    def list_tables(self, database_name: str) -> List[str]:
        """
        Get names of all tables and views under this database. An empty list is returned if none
        exists.

        :param database_name: Name of the given database.
        :return: A list of the names of all tables and views in this database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listTables(database_name))

    def list_views(self, database_name: str) -> List[str]:
        """
        Get names of all views under this database. An empty list is returned if none exists.

        :param database_name: Name of the given database.
        :return: A list of the names of all views in the given database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listViews(database_name))

    def get_table(self, table_path: 'ObjectPath') -> 'CatalogBaseTable':
        """
        Get a CatalogTable or CatalogView identified by tablePath.

        :param table_path: Path :class:`ObjectPath` of the table or view.
        :return: The requested table or view :class:`CatalogBaseTable`.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the target does not exist.
        """
        return CatalogBaseTable._get(self._j_catalog.getTable(table_path._j_object_path))

    def table_exists(self, table_path: 'ObjectPath') -> bool:
        """
        Check if a table or view exists in this catalog.

        :param table_path: Path :class:`ObjectPath` of the table or view.
        :return: true if the given table exists in the catalog false otherwise.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.tableExists(table_path._j_object_path)

    def drop_table(self, table_path: 'ObjectPath', ignore_if_not_exists: bool):
        """
        Drop a table or view.

        :param table_path: Path :class:`ObjectPath` of the table or view to be dropped.
        :param ignore_if_not_exists: Flag to specify behavior when the table or view does not exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table or view does not exist.
        """
        self._j_catalog.dropTable(table_path._j_object_path, ignore_if_not_exists)

    def rename_table(self, table_path: 'ObjectPath', new_table_name: str,
                     ignore_if_not_exists: bool):
        """
        Rename an existing table or view.

        :param table_path: Path :class:`ObjectPath` of the table or view to be renamed.
        :param new_table_name: The new name of the table or view.
        :param ignore_if_not_exists: Flag to specify behavior when the table or view does not exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist.
        """
        self._j_catalog.renameTable(table_path._j_object_path, new_table_name, ignore_if_not_exists)

    def create_table(self, table_path: 'ObjectPath', table: 'CatalogBaseTable',
                     ignore_if_exists: bool):
        """
        Create a new table or view.

        :param table_path: Path :class:`ObjectPath` of the table or view to be created.
        :param table: The table definition :class:`CatalogBaseTable`.
        :param ignore_if_exists: Flag to specify behavior when a table or view already exists at
                                 the given path:
                                 if set to false, it throws a TableAlreadyExistException,
                                 if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database in tablePath doesn't exist.
                TableAlreadyExistException if table already exists and ignoreIfExists is false.
        """
        self._j_catalog.createTable(table_path._j_object_path, table._j_catalog_base_table,
                                    ignore_if_exists)

    def alter_table(self, table_path: 'ObjectPath', new_table: 'CatalogBaseTable',
                    ignore_if_not_exists):
        """
        Modify an existing table or view.
        Note that the new and old CatalogBaseTable must be of the same type. For example,
        this doesn't allow alter a regular table to partitioned table, or alter a view to a table,
        and vice versa.

        :param table_path: Path :class:`ObjectPath` of the table or view to be modified.
        :param new_table: The new table definition :class:`CatalogBaseTable`.
        :param ignore_if_not_exists: Flag to specify behavior when the table or view does not exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist.
        """
        self._j_catalog.alterTable(table_path._j_object_path, new_table._j_catalog_base_table,
                                   ignore_if_not_exists)

    def list_partitions(self,
                        table_path: 'ObjectPath',
                        partition_spec: 'CatalogPartitionSpec' = None)\
            -> List['CatalogPartitionSpec']:
        """
        Get CatalogPartitionSpec of all partitions of the table.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: The partition spec :class:`CatalogPartitionSpec` to list.
        :return: A list of :class:`CatalogPartitionSpec` of the table.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException thrown if the table does not exist in the catalog.
                TableNotPartitionedException thrown if the table is not partitioned.
        """
        if partition_spec is None:
            return [CatalogPartitionSpec(p) for p in self._j_catalog.listPartitions(
                table_path._j_object_path)]
        else:
            return [CatalogPartitionSpec(p) for p in self._j_catalog.listPartitions(
                table_path._j_object_path, partition_spec._j_catalog_partition_spec)]

    def get_partition(self, table_path: 'ObjectPath', partition_spec: 'CatalogPartitionSpec') \
            -> 'CatalogPartition':
        """
        Get a partition of the given table.
        The given partition spec keys and values need to be matched exactly for a result.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: The partition spec :class:`CatalogPartitionSpec` of partition to get.
        :return: The requested partition :class:`CatalogPartition`.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException thrown if the partition doesn't exist.
        """
        return CatalogPartition._get(self._j_catalog.getPartition(
            table_path._j_object_path, partition_spec._j_catalog_partition_spec))

    def partition_exists(self, table_path: 'ObjectPath',
                         partition_spec: 'CatalogPartitionSpec') -> bool:
        """
        Check whether a partition exists or not.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition to
                               check.
        :return: true if the partition exists.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.partitionExists(
            table_path._j_object_path, partition_spec._j_catalog_partition_spec)

    def create_partition(self, table_path: 'ObjectPath', partition_spec: 'CatalogPartitionSpec',
                         partition: 'CatalogPartition', ignore_if_exists: bool):
        """
        Create a partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition.
        :param partition: The partition :class:`CatalogPartition` to add.
        :param ignore_if_exists: Flag to specify behavior if a table with the given name already
                                 exists:
                                 if set to false, it throws a TableAlreadyExistException,
                                 if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException thrown if the target table does not exist.
                TableNotPartitionedException thrown if the target table is not partitioned.
                PartitionSpecInvalidException thrown if the given partition spec is invalid.
                PartitionAlreadyExistsException thrown if the target partition already exists.
        """
        self._j_catalog.createPartition(table_path._j_object_path,
                                        partition_spec._j_catalog_partition_spec,
                                        partition._j_catalog_partition,
                                        ignore_if_exists)

    def drop_partition(self, table_path: 'ObjectPath', partition_spec: 'CatalogPartitionSpec',
                       ignore_if_not_exists: bool):
        """
        Drop a partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition to
                               drop.
        :param ignore_if_not_exists: Flag to specify behavior if the database does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException thrown if the target partition does not exist.
        """
        self._j_catalog.dropPartition(table_path._j_object_path,
                                      partition_spec._j_catalog_partition_spec,
                                      ignore_if_not_exists)

    def alter_partition(self, table_path: 'ObjectPath', partition_spec: 'CatalogPartitionSpec',
                        new_partition: 'CatalogPartition', ignore_if_not_exists: bool):
        """
        Alter a partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition to
                               alter.
        :param new_partition: New partition :class:`CatalogPartition` to replace the old one.
        :param ignore_if_not_exists: Flag to specify behavior if the database does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException thrown if the target partition does not exist.
        """
        self._j_catalog.alterPartition(table_path._j_object_path,
                                       partition_spec._j_catalog_partition_spec,
                                       new_partition._j_catalog_partition,
                                       ignore_if_not_exists)

    def list_functions(self, database_name: str) -> List[str]:
        """
        List the names of all functions in the given database. An empty list is returned if none is
        registered.

        :param database_name: Name of the database.
        :return: A list of the names of the functions in this database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listFunctions(database_name))

    def list_procedures(self, database_name: str) -> List[str]:
        """
        List the names of all procedures in the given database. An empty list is returned if none is
        registered.

        :param database_name: Name of the database.
        :return: A list of the names of the procedures in this database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listProcedures(database_name))

    def get_function(self, function_path: 'ObjectPath') -> 'CatalogFunction':
        """
        Get the function.

        :param function_path: Path :class:`ObjectPath` of the function.
        :return: The requested function :class:`CatalogFunction`.
        :raise: CatalogException in case of any runtime exception.
                FunctionNotExistException if the function does not exist in the catalog.
        """
        return CatalogFunction._get(self._j_catalog.getFunction(function_path._j_object_path))

    def get_procedure(self, procedure_path: 'ObjectPath') -> 'Procedure':
        """
        Get the procedure.

        :param procedure_path: Path :class:`ObjectPath` of the procedure.
        :return: The requested procedure :class:`Procedure`.
        :raise: CatalogException in case of any runtime exception.
                ProcedureNotExistException if the procedure does not exist in the catalog.
        """
        return Procedure._get(self._j_catalog.getProcedure(procedure_path._j_object_path))

    def function_exists(self, function_path: 'ObjectPath') -> bool:
        """
        Check whether a function exists or not.

        :param function_path: Path :class:`ObjectPath` of the function.
        :return: true if the function exists in the catalog false otherwise.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.functionExists(function_path._j_object_path)

    def create_function(self, function_path: 'ObjectPath', function: 'CatalogFunction',
                        ignore_if_exists: bool):
        """
        Create a function.

        :param function_path: Path :class:`ObjectPath` of the function.
        :param function: The function :class:`CatalogFunction` to be created.
        :param ignore_if_exists: Flag to specify behavior if a function with the given name
                                 already exists:
                                 if set to false, it throws a FunctionAlreadyExistException,
                                 if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                FunctionAlreadyExistException if the function already exist.
                DatabaseNotExistException     if the given database does not exist.
        """
        self._j_catalog.createFunction(function_path._j_object_path,
                                       function._j_catalog_function,
                                       ignore_if_exists)

    def alter_function(self, function_path: 'ObjectPath', new_function: 'CatalogFunction',
                       ignore_if_not_exists: bool):
        """
        Modify an existing function.

        :param function_path: Path :class:`ObjectPath` of the function.
        :param new_function: The function :class:`CatalogFunction` to be modified.
        :param ignore_if_not_exists: Flag to specify behavior if the function does not exist:
                                     if set to false, throw an exception
                                     if set to true, nothing happens
        :raise: CatalogException in case of any runtime exception.
                FunctionNotExistException if the function does not exist.
        """
        self._j_catalog.alterFunction(function_path._j_object_path,
                                      new_function._j_catalog_function,
                                      ignore_if_not_exists)

    def drop_function(self, function_path: 'ObjectPath', ignore_if_not_exists: bool):
        """
        Drop a function.

        :param function_path: Path :class:`ObjectPath` of the function to be dropped.
        :param ignore_if_not_exists: Flag to specify behavior if the function does not exist:
                                     if set to false, throw an exception
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                FunctionNotExistException if the function does not exist.
        """
        self._j_catalog.dropFunction(function_path._j_object_path, ignore_if_not_exists)

    def list_models(self, database_name: str) -> List[str]:
        """
        List the names of all models in the given database. An empty list is returned if none is
        registered.

        :param database_name: Name of the database.
        :return: A list of the names of the models in this database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listModels(database_name))

    def get_model(self, model_path: 'ObjectPath') -> 'CatalogModel':
        """
        Get the model.

        :param model_path: Path :class:`ObjectPath` of the model.
        :return: The requested :class:`CatalogModel`.
        :raise: CatalogException in case of any runtime exception.
                ModelNotExistException if the model does not exist in the catalog.
        """
        return CatalogModel._get(self._j_catalog.getModel(model_path._j_object_path))

    def model_exists(self, model_path: 'ObjectPath') -> bool:
        """
        Check whether a model exists or not.

        :param model_path: Path :class:`ObjectPath` of the model.
        :return: true if the model exists in the catalog false otherwise.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.modelExists(model_path._j_object_path)

    def drop_model(self, model_path: 'ObjectPath', ignore_if_not_exists: bool):
        """
        Drop a model.

        :param model_path: Path :class:`ObjectPath` of the model to be dropped.
        :param ignore_if_not_exists: Flag to specify behavior if the model does not exist:
                                     if set to false, throw an exception
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                ModelNotExistException if the model does not exist.
        """
        self._j_catalog.dropModel(model_path._j_object_path, ignore_if_not_exists)

    def rename_model(self, model_path: 'ObjectPath', new_model_name: str,
                     ignore_if_not_exists: bool):
        """
        Rename an existing model.

        :param model_path: Path :class:`ObjectPath` of the model to be renamed.
        :param new_model_name: The new name of the model.
        :param ignore_if_not_exists: Flag to specify behavior when the model does not exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                ModelNotExistException if the model does not exist.
        """
        self._j_catalog.renameModel(model_path._j_object_path, new_model_name, ignore_if_not_exists)

    def create_model(self, model_path: 'ObjectPath', model: 'CatalogModel',
                     ignore_if_exists: bool):
        """
        Create a new model.

        :param model_path: Path :class:`ObjectPath` of the model to be created.
        :param model: The model definition :class:`CatalogModel`.
        :param ignore_if_exists: Flag to specify behavior when a model already exists at
                                 the given path:
                                 if set to false, it throws a ModelAlreadyExistException,
                                 if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database in tablePath doesn't exist.
                ModelAlreadyExistException if model already exists and ignoreIfExists is false.
        """
        self._j_catalog.createModel(model_path._j_object_path, model._j_catalog_model,
                                    ignore_if_exists)

    def alter_model(self, model_path: 'ObjectPath', new_model: 'CatalogModel',
                    ignore_if_not_exists):
        """
        Modify an existing model.

        :param model_path: Path :class:`ObjectPath` of the model to be modified.
        :param new_model: The new model definition :class:`CatalogModel`.
        :param ignore_if_not_exists: Flag to specify behavior when the model does not exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                ModelNotExistException if the model does not exist.
        """
        self._j_catalog.alterModel(model_path._j_object_path, new_model._j_catalog_model,
                                   ignore_if_not_exists)

    def get_table_statistics(self, table_path: 'ObjectPath') -> 'CatalogTableStatistics':
        """
        Get the statistics of a table.

        :param table_path: Path :class:`ObjectPath` of the table.
        :return: The statistics :class:`CatalogTableStatistics` of the given table.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist in the catalog.
        """
        return CatalogTableStatistics(
            j_catalog_table_statistics=self._j_catalog.getTableStatistics(
                table_path._j_object_path))

    def get_table_column_statistics(self, table_path: 'ObjectPath') -> 'CatalogColumnStatistics':
        """
        Get the column statistics of a table.

        :param table_path: Path :class:`ObjectPath` of the table.
        :return: The column statistics :class:`CatalogColumnStatistics` of the given table.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist in the catalog.
        """
        return CatalogColumnStatistics(
            j_catalog_column_statistics=self._j_catalog.getTableColumnStatistics(
                table_path._j_object_path))

    def get_partition_statistics(self,
                                 table_path: 'ObjectPath',
                                 partition_spec: 'CatalogPartitionSpec') \
            -> 'CatalogTableStatistics':
        """
        Get the statistics of a partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition.
        :return: The statistics :class:`CatalogTableStatistics` of the given partition.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        return CatalogTableStatistics(
            j_catalog_table_statistics=self._j_catalog.getPartitionStatistics(
                table_path._j_object_path, partition_spec._j_catalog_partition_spec))

    def bulk_get_partition_statistics(self,
                                      table_path: 'ObjectPath',
                                      partition_specs: List['CatalogPartitionSpec']) \
            -> List['CatalogTableStatistics']:
        """
        Get a list of statistics of given partitions.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_specs: The list of :class:`CatalogPartitionSpec` of the given partitions.
        :return: The statistics list of :class:`CatalogTableStatistics` of the given partitions.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        return [CatalogTableStatistics(j_catalog_table_statistics=p)
                for p in self._j_catalog.bulkGetPartitionStatistics(table_path._j_object_path,
                partition_specs)]

    def get_partition_column_statistics(self,
                                        table_path: 'ObjectPath',
                                        partition_spec: 'CatalogPartitionSpec') \
            -> 'CatalogColumnStatistics':
        """
        Get the column statistics of a partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition.
        :return: The column statistics :class:`CatalogColumnStatistics` of the given partition.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        return CatalogColumnStatistics(
            j_catalog_column_statistics=self._j_catalog.getPartitionColumnStatistics(
                table_path._j_object_path, partition_spec._j_catalog_partition_spec))

    def bulk_get_partition_column_statistics(self,
                                             table_path: 'ObjectPath',
                                             partition_specs: List['CatalogPartitionSpec']) \
            -> List['CatalogColumnStatistics']:
        """
        Get a list of the column statistics for the given partitions.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_specs: The list of :class:`CatalogPartitionSpec` of the given partitions.
        :return: The statistics list of :class:`CatalogTableStatistics` of the given partitions.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        return [CatalogColumnStatistics(j_catalog_column_statistics=p)
                for p in self._j_catalog.bulkGetPartitionStatistics(
                table_path._j_object_path, partition_specs)]

    def alter_table_statistics(self,
                               table_path: 'ObjectPath',
                               table_statistics: 'CatalogTableStatistics',
                               ignore_if_not_exists: bool):
        """
        Update the statistics of a table.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param table_statistics: New statistics :class:`CatalogTableStatistics` to update.
        :param ignore_if_not_exists: Flag to specify behavior if the table does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist in the catalog.
        """
        self._j_catalog.alterTableStatistics(
            table_path._j_object_path,
            table_statistics._j_catalog_table_statistics,
            ignore_if_not_exists)

    def alter_table_column_statistics(self,
                                      table_path: 'ObjectPath',
                                      column_statistics: 'CatalogColumnStatistics',
                                      ignore_if_not_exists: bool):
        """
        Update the column statistics of a table.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param column_statistics: New column statistics :class:`CatalogColumnStatistics` to update.
        :param ignore_if_not_exists: Flag to specify behavior if the column does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist in the catalog.
        """
        self._j_catalog.alterTableColumnStatistics(
            table_path._j_object_path,
            column_statistics._j_catalog_column_statistics,
            ignore_if_not_exists)

    def alter_partition_statistics(self,
                                   table_path: 'ObjectPath',
                                   partition_spec: 'CatalogPartitionSpec',
                                   partition_statistics: 'CatalogTableStatistics',
                                   ignore_if_not_exists: bool):
        """
        Update the statistics of a table partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition.
        :param partition_statistics: New statistics :class:`CatalogTableStatistics` to update.
        :param ignore_if_not_exists: Flag to specify behavior if the partition does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        self._j_catalog.alterPartitionStatistics(
            table_path._j_object_path,
            partition_spec._j_catalog_partition_spec,
            partition_statistics._j_catalog_table_statistics,
            ignore_if_not_exists)

    def alter_partition_column_statistics(self,
                                          table_path: 'ObjectPath',
                                          partition_spec: 'CatalogPartitionSpec',
                                          column_statistics: 'CatalogColumnStatistics',
                                          ignore_if_not_exists: bool):
        """
        Update the column statistics of a table partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition.
        :param column_statistics: New column statistics :class:`CatalogColumnStatistics` to update.
        :param ignore_if_not_exists: Flag to specify behavior if the partition does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        self._j_catalog.alterPartitionColumnStatistics(
            table_path._j_object_path,
            partition_spec._j_catalog_partition_spec,
            column_statistics._j_catalog_column_statistics,
            ignore_if_not_exists)


class CatalogDatabase(object):
    """
    Represents a database object in a catalog.
    """

    def __init__(self, j_catalog_database):
        self._j_catalog_database = j_catalog_database

    @staticmethod
    def create_instance(
        properties: Dict[str, str],
        comment: str = None
    ) -> "CatalogDatabase":
        """
        Creates an instance of CatalogDatabase.

        :param properties: Property of the database
        :param comment: Comment of the database
        """
        assert properties is not None

        gateway = get_gateway()
        return CatalogDatabase(gateway.jvm.org.apache.flink.table.catalog.CatalogDatabaseImpl(
            properties, comment))

    @staticmethod
    def _get(j_catalog_database):
        return CatalogDatabase(j_catalog_database)

    def get_properties(self) -> Dict[str, str]:
        """
        Get a map of properties associated with the database.
        """
        return dict(self._j_catalog_database.getProperties())

    def get_comment(self) -> str:
        """
        Get comment of the database.

        :return: Comment of the database.
        """
        return self._j_catalog_database.getComment()

    def copy(self) -> 'CatalogDatabase':
        """
        Get a deep copy of the CatalogDatabase instance.

        :return: A copy of CatalogDatabase instance.
        """
        return CatalogDatabase(self._j_catalog_database.copy())

    def get_description(self) -> Optional[str]:
        """
        Get a brief description of the database.

        :return: An optional short description of the database.
        """
        description = self._j_catalog_database.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self) -> Optional[str]:
        """
        Get a detailed description of the database.

        :return: An optional long description of the database.
        """
        detailed_description = self._j_catalog_database.getDetailedDescription()
        if detailed_description.isPresent():
            return detailed_description.get()
        else:
            return None


class CatalogBaseTable(object):
    """
    CatalogBaseTable is the common parent of table and view. It has a map of
    key-value pairs defining the properties of the table.
    """

    def __init__(self, j_catalog_base_table):
        self._j_catalog_base_table = j_catalog_base_table

    @staticmethod
    def create_table(
        schema: TableSchema,
        partition_keys: List[str] = [],
        properties: Dict[str, str] = {},
        comment: str = None
    ) -> "CatalogBaseTable":
        """
        Create an instance of CatalogBaseTable for the catalog table.

        :param schema: the table schema
        :param partition_keys: the partition keys, default empty
        :param properties: the properties of the catalog table
        :param comment: the comment of the catalog table
        """
        assert schema is not None
        assert partition_keys is not None
        assert properties is not None

        gateway = get_gateway()
        return CatalogBaseTable(
            gateway.jvm.org.apache.flink.table.catalog.CatalogTable.newBuilder()
            .schema(schema._j_table_schema.toSchema())
            .comment(comment)
            .partitionKeys(partition_keys)
            .options(properties)
            .build())

    @staticmethod
    def create_view(
        original_query: str,
        expanded_query: str,
        schema: TableSchema,
        properties: Dict[str, str],
        comment: str = None
    ) -> "CatalogBaseTable":
        """
        Create an instance of CatalogBaseTable for the catalog view.

        :param original_query: the original text of the view definition
        :param expanded_query: the expanded text of the original view definition, this is needed
                               because the context such as current DB is lost after the session,
                               in which view is defined, is gone. Expanded query text takes care
                               of the this, as an example.
        :param schema: the table schema
        :param properties: the properties of the catalog view
        :param comment: the comment of the catalog view
        """
        assert original_query is not None
        assert expanded_query is not None
        assert schema is not None
        assert properties is not None

        gateway = get_gateway()
        return CatalogBaseTable(
            gateway.jvm.org.apache.flink.table.catalog.CatalogViewImpl(
                original_query, expanded_query, schema._j_table_schema, properties, comment))

    @staticmethod
    def _get(j_catalog_base_table):
        return CatalogBaseTable(j_catalog_base_table)

    def get_options(self):
        """
        Returns a map of string-based options.

        In case of CatalogTable, these options may determine the kind of connector and its
        configuration for accessing the data in the external system.

        :return: Property map of the table/view.

        .. versionadded:: 1.11.0
        """
        return dict(self._j_catalog_base_table.getOptions())

    def get_unresolved_schema(self) -> Schema:
        """
        Returns the schema of the table or view.

        The schema can reference objects from other catalogs and will be resolved and validated by
        the framework when accessing the table or view.
        """
        return Schema(self._j_catalog_base_table.getUnresolvedSchema())

    def get_comment(self) -> str:
        """
        Get comment of the table or view.

        :return: Comment of the table/view.
        """
        return self._j_catalog_base_table.getComment()

    def copy(self) -> 'CatalogBaseTable':
        """
        Get a deep copy of the CatalogBaseTable instance.

        :return: An copy of the CatalogBaseTable instance.
        """
        return CatalogBaseTable(self._j_catalog_base_table.copy())

    def get_description(self) -> Optional[str]:
        """
        Get a brief description of the table or view.

        :return: An optional short description of the table/view.
        """
        description = self._j_catalog_base_table.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self) -> Optional[str]:
        """
        Get a detailed description of the table or view.

        :return: An optional long description of the table/view.
        """
        detailed_description = self._j_catalog_base_table.getDetailedDescription()
        if detailed_description.isPresent():
            return detailed_description.get()
        else:
            return None


class CatalogPartition(object):
    """
    Represents a partition object in catalog.
    """

    def __init__(self, j_catalog_partition):
        self._j_catalog_partition = j_catalog_partition

    @staticmethod
    def create_instance(
        properties: Dict[str, str],
        comment: str = None
    ) -> "CatalogPartition":
        """
        Creates an instance of CatalogPartition.

        :param properties: Property of the partition
        :param comment: Comment of the partition
        """
        assert properties is not None

        gateway = get_gateway()
        return CatalogPartition(
            gateway.jvm.org.apache.flink.table.catalog.CatalogPartitionImpl(
                properties, comment))

    @staticmethod
    def _get(j_catalog_partition):
        return CatalogPartition(j_catalog_partition)

    def get_properties(self) -> Dict[str, str]:
        """
        Get a map of properties associated with the partition.

        :return: A map of properties with the partition.
        """
        return dict(self._j_catalog_partition.getProperties())

    def copy(self) -> 'CatalogPartition':
        """
        Get a deep copy of the CatalogPartition instance.

        :return: A copy of CatalogPartition instance.
        """
        return CatalogPartition(self._j_catalog_partition.copy())

    def get_description(self) -> Optional[str]:
        """
        Get a brief description of the partition object.

        :return: An optional short description of partition object.
        """
        description = self._j_catalog_partition.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self) -> Optional[str]:
        """
        Get a detailed description of the partition object.

        :return: An optional long description of the partition object.
        """
        detailed_description = self._j_catalog_partition.getDetailedDescription()
        if detailed_description.isPresent():
            return detailed_description.get()
        else:
            return None

    def get_comment(self) -> str:
        """
        Get comment of the partition.

        :return: Comment of the partition.
        """
        return self._j_catalog_partition.getComment()


class CatalogFunction(object):
    """
    Interface for a function in a catalog.
    """

    def __init__(self, j_catalog_function):
        self._j_catalog_function = j_catalog_function

    @staticmethod
    def create_instance(
        class_name: str,
        function_language: str = 'Python'
    ) -> "CatalogFunction":
        """
        Creates an instance of CatalogDatabase.

        :param class_name: full qualified path of the class name
        :param function_language: language of the function, must be one of
                                  'Python', 'Java' or 'Scala'. (default Python)
        """
        assert class_name is not None

        gateway = get_gateway()
        FunctionLanguage = gateway.jvm.org.apache.flink.table.catalog.FunctionLanguage
        if function_language.lower() == 'python':
            function_language = FunctionLanguage.PYTHON
        elif function_language.lower() == 'java':
            function_language = FunctionLanguage.JAVA
        elif function_language.lower() == 'scala':
            function_language = FunctionLanguage.SCALA
        else:
            raise ValueError("function_language must be one of 'Python', 'Java' or 'Scala'")
        return CatalogFunction(
            gateway.jvm.org.apache.flink.table.catalog.CatalogFunctionImpl(
                class_name, function_language))

    @staticmethod
    def _get(j_catalog_function):
        return CatalogFunction(j_catalog_function)

    def get_class_name(self) -> str:
        """
        Get the full name of the class backing the function.

        :return: The full name of the class.
        """
        return self._j_catalog_function.getClassName()

    def copy(self) -> 'CatalogFunction':
        """
        Create a deep copy of the function.

        :return: A deep copy of "this" instance.
        """
        return CatalogFunction(self._j_catalog_function.copy())

    def get_description(self) -> Optional[str]:
        """
        Get a brief description of the function.

        :return: An optional short description of function.
        """
        description = self._j_catalog_function.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self) -> Optional[str]:
        """
        Get a detailed description of the function.

        :return: An optional long description of the function.
        """
        detailed_description = self._j_catalog_function.getDetailedDescription()
        if detailed_description.isPresent():
            return detailed_description.get()
        else:
            return None

    def get_function_language(self):
        """
        Get the language used for the function definition.

        :return: the language type of the function definition

        .. versionadded:: 1.10.0
        """
        return self._j_catalog_function.getFunctionLanguage()


class CatalogModel(object):
    """
    Interface for a model in a catalog.
    """

    def __init__(self, j_catalog_model):
        self._j_catalog_model = j_catalog_model

    @staticmethod
    def create_model(
        input_schema: Schema,
        output_schema: Schema,
        options: Dict[str, str] = {},
        comment: str = None
    ) -> "CatalogModel":
        """
        Create an instance of CatalogModel for the catalog model.

        :param input_schema: the model input schema
        :param output_schema: the model output schema
        :param options: the properties of the catalog model
        :param comment: the comment of the catalog model
        """
        assert input_schema is not None
        assert output_schema is not None
        assert options is not None

        gateway = get_gateway()
        return CatalogModel(
            gateway.jvm.org.apache.flink.table.catalog.CatalogModel.of(
                input_schema._j_schema, output_schema._j_schema, options, comment))

    @staticmethod
    def _get(j_catalog_model):
        return CatalogModel(j_catalog_model)

    def copy(self) -> 'CatalogModel':
        """
        Create a deep copy of the model.

        :return: A deep copy of "this" instance.
        """
        return CatalogModel(self._j_catalog_model.copy())

    def get_comment(self) -> str:
        """
        Get comment of the model.

        :return: Comment of model.
        """
        return self._j_catalog_model.getComment()

    def get_options(self):
        """
        Returns a map of string-based options.

        :return: Property map of the model.

        .. versionadded:: 1.11.0
        """
        return dict(self._j_catalog_model.getOptions())


class Procedure(object):
    """
    Interface for a procedure in a catalog.
    """

    def __init__(self, j_procedure):
        self._j_procedure = j_procedure

    @staticmethod
    def _get(j_procedure):
        return Procedure(j_procedure)


class ObjectPath(object):
    """
    A database name and object (table/view/function) name combo in a catalog.
    """

    def __init__(self, database_name=None, object_name=None, j_object_path=None):
        if j_object_path is None:
            gateway = get_gateway()
            self._j_object_path = gateway.jvm.ObjectPath(database_name, object_name)
        else:
            self._j_object_path = j_object_path

    def __str__(self):
        return self._j_object_path.toString()

    def __hash__(self):
        return self._j_object_path.hashCode()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._j_object_path.equals(
            other._j_object_path)

    def get_database_name(self) -> str:
        return self._j_object_path.getDatabaseName()

    def get_object_name(self) -> str:
        return self._j_object_path.getObjectName()

    def get_full_name(self) -> str:
        return self._j_object_path.getFullName()

    @staticmethod
    def from_string(full_name: str) -> 'ObjectPath':
        gateway = get_gateway()
        return ObjectPath(j_object_path=gateway.jvm.ObjectPath.fromString(full_name))


class CatalogPartitionSpec(object):
    """
    Represents a partition spec object in catalog.
    Partition columns and values are NOT of strict order, and they need to be re-arranged to the
    correct order by comparing with a list of strictly ordered partition keys.
    """

    def __init__(self, partition_spec):
        if isinstance(partition_spec, dict):
            gateway = get_gateway()
            self._j_catalog_partition_spec = gateway.jvm.CatalogPartitionSpec(partition_spec)
        else:
            self._j_catalog_partition_spec = partition_spec

    def __str__(self):
        return self._j_catalog_partition_spec.toString()

    def __hash__(self):
        return self._j_catalog_partition_spec.hashCode()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._j_catalog_partition_spec.equals(
            other._j_catalog_partition_spec)

    def get_partition_spec(self) -> Dict[str, str]:
        """
        Get the partition spec as key-value map.

        :return: A map of partition spec keys and values.
        """
        return dict(self._j_catalog_partition_spec.getPartitionSpec())


class CatalogTableStatistics(object):
    """
    Statistics for a non-partitioned table or a partition of a partitioned table.
    """

    def __init__(self, row_count=None, field_count=None, total_size=None, raw_data_size=None,
                 properties=None, j_catalog_table_statistics=None):
        gateway = get_gateway()
        java_import(gateway.jvm, "org.apache.flink.table.catalog.stats.CatalogTableStatistics")
        if j_catalog_table_statistics is None:
            if properties is None:
                self._j_catalog_table_statistics = gateway.jvm.CatalogTableStatistics(
                    row_count, field_count, total_size, raw_data_size)
            else:
                self._j_catalog_table_statistics = gateway.jvm.CatalogTableStatistics(
                    row_count, field_count, total_size, raw_data_size, properties)
        else:
            self._j_catalog_table_statistics = j_catalog_table_statistics

    def get_row_count(self) -> int:
        """
        The number of rows in the table or partition.
        """
        return self._j_catalog_table_statistics.getRowCount()

    def get_field_count(self) -> int:
        """
        The number of files on disk.
        """
        return self._j_catalog_table_statistics.getFileCount()

    def get_total_size(self) -> int:
        """
        The total size in bytes.
        """
        return self._j_catalog_table_statistics.getTotalSize()

    def get_raw_data_size(self) -> int:
        """
        The raw data size (size when loaded in memory) in bytes.
        """
        return self._j_catalog_table_statistics.getRawDataSize()

    def get_properties(self) -> Dict[str, str]:
        return dict(self._j_catalog_table_statistics.getProperties())

    def copy(self) -> 'CatalogTableStatistics':
        """
        Create a deep copy of "this" instance.
        """
        return CatalogTableStatistics(
            j_catalog_table_statistics=self._j_catalog_table_statistics.copy())


class CatalogColumnStatistics(object):
    """
    Column statistics of a table or partition.
    """

    def __init__(self, column_statistics_data=None, properties=None,
                 j_catalog_column_statistics=None):
        if j_catalog_column_statistics is None:
            gateway = get_gateway()
            java_import(gateway.jvm, "org.apache.flink.table.catalog.stats.CatalogColumnStatistics")
            if properties is None:
                self._j_catalog_column_statistics = gateway.jvm.CatalogColumnStatistics(
                    column_statistics_data)
            else:
                self._j_catalog_column_statistics = gateway.jvm.CatalogColumnStatistics(
                    column_statistics_data, properties)
        else:
            self._j_catalog_column_statistics = j_catalog_column_statistics

    def get_column_statistics_data(self):
        return self._j_catalog_column_statistics.getColumnStatisticsData()

    def get_properties(self) -> Dict[str, str]:
        return dict(self._j_catalog_column_statistics.getProperties())

    def copy(self) -> 'CatalogColumnStatistics':
        return CatalogColumnStatistics(
            j_catalog_column_statistics=self._j_catalog_column_statistics.copy())


class HiveCatalog(Catalog):
    """
    A catalog implementation for Hive.
    """

    def __init__(self, catalog_name: str, default_database: str = None, hive_conf_dir: str = None,
                 hadoop_conf_dir: str = None, hive_version: str = None):
        assert catalog_name is not None

        gateway = get_gateway()

        j_hive_catalog = gateway.jvm.org.apache.flink.table.catalog.hive.HiveCatalog(
            catalog_name, default_database, hive_conf_dir, hadoop_conf_dir, hive_version)
        super(HiveCatalog, self).__init__(j_hive_catalog)


class JdbcCatalog(Catalog):
    """
    A catalog implementation for Jdbc.
    """
    def __init__(self, catalog_name: str, default_database: str, username: str, pwd: str,
                 base_url: str):
        assert catalog_name is not None
        assert default_database is not None
        assert username is not None
        assert pwd is not None
        assert base_url is not None

        from pyflink.java_gateway import get_gateway
        gateway = get_gateway()

        j_jdbc_catalog = gateway.jvm.org.apache.flink.connector.jdbc.catalog.JdbcCatalog(
            catalog_name, default_database, username, pwd, base_url)
        super(JdbcCatalog, self).__init__(j_jdbc_catalog)


class CatalogDescriptor:
    """
    Describes a catalog with the catalog name and configuration.
    A CatalogDescriptor is a template for creating a catalog instance. It closely resembles the
    "CREATE CATALOG" SQL DDL statement, containing catalog name and catalog configuration.
    """
    def __init__(self, j_catalog_descriptor):
        self._j_catalog_descriptor = j_catalog_descriptor

    @staticmethod
    def of(catalog_name: str, configuration: Configuration, comment: str = None):
        assert catalog_name is not None
        assert configuration is not None

        from pyflink.java_gateway import get_gateway
        gateway = get_gateway()

        j_catalog_descriptor = gateway.jvm.org.apache.flink.table.catalog.CatalogDescriptor.of(
            catalog_name, configuration._j_configuration, comment)
        return CatalogDescriptor(j_catalog_descriptor)


class ObjectIdentifier(object):
    """
    Identifies an object in a catalog, including tables, views, function, or types.
    An :class:`ObjectIdentifier` must be fully qualified. It is the responsibility of the catalog
    manager to resolve an :class:`ObjectIdentifier` to an object.

    While Path :class:`ObjectPath` is used within the same catalog, instances of this class can be
    used across catalogs. An :class:`ObjectPath` only describes the name and database of an
    object and so is scoped over a particular catalog, but an :class:`ObjectIdentifier` is fully
    qualified and describes the name, database and catalog of the object.

    Two objects are considered equal if they share the same :class:`ObjectIdentifier` in a session
    context, such as a :class:`~pyflink.table.TableEnvironment`, where catalogs (or objects in a
    catalog) have not been added, deleted or modified.
    """

    def __init__(self, j_object_identifier):
        self._j_object_identifier = j_object_identifier

    def __str__(self):
        return self._j_object_identifier.toString()

    def __hash__(self):
        return self._j_object_identifier.hashCode()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._j_object_identifier.equals(
            other._j_object_identifier
        )

    @staticmethod
    def of(catalog_name: str, database_name: str, object_name: str) -> "ObjectIdentifier":
        assert catalog_name is not None, "Catalog name must not be null."
        assert database_name is not None, "Database name must not be null."
        assert object_name is not None, "Object name must not be null."

        gateway = get_gateway()
        j_object_identifier = gateway.jvm.org.apache.flink.table.catalog.ObjectIdentifier.of(
            catalog_name, database_name, object_name
        )
        return ObjectIdentifier(j_object_identifier=j_object_identifier)

    def get_catalog_name(self) -> str:
        return self._j_object_identifier.getCatalogName()

    def get_database_name(self) -> str:
        return self._j_object_identifier.getDatabaseName()

    def get_object_name(self) -> str:
        return self._j_object_identifier.getObjectName()

    def to_object_path(self) -> ObjectPath:
        """
        Convert this :class:`ObjectIdentifier` to :class:`ObjectPath`.

        Throws a TableException if the identifier cannot be converted.
        """
        j_object_path = self._j_object_identifier.toObjectPath()
        return ObjectPath(j_object_path=j_object_path)

    def to_list(self) -> List[str]:
        """
        List of the component names of this object identifier.
        """
        return self._j_object_identifier.toList()

    def as_serializable_string(self) -> str:
        """
        Returns a string that fully serializes this instance. The serialized string can be used for
        transmitting or persisting an object identifier.

        Throws a TableException if the identifier cannot be serialized.
        """
        return self._j_object_identifier.asSerializableString()

    def as_summary_string(self) -> str:
        """
        Returns a string that summarizes this instance for printing to a console or log.
        """
        return self._j_object_identifier.asSummaryString()


class Column(metaclass=ABCMeta):
    """
    Representation of a column in a :class:`ResolvedSchema`.

    A table column describes either a :class:`PhysicalColumn`, :class:`ComputedColumn`, or
    :class:`MetadataColumn`.
    """

    def __init__(self, j_column):
        self._j_column = j_column

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._j_column.equals(other._j_column)

    def __hash__(self):
        return self._j_column.hashCode()

    def __str__(self):
        return self._j_column.toString()

    @staticmethod
    def _from_j_column(j_column) -> Optional["Column"]:
        """
        Returns a non-abstract column, either a :class:`PhysicalColumn`, a :class:`ComputedColumn`,
        or a :class:`MetadataColumn` from an org.apache.flink.table.catalog.Column.
        """
        if j_column is None:
            return None
        gateway = get_gateway()
        JColumn = gateway.jvm.org.apache.flink.table.catalog.Column
        JPhysicalColumn = gateway.jvm.org.apache.flink.table.catalog.Column.PhysicalColumn
        JComputedColumn = gateway.jvm.org.apache.flink.table.catalog.Column.ComputedColumn
        JMetadataColumn = gateway.jvm.org.apache.flink.table.catalog.Column.MetadataColumn
        j_clz = j_column.getClass()

        if not get_java_class(JColumn).isAssignableFrom(j_clz):
            raise TypeError("The input %s is not an instance of Column." % j_column)

        if get_java_class(JPhysicalColumn).isAssignableFrom(j_column.getClass()):
            return PhysicalColumn(j_physical_column=j_column.getClass())
        elif get_java_class(JComputedColumn).isAssignableFrom(j_column.getClass()):
            return ComputedColumn(j_computed_column=j_column.getClass())
        elif get_java_class(JMetadataColumn).isAssignableFrom(j_column.getClass()):
            return MetadataColumn(j_metadata_column=j_column.getClass())
        else:
            return None

    @staticmethod
    def physical(name: str, data_type: DataType) -> "PhysicalColumn":
        """
        Creates a regular table column that represents physical data.
        """
        gateway = get_gateway()
        j_data_type = _to_java_data_type(data_type)
        j_physical_column = gateway.jvm.org.apache.flink.table.catalog.Column.physical(
            name, j_data_type
        )
        return PhysicalColumn(j_physical_column)

    @staticmethod
    def computed(name: str, resolved_expression: ResolvedExpression) -> "ComputedColumn":
        """
        Creates a computed column that is computed from the given
        :class:`~pyflink.table.ResolvedExpression`.
        """
        gateway = get_gateway()
        j_resolved_expression = resolved_expression
        j_computed_column = gateway.jvm.org.apache.flink.table.catalog.Column.computed(
            name, j_resolved_expression
        )
        return ComputedColumn(j_computed_column)

    @staticmethod
    def metadata(
        name: str, data_type: DataType, metadata_key: Optional[str], is_virtual: bool
    ) -> "MetadataColumn":
        """
        Creates a metadata column from metadata of the given column name or from metadata of the
        given key (if not null).

        Allows to specify whether the column is virtual or not.
        """
        gateway = get_gateway()
        j_data_type = _to_java_data_type(data_type)
        j_metadata_column = gateway.jvm.org.apache.flink.table.catalog.Column.metadata(
            name, j_data_type, metadata_key, is_virtual
        )
        return MetadataColumn(j_metadata_column)

    @abstractmethod
    def with_comment(self, comment: Optional[str]):
        """
        Add the comment to the column and return the new object.
        """
        pass

    @abstractmethod
    def is_physical(self) -> bool:
        """
        Returns whether the given column is a physical column of a table; neither computed nor
        metadata.
        """
        pass

    @abstractmethod
    def is_persisted(self) -> bool:
        """
        Returns whether the given column is persisted in a sink operation.
        """
        pass

    def get_data_type(self) -> DataType:
        """
        Returns the data type of this column.
        """
        j_data_type = self._j_column.getDataType()
        return DataType(_from_java_data_type(j_data_type))

    def get_name(self):
        """
        Returns the name of this column.
        """
        return self._j_column.getName()

    def get_comment(self) -> Optional[str]:
        """
        Returns the comment of this column.
        """
        optional_result = self._j_column.getComment()
        return optional_result.get() if optional_result.isPresent() else None

    def as_summary_string(self) -> str:
        """
        Returns a string that summarizes this column for printing to a console.
        """
        return self._j_column.asSummaryString()

    @abstractmethod
    def explain_extras(self) -> Optional[str]:
        """
        Returns an explanation of specific column extras next to name and type.
        """
        pass

    @abstractmethod
    def copy(self, new_type: DataType) -> "Column":
        """
        Returns a copy of the column with a replaced :class:`~pyflink.table.types.DataType`.
        """
        pass

    @abstractmethod
    def rename(self, new_name: str) -> "Column":
        """
        Returns a copy of the column with a replaced name.
        """
        pass


class PhysicalColumn(Column):
    """
    Representation of a physical column.
    """

    def __init__(self, j_physical_column):
        super().__init__(j_physical_column)
        self._j_physical_column = j_physical_column

    def with_comment(self, comment: str) -> "PhysicalColumn":
        return self._j_physical_column.withComment(comment)

    def is_physical(self) -> bool:
        return True

    def is_persisted(self) -> bool:
        return True

    def explain_extras(self) -> Optional[str]:
        return None

    def copy(self, new_data_type: DataType) -> Column:
        return self._j_physical_column.copy(new_data_type)

    def rename(self, new_name: str) -> Column:
        return self._j_physical_column.rename(new_name)


class ComputedColumn(Column):
    """
    Representation of a computed column.
    """

    def __init__(self, j_computed_column):
        super().__init__(j_computed_column)
        self._j_computed_column = j_computed_column

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._j_computed_column.equals(
            other._j_computed_column
        )

    def __hash__(self):
        return self._j_computed_column.hashCode()

    def with_comment(self, comment: str) -> "ComputedColumn":
        return self._j_computed_column.withComment(comment)

    def is_physical(self) -> bool:
        return False

    def is_persisted(self) -> bool:
        return False

    def get_expression(self) -> None:
        return self._j_computed_column.getExpression()

    def explain_extras(self) -> Optional[str]:
        optional_result = self._j_computed_column.explainExtras()
        return optional_result.get() if optional_result.isPresent() else None

    def copy(self, new_data_type: DataType) -> Column:
        return self._j_computed_column.copy(new_data_type)

    def rename(self, new_name: str) -> Column:
        return self._j_computed_column.rename(new_name)


class MetadataColumn(Column):
    """
    Representation of a metadata column.
    """

    def __init__(self, j_metadata_column):
        super().__init__(j_metadata_column)
        self._j_metadata_column = j_metadata_column

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._j_metadata_column.equals(
            other._j_metadata_column
        )

    def __hash__(self):
        return self._j_metadata_column.hashCode()

    def is_virtual(self) -> bool:
        return self._j_metadata_column.isVirtual()

    def get_metadata_key(self) -> Optional[str]:
        optional_result = self._j_metadata_column.getMetadataKey()
        return optional_result.get() if optional_result.isPresent() else None

    def with_comment(self, comment: str) -> "MetadataColumn":
        return self._j_metadata_column.withComment(comment)

    def is_physical(self) -> bool:
        return False

    def is_persisted(self) -> bool:
        return self._j_metadata_column.isPersisted()

    def explain_extras(self) -> Optional[str]:
        optional_result = self._j_metadata_column.explainExtras()
        return optional_result.get() if optional_result.isPresent() else None

    def copy(self, new_data_type: DataType) -> Column:
        return self._j_metadata_column.copy(new_data_type)

    def rename(self, new_name: str) -> Column:
        return self._j_metadata_column.rename(new_name)


class WatermarkSpec:
    """
    Representation of a watermark specification in :class:`ResolvedSchema`.

    It defines the rowtime attribute and a :class:`~pyflink.table.ResolvedExpression`
    for watermark generation.
    """

    def __init__(self, j_watermark_spec):
        self._j_watermark_spec = j_watermark_spec

    def __str__(self):
        return self._j_watermark_spec.toString()

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._j_watermark_spec.equals(
            other._j_watermark_spec
        )

    def __hash__(self):
        return self._j_watermark_spec.hashCode()

    @staticmethod
    def of(rowtime_attribute: str, watermark_expression: ResolvedExpression):
        """
        Creates a :class:`WatermarkSpec` from a given rowtime attribute and a watermark
        expression.
        """
        gateway = get_gateway()
        j_watermark_spec = gateway.jvm.org.apache.flink.table.catalog.WatermarkSpec.of(
            rowtime_attribute, watermark_expression._j_resolved_expr
        )
        return WatermarkSpec(j_watermark_spec)

    def get_rowtime_attribute(self) -> str:
        """
        Returns the name of a rowtime attribute.

        The referenced attribute must be present in the :class:`ResolvedSchema`
        and must be of :class:`~pyflink.table.types.TimestampType`
        """
        return self._j_watermark_spec.getRowtimeAttribute()

    def get_watermark_expression(self) -> ResolvedExpression:
        """
        Returns the :class:`~pyflink.table.ResolvedExpression` for watermark generation.
        """
        j_watermark_expression = self._j_watermark_spec.getWatermarkExpression()
        return ResolvedExpression(j_watermark_expression)

    def as_summary_string(self) -> str:
        """
        Prints the watermark spec in a readable way.
        """
        return self._j_watermark_spec.asSummaryString()


class Constraint(metaclass=ABCMeta):
    """
    Integrity constraints, generally referred to simply as constraints, define the valid states of
    SQL-data by constraining the values in the base tables.
    """

    def __init__(self, j_constraint):
        self._j_constraint = j_constraint

    def get_name(self) -> str:
        """
        Returns the name of the constraint.
        """
        return self._j_constraint.getName()

    def is_enforced(self) -> bool:
        """
        Constraints can either be enforced or non-enforced. If a constraint is enforced it will be
        checked whenever any SQL statement is executed that results in data or schema changes. If
        the constraint is not enforced the owner of the data is responsible for ensuring data
        integrity.
        Flink will rely on the information as valid and might use it for query optimisations.
        """
        return self._j_constraint.isEnforced()

    def get_type(self) -> "ConstraintType":
        """
        Returns the type of the constraint, which could be `PRIMARY_KEY` or `UNIQUE_KEY`.
        """
        j_constraint_type = self._j_constraint.getType().name()
        return self.ConstraintType[j_constraint_type]

    def as_summary_string(self) -> str:
        """
        Prints the constraint in a readable way.
        """
        return self._j_constraint.asSummaryString()

    class ConstraintType(Enum):
        """
        Type of the constraint.

        Unique constraints:

        - UNIQUE - is satisfied if and only if there do not exist two rows that have same
         non-null values in the unique columns
        - PRIMARY KEY - additionally to UNIQUE constraint, it requires none of the values in
          specified columns be a null value. Moreover there can be only a single PRIMARY KEY
          defined for a Table.
        """

        PRIMARY_KEY = 0
        UNIQUE_KEY = 1


class UniqueConstraint(Constraint):
    """
    A unique key constraint. It can be declared also as a PRIMARY KEY.
    """

    def __init__(self, j_unique_constraint=None):
        self._j_unique_constraint = j_unique_constraint
        super().__init__(j_unique_constraint)

    @staticmethod
    def primary_key(name: str, columns: List[str]) -> 'UniqueConstraint':
        """
        Creates a non enforced PRIMARY_KEY constraint.
        """
        gateway = get_gateway()
        j_unique_constraint = gateway.jvm.org.apache.flink.table.catalog.UniqueConstraint(
            name, columns
        )
        return UniqueConstraint(j_unique_constraint=j_unique_constraint)

    def get_columns(self) -> List[str]:
        """
        List of column names for which the primary key was defined.
        """
        return self._j_unique_constraint.getColumns()

    def get_type_string(self) -> str:
        """
        Returns a string representation of the underlying constraint type.
        """
        return self._j_unique_constraint.getTypeString()


class ResolvedSchema(object):
    """
    Schema of a table or view consisting of columns, constraints, and watermark specifications.

    This class is the result of resolving a :class:`~pyflink.table.Schema` into a final validated
    representation.

    - Data types and functions have been expanded to fully qualified identifiers.
    - Time attributes are represented in the column's data type.
    - :class:`pyflink.table.Expression` have been translated to
      :class:`pyflink.table.ResolvedExpression`

    This class should not be passed into a connector. It is therefore also not serializable.
    Instead, the :func:`to_physical_row_data_type` can be passed around where necessary.
    """

    _j_resolved_schema: JavaObject

    def __init__(
        self,
        columns: List[Column] = None,
        watermark_specs: List[WatermarkSpec] = None,
        primary_key: Optional[UniqueConstraint] = None,
        j_resolved_schema=None,
    ):
        if j_resolved_schema is None:
            assert columns is not None
            assert watermark_specs is not None

            gateway = get_gateway()
            j_columns = to_jarray(
                gateway.jvm.org.apache.flink.table.catalog.Column, [c._j_column for c in columns]
            )
            j_watermark_specs = to_jarray(
                gateway.jvm.org.apache.flink.table.catalog.WatermarkSpec,
                [w._j_watermark_spec for w in watermark_specs],
            )
            j_primary_key = primary_key._j_unique_constraint if primary_key is not None else None
            self._j_resolved_schema = gateway.jvm.org.apache.flink.table.catalog.ResolvedSchema(
                j_columns, j_watermark_specs, j_primary_key
            )
        else:
            self._j_resolved_schema = j_resolved_schema

    def __str__(self):
        return self._j_resolved_schema.toString()

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._j_resolved_schema.equals(
            other._j_resolved_schema
        )

    def __hash__(self):
        return self._j_resolved_schema.hashCode()

    @staticmethod
    def of(columns: List[Column]) -> "ResolvedSchema":
        """
        Shortcut for a resolved schema of only columns.
        """
        gateway = get_gateway()
        j_columns = to_jarray(
            gateway.jvm.org.apache.flink.table.catalog.Column, [c._j_column for c in columns]
        )
        j_resolved_schema = gateway.jvm.org.apache.flink.table.catalog.ResolvedSchema.of(j_columns)
        return ResolvedSchema(j_resolved_schema=j_resolved_schema)

    @staticmethod
    def physical(column_names: List[str], column_data_types: List[DataType]) -> "ResolvedSchema":
        """
        Shortcut for a resolved schema of only physical columns.
        """
        gateway = get_gateway()
        j_col_names = to_jarray(gateway.jvm.String, column_names)
        j_col_data_types = to_jarray(
            gateway.jvm.org.apache.flink.table.types.DataType,
            [_to_java_data_type(c) for c in column_data_types],
        )
        j_resolved_schema = gateway.jvm.org.apache.flink.table.catalog.ResolvedSchema.physical(
            j_col_names, j_col_data_types
        )
        return ResolvedSchema(j_resolved_schema=j_resolved_schema)

    def get_column_count(self) -> int:
        """
        Returns the number of :class:`~pyflink.table.catalog.Column` of this schema.
        """
        return self._j_resolved_schema.getColumnCount()

    def get_columns(self) -> List[Column]:
        """
        Returns all :class:`~pyflink.table.catalog.Column` of this schema.
        """
        j_columns = self._j_resolved_schema.getColumns()
        return [Column._from_j_column(j_column) for j_column in j_columns]

    def get_column_names(self) -> List[str]:
        """
        Returns all column names. It does not distinguish between different kinds of columns.
        """
        return self._j_resolved_schema.getColumnNames()

    def get_column_data_types(self) -> List[DataType]:
        """
        Returns all column data types. It does not distinguish between different kinds of columns.
        """
        j_data_types = self._j_resolved_schema.getColumnDataTypes()
        return [_from_java_data_type(j_data_type) for j_data_type in j_data_types]

    def get_column(self, column_index_or_name: Union[int, str]) -> Optional[Column]:
        """
        Returns the :class:`~pyflink.table.catalog.Column` instance for the given column index or
        name.

        :param column_index_or_name: either the index of the column or the name of the column
        """
        optional_result = self._j_resolved_schema.getColumn(column_index_or_name)
        return Column._from_j_column(optional_result.get()) if optional_result.isPresent() else None

    def get_watermark_specs(self) -> List[WatermarkSpec]:
        """
        Returns a list of watermark specifications each consisting of a rowtime attribute and
        watermark strategy expression.

        Note: Currently, there is at most one :class:`~pyflink.table.catalog.WatermarkSpec`
        in the list, because we don't support multiple watermark definitions yet.
        """
        j_watermark_specs = self._j_resolved_schema.getWatermarkSpecs()
        return [WatermarkSpec(j_watermark_spec) for j_watermark_spec in j_watermark_specs]

    def get_primary_key(self) -> Optional[UniqueConstraint]:
        """
        Returns the primary key if it has been defined.
        """
        optional_result = self._j_resolved_schema.getPrimaryKey()
        return (
            UniqueConstraint(j_unique_constraint=optional_result.get())
            if optional_result.isPresent()
            else None
        )

    def get_primary_key_indexes(self) -> List[int]:
        """
        Returns the primary key indexes, if any, otherwise returns an empty list.
        """
        return self._j_resolved_schema.getPrimaryKeyIndexes()

    def to_source_row_data_type(self) -> DataType:
        """
        Converts all columns of this schema into a (possibly nested) row data type.

        This method returns the **source-to-query schema**.

        Note: The returned row data type contains physical, computed, and metadata columns. Be
        careful when using this method in a table source or table sink. In many cases,
        :func:`to_physical_row_data_type` might be more appropriate.
        """
        j_data_type = self._j_resolved_schema.toSourceRowDataType()
        return _from_java_data_type(j_data_type)

    def to_physical_row_data_type(self) -> DataType:
        """
        Converts all physical columns of this schema into a (possibly nested) row data type.

        Note: The returned row data type contains only physical columns. It does not include
        computed or metadata columns.
        """
        j_data_type = self._j_resolved_schema.toPhysicalRowDataType()
        return _from_java_data_type(j_data_type)

    def to_sink_row_data_type(self):
        """
        Converts all persisted columns of this schema into a (possibly nested) row data type.

        This method returns the **query-to-sink schema**.

        Note: Computed columns and virtual columns are excluded in the returned row data type. The
        data type contains the columns of :func:`to_physical_row_data_type` plus persisted metadata
        columns.
        """
        j_data_type = self._j_resolved_schema.toSinkRowDataType()
        return _from_java_data_type(j_data_type)
