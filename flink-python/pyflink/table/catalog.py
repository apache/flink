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

from py4j.java_gateway import java_import

from pyflink.java_gateway import get_gateway
from pyflink.table.table_schema import TableSchema

__all__ = ['Catalog', 'CatalogDatabase', 'CatalogBaseTable', 'CatalogPartition', 'CatalogFunction',
           'ObjectPath', 'CatalogPartitionSpec', 'CatalogTableStatistics',
           'CatalogColumnStatistics', 'HiveCatalog']


class Catalog(object):
    """
    Catalog is responsible for reading and writing metadata such as database/table/views/UDFs
    from a registered catalog. It connects a registered catalog and Flink's Table API.
    """

    def __init__(self, j_catalog):
        self._j_catalog = j_catalog

    @staticmethod
    def _get(j_catalog):
        if j_catalog.getClass().getName() == "org.apache.flink.table.catalog.hive.HiveCatalog":
            return HiveCatalog(j_hive_catalog=j_catalog)
        else:
            return Catalog(j_catalog)

    def get_default_database(self):
        """
        Get the name of the default database for this catalog. The default database will be the
        current database for the catalog when user's session doesn't specify a current database.
        The value probably comes from configuration, will not change for the life time of the
        catalog instance.

        :return: The name of the current database.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.getDefaultDatabase()

    def list_databases(self):
        """
        Get the names of all databases in this catalog.

        :return: A list of the names of all databases.
        :raise: CatalogException in case of any runtime exception.
        """
        return list(self._j_catalog.listDatabases())

    def get_database(self, database_name):
        """
        Get a database from this catalog.

        :param database_name: Name of the database.
        :return: The requested database :class:`CatalogDatabase`.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return CatalogDatabase._get(self._j_catalog.getDatabase(database_name))

    def database_exists(self, database_name):
        """
        Check if a database exists in this catalog.

        :param database_name: Name of the database.
        :return: true if the given database exists in the catalog false otherwise.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.databaseExists(database_name)

    def create_database(self, name, database, ignore_if_exists):
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

    def drop_database(self, name, ignore_if_exists):
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

    def alter_database(self, name, new_database, ignore_if_not_exists):
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

    def list_tables(self, database_name):
        """
        Get names of all tables and views under this database. An empty list is returned if none
        exists.

        :param database_name: Name of the given database.
        :return: A list of the names of all tables and views in this database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listTables(database_name))

    def list_views(self, database_name):
        """
        Get names of all views under this database. An empty list is returned if none exists.

        :param database_name: Name of the given database.
        :return: A list of the names of all views in the given database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listViews(database_name))

    def get_table(self, table_path):
        """
        Get a CatalogTable or CatalogView identified by tablePath.

        :param table_path: Path :class:`ObjectPath` of the table or view.
        :return: The requested table or view :class:`CatalogBaseTable`.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the target does not exist.
        """
        return CatalogBaseTable._get(self._j_catalog.getTable(table_path._j_object_path))

    def table_exists(self, table_path):
        """
        Check if a table or view exists in this catalog.

        :param table_path: Path :class:`ObjectPath` of the table or view.
        :return: true if the given table exists in the catalog false otherwise.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.tableExists(table_path._j_object_path)

    def drop_table(self, table_path, ignore_if_not_exists):
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

    def rename_table(self, table_path, new_table_name, ignore_if_not_exists):
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

    def create_table(self, table_path, table, ignore_if_exists):
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

    def alter_table(self, table_path, new_table, ignore_if_not_exists):
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

    def list_partitions(self, table_path, partition_spec=None):
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

    def get_partition(self, table_path, partition_spec):
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

    def partition_exists(self, table_path, partition_spec):
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

    def create_partition(self, table_path, partition_spec, partition, ignore_if_exists):
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

    def drop_partition(self, table_path, partition_spec, ignore_if_not_exists):
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

    def alter_partition(self, table_path, partition_spec, new_partition, ignore_if_not_exists):
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

    def list_functions(self, database_name):
        """
        List the names of all functions in the given database. An empty list is returned if none is
        registered.

        :param database_name: Name of the database.
        :return: A list of the names of the functions in this database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listFunctions(database_name))

    def get_function(self, function_path):
        """
        Get the function.

        :param function_path: Path :class:`ObjectPath` of the function.
        :return: The requested function :class:`CatalogFunction`.
        :raise: CatalogException in case of any runtime exception.
                FunctionNotExistException if the function does not exist in the catalog.
        """
        return CatalogFunction._get(self._j_catalog.getFunction(function_path._j_object_path))

    def function_exists(self, function_path):
        """
        Check whether a function exists or not.

        :param function_path: Path :class:`ObjectPath` of the function.
        :return: true if the function exists in the catalog false otherwise.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.functionExists(function_path._j_object_path)

    def create_function(self, function_path, function, ignore_if_exists):
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

    def alter_function(self, function_path, new_function, ignore_if_not_exists):
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

    def drop_function(self, function_path, ignore_if_not_exists):
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

    def get_table_statistics(self, table_path):
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

    def get_table_column_statistics(self, table_path):
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

    def get_partition_statistics(self, table_path, partition_spec):
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

    def get_partition_column_statistics(self, table_path, partition_spec):
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

    def alter_table_statistics(self, table_path, table_statistics, ignore_if_not_exists):
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

    def alter_table_column_statistics(self, table_path, column_statistics, ignore_if_not_exists):
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

    def alter_partition_statistics(self, table_path, partition_spec, partition_statistics,
                                   ignore_if_not_exists):
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

    def alter_partition_column_statistics(self, table_path, partition_spec, column_statistics,
                                          ignore_if_not_exists):
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
    def _get(j_catalog_database):
        return CatalogDatabase(j_catalog_database)

    def get_properties(self):
        """
        Get a map of properties associated with the database.
        """
        return dict(self._j_catalog_database.getProperties())

    def get_comment(self):
        """
        Get comment of the database.

        :return: Comment of the database.
        """
        return self._j_catalog_database.getComment()

    def copy(self):
        """
        Get a deep copy of the CatalogDatabase instance.

        :return: A copy of CatalogDatabase instance.
        """
        return CatalogDatabase(self._j_catalog_database.copy())

    def get_description(self):
        """
        Get a brief description of the database.

        :return: An optional short description of the database.
        """
        description = self._j_catalog_database.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self):
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
    def _get(j_catalog_base_table):
        return CatalogBaseTable(j_catalog_base_table)

    def get_properties(self):
        """
        Get the properties of the table.

        :return: Property map of the table/view.
        """
        return dict(self._j_catalog_base_table.getProperties())

    def get_schema(self):
        """
        Get the schema of the table.

        :return: Schema of the table/view.
        """
        return TableSchema(j_table_schema=self._j_catalog_base_table.getSchema())

    def get_comment(self):
        """
        Get comment of the table or view.

        :return: Comment of the table/view.
        """
        return self._j_catalog_base_table.getComment()

    def copy(self):
        """
        Get a deep copy of the CatalogBaseTable instance.

        :return: An copy of the CatalogBaseTable instance.
        """
        return CatalogBaseTable(self._j_catalog_base_table.copy())

    def get_description(self):
        """
        Get a brief description of the table or view.

        :return: An optional short description of the table/view.
        """
        description = self._j_catalog_base_table.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self):
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
    def _get(j_catalog_partition):
        return CatalogPartition(j_catalog_partition)

    def get_properties(self):
        """
        Get a map of properties associated with the partition.

        :return: A map of properties with the partition.
        """
        return dict(self._j_catalog_partition.getProperties())

    def copy(self):
        """
        Get a deep copy of the CatalogPartition instance.

        :return: A copy of CatalogPartition instance.
        """
        return CatalogPartition(self._j_catalog_partition.copy())

    def get_description(self):
        """
        Get a brief description of the partition object.

        :return: An optional short description of partition object.
        """
        description = self._j_catalog_partition.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self):
        """
        Get a detailed description of the partition object.

        :return: An optional long description of the partition object.
        """
        detailed_description = self._j_catalog_partition.getDetailedDescription()
        if detailed_description.isPresent():
            return detailed_description.get()
        else:
            return None

    def get_comment(self):
        """
        Get comment of the partition.

        :return: Comment of the partition.
        :rtype: str
        """
        return self._j_catalog_partition.getComment()


class CatalogFunction(object):
    """
    Interface for a function in a catalog.
    """

    def __init__(self, j_catalog_function):
        self._j_catalog_function = j_catalog_function

    @staticmethod
    def _get(j_catalog_function):
        return CatalogFunction(j_catalog_function)

    def get_class_name(self):
        """
        Get the full name of the class backing the function.

        :return: The full name of the class.
        """
        return self._j_catalog_function.getClassName()

    def get_properties(self):
        """
        Get the properties of the function.

        :return: The properties of the function.
        """
        return dict(self._j_catalog_function.getProperties())

    def copy(self):
        """
        Create a deep copy of the function.

        :return: A deep copy of "this" instance.
        """
        return CatalogFunction(self._j_catalog_function.copy())

    def get_description(self):
        """
        Get a brief description of the function.

        :return: An optional short description of function.
        """
        description = self._j_catalog_function.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self):
        """
        Get a detailed description of the function.

        :return: An optional long description of the function.
        """
        detailed_description = self._j_catalog_function.getDetailedDescription()
        if detailed_description.isPresent():
            return detailed_description.get()
        else:
            return None


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

    def get_database_name(self):
        return self._j_object_path.getDatabaseName()

    def get_object_name(self):
        return self._j_object_path.getObjectName()

    def get_full_name(self):
        return self._j_object_path.getFullName()

    @staticmethod
    def from_string(full_name):
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

    def get_partition_spec(self):
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

    def get_row_count(self):
        """
        The number of rows in the table or partition.
        """
        return self._j_catalog_table_statistics.getRowCount()

    def get_field_count(self):
        """
        The number of files on disk.
        """
        return self._j_catalog_table_statistics.getFileCount()

    def get_total_size(self):
        """
        The total size in bytes.
        """
        return self._j_catalog_table_statistics.getTotalSize()

    def get_raw_data_size(self):
        """
        The raw data size (size when loaded in memory) in bytes.
        """
        return self._j_catalog_table_statistics.getRawDataSize()

    def get_properties(self):
        return dict(self._j_catalog_table_statistics.getProperties())

    def copy(self):
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

    def get_properties(self):
        return dict(self._j_catalog_column_statistics.getProperties())

    def copy(self):
        return CatalogColumnStatistics(
            j_catalog_column_statistics=self._j_catalog_column_statistics.copy())


class HiveCatalog(Catalog):
    """
    A catalog implementation for Hive.
    """

    def __init__(self, catalog_name=None, default_database="default", hive_conf_dir=None,
                 j_hive_catalog=None):
        gateway = get_gateway()

        if j_hive_catalog is None:
            j_hive_catalog = gateway.jvm.org.apache.flink.table.catalog.hive.HiveCatalog(
                catalog_name, default_database, hive_conf_dir)
        super(HiveCatalog, self).__init__(j_hive_catalog)
