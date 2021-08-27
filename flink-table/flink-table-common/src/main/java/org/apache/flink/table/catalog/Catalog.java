/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.factories.TableFactory;

import java.util.List;
import java.util.Optional;

/**
 * This interface is responsible for reading and writing metadata such as database/table/views/UDFs
 * from a registered catalog. It connects a registered catalog and Flink's Table API. This interface
 * only processes permanent metadata objects. In order to process temporary objects, a catalog can
 * also implement the {@link TemporaryOperationListener} interface.
 */
@PublicEvolving
public interface Catalog {

    /**
     * Returns a factory for creating instances from catalog objects.
     *
     * <p>This method enables bypassing the discovery process. Implementers can directly pass
     * internal catalog-specific objects to their own factory. For example, a custom {@link
     * CatalogTable} can be processed by a custom {@link DynamicTableFactory}.
     *
     * <p>Because all factories are interfaces, the returned {@link Factory} instance can implement
     * multiple supported extension points. An {@code instanceof} check is performed by the caller
     * that checks whether a required factory is implemented; otherwise the discovery process is
     * used.
     */
    default Optional<Factory> getFactory() {
        return Optional.empty();
    }

    /**
     * Get an optional {@link TableFactory} instance that's responsible for generating table-related
     * instances stored in this catalog, instances such as source/sink.
     *
     * @return an optional TableFactory instance
     * @deprecated Use {@link #getFactory()} for the new factory stack. The new factory stack uses
     *     the new table sources and sinks defined in FLIP-95 and a slightly different discovery
     *     mechanism.
     */
    @Deprecated
    default Optional<TableFactory> getTableFactory() {
        return Optional.empty();
    }

    /**
     * Get an optional {@link FunctionDefinitionFactory} instance that's responsible for
     * instantiating function definitions.
     *
     * @return an optional FunctionDefinitionFactory instance
     */
    default Optional<FunctionDefinitionFactory> getFunctionDefinitionFactory() {
        return Optional.empty();
    }

    /**
     * Open the catalog. Used for any required preparation in initialization phase.
     *
     * @throws CatalogException in case of any runtime exception
     */
    void open() throws CatalogException;

    /**
     * Close the catalog when it is no longer needed and release any resource that it might be
     * holding.
     *
     * @throws CatalogException in case of any runtime exception
     */
    void close() throws CatalogException;

    // ------ databases ------

    /**
     * Get the name of the default database for this catalog. The default database will be the
     * current database for the catalog when user's session doesn't specify a current database. The
     * value probably comes from configuration, will not change for the life time of the catalog
     * instance.
     *
     * @return the name of the current database
     * @throws CatalogException in case of any runtime exception
     */
    String getDefaultDatabase() throws CatalogException;

    /**
     * Get the names of all databases in this catalog.
     *
     * @return a list of the names of all databases
     * @throws CatalogException in case of any runtime exception
     */
    List<String> listDatabases() throws CatalogException;

    /**
     * Get a database from this catalog.
     *
     * @param databaseName Name of the database
     * @return The requested database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException;

    /**
     * Check if a database exists in this catalog.
     *
     * @param databaseName Name of the database
     * @return true if the given database exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    boolean databaseExists(String databaseName) throws CatalogException;

    /**
     * Create a database.
     *
     * @param name Name of the database to be created
     * @param database The database definition
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *     exists: if set to false, throw a DatabaseAlreadyExistException, if set to true, do
     *     nothing.
     * @throws DatabaseAlreadyExistException if the given database already exists and ignoreIfExists
     *     is false
     * @throws CatalogException in case of any runtime exception
     */
    void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException;

    /**
     * Drop a database.
     *
     * @param name Name of the database to be dropped.
     * @param ignoreIfNotExists Flag to specify behavior when the database does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    default void dropDatabase(String name, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        dropDatabase(name, ignoreIfNotExists, false);
    }

    /**
     * Drop a database.
     *
     * @param name Name of the database to be dropped.
     * @param ignoreIfNotExists Flag to specify behavior when the database does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @param cascade Flag to specify behavior when the database contains table or function: if set
     *     to true, delete all tables and functions in the database and then delete the database, if
     *     set to false, throw an exception.
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws DatabaseNotEmptyException if the given database is not empty and isRestrict is true
     * @throws CatalogException in case of any runtime exception
     */
    void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException;

    /**
     * Modify an existing database.
     *
     * @param name Name of the database to be modified
     * @param newDatabase The new database definition
     * @param ignoreIfNotExists Flag to specify behavior when the given database does not exist: if
     *     set to false, throw an exception, if set to true, do nothing.
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException;

    // ------ tables and views ------

    /**
     * Get names of all tables and views under this database. An empty list is returned if none
     * exists.
     *
     * @return a list of the names of all tables and views in this database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException;

    /**
     * Get names of all views under this database. An empty list is returned if none exists.
     *
     * @param databaseName the name of the given database
     * @return a list of the names of all views in the given database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException;

    /**
     * Returns a {@link CatalogTable} or {@link CatalogView} identified by the given {@link
     * ObjectPath}. The framework will resolve the metadata objects when necessary.
     *
     * @param tablePath Path of the table or view
     * @return The requested table or view
     * @throws TableNotExistException if the target does not exist
     * @throws CatalogException in case of any runtime exception
     */
    CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException;

    /**
     * Check if a table or view exists in this catalog.
     *
     * @param tablePath Path of the table or view
     * @return true if the given table exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    boolean tableExists(ObjectPath tablePath) throws CatalogException;

    /**
     * Drop a table or view.
     *
     * @param tablePath Path of the table or view to be dropped
     * @param ignoreIfNotExists Flag to specify behavior when the table or view does not exist: if
     *     set to false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table or view does not exist
     * @throws CatalogException in case of any runtime exception
     */
    void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException;

    /**
     * Rename an existing table or view.
     *
     * @param tablePath Path of the table or view to be renamed
     * @param newTableName the new name of the table or view
     * @param ignoreIfNotExists Flag to specify behavior when the table or view does not exist: if
     *     set to false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     * @throws CatalogException in case of any runtime exception
     */
    void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException;

    /**
     * Creates a new table or view.
     *
     * <p>The framework will make sure to call this method with fully validated {@link
     * ResolvedCatalogTable} or {@link ResolvedCatalogView}. Those instances are easy to serialize
     * for a durable catalog implementation.
     *
     * @param tablePath path of the table or view to be created
     * @param table the table definition
     * @param ignoreIfExists flag to specify behavior when a table or view already exists at the
     *     given path: if set to false, it throws a TableAlreadyExistException, if set to true, do
     *     nothing.
     * @throws TableAlreadyExistException if table already exists and ignoreIfExists is false
     * @throws DatabaseNotExistException if the database in tablePath doesn't exist
     * @throws CatalogException in case of any runtime exception
     */
    void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException;

    /**
     * Modifies an existing table or view. Note that the new and old {@link CatalogBaseTable} must
     * be of the same kind. For example, this doesn't allow altering a regular table to partitioned
     * table, or altering a view to a table, and vice versa.
     *
     * <p>The framework will make sure to call this method with fully validated {@link
     * ResolvedCatalogTable} or {@link ResolvedCatalogView}. Those instances are easy to serialize
     * for a durable catalog implementation.
     *
     * @param tablePath path of the table or view to be modified
     * @param newTable the new table definition
     * @param ignoreIfNotExists flag to specify behavior when the table or view does not exist: if
     *     set to false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     * @throws CatalogException in case of any runtime exception
     */
    void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException;

    // ------ partitions ------

    /**
     * Get CatalogPartitionSpec of all partitions of the table.
     *
     * @param tablePath path of the table
     * @return a list of CatalogPartitionSpec of the table
     * @throws TableNotExistException thrown if the table does not exist in the catalog
     * @throws TableNotPartitionedException thrown if the table is not partitioned
     * @throws CatalogException in case of any runtime exception
     */
    List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException;

    /**
     * Get CatalogPartitionSpec of all partitions that is under the given CatalogPartitionSpec in
     * the table.
     *
     * @param tablePath path of the table
     * @param partitionSpec the partition spec to list
     * @return a list of CatalogPartitionSpec that is under the given CatalogPartitionSpec in the
     *     table
     * @throws TableNotExistException thrown if the table does not exist in the catalog
     * @throws TableNotPartitionedException thrown if the table is not partitioned
     * @throws CatalogException in case of any runtime exception
     */
    List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, CatalogException;

    /**
     * Get CatalogPartitionSpec of partitions by expression filters in the table.
     *
     * <p>NOTE: For FieldReferenceExpression, the field index is based on schema of this table
     * instead of partition columns only.
     *
     * <p>The passed in predicates have been translated in conjunctive form.
     *
     * <p>If catalog does not support this interface at present, throw an {@link
     * UnsupportedOperationException} directly. If the catalog does not have a valid filter, throw
     * the {@link UnsupportedOperationException} directly. Planner will fallback to get all
     * partitions and filter by itself.
     *
     * @param tablePath path of the table
     * @param filters filters to push down filter to catalog
     * @return a list of CatalogPartitionSpec that is under the given CatalogPartitionSpec in the
     *     table
     * @throws TableNotExistException thrown if the table does not exist in the catalog
     * @throws TableNotPartitionedException thrown if the table is not partitioned
     * @throws CatalogException in case of any runtime exception
     */
    List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException;

    /**
     * Get a partition of the given table. The given partition spec keys and values need to be
     * matched exactly for a result.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of partition to get
     * @return the requested partition
     * @throws PartitionNotExistException thrown if the partition doesn't exist
     * @throws CatalogException in case of any runtime exception
     */
    CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException;

    /**
     * Check whether a partition exists or not.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition to check
     * @throws CatalogException in case of any runtime exception
     */
    boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException;

    /**
     * Create a partition.
     *
     * @param tablePath path of the table.
     * @param partitionSpec partition spec of the partition
     * @param partition the partition to add.
     * @param ignoreIfExists flag to specify behavior if a table with the given name already exists:
     *     if set to false, it throws a TableAlreadyExistException, if set to true, nothing happens.
     * @throws TableNotExistException thrown if the target table does not exist
     * @throws TableNotPartitionedException thrown if the target table is not partitioned
     * @throws PartitionSpecInvalidException thrown if the given partition spec is invalid
     * @throws PartitionAlreadyExistsException thrown if the target partition already exists
     * @throws CatalogException in case of any runtime exception
     */
    void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException;

    /**
     * Drop a partition.
     *
     * @param tablePath path of the table.
     * @param partitionSpec partition spec of the partition to drop
     * @param ignoreIfNotExists flag to specify behavior if the database does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException thrown if the target partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException;

    /**
     * Alter a partition.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition
     * @param newPartition new partition to replace the old one
     * @param ignoreIfNotExists flag to specify behavior if the database does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException thrown if the target partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException;

    // ------ functions ------

    /**
     * List the names of all functions in the given database. An empty list is returned if none is
     * registered.
     *
     * @param dbName name of the database.
     * @return a list of the names of the functions in this database
     * @throws DatabaseNotExistException if the database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException;

    /**
     * Get the function. Function name should be handled in a case insensitive way.
     *
     * @param functionPath path of the function
     * @return the requested function
     * @throws FunctionNotExistException if the function does not exist in the catalog
     * @throws CatalogException in case of any runtime exception
     */
    CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException;

    /**
     * Check whether a function exists or not. Function name should be handled in a case insensitive
     * way.
     *
     * @param functionPath path of the function
     * @return true if the function exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    boolean functionExists(ObjectPath functionPath) throws CatalogException;

    /**
     * Create a function. Function name should be handled in a case insensitive way.
     *
     * @param functionPath path of the function
     * @param function the function to be created
     * @param ignoreIfExists flag to specify behavior if a function with the given name already
     *     exists: if set to false, it throws a FunctionAlreadyExistException, if set to true,
     *     nothing happens.
     * @throws FunctionAlreadyExistException if the function already exist
     * @throws DatabaseNotExistException if the given database does not exist
     * @throws CatalogException in case of any runtime exception
     */
    void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException;

    /**
     * Modify an existing function. Function name should be handled in a case insensitive way.
     *
     * @param functionPath path of the function
     * @param newFunction the function to be modified
     * @param ignoreIfNotExists flag to specify behavior if the function does not exist: if set to
     *     false, throw an exception if set to true, nothing happens
     * @throws FunctionNotExistException if the function does not exist
     * @throws CatalogException in case of any runtime exception
     */
    void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException;

    /**
     * Drop a function. Function name should be handled in a case insensitive way.
     *
     * @param functionPath path of the function to be dropped
     * @param ignoreIfNotExists plag to specify behavior if the function does not exist: if set to
     *     false, throw an exception if set to true, nothing happens
     * @throws FunctionNotExistException if the function does not exist
     * @throws CatalogException in case of any runtime exception
     */
    void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException;

    // ------ statistics ------

    /**
     * Get the statistics of a table.
     *
     * @param tablePath path of the table
     * @return statistics of the given table
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException in case of any runtime exception
     */
    CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException;

    /**
     * Get the column statistics of a table.
     *
     * @param tablePath path of the table
     * @return column statistics of the given table
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException in case of any runtime exception
     */
    CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException;

    /**
     * Get the statistics of a partition.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition
     * @return statistics of the given partition
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException;

    /**
     * Get the column statistics of a partition.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition
     * @return column statistics of the given partition
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException;

    /**
     * Update the statistics of a table.
     *
     * @param tablePath path of the table
     * @param tableStatistics new statistics to update
     * @param ignoreIfNotExists flag to specify behavior if the table does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException in case of any runtime exception
     */
    void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException;

    /**
     * Update the column statistics of a table.
     *
     * @param tablePath path of the table
     * @param columnStatistics new column statistics to update
     * @param ignoreIfNotExists flag to specify behavior if the table does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws TableNotExistException if the table does not exist in the catalog
     * @throws CatalogException in case of any runtime exception
     */
    void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException;

    /**
     * Update the statistics of a table partition.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition
     * @param partitionStatistics new statistics to update
     * @param ignoreIfNotExists flag to specify behavior if the partition does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException;

    /**
     * Update the column statistics of a table partition.
     *
     * @param tablePath path of the table
     * @param partitionSpec partition spec of the partition @@param columnStatistics new column
     *     statistics to update
     * @param ignoreIfNotExists flag to specify behavior if the partition does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException if the partition does not exist
     * @throws CatalogException in case of any runtime exception
     */
    void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException;
}
