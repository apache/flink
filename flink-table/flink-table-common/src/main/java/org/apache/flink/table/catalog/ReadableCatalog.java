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

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;

import java.util.List;

/**
 * This interface is responsible for reading database/table/views/UDFs from a registered catalog.
 * It connects a registered catalog and Flink's Table API.
 */
public interface ReadableCatalog {

	/**
	 * Open the catalog. Used for any required preparation in initialization phase.
	 *
	 * @throws CatalogException in case of any runtime exception
	 */
	void open() throws CatalogException;

	/**
	 * Close the catalog when it is no longer needed and release any resource that it might be holding.
	 *
	 * @throws CatalogException in case of any runtime exception
	 */
	void close() throws CatalogException;

	// ------ databases ------

	/**
	 * Get the name of the current database of this type of catalog. This is used when users refers an object in the catalog
	 * without specifying a database. For example, the current db in a Hive Metastore is 'default' by default.
	 *
	 * @return the name of the current database
	 * @throws CatalogException in case of any runtime exception
	 */
	String getCurrentDatabase() throws CatalogException;

	/**
	 * Set the database with the given name as the current database. A current database is used when users refers an object
	 * in the catalog without specifying a database.
	 *
	 * @param databaseName	the name of the database
	 * @throws DatabaseNotExistException if the given database doesn't exist in the catalog
	 * @throws CatalogException in case of any runtime exception
	 */
	void setCurrentDatabase(String databaseName) throws DatabaseNotExistException, CatalogException;

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
	 * @param databaseName	Name of the database
	 * @return The requested database
	 * @throws DatabaseNotExistException if the database does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException;

	/**
	 * Check if a database exists in this catalog.
	 *
	 * @param databaseName		Name of the database
	 * @return true if the given database exists in the catalog
	 *         false otherwise
	 * @throws CatalogException in case of any runtime exception
	 */
	boolean databaseExists(String databaseName) throws CatalogException;

	// ------ tables and views ------

	/**
	 * Get names of all tables and views under this database. An empty list is returned if none exists.
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
	 * Get a CatalogTable or CatalogView identified by tablePath.
	 *
	 * @param tablePath		Path of the table or view
	 * @return The requested table or view
	 * @throws TableNotExistException if the target does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException;

	/**
	 * Check if a table or view exists in this catalog.
	 *
	 * @param tablePath    Path of the table or view
	 * @return true if the given table exists in the catalog
	 *         false otherwise
	 * @throws CatalogException in case of any runtime exception
	 */
	boolean tableExists(ObjectPath tablePath) throws CatalogException;

	// ------ partitions ------

	/**
	 * Get CatalogPartitionSpec of all partitions of the table.
	 *
	 * @param tablePath	path of the table
	 * @return a list of CatalogPartitionSpec of the table
	 *
	 * @throws TableNotExistException thrown if the table does not exist in the catalog
	 * @throws TableNotPartitionedException thrown if the table is not partitioned
	 * @throws CatalogException	in case of any runtime exception
	 */
	List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
		throws TableNotExistException, TableNotPartitionedException, CatalogException;

	/**
	 * Get CatalogPartitionSpec of all partitions that is under the given CatalogPartitionSpec in the table.
	 *
	 * @param tablePath	path of the table
	 * @param partitionSpec the partition spec to list
	 * @return a list of CatalogPartitionSpec that is under the given CatalogPartitionSpec in the table
	 *
	 * @throws TableNotExistException thrown if the table does not exist in the catalog
	 * @throws TableNotPartitionedException thrown if the table is not partitioned
	 * @throws CatalogException in case of any runtime exception
	 */
	List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws TableNotExistException, TableNotPartitionedException, CatalogException;

	/**
	 * Get a partition of the given table.
	 * The given partition spec keys and values need to be matched exactly for a result.
	 *
	 * @param tablePath path of the table
	 * @param partitionSpec partition spec of partition to get
	 * @return the requested partition
	 *
	 * @throws PartitionNotExistException thrown if the partition is not partitioned
	 * @throws CatalogException	in case of any runtime exception
	 */
	CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws PartitionNotExistException, CatalogException;

	/**
	 * Check whether a partition exists or not.
	 *
	 * @param tablePath	path of the table
	 * @param partitionSpec partition spec of the partition to check
	 * @throws CatalogException in case of any runtime exception
	 */
	boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException;

	// ------ functions ------

	/**
	 * List the names of all functions in the given database. An empty list is returned if none is registered.
	 *
	 * @param dbName name of the database.
	 * @return a list of the names of the functions in this database
	 * @throws DatabaseNotExistException if the database does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException;

	/**
	 * Get the function.
	 *
	 * @param functionPath path of the function
	 * @return the requested function
	 * @throws FunctionNotExistException if the function does not exist in the catalog
	 * @throws CatalogException in case of any runtime exception
	 */
	CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException;

	/**
	 * Check whether a function exists or not.
	 *
	 * @param functionPath path of the function
	 * @return true if the function exists in the catalog
	 *         false otherwise
	 * @throws CatalogException in case of any runtime exception
	 */
	boolean functionExists(ObjectPath functionPath) throws CatalogException;

}
