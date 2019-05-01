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

/**
 * An interface responsible for manipulating catalog metadata.
 */
public interface ReadableWritableCatalog extends ReadableCatalog {

	// ------ databases ------

	/**
	 * Create a database.
	 *
	 * @param name           Name of the database to be created
	 * @param database       The database definition
	 * @param ignoreIfExists Flag to specify behavior when a database with the given name already exists:
	 *                       if set to false, throw a DatabaseAlreadyExistException,
	 *                       if set to true, do nothing.
	 * @throws DatabaseAlreadyExistException if the given database already exists and ignoreIfExists is false
	 * @throws CatalogException in case of any runtime exception
	 */
	void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException, CatalogException;

	/**
	 * Drop a database.
	 *
	 * @param name              Name of the database to be dropped.
	 * @param ignoreIfNotExists Flag to specify behavior when the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws DatabaseNotExistException if the given database does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	void dropDatabase(String name, boolean ignoreIfNotExists) throws DatabaseNotExistException,
		DatabaseNotEmptyException, CatalogException;

	/**
	 * Modify an existing database.
	 *
	 * @param name        Name of the database to be modified
	 * @param newDatabase    The new database definition
	 * @param ignoreIfNotExists Flag to specify behavior when the given database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws DatabaseNotExistException if the given database does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, CatalogException;

	// ------ tables and views ------

	/**
	 * Drop a table or view.
	 *
	 * @param tablePath         Path of the table or view to be dropped
	 * @param ignoreIfNotExists Flag to specify behavior when the table or view does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws TableNotExistException if the table or view does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException;

	/**
	 * Rename an existing table or view.
	 *
	 * @param tablePath       Path of the table or view to be renamed
	 * @param newTableName     the new name of the table or view
	 * @param ignoreIfNotExists Flag to specify behavior when the table or view does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws TableNotExistException if the table does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, TableAlreadyExistException, CatalogException;

	/**
	 * Create a new table or view.
	 *
	 * @param tablePath path of the table or view to be created
	 * @param table the table definition
	 * @param ignoreIfExists flag to specify behavior when a table or view already exists at the given path:
	 *                       if set to false, it throws a TableAlreadyExistException,
	 *                       if set to true, do nothing.
	 * @throws TableAlreadyExistException if table already exists and ignoreIfExists is false
	 * @throws DatabaseNotExistException if the database in tablePath doesn't exist
	 * @throws CatalogException in case of any runtime exception
	 */
	void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException, CatalogException;

	/**
	 * Modify an existing table or view.
	 * Note that the new and old CatalogBaseTable must be of the same type. For example, this doesn't
	 * allow alter a regular table to partitioned table, or alter a view to a table, and vice versa.
	 *
	 * @param tablePath path of the table or view to be modified
	 * @param newTable the new table definition
	 * @param ignoreIfNotExists flag to specify behavior when the table or view does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws TableNotExistException if the table does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException;

	// ------ partitions ------

	/**
	 * Create a partition.
	 *
	 * @param tablePath path of the table.
	 * @param partitionSpec partition spec of the partition
	 * @param partition the partition to add.
	 * @param ignoreIfExists flag to specify behavior if a table with the given name already exists:
	 *                       if set to false, it throws a TableAlreadyExistException,
	 *                       if set to true, nothing happens.
	 *
	 * @throws TableNotExistException thrown if the target table does not exist
	 * @throws TableNotPartitionedException thrown if the target table is not partitioned
	 * @throws PartitionSpecInvalidException thrown if the given partition spec is invalid
	 * @throws PartitionAlreadyExistsException thrown if the target partition already exists
	 * @throws CatalogException in case of any runtime exception
	 */
	void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException;

	/**
	 * Drop a partition.
	 *
	 * @param tablePath path of the table.
	 * @param partitionSpec partition spec of the partition to drop
	 * @param ignoreIfNotExists flag to specify behavior if the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 *
	 * @throws PartitionNotExistException thrown if the target partition does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException;

	/**
	 * Alter a partition.
	 *
	 * @param tablePath	path of the table
	 * @param partitionSpec partition spec of the partition
	 * @param newPartition new partition to replace the old one
	 * @param ignoreIfNotExists flag to specify behavior if the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 *
	 * @throws PartitionNotExistException thrown if the target partition does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException;

	// ------ functions ------

	/**
	 * Create a function.
	 *
	 * @param functionPath      path of the function
	 * @param function          the function to be created
	 * @param ignoreIfExists    flag to specify behavior if a function with the given name already exists:
	 *                          if set to false, it throws a FunctionAlreadyExistException,
	 *                          if set to true, nothing happens.
	 * @throws FunctionAlreadyExistException if the function already exist
	 * @throws DatabaseNotExistException     if the given database does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
		throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException;

	/**
	 * Modify an existing function.
	 *
	 * @param functionPath       path of the function
	 * @param newFunction        the function to be modified
	 * @param ignoreIfNotExists  flag to specify behavior if the function does not exist:
	 *                           if set to false, throw an exception
	 *                           if set to true, nothing happens
	 * @throws FunctionNotExistException if the function does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
		throws FunctionNotExistException, CatalogException;

	/**
	 * Drop a function.
	 *
	 * @param functionPath       path of the function to be dropped
	 * @param ignoreIfNotExists  plag to specify behavior if the function does not exist:
	 *                           if set to false, throw an exception
	 *                           if set to true, nothing happens
	 * @throws FunctionNotExistException if the function does not exist
	 * @throws CatalogException in case of any runtime exception
	 */
	void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
		throws FunctionNotExistException, CatalogException;
}
