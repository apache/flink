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

import org.apache.flink.table.api.DatabaseAlreadyExistException;
import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.exceptions.PartitionAlreadyExistException;
import org.apache.flink.table.api.exceptions.PartitionNotExistException;
import org.apache.flink.table.api.exceptions.TableNotPartitionedException;
import org.apache.flink.table.plan.stats.TableStats;

/**
 * An interface responsible for manipulating catalog metadata.
 */
public interface ReadableWritableCatalog extends ReadableCatalog {

	// ------ databases ------

	/**
	 * Adds a database to this catalog.
	 *
	 * @param dbName    	The name of the database to add.
	 * @param database        The database to add.
	 * @param ignoreIfExists Flag to specify behavior if a database with the given name already
	 *                       exists: if set to false, it throws a DatabaseAlreadyExistException,
	 *                       if set to true, nothing happens.
	 * @throws DatabaseAlreadyExistException
	 *                       thrown if the database does already exist in the catalog
	 *                       and ignoreIfExists is false
	 */
	void createDatabase(String dbName, CatalogDatabase database, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException;

	/**
	 * Deletes a database from this catalog.
	 *
	 * @param dbName        Name of the database to delete.
	 * @param ignoreIfNotExists Flag to specify behavior if the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
	 */
	void dropDatabase(String dbName, boolean ignoreIfNotExists) throws DatabaseNotExistException;

	/**
	 * Modifies an existing database.
	 *
	 * @param dbName        Name of the database to modify.
	 * @param newDatabase           The new database to replace the existing database.
	 * @param ignoreIfNotExists Flag to specify behavior if the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
	 */
	void alterDatabase(String dbName, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException;

	/**
	 * Renames an existing database.
	 *
	 * @param dbName        Name of the database to modify.
	 * @param newDbName     New name of the database.
	 * @param ignoreIfNotExists Flag to specify behavior if the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
	 */
	void renameDatabase(String dbName, String newDbName, boolean ignoreIfNotExists)
		throws DatabaseNotExistException;

	// ------ tables ------

	/**
	 * Adds a table.
	 * Note that TableStats in the CatalogTable will not be used for creation.
	 * Use {@link #alterTableStats(ObjectPath, TableStats, boolean)} to alter table stats.
	 *
	 * @param tableName      Path of the table to add.
	 * @param table          The table to add.
	 * @param ignoreIfExists Flag to specify behavior if a table with the given name already exists:
	 *                       if set to false, it throws a TableAlreadyExistException,
	 *                       if set to true, nothing happens.
	 * @throws TableAlreadyExistException thrown if table already exists and ignoreIfExists is false
	 * @throws DatabaseNotExistException thrown if the database that this table belongs to doesn't exist
	 */
	void createTable(ObjectPath tableName, CatalogTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException;

	/**
	 * Deletes a table.
	 *
	 * @param tableName         Path of the table to delete.
	 * @param ignoreIfNotExists Flag to specify behavior if the table does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws TableNotExistException    thrown if the table does not exist
	 */
	void dropTable(ObjectPath tableName, boolean ignoreIfNotExists) throws TableNotExistException;

	/**
	 * Modifies an existing newTable.
	 * Note that TableStats in the CatalogTable will not be used for alteration.
	 * Use {@link #alterTableStats(ObjectPath, TableStats, boolean)} to alter table stats.
	 *
	 * @param tableName         Path of the table to modify.
	 * @param newTable             The new table which replaces the existing table.
	 * @param ignoreIfNotExists Flag to specify behavior if the table does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws TableNotExistException   thrown if the new table does not exist
	 */
	void alterTable(ObjectPath tableName, CatalogTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException;

	/**
	 * Renames an existing table.
	 *
	 * @param tableName        Path of the table to modify.
	 * @param newTableName     New name of the table.
	 * @param ignoreIfNotExists Flag to specify behavior if the table does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws TableNotExistException thrown if the table does not exist
	 * @throws DatabaseNotExistException thrown if the database that this table belongs to doesn't exist
	 */
	void renameTable(ObjectPath tableName, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, DatabaseNotExistException;


	// ------ table and column stats ------

	/**
	 * Alter table stats. This only works for non-partitioned tables.
	 *
	 * @param tablePath		Path of the table.
	 * @param newtTableStats	The new TableStats.
	 * @param ignoreIfNotExists    Flag to specify behavior if the table does not exist:
	 *                           if set to false, throw an exception,
	 *                           if set to true, nothing happens.
	 * @throws TableNotExistException thrown if the table does not exist
	 */
	void alterTableStats(ObjectPath tablePath, TableStats newtTableStats, boolean ignoreIfNotExists)
		throws TableNotExistException;

	// ------ partitions ------

	/**
	 * Creates a partition.
	 *
	 * @param tablePath		Path of the table.
	 * @param partition		The partition to add.
	 * @param ignoreIfExists Flag to specify behavior if a table with the given name already exists:
	 *                       if set to false, it throws a TableAlreadyExistException,
	 *                       if set to true, nothing happens.
	 * @throws TableNotExistException
	 * @throws TableNotPartitionedException
	 * @throws PartitionAlreadyExistException
	 */
	void createPartition(ObjectPath tablePath, CatalogPartition partition, boolean ignoreIfExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionAlreadyExistException;

	/**
	 * Drops a partition.
	 *
	 * @param tablePath			Path of the table.
	 * @param partitionSpec		Partition spec of the partition to drop
	 * @param ignoreIfNotExists Flag to specify behavior if the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws TableNotExistException
	 * @throws TableNotPartitionedException
	 * @throws PartitionNotExistException
	 */
	void dropPartition(ObjectPath tablePath, CatalogPartition.PartitionSpec partitionSpec, boolean ignoreIfNotExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException;

	/**
	 * Alters a partition.
	 *
	 * @param tablePath			Path of the table
	 * @param newPartition		New partition to replace the old one
	 * @param ignoreIfNotExists Flag to specify behavior if the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws TableNotExistException
	 * @throws TableNotPartitionedException
	 * @throws PartitionNotExistException
	 */
	void alterPartition(ObjectPath tablePath, CatalogPartition newPartition, boolean ignoreIfNotExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException;
}
