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

package org.apache.flink.table.api.catalog;

import org.apache.flink.table.api.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.api.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.api.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.api.catalog.exceptions.TableNotExistException;
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

	// ------ tables and views ------

	/**
	 * Deletes a table or view.
	 *
	 * @param objectName         Path of the table or view to delete.
	 * @param ignoreIfNotExists Flag to specify behavior if the table or view does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws TableNotExistException    thrown if the table or view does not exist
	 */
	void dropTable(ObjectPath objectName, boolean ignoreIfNotExists) throws TableNotExistException;


	// ------ tables ------

	/**
	 * Create a new table in the catalog.
	 * Note that TableStats in the CatalogTable will not be used for creation.
	 * Use {@link #alterTableStats(ObjectPath, TableStats, boolean)} to alter table stats.
	 *
	 * @param tableName      Path of the table to create.
	 * @param table          The table to create.
	 * @param ignoreIfExists Flag to specify behavior if a table with the given name already exists:
	 *                       if set to false, it throws a TableAlreadyExistException,
	 *                       if set to true, nothing happens.
	 * @throws TableAlreadyExistException thrown if table already exists and ignoreIfExists is false
	 * @throws DatabaseNotExistException thrown if the database that this table belongs to doesn't exist
	 */
	void createTable(ObjectPath tableName, CatalogTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException;

	/**
	 * Modifies an existing table.
	 * Note that TableStats in the CatalogTable will not be used for alteration.
	 * Use {@link #alterTableStats(ObjectPath, TableStats, boolean)} to alter table stats.
	 *
	 * @param tableName         Path of the table to modify.
	 * @param newTable             The new table which replaces the existing table.
	 * @param ignoreIfNotExists Flag to specify behavior if the table does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws TableNotExistException   thrown if the table does not exist
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

	// ----- views ------

	/**
	 * Creates a view.
	 *
	 * @param viewPath		Path of the view
	 * @param view			The CatalogView to create
	 * @param ignoreIfExists    Flag to specify behavior if a table/view with the given name already exists:
	 *                       if set to false, it throws a TableAlreadyExistException,
	 *                       if set to true, nothing happens.
	 * @throws TableAlreadyExistException thrown if table already exists and ignoreIfExists is false
	 * @throws DatabaseNotExistException thrown if the database that this table belongs to doesn't exist
	 */
	void createView(ObjectPath viewPath, CatalogView view, boolean ignoreIfExists) throws TableAlreadyExistException,
		DatabaseNotExistException;

	/**
	 * Modifies an existing newView.
	 *
	 * @param viewPath         Path of the newView to modify.
	 * @param newView             The new newView which replaces the existing newView.
	 * @param ignoreIfNotExists Flag to specify behavior if the newView does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws TableNotExistException   thrown if the newView does not exist
	 */
	void alterView(ObjectPath viewPath, CatalogView newView, boolean ignoreIfNotExists) throws TableNotExistException;

}
