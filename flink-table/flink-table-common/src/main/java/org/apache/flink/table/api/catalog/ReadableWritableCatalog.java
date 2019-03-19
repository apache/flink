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
	 */
	void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException;

	/**
	 * Drop a database.
	 *
	 * @param name              Name of the database to be dropped.
	 * @param ignoreIfNotExists Flag to specify behavior when the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws DatabaseNotExistException if the given database does not exist
	 */
	void dropDatabase(String name, boolean ignoreIfNotExists) throws DatabaseNotExistException;

	/**
	 * Modify an existing database.
	 *
	 * @param name        Name of the database to be modified
	 * @param newDatabase    The new database definition
	 * @param ignoreIfNotExists Flag to specify behavior when the given database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws DatabaseNotExistException if the given database does not exist
	 */
	void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException;

	/**
	 * Rename an existing database.
	 *
	 * @param name        Name of the database to be renamed
	 * @param newName     the new name of the database
	 * @param ignoreIfNotExists Flag to specify behavior when the given database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws DatabaseNotExistException if the database does not exist
	 */
	void renameDatabase(String name, String newName, boolean ignoreIfNotExists)
		throws DatabaseNotExistException;

	// ------ tables and views ------

	/**
	 * Drop a table or view.
	 *
	 * @param tablePath         Path of the table or view to be dropped
	 * @param ignoreIfNotExists Flag to specify behavior when the table or view does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws TableNotExistException if the table or view does not exist
	 */
	void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException;

	// ------ tables ------

	/**
	 * Create a new table. Note that TableStats in the table is ignored for table creation.
	 *
	 * @param tablePath      Path of the table to be created
	 * @param table          The table definition
	 * @param ignoreIfExists Flag to specify behavior when a table already exists at the given path:
	 *                       if set to false, it throws a TableAlreadyExistException,
	 *                       if set to true, do nothing.
	 * @throws TableAlreadyExistException if table already exists and ignoreIfExists is false
	 * @throws DatabaseNotExistException if the database in tablePath doesn't exist
	 */
	void createTable(ObjectPath tablePath, CatalogTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException;

	/**
	 * Modify an existing table. Note that TableStats in the newTable is ignored for table modification.
	 *
	 * @param tableName         Path of the table to be modified
	 * @param newTable          The new table definition
	 * @param ignoreIfNotExists Flag to specify behavior when the table does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws TableNotExistException if the table does not exist
	 */
	void alterTable(ObjectPath tableName, CatalogTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException;

	/**
	 * Rename an existing table.
	 *
	 * @param tablePath       Path of the table to rename
	 * @param newTableName     the new name of the table
	 * @param ignoreIfNotExists Flag to specify behavior when the table does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws TableNotExistException if the table does not exist
	 * @throws DatabaseNotExistException if the database in tablePath to doesn't exist
	 */
	void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, DatabaseNotExistException;

	// ----- views ------

	/**
	 * Create a view.
	 *
	 * @param viewPath		Path of the view to be created
	 * @param view			The view definition
	 * @param ignoreIfExists    Flag to specify behavior when a table/view already exists at the given path:
	 *                       if set to false, it throws a TableAlreadyExistException,
	 *                       if set to true, do nothing.
	 * @throws TableAlreadyExistException if a table/view already exists and ignoreIfExists is false
	 * @throws DatabaseNotExistException if the database in viewPath doesn't exist
	 */
	void createView(ObjectPath viewPath, CatalogView view, boolean ignoreIfExists) throws TableAlreadyExistException,
		DatabaseNotExistException;

	/**
	 * Modify an existing view.
	 *
	 * @param viewPath         Path of the view to to be modified
	 * @param newView          The new view definition
	 * @param ignoreIfNotExists Flag to specify behavior when the given view does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, do nothing.
	 * @throws TableNotExistException if the given view does not exist
	 */
	void alterView(ObjectPath viewPath, CatalogView newView, boolean ignoreIfNotExists) throws TableNotExistException;

}
