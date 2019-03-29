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

import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A generic catalog implementation that holds all meta objects in memory.
 */
public class GenericInMemoryCatalog implements ReadableWritableCatalog {

	public static final String DEFAULT_DB = "default";

	private String defaultDatabaseName = DEFAULT_DB;

	private final String catalogName;
	private final Map<String, CatalogDatabase> databases;
	private final Map<ObjectPath, CommonTable> tables;

	public GenericInMemoryCatalog(String name) {
		Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty");

		this.catalogName = name;
		this.databases = new LinkedHashMap<>();
		this.databases.put(DEFAULT_DB, new GenericCatalogDatabase());
		this.tables = new LinkedHashMap<>();
	}

	@Override
	public String getDefaultDatabaseName() {
		return defaultDatabaseName;
	}

	@Override
	public void setDefaultDatabaseName(String databaseName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

		defaultDatabaseName = databaseName;
	}

	@Override
	public void open() {

	}

	@Override
	public void close() {

	}

	// ------ databases ------

	@Override
	public void createDatabase(String databaseName, CatalogDatabase db, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException {
		if (databaseExists(databaseName)) {
			if (!ignoreIfExists) {
				throw new DatabaseAlreadyExistException(catalogName, databaseName);
			}
		} else {
			databases.put(databaseName, db.copy());
		}
	}

	@Override
	public void dropDatabase(String dbName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		if (databases.containsKey(dbName)) {

			// Make sure the database is empty
			if (isDatabaseEmpty(dbName)) {
				databases.remove(dbName);
			} else {
				throw new DatabaseNotEmptyException(catalogName, dbName);
			}
		} else if (!ignoreIfNotExists) {
			throw new DatabaseNotExistException(catalogName, dbName);
		}
	}

	private boolean isDatabaseEmpty(String databaseName) {
		return tables.keySet().stream().noneMatch(op -> op.getDatabaseName().equals(databaseName));
		// TODO: also check function when function is added.
	}

	@Override
	public void alterDatabase(String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException {
		if (databaseExists(databaseName)) {
			databases.put(databaseName, newDatabase.copy());
		} else if (!ignoreIfNotExists) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		}
	}

	@Override
	public List<String> listDatabases() {
		return new ArrayList<>(databases.keySet());
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException {
		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		} else {
			return databases.get(databaseName).copy();
		}
	}

	@Override
	public boolean databaseExists(String dbName) {
		return databases.containsKey(dbName);
	}

	@Override
	public void renameDatabase(String name, String newName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		if (databaseExists(name)) {
			databases.put(newName, databases.remove(name));
			tables.forEach((objectPath, table) -> {
				if (objectPath.getDatabaseName().equals(name)) {
					tables.put(new ObjectPath(newName, objectPath.getObjectName()), table);
				}
			});
		} else if (!ignoreIfNotExists) {
			throw new DatabaseNotExistException(catalogName, name);
		}

	}

	// ------ tables ------

	@Override
	public void createTable(ObjectPath tablePath, CommonTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException {

		if (!databaseExists(tablePath.getDatabaseName())) {
			throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
		}

		if (tableExists(tablePath)) {
			if (!ignoreIfExists) {
				throw new TableAlreadyExistException(catalogName, tablePath);
			}
		} else {
			tables.put(tablePath, table.copy());
		}
	}

	@Override
	public void alterTable(ObjectPath tablePath, CommonTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException {
		if (tableExists(tablePath)) {
			tables.put(tablePath, newTable.copy());
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tablePath);
		}
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, TableAlreadyExistException {

		if (tableExists(tablePath)) {
			ObjectPath newPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);

			if (tableExists(newPath)) {
				throw new TableAlreadyExistException(catalogName, newPath);
			} else {
				tables.put(newPath, tables.remove(tablePath));
			}
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tablePath);
		}
	}

	// ------ tables and views ------

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException {
		if (tableExists(tablePath)) {
			tables.remove(tablePath);
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tablePath);
		}
	}

	@Override
	public List<String> listTables(String dbName) throws DatabaseNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(dbName), "dbName cannot be null or empty");

		if (!databaseExists(dbName)) {
			throw new DatabaseNotExistException(catalogName, dbName);
		}

		return tables.keySet().stream()
			.filter(k -> k.getDatabaseName().equals(dbName)).map(k -> k.getObjectName())
			.collect(Collectors.toList());
	}

	@Override
	public CommonTable getTable(ObjectPath tableName) throws TableNotExistException {

		if (!tableExists(tableName)) {
			throw new TableNotExistException(catalogName, tableName);
		} else {
			return tables.get(tableName).copy();
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) {
		return databaseExists(tablePath.getDatabaseName()) && tables.containsKey(tablePath);
	}

	// ------ views ------

	@Override
	public List<String> listViews(String databaseName) throws DatabaseNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "dbName cannot be null or empty");

		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		}

		return tables.keySet().stream()
			.filter(k -> k.getDatabaseName().equals(databaseName))
			.filter(k -> (tables.get(k) instanceof CatalogView)).map(k -> k.getObjectName())
			.collect(Collectors.toList());
	}

	@Override
	public void createView(ObjectPath viewPath, CatalogView view, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException {
		createTable(viewPath, view, ignoreIfExists);
	}

	@Override
	public void alterView(ObjectPath viewPath, CatalogView newView, boolean ignoreIfNotExists)
		throws TableNotExistException {
		alterTable(viewPath, newView, ignoreIfNotExists);
	}

	@Override
	public void renameView(ObjectPath viewPath, String newViewName, boolean ignoreIfNotExists)
		throws TableNotExistException {
		renameTable(viewPath, newViewName, ignoreIfNotExists);
	}

}
