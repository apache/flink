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
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
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

	private String currentDatabase = DEFAULT_DB;

	private final String catalogName;
	private final Map<String, CatalogDatabase> databases;
	private final Map<ObjectPath, CatalogBaseTable> tables;

	public GenericInMemoryCatalog(String name) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty");

		this.catalogName = name;
		this.databases = new LinkedHashMap<>();
		this.databases.put(DEFAULT_DB, new GenericCatalogDatabase(new HashMap<>()));
		this.tables = new LinkedHashMap<>();
	}

	@Override
	public void open() {

	}

	@Override
	public void close() {

	}

	// ------ databases ------

	@Override
	public String getCurrentDatabase() {
		return currentDatabase;
	}

	@Override
	public void setCurrentDatabase(String databaseName) throws DatabaseNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		}

		currentDatabase = databaseName;
	}

	@Override
	public void createDatabase(String databaseName, CatalogDatabase db, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
		checkArgument(db != null);

		if (databaseExists(databaseName)) {
			if (!ignoreIfExists) {
				throw new DatabaseAlreadyExistException(catalogName, databaseName);
			}
		} else {
			databases.put(databaseName, db.copy());
		}
	}

	@Override
	public void dropDatabase(String databaseName, boolean ignoreIfNotExists) throws DatabaseNotExistException,
		DatabaseNotEmptyException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

		if (databases.containsKey(databaseName)) {

			// Make sure the database is empty
			if (isDatabaseEmpty(databaseName)) {
				databases.remove(databaseName);
			} else {
				throw new DatabaseNotEmptyException(catalogName, databaseName);
			}
		} else if (!ignoreIfNotExists) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		}
	}

	private boolean isDatabaseEmpty(String databaseName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

		return tables.keySet().stream().noneMatch(op -> op.getDatabaseName().equals(databaseName));
		// TODO: also check function when function is added.
	}

	@Override
	public void alterDatabase(String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
		checkArgument(newDatabase != null);

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
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		} else {
			return databases.get(databaseName).copy();
		}
	}

	@Override
	public boolean databaseExists(String databaseName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

		return databases.containsKey(databaseName);
	}

	// ------ tables ------

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException {
		checkArgument(tablePath != null);
		checkArgument(table != null);

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
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException {
		checkArgument(tablePath != null);
		checkArgument(newTable != null);

		if (tableExists(tablePath)) {
			tables.put(tablePath, newTable.copy());
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tablePath);
		}
	}

	// ------ tables and views ------

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException {
		checkArgument(tablePath != null);

		if (tableExists(tablePath)) {
			tables.remove(tablePath);
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tablePath);
		}
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, TableAlreadyExistException {
		checkArgument(tablePath != null);
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(newTableName));

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

	@Override
	public List<String> listTables(String databaseName) throws DatabaseNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		}

		return tables.keySet().stream()
			.filter(k -> k.getDatabaseName().equals(databaseName)).map(k -> k.getObjectName())
			.collect(Collectors.toList());
	}

	@Override
	public List<String> listViews(String databaseName) throws DatabaseNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		}

		return tables.keySet().stream()
			.filter(k -> k.getDatabaseName().equals(databaseName))
			.filter(k -> (tables.get(k) instanceof CatalogView)).map(k -> k.getObjectName())
			.collect(Collectors.toList());
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
		checkArgument(tablePath != null);

		if (!tableExists(tablePath)) {
			throw new TableNotExistException(catalogName, tablePath);
		} else {
			return tables.get(tablePath).copy();
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) {
		return tablePath != null && databaseExists(tablePath.getDatabaseName()) && tables.containsKey(tablePath);
	}

}
