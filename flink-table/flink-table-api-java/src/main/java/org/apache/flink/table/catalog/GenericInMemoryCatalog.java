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
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A generic catalog implementation that holds all meta objects in memory.
 */
public class GenericInMemoryCatalog implements ReadableWritableCatalog {

	public static final String DEFAULT_DB = "default";

	private String currentDatabase = DEFAULT_DB;

	private final String catalogName;
	private final Map<String, CatalogDatabase> databases;
	private final Map<ObjectPath, CatalogBaseTable> tables;
	private final Map<ObjectPath, CatalogFunction> functions;
	private final Map<ObjectPath, Map<CatalogPartitionSpec, CatalogPartition>> partitions;

	public GenericInMemoryCatalog(String name) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty");

		this.catalogName = name;
		this.databases = new LinkedHashMap<>();
		this.databases.put(DEFAULT_DB, new GenericCatalogDatabase(new HashMap<>()));
		this.tables = new LinkedHashMap<>();
		this.functions = new LinkedHashMap<>();
		this.partitions = new LinkedHashMap<>();
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
		checkNotNull(db);

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

		return tables.keySet().stream().noneMatch(op -> op.getDatabaseName().equals(databaseName)) &&
			functions.keySet().stream().noneMatch(op -> op.getDatabaseName().equals(databaseName));
	}

	@Override
	public void alterDatabase(String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
		checkNotNull(newDatabase);

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
		checkNotNull(tablePath);
		checkNotNull(table);

		if (!databaseExists(tablePath.getDatabaseName())) {
			throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
		}

		if (tableExists(tablePath)) {
			if (!ignoreIfExists) {
				throw new TableAlreadyExistException(catalogName, tablePath);
			}
		} else {
			tables.put(tablePath, table.copy());

			if (isPartitionedTable(tablePath)) {
				partitions.put(tablePath, new LinkedHashMap<>());
			}
		}
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException {
		checkNotNull(tablePath);
		checkNotNull(newTable);

		// TODO: validate the new and old CatalogBaseTable must be of the same type. For example, this doesn't
		//		allow alter a regular table to partitioned table, or alter a view to a table, and vice versa.
		//		And also add unit tests.

		if (tableExists(tablePath)) {
			tables.put(tablePath, newTable.copy());
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tablePath);
		}
	}

	// ------ tables and views ------

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException {
		checkNotNull(tablePath);

		if (tableExists(tablePath)) {
			tables.remove(tablePath);

			partitions.remove(tablePath);
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tablePath);
		}
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, TableAlreadyExistException {
		checkNotNull(tablePath);
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(newTableName));

		if (tableExists(tablePath)) {
			ObjectPath newPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);

			if (tableExists(newPath)) {
				throw new TableAlreadyExistException(catalogName, newPath);
			} else {
				tables.put(newPath, tables.remove(tablePath));

				if (partitions.containsKey(tablePath)) {
					partitions.put(newPath, partitions.remove(tablePath));
				}
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
		checkNotNull(tablePath);

		if (!tableExists(tablePath)) {
			throw new TableNotExistException(catalogName, tablePath);
		} else {
			return tables.get(tablePath).copy();
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) {
		checkNotNull(tablePath);

		return databaseExists(tablePath.getDatabaseName()) && tables.containsKey(tablePath);
	}

	// ------ functions ------

	@Override
	public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
			throws FunctionAlreadyExistException, DatabaseNotExistException {
		checkNotNull(functionPath);
		checkNotNull(function);

		if (!databaseExists(functionPath.getDatabaseName())) {
			throw new DatabaseNotExistException(catalogName, functionPath.getDatabaseName());
		}

		if (functionExists(functionPath)) {
			if (!ignoreIfExists) {
				throw new FunctionAlreadyExistException(catalogName, functionPath);
			}
		} else {
			functions.put(functionPath, function.copy());
		}
	}

	@Override
	public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
			throws FunctionNotExistException {
		checkNotNull(functionPath);
		checkNotNull(newFunction);

		if (functionExists(functionPath)) {
			functions.put(functionPath, newFunction.copy());
		} else if (!ignoreIfNotExists) {
			throw new FunctionNotExistException(catalogName, functionPath);
		}
	}

	@Override
	public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException {
		checkNotNull(functionPath);

		if (functionExists(functionPath)) {
			functions.remove(functionPath);
		} else if (!ignoreIfNotExists) {
			throw new FunctionNotExistException(catalogName, functionPath);
		}
	}

	@Override
	public List<String> listFunctions(String databaseName) throws DatabaseNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		}

		return functions.keySet().stream()
			.filter(k -> k.getDatabaseName().equals(databaseName)).map(k -> k.getObjectName())
			.collect(Collectors.toList());
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException {
		checkNotNull(functionPath);

		if (!functionExists(functionPath)) {
			throw new FunctionNotExistException(catalogName, functionPath);
		} else {
			return functions.get(functionPath).copy();
		}
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) {
		checkNotNull(functionPath);
		return databaseExists(functionPath.getDatabaseName()) && functions.containsKey(functionPath);
	}

	// ------ partitions ------

	@Override
	public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists)
			throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
		checkNotNull(tablePath);
		checkNotNull(partitionSpec);
		checkNotNull(partition);

		if (!tableExists(tablePath)) {
			throw new TableNotExistException(catalogName, tablePath);
		}

		if (!isPartitionedTable(tablePath)) {
			throw new TableNotPartitionedException(catalogName, tablePath);
		}

		if (partitionExists(tablePath, partitionSpec)) {
			if (!ignoreIfExists) {
				throw new PartitionAlreadyExistsException(catalogName, tablePath, partitionSpec);
			}
		}

		if (!isPartitionSpecValid(tablePath, partitionSpec)) {
			throw new PartitionSpecInvalidException(catalogName, ((CatalogTable) getTable(tablePath)).getPartitionKeys(),
				tablePath, partitionSpec);
		}

		partitions.get(tablePath).put(partitionSpec, partition.copy());
	}

	@Override
	public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
			throws PartitionNotExistException, CatalogException {
		checkNotNull(tablePath);
		checkNotNull(partitionSpec);

		if (partitionExists(tablePath, partitionSpec)) {
			partitions.get(tablePath).remove(partitionSpec);
		} else if (!ignoreIfNotExists) {
			throw new PartitionNotExistException(catalogName, tablePath, partitionSpec);
		}
	}

	@Override
	public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists)
			throws PartitionNotExistException, CatalogException {
		checkNotNull(tablePath);
		checkNotNull(partitionSpec);
		checkNotNull(newPartition);

		if (partitionExists(tablePath, partitionSpec)) {
			partitions.get(tablePath).put(partitionSpec, newPartition.copy());
		} else if (!ignoreIfNotExists) {
			throw new PartitionNotExistException(catalogName, tablePath, partitionSpec);
		}
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
			throws TableNotExistException, TableNotPartitionedException, CatalogException {
		checkNotNull(tablePath);

		if (!tableExists(tablePath)) {
			throw new TableNotExistException(catalogName, tablePath);
		}

		if (!isPartitionedTable(tablePath)) {
			throw new TableNotPartitionedException(catalogName, tablePath);
		}

		return new ArrayList<>(partitions.get(tablePath).keySet());
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws TableNotExistException, TableNotPartitionedException, CatalogException {
		checkNotNull(tablePath);
		checkNotNull(partitionSpec);

		if (!tableExists(tablePath)) {
			throw new TableNotExistException(catalogName, tablePath);
		}

		if (!isPartitionedTable(tablePath)) {
			throw new TableNotPartitionedException(catalogName, tablePath);
		}

		if (!isPartitionSpecValid(tablePath, partitionSpec)) {
			return new ArrayList<>();
		}

		return partitions.get(tablePath).keySet().stream()
			.filter(ps -> ps.getPartitionSpec().entrySet().containsAll(partitionSpec.getPartitionSpec().entrySet()))
			.collect(Collectors.toList());
	}

	@Override
	public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws PartitionNotExistException, CatalogException {
		checkNotNull(tablePath);
		checkNotNull(partitionSpec);

		if (!partitionExists(tablePath, partitionSpec)) {
			throw new PartitionNotExistException(catalogName, tablePath, partitionSpec);
		}

		return partitions.get(tablePath).get(partitionSpec).copy();
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws CatalogException {
		checkNotNull(tablePath);
		checkNotNull(partitionSpec);

		return partitions.containsKey(tablePath) && partitions.get(tablePath).containsKey(partitionSpec);
	}

	/**
	 * Check if the given partitionSpec is valid for the given table.
	 * Note that partition spec is considered invalid if the table doesn't exist or isn't partitioned.
	 */
	private boolean isPartitionSpecValid(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
		CatalogBaseTable baseTable;
		try {
			baseTable = getTable(tablePath);
		} catch (TableNotExistException e) {
			return false;
		}

		if (!(baseTable instanceof CatalogTable)) {
			return false;
		}

		CatalogTable table =  (CatalogTable) baseTable;
		List<String> partitionKeys = table.getPartitionKeys();
		Map<String, String> spec = partitionSpec.getPartitionSpec();

		// The size of partition spec should not exceed the size of partition keys
		if (partitionKeys.size() < spec.size()) {
			return false;
		} else {
			int size = spec.size();

			// PartitionSpec should contain the first 'size' number of keys in partition key list
			for (int i = 0; i < size; i++) {
				if (!spec.containsKey(partitionKeys.get(i))) {
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Check if the given table is a partitioned table.
	 * Note that "false" is returned if the table doesn't exists.
	 */
	private boolean isPartitionedTable(ObjectPath tablePath) {
		CatalogBaseTable table = null;
		try {
			table = getTable(tablePath);
		} catch (TableNotExistException e) {
			return false;
		}

		return (table instanceof CatalogTable) && ((CatalogTable) table).isPartitioned();
	}

}
