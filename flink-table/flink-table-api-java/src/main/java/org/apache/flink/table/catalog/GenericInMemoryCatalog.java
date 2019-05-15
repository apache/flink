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
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
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
public class GenericInMemoryCatalog implements Catalog {
	private static final String DEFAULT_DB = "default";

	private final String defaultDatabase;

	private final String catalogName;
	private final Map<String, CatalogDatabase> databases;
	private final Map<ObjectPath, CatalogBaseTable> tables;
	private final Map<ObjectPath, CatalogFunction> functions;
	private final Map<ObjectPath, Map<CatalogPartitionSpec, CatalogPartition>> partitions;

	private final Map<ObjectPath, CatalogTableStatistics> tableStats;
	private final Map<ObjectPath, CatalogColumnStatistics> tableColumnStats;
	private final Map<ObjectPath, Map<CatalogPartitionSpec, CatalogTableStatistics>> partitionStats;
	private final Map<ObjectPath, Map<CatalogPartitionSpec, CatalogColumnStatistics>> partitionColumnStats;

	public GenericInMemoryCatalog(String name) {
		this(name, DEFAULT_DB);
	}

	public GenericInMemoryCatalog(String name, String defaultDatabase) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(defaultDatabase), "defaultDatabase cannot be null or empty");

		this.catalogName = name;
		this.defaultDatabase = defaultDatabase;
		this.databases = new LinkedHashMap<>();
		this.databases.put(defaultDatabase, new GenericCatalogDatabase(new HashMap<>()));
		this.tables = new LinkedHashMap<>();
		this.functions = new LinkedHashMap<>();
		this.partitions = new LinkedHashMap<>();
		this.tableStats = new LinkedHashMap<>();
		this.tableColumnStats = new LinkedHashMap<>();
		this.partitionStats = new LinkedHashMap<>();
		this.partitionColumnStats = new LinkedHashMap<>();
	}

	@Override
	public void open() {
	}

	@Override
	public void close() {

	}

	// ------ databases ------

	@Override
	public String getDefaultDatabase() {
		return defaultDatabase;
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
				partitionStats.put(tablePath, new LinkedHashMap<>());
				partitionColumnStats.put(tablePath, new LinkedHashMap<>());
			}
		}
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException {
		checkNotNull(tablePath);
		checkNotNull(newTable);

		if (tableExists(tablePath)) {
			CatalogBaseTable oldTable = tables.get(tablePath);

			if (oldTable.getClass() != newTable.getClass()) {
				throw new CatalogException(
					String.format("Table classes don't match. Existing table is '%s' and new table is '%s'. They should be of the same class.",
						oldTable.getClass().getName(), newTable.getClass().getName()));
			}

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
			tableStats.remove(tablePath);
			tableColumnStats.remove(tablePath);

			partitions.remove(tablePath);
			partitionStats.remove(tablePath);
			partitionColumnStats.remove(tablePath);
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

				// table statistics
				if (tableStats.containsKey(tablePath)) {
					tableStats.put(newPath, tableStats.remove(tablePath));
				}

				// table column statistics
				if (tableColumnStats.containsKey(tablePath)) {
					tableColumnStats.put(newPath, tableColumnStats.remove(tablePath));
				}

				// partitions
				if (partitions.containsKey(tablePath)) {
					partitions.put(newPath, partitions.remove(tablePath));
				}

				// partition statistics
				if (partitionStats.containsKey(tablePath)) {
					partitionStats.put(newPath, partitionStats.remove(tablePath));
				}

				// partition column statistics
				if (partitionColumnStats.containsKey(tablePath)) {
					partitionColumnStats.put(newPath, partitionColumnStats.remove(tablePath));
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
			partitionStats.get(tablePath).remove(partitionSpec);
			partitionColumnStats.get(tablePath).remove(partitionSpec);
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

	// ------ statistics ------

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException {
		checkNotNull(tablePath);

		if (!tableExists(tablePath)) {
			throw new TableNotExistException(catalogName, tablePath);
		}

		CatalogTableStatistics result = tableStats.get(tablePath);
		return result != null ? result.copy() : CatalogTableStatistics.UNKNOWN;
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException {
		checkNotNull(tablePath);

		if (!tableExists(tablePath)) {
			throw new TableNotExistException(catalogName, tablePath);
		}

		CatalogColumnStatistics result = tableColumnStats.get(tablePath);
		return result != null ? result.copy() : CatalogColumnStatistics.UNKNOWN;
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws PartitionNotExistException {
		checkNotNull(tablePath);
		checkNotNull(partitionSpec);

		if (!partitionExists(tablePath, partitionSpec)) {
			throw new PartitionNotExistException(catalogName, tablePath, partitionSpec);
		}

		CatalogTableStatistics result = partitionStats.get(tablePath).get(partitionSpec);
		return result != null ? result.copy() : CatalogTableStatistics.UNKNOWN;
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws PartitionNotExistException {
		checkNotNull(tablePath);
		checkNotNull(partitionSpec);

		if (!partitionExists(tablePath, partitionSpec)) {
			throw new PartitionNotExistException(catalogName, tablePath, partitionSpec);
		}

		CatalogColumnStatistics result = partitionColumnStats.get(tablePath).get(partitionSpec);
		return result != null ? result.copy() : CatalogColumnStatistics.UNKNOWN;
	}

	@Override
	public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
			throws TableNotExistException {
		checkNotNull(tablePath);
		checkNotNull(tableStatistics);

		if (tableExists(tablePath)) {
			tableStats.put(tablePath, tableStatistics.copy());
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tablePath);
		}
	}

	@Override
	public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics,
			boolean ignoreIfNotExists) throws TableNotExistException {
		checkNotNull(tablePath);
		checkNotNull(columnStatistics);

		if (tableExists(tablePath)) {
			tableColumnStats.put(tablePath, columnStatistics.copy());
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tablePath);
		}
	}

	@Override
	public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics,
			boolean ignoreIfNotExists) throws PartitionNotExistException {
		checkNotNull(tablePath);
		checkNotNull(partitionSpec);
		checkNotNull(partitionStatistics);

		if (partitionExists(tablePath, partitionSpec)) {
			partitionStats.get(tablePath).put(partitionSpec, partitionStatistics.copy());
		} else if (!ignoreIfNotExists) {
			throw new PartitionNotExistException(catalogName, tablePath, partitionSpec);
		}
	}

	@Override
	public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics,
			boolean ignoreIfNotExists) throws PartitionNotExistException {
		checkNotNull(tablePath);
		checkNotNull(partitionSpec);
		checkNotNull(columnStatistics);

		if (partitionExists(tablePath, partitionSpec)) {
			partitionColumnStats.get(tablePath).put(partitionSpec, columnStatistics.copy());
		} else if (!ignoreIfNotExists) {
			throw new PartitionNotExistException(catalogName, tablePath, partitionSpec);
		}
	}

}
