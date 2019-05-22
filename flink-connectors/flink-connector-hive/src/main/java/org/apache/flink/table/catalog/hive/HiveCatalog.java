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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.AbstractCatalogTable;
import org.apache.flink.table.catalog.AbstractCatalogView;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.GenericCatalogDatabase;
import org.apache.flink.table.catalog.GenericCatalogFunction;
import org.apache.flink.table.catalog.GenericCatalogTable;
import org.apache.flink.table.catalog.GenericCatalogView;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
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
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A catalog implementation for Hive.
 */
public class HiveCatalog implements Catalog {
	private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);
	private static final String DEFAULT_DB = "default";

	// Prefix used to distinguish properties created by Hive and Flink,
	// as Hive metastore has its own properties created upon table creation and migration between different versions of metastore.
	private static final String FLINK_PROPERTY_PREFIX = "flink.";
	private static final String FLINK_PROPERTY_IS_GENERIC = FLINK_PROPERTY_PREFIX + GenericInMemoryCatalog.FLINK_IS_GENERIC_KEY;

	// Prefix used to distinguish Flink functions from Hive functions.
	// It's appended to Flink function's class name
	// because Hive's Function object doesn't have properties or other place to store the flag for Flink functions.
	private static final String FLINK_FUNCTION_PREFIX = "flink:";

	protected final String catalogName;
	protected final HiveConf hiveConf;

	private final String defaultDatabase;
	protected IMetaStoreClient client;

	public HiveCatalog(String catalogName, String hivemetastoreURI) {
		this(catalogName, DEFAULT_DB, getHiveConf(hivemetastoreURI));
	}

	public HiveCatalog(String catalogName, HiveConf hiveConf) {
		this(catalogName, DEFAULT_DB, hiveConf);
	}

	public HiveCatalog(String catalogName, String defaultDatabase, HiveConf hiveConf) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(defaultDatabase), "defaultDatabase cannot be null or empty");
		this.catalogName = catalogName;
		this.defaultDatabase = defaultDatabase;
		this.hiveConf = checkNotNull(hiveConf, "hiveConf cannot be null");

		LOG.info("Created HiveCatalog '{}'", catalogName);
	}

	private static HiveConf getHiveConf(String hiveMetastoreURI) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(hiveMetastoreURI), "hiveMetastoreURI cannot be null or empty");

		HiveConf hiveConf = new HiveConf();
		hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetastoreURI);
		return hiveConf;
	}

	private static IMetaStoreClient getMetastoreClient(HiveConf hiveConf) {
		try {
			return RetryingMetaStoreClient.getProxy(
				hiveConf,
				null,
				null,
				HiveMetaStoreClient.class.getName(),
				true);
		} catch (MetaException e) {
			throw new CatalogException("Failed to create Hive metastore client", e);
		}
	}

	@Override
	public void open() throws CatalogException {
		if (client == null) {
			client = getMetastoreClient(hiveConf);
			LOG.info("Connected to Hive metastore");
		}

		if (!databaseExists(defaultDatabase)) {
			throw new CatalogException(String.format("Configured default database %s doesn't exist in catalog %s.",
				defaultDatabase, catalogName));
		}
	}

	@Override
	public void close() throws CatalogException {
		if (client != null) {
			client.close();
			client = null;
			LOG.info("Close connection to Hive metastore");
		}
	}

	// ------ databases ------

	public String getDefaultDatabase() throws CatalogException {
		return defaultDatabase;
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		Database hiveDatabase = getHiveDatabase(databaseName);

		Map<String, String> properties = hiveDatabase.getParameters();
		boolean isGeneric = Boolean.valueOf(properties.get(FLINK_PROPERTY_IS_GENERIC));
		return !isGeneric ? new HiveCatalogDatabase(properties, hiveDatabase.getLocationUri(), hiveDatabase.getDescription()) :
			new GenericCatalogDatabase(retrieveFlinkProperties(properties), hiveDatabase.getDescription());
	}

	@Override
	public void createDatabase(String databaseName, CatalogDatabase database, boolean ignoreIfExists)
			throws DatabaseAlreadyExistException, CatalogException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
		checkNotNull(database, "database cannot be null");

		Database hiveDatabase = instantiateHiveDatabase(databaseName, database);

		try {
			client.createDatabase(hiveDatabase);
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new DatabaseAlreadyExistException(catalogName, hiveDatabase.getName());
			}
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to create database %s", hiveDatabase.getName()), e);
		}
	}

	private static Database instantiateHiveDatabase(String databaseName, CatalogDatabase database) {
		if (database instanceof HiveCatalogDatabase) {
			HiveCatalogDatabase db = (HiveCatalogDatabase) database;
			return new Database(
				databaseName,
				db.getComment(),
				db.getLocation(),
				db.getProperties());
		} else if (database instanceof GenericCatalogDatabase) {
			GenericCatalogDatabase db = (GenericCatalogDatabase) database;

			return new Database(
				databaseName,
				db.getComment(),
				// HDFS location URI which GenericCatalogDatabase shouldn't care
				null,
				maskFlinkProperties(db.getProperties()));
		} else {
			throw new CatalogException(String.format("Unsupported catalog database type %s", database.getClass()), null);
		}
	}

	@Override
	public void alterDatabase(String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
			throws DatabaseNotExistException, CatalogException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
		checkNotNull(newDatabase, "newDatabase cannot be null");

		Database newHiveDatabase = instantiateHiveDatabase(databaseName, newDatabase);

		try {
			if (databaseExists(databaseName)) {
				client.alterDatabase(databaseName, newHiveDatabase);
			} else if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(catalogName, databaseName);
			}
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to alter database %s", databaseName), e);
		}
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		try {
			return client.getAllDatabases();
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to list all databases in %s", catalogName), e);
		}
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		try {
			return client.getDatabase(databaseName) != null;
		} catch (NoSuchObjectException e) {
			return false;
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to determine whether database %s exists or not", databaseName), e);
		}
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists) throws DatabaseNotExistException,
			DatabaseNotEmptyException, CatalogException {
		try {
			client.dropDatabase(name, true, ignoreIfNotExists);
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(catalogName, name);
			}
		} catch (InvalidOperationException e) {
			throw new DatabaseNotEmptyException(catalogName, name);
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to drop database %s", name), e);
		}
	}

	private Database getHiveDatabase(String databaseName) throws DatabaseNotExistException {
		try {
			return client.getDatabase(databaseName);
		} catch (NoSuchObjectException e) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to get database %s from %s", databaseName, catalogName), e);
		}
	}

	// ------ tables ------

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		checkNotNull(tablePath, "tablePath cannot be null");

		Table hiveTable = getHiveTable(tablePath);
		return instantiateHiveCatalogTable(hiveTable);
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
			throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
		checkNotNull(tablePath, "tablePath cannot be null");
		checkNotNull(table, "table cannot be null");

		if (!databaseExists(tablePath.getDatabaseName())) {
			throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
		}

		Table hiveTable = instantiateHiveTable(tablePath, table);

		try {
			client.createTable(hiveTable);
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new TableAlreadyExistException(catalogName, tablePath);
			}
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to create table %s", tablePath.getFullName()), e);
		}
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
			throws TableNotExistException, TableAlreadyExistException, CatalogException {
		checkNotNull(tablePath, "tablePath cannot be null");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(newTableName), "newTableName cannot be null or empty");

		try {
			// alter_table() doesn't throw a clear exception when target table doesn't exist.
			// Thus, check the table existence explicitly
			if (tableExists(tablePath)) {
				ObjectPath newPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
				// alter_table() doesn't throw a clear exception when new table already exists.
				// Thus, check the table existence explicitly
				if (tableExists(newPath)) {
					throw new TableAlreadyExistException(catalogName, newPath);
				} else {
					Table table = getHiveTable(tablePath);
					table.setTableName(newTableName);
					client.alter_table(tablePath.getDatabaseName(), tablePath.getObjectName(), table);
				}
			} else if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, tablePath);
			}
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to rename table %s", tablePath.getFullName()), e);
		}
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newCatalogTable, boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException {
		checkNotNull(tablePath, "tablePath cannot be null");
		checkNotNull(newCatalogTable, "newCatalogTable cannot be null");

		if (!tableExists(tablePath)) {
			if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, tablePath);
			}
			return;
		}

		Table oldTable = getHiveTable(tablePath);
		TableType oldTableType = TableType.valueOf(oldTable.getTableType());

		if (oldTableType == TableType.VIRTUAL_VIEW) {
			if (!(newCatalogTable instanceof CatalogView)) {
				throw new CatalogException(
					String.format("Table types don't match. The existing table is a view, but the new catalog base table is not."));
			}
			// Else, do nothing
		} else if ((oldTableType == TableType.MANAGED_TABLE)) {
			if (!(newCatalogTable instanceof CatalogTable)) {
				throw new CatalogException(
					String.format("Table types don't match. The existing table is a table, but the new catalog base table is not."));
			}
			// Else, do nothing
		} else {
			throw new CatalogException(
				String.format("Hive table type '%s' is not supported yet.",
					oldTableType.name()));
		}

		Table newTable = instantiateHiveTable(tablePath, newCatalogTable);

		// client.alter_table() requires a valid location
		// thus, if new table doesn't have that, it reuses location of the old table
		if (!newTable.getSd().isSetLocation()) {
			newTable.getSd().setLocation(oldTable.getSd().getLocation());
		}

		try {
			client.alter_table(tablePath.getDatabaseName(), tablePath.getObjectName(), newTable);
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to rename table %s", tablePath.getFullName()), e);
		}
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
		checkNotNull(tablePath, "tablePath cannot be null");

		try {
			client.dropTable(
				tablePath.getDatabaseName(),
				tablePath.getObjectName(),
				// Indicate whether associated data should be deleted.
				// Set to 'true' for now because Flink tables shouldn't have data in Hive. Can be changed later if necessary
				true,
				ignoreIfNotExists);
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, tablePath);
			}
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to drop table %s", tablePath.getFullName()), e);
		}
	}

	@Override
	public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

		try {
			return client.getAllTables(databaseName);
		} catch (UnknownDBException e) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to list tables in database %s", databaseName), e);
		}
	}

	@Override
	public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

		try {
			return client.getTables(
				databaseName,
				null, // table pattern
				TableType.VIRTUAL_VIEW);
		} catch (UnknownDBException e) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to list views in database %s", databaseName), e);
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		checkNotNull(tablePath, "tablePath cannot be null");

		try {
			return client.tableExists(tablePath.getDatabaseName(), tablePath.getObjectName());
		} catch (UnknownDBException e) {
			return false;
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to check whether table %s exists or not.", tablePath.getFullName()), e);
		}
	}

	private Table getHiveTable(ObjectPath tablePath) throws TableNotExistException {
		try {
			return client.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
		} catch (NoSuchObjectException e) {
			throw new TableNotExistException(catalogName, tablePath);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to get table %s from Hive metastore", tablePath.getFullName()), e);
		}
	}

	private static CatalogBaseTable instantiateHiveCatalogTable(Table hiveTable) {
		boolean isView = TableType.valueOf(hiveTable.getTableType()) == TableType.VIRTUAL_VIEW;

		// Table properties
		Map<String, String> properties = hiveTable.getParameters();
		boolean isGeneric = Boolean.valueOf(properties.get(FLINK_PROPERTY_IS_GENERIC));
		if (isGeneric) {
			properties = retrieveFlinkProperties(properties);
		}
		String comment = properties.remove(HiveTableConfig.TABLE_COMMENT);

		// Table schema
		TableSchema tableSchema =
			HiveTableUtil.createTableSchema(hiveTable.getSd().getCols(), hiveTable.getPartitionKeys());

		// Partition keys
		List<String> partitionKeys = new ArrayList<>();
		if (!hiveTable.getPartitionKeys().isEmpty()) {
			partitionKeys = hiveTable.getPartitionKeys().stream().map(fs -> fs.getName()).collect(Collectors.toList());
		}

		if (isView) {
			if (isGeneric) {
				return new GenericCatalogView(
					hiveTable.getViewOriginalText(),
					hiveTable.getViewExpandedText(),
					tableSchema,
					properties,
					comment
				);
			} else {
				return new HiveCatalogView(
					hiveTable.getViewOriginalText(),
					hiveTable.getViewExpandedText(),
					tableSchema,
					properties,
					comment
				);
			}
		} else {
			if (isGeneric) {
				return new GenericCatalogTable(tableSchema, partitionKeys, properties, comment);
			} else {
				return new HiveCatalogTable(tableSchema, partitionKeys, properties, comment);
			}
		}
	}

	private  static Table instantiateHiveTable(ObjectPath tablePath, CatalogBaseTable table) {
		Table hiveTable = new Table();
		hiveTable.setDbName(tablePath.getDatabaseName());
		hiveTable.setTableName(tablePath.getObjectName());
		hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));

		Map<String, String> properties = new HashMap<>(table.getProperties());
		// Table comment
		properties.put(HiveTableConfig.TABLE_COMMENT, table.getComment());
		if (table instanceof GenericCatalogTable || table instanceof GenericCatalogView) {
			properties = maskFlinkProperties(properties);
		}
		// Table properties
		hiveTable.setParameters(properties);

		// Hive table's StorageDescriptor
		// TODO: This is very basic Hive table.
		//  [FLINK-11479] Add input/output format and SerDeLib information for Hive tables.
		StorageDescriptor sd = new StorageDescriptor();
		hiveTable.setSd(sd);
		sd.setSerdeInfo(new SerDeInfo(null, null, new HashMap<>()));

		List<FieldSchema> allColumns = HiveTableUtil.createHiveColumns(table.getSchema());

		// Table columns and partition keys
		if (table instanceof AbstractCatalogTable) {
			AbstractCatalogTable catalogTable = (AbstractCatalogTable) table;

			if (catalogTable.isPartitioned()) {
				int partitionKeySize = catalogTable.getPartitionKeys().size();
				List<FieldSchema> regularColumns = allColumns.subList(0, allColumns.size() - partitionKeySize);
				List<FieldSchema> partitionColumns = allColumns.subList(allColumns.size() - partitionKeySize, allColumns.size());

				sd.setCols(regularColumns);
				hiveTable.setPartitionKeys(partitionColumns);
			} else {
				sd.setCols(allColumns);
				hiveTable.setPartitionKeys(new ArrayList<>());
			}
		} else if (table instanceof AbstractCatalogView) {
			AbstractCatalogView view = (AbstractCatalogView) table;

			// TODO: [FLINK-12398] Support partitioned view in catalog API
			sd.setCols(allColumns);
			hiveTable.setPartitionKeys(new ArrayList<>());

			hiveTable.setViewOriginalText(view.getOriginalQuery());
			hiveTable.setViewExpandedText(view.getExpandedQuery());
			hiveTable.setTableType(TableType.VIRTUAL_VIEW.name());
		} else {
			throw new CatalogException(
				"HiveCatalog only supports HiveCatalogTable and HiveCatalogView");
		}

		return hiveTable;
	}

	/**
	 * Filter out Hive-created properties, and return Flink-created properties.
	 */
	private static Map<String, String> retrieveFlinkProperties(Map<String, String> hiveTableParams) {
		return hiveTableParams.entrySet().stream()
			.filter(e -> e.getKey().startsWith(FLINK_PROPERTY_PREFIX))
			.collect(Collectors.toMap(e -> e.getKey().replace(FLINK_PROPERTY_PREFIX, ""), e -> e.getValue()));
	}

	/**
	 * Add a prefix to Flink-created properties to distinguish them from Hive-created properties.
	 */
	private static Map<String, String> maskFlinkProperties(Map<String, String> properties) {
		return properties.entrySet().stream()
			.filter(e -> e.getKey() != null && e.getValue() != null)
			.collect(Collectors.toMap(e -> FLINK_PROPERTY_PREFIX + e.getKey(), e -> e.getValue()));
	}

	// ------ partitions ------

	@Override
	public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists)
			throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
			throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists)
			throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
			throws TableNotExistException, TableNotPartitionedException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws TableNotExistException, TableNotPartitionedException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	// ------ functions ------

	@Override
	public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
			throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
		checkNotNull(functionPath, "functionPath cannot be null");
		checkNotNull(function, "function cannot be null");

		Function hiveFunction;
		if (function instanceof GenericCatalogFunction) {
			hiveFunction = instantiateHiveFunction(functionPath, (GenericCatalogFunction) function);
		} else {
			throw new CatalogException(
				String.format("Unsupported catalog function type %s", function.getClass().getName()));
		}

		try {
			client.createFunction(hiveFunction);
		} catch (NoSuchObjectException e) {
			throw new DatabaseNotExistException(catalogName, functionPath.getDatabaseName(), e);
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new FunctionAlreadyExistException(catalogName, functionPath, e);
			}
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to create function %s", functionPath.getFullName()), e);
		}
	}

	@Override
	public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
			throws FunctionNotExistException, CatalogException {
		checkNotNull(functionPath, "functionPath cannot be null");
		checkNotNull(newFunction, "newFunction cannot be null");

		try {
			CatalogFunction existingFunction = getFunction(functionPath);

			if (existingFunction.getClass() != newFunction.getClass()) {
				throw new CatalogException(
					String.format("Function types don't match. Existing function is '%s' and new function is '%s'.",
						existingFunction.getClass().getName(), newFunction.getClass().getName()));
			}

			Function hiveFunction;
			if (existingFunction instanceof GenericCatalogFunction && newFunction instanceof GenericCatalogFunction) {
					hiveFunction = instantiateHiveFunction(functionPath, (GenericCatalogFunction) newFunction);
			} else {
				throw new CatalogException(
					String.format("Unsupported catalog function type %s", newFunction.getClass().getName()));
			}

			client.alterFunction(
				functionPath.getDatabaseName(),
				functionPath.getObjectName(),
				hiveFunction);
		} catch (FunctionNotExistException e) {
			if (!ignoreIfNotExists) {
				throw e;
			}
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to alter function %s", functionPath.getFullName()), e);
		}
	}

	@Override
	public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
			throws FunctionNotExistException, CatalogException {
		checkNotNull(functionPath, "functionPath cannot be null");

		try {
			client.dropFunction(functionPath.getDatabaseName(), functionPath.getObjectName());
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new FunctionNotExistException(catalogName, functionPath, e);
			}
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to drop function %s", functionPath.getFullName()), e);
		}
	}

	@Override
	public List<String> listFunctions(String databaseName) throws DatabaseNotExistException, CatalogException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

		// client.getFunctions() returns empty list when the database doesn't exist
		// thus we need to explicitly check whether the database exists or not
		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		}

		try {
			return client.getFunctions(databaseName, null);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to list functions in database %s", databaseName), e);
		}
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
		checkNotNull(functionPath, "functionPath cannot be null or empty");

		try {
			Function function = client.getFunction(functionPath.getDatabaseName(), functionPath.getObjectName());

			if (function.getClassName().startsWith(FLINK_FUNCTION_PREFIX)) {
				// TODO: extract more properties from Hive function and add to CatalogFunction's properties

				Map<String, String> properties = new HashMap<>();
				properties.put(GenericInMemoryCatalog.FLINK_IS_GENERIC_KEY, GenericInMemoryCatalog.FLINK_IS_GENERIC_VALUE);

				return new GenericCatalogFunction(
					function.getClassName().substring(FLINK_FUNCTION_PREFIX.length()), properties);
			} else {
				throw new CatalogException("Hive function is not supported yet");
			}
		} catch (NoSuchObjectException e) {
			throw new FunctionNotExistException(catalogName, functionPath, e);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to get function %s", functionPath.getFullName()), e);
		}
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {
		checkNotNull(functionPath, "functionPath cannot be null or empty");

		try {
			return client.getFunction(functionPath.getDatabaseName(), functionPath.getObjectName()) != null;
		} catch (NoSuchObjectException e) {
			return false;
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to check whether function %s exists or not", functionPath.getFullName()), e);
		}
	}

	private static Function instantiateHiveFunction(ObjectPath functionPath, GenericCatalogFunction function) {
		return new Function(
			functionPath.getObjectName(),
			functionPath.getDatabaseName(),
			FLINK_FUNCTION_PREFIX + function.getClassName(),
			null,			// Owner name
			PrincipalType.GROUP,	// Temporarily set to GROUP type because it's required by Hive. May change later
			(int) (System.currentTimeMillis() / 1000),
			FunctionType.JAVA,		// FunctionType only has JAVA now
			new ArrayList<>()		// Resource URIs
		);
	}

	// ------ stats ------

	@Override
	public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

	}

	@Override
	public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

	}

	@Override
	public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

	}

	@Override
	public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

	}

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

}
