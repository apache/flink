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

import org.apache.flink.table.api.DatabaseAlreadyExistException;
import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.exceptions.PartitionAlreadyExistException;
import org.apache.flink.table.api.exceptions.PartitionNotExistException;
import org.apache.flink.table.api.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ReadableWritableCatalog;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.parquet.Strings;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A catalog that connects to Hive MetaStore and reads/writes Hive tables.
 */
public class HiveCatalog implements ReadableWritableCatalog {
	private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

	private static final String DEFAULT_DB = "default";

	private final String catalogName;

	private String defaultDatabaseName = DEFAULT_DB;
	private HiveConf hiveConf;
	private IMetaStoreClient client;

	public HiveCatalog(String catalogName, String hiveMetastoreURI) {
		this(catalogName, getHiveConf(hiveMetastoreURI));
	}

	public HiveCatalog(String catalogName, HiveConf hiveConf) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		this.catalogName = catalogName;

		this.hiveConf = checkNotNull(hiveConf, "hiveConf cannot be null");
		LOG.info("Created HiveCatalog '{}'", catalogName);
	}

	private static HiveConf getHiveConf(String hiveMetastoreURI) {
		checkArgument(!Strings.isNullOrEmpty(hiveMetastoreURI), "hiveMetastoreURI cannot be null or empty");

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
			throw new FlinkHiveException("Failed to create Hive metastore client", e);
		}
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
		if (client == null) {
			client = getMetastoreClient(hiveConf);
			LOG.info("Connect to Hive metastore");
		}
	}

	@Override
	public void close() {
		if (client != null) {
			client.close();
			client = null;
			LOG.info("Close connection to Hive metastore");
		}
	}

	// ------ tables ------

	@Override
	public CatalogTable getTable(ObjectPath path) throws TableNotExistException {
		Table hiveTable = getHiveTable(path);

		TableSchema tableSchema = HiveMetadataUtil.createTableSchema(hiveTable.getSd().getCols(), hiveTable.getPartitionKeys());

		// Get the column statistics for a set of columns in a table. This only works for non-partitioned tables.
		Map<String, ColumnStats> colStats = new HashMap<>();
		if (hiveTable.getPartitionKeysSize() == 0) {
			colStats = HiveMetadataUtil.createColumnStats(
				getHiveTableColumnStats(path.getDbName(), path.getObjectName(), Arrays.asList(tableSchema.getFieldNames())));
		}

		TableStats tableStats = new TableStats(getRowCount(hiveTable), colStats);

		CatalogTable catalogTable = HiveMetadataUtil.createCatalogTable(hiveTable, tableSchema, tableStats);
		catalogTable.getProperties().put(HiveConf.ConfVars.METASTOREURIS.varname,
			hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
		return catalogTable;
	}

	/**
	 * Get the column statistics for a set of columns in a table. This only works for non-partitioned tables.
	 */
	private List<ColumnStatisticsObj> getHiveTableColumnStats(String dbName, String tableName, List<String> cols) {
		try {
			return client.getTableColumnStatistics(dbName, tableName, cols);
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to get table column stats for columns %s of table %s", cols,
					new ObjectPath(dbName, tableName)));
		}
	}

	private Table getHiveTable(ObjectPath path) throws TableNotExistException {
		try {
			return client.getTable(path.getDbName(), path.getObjectName());
		} catch (NoSuchObjectException e) {
			throw new TableNotExistException(catalogName, path.getFullName());
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to get table %s", path.getFullName()), e);
		}
	}

	private Long getRowCount(Table hiveTable) {
		if (hiveTable.getParameters().get(StatsSetupConst.ROW_COUNT) != null) {
			return Math.max(0L, Long.parseLong(hiveTable.getParameters().get(StatsSetupConst.ROW_COUNT)));
		} else {
			return 0L;
		}
	}

	@Override
	public List<ObjectPath> listTables(String dbName) throws DatabaseNotExistException {
		try {
			return client.getAllTables(dbName).stream()
				.map(t -> new ObjectPath(dbName, t))
				.collect(Collectors.toList());
		} catch (UnknownDBException e) {
			throw new DatabaseNotExistException(catalogName, dbName);
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to list tables in database %s", dbName), e);
		}
	}

	@Override
	public List<ObjectPath> listAllTables() {
		List<String> dbs = listDatabases();
		List<ObjectPath> result = new ArrayList<>();

		for (String db : dbs) {
			result.addAll(listTables(db));
		}

		return result;
	}

	@Override
	public void createTable(ObjectPath path, CatalogTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException {

		try {
			if (tableExists(path)) {
				if (!ignoreIfExists) {
					throw new TableAlreadyExistException(catalogName, path.getFullName());
				}
			} else {
				// Testing shows that createTable() API in Hive 2.3.4 doesn't throw UnknownDBException as it claims
				// Thus we have to manually check if the db exists or not
				if (!dbExists(path.getDbName())) {
					throw new DatabaseNotExistException(catalogName, path.getDbName());
				}

				client.createTable(HiveMetadataUtil.createHiveTable(path, table));
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to create table %s", path.getFullName()), e);
		}
	}

	@Override
	public void dropTable(ObjectPath path, boolean ignoreIfNotExists) throws TableNotExistException {
		try {
			client.dropTable(path.getDbName(), path.getObjectName(), true, ignoreIfNotExists);
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, path.getFullName());
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to drop table %s", path.getFullName()), e);
		}
	}

	@Override
	public void alterTable(ObjectPath path, CatalogTable newTable, boolean ignoreIfNotExists) throws TableNotExistException {
		try {
			if (tableExists(path)) {
				client.alter_table(path.getDbName(), path.getObjectName(), HiveMetadataUtil.createHiveTable(path, newTable));
			} else if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, path.getFullName());
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to alter table %s", path.getFullName()), e);
		}
	}

	@Override
	public void renameTable(ObjectPath tableName, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, DatabaseNotExistException {
		// Hive metastore client doesn't support renaming yet
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean tableExists(ObjectPath path) {
		try {
			return client.tableExists(path.getDbName(), path.getObjectName());
		} catch (UnknownDBException e) {
			return false;
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to check if table %s exists in database %s", path.getObjectName(), path.getDbName()), e);
		}
	}

	// ------ table and column stats ------

	@Override
	public TableStats getTableStats(ObjectPath path) throws TableNotExistException {
		Table hiveTable = getHiveTable(path);

		// Get the column statistics for a set of columns in a table. This only works for non-partitioned tables.
		// For partitioned tables, get partition columns stats from HiveCatalog.getPartition()
		Map<String, ColumnStats> colStats = new HashMap<>();

		if (!isTablePartitioned(hiveTable)) {
			colStats = HiveMetadataUtil.createColumnStats(
				getHiveTableColumnStats(
					path.getDbName(),
					path.getObjectName(),
					hiveTable.getSd().getCols().stream()
						.map(fs -> fs.getName())
						.collect(Collectors.toList())
				));
		}

		return new TableStats(getRowCount(hiveTable), colStats);
	}

	@Override
	public void alterTableStats(ObjectPath path, TableStats newtTableStats, boolean ignoreIfNotExists) throws TableNotExistException {
		try {
			Table hiveTable = getHiveTable(path);

			// Set table column stats. This only works for non-partitioned tables.
			if (!isTablePartitioned(hiveTable)) {
				client.updateTableColumnStatistics(HiveMetadataUtil.createColumnStats(hiveTable, newtTableStats.colStats()));
			}

			// Set table stats
			String oldRowCount = hiveTable.getParameters().getOrDefault(StatsSetupConst.ROW_COUNT, "0");
			if (newtTableStats.rowCount() != Long.parseLong(oldRowCount)) {
				hiveTable.getParameters().put(StatsSetupConst.ROW_COUNT, String.valueOf(newtTableStats.rowCount()));
				client.alter_table(path.getDbName(), path.getObjectName(), hiveTable);
			}
		} catch (TableNotExistException e) {
			if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, path.getFullName());
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to alter table stats of table %s", path.getFullName()), e);
		}
	}

	// ------ databases ------

	@Override
	public void createDatabase(String dbName, CatalogDatabase db, boolean ignoreIfExists) throws DatabaseAlreadyExistException {
		try {
			client.createDatabase(HiveMetadataUtil.createHiveDatabase(dbName, db));
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new DatabaseAlreadyExistException(catalogName, dbName);
			}
		} catch (TException e) {
			throw new FlinkHiveException(String.format("Failed to create database %s", dbName), e);
		}
	}

	@Override
	public void dropDatabase(String dbName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		try {
			client.dropDatabase(dbName, true, ignoreIfNotExists);
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(catalogName, dbName);
			}
		} catch (TException e) {
			throw new FlinkHiveException(String.format("Failed to drop database %s", dbName), e);
		}
	}

	@Override
	public void alterDatabase(String dbName, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		try {
			if (dbExists(dbName)) {
				client.alterDatabase(dbName, HiveMetadataUtil.createHiveDatabase(dbName, newDatabase));
			} else if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(catalogName, dbName);
			}
		} catch (TException e) {
			throw new FlinkHiveException(String.format("Failed to alter database %s", dbName), e);
		}
	}

	@Override
	public void renameDatabase(String dbName, String newDbName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		// Hive metastore client doesn't support renaming yet
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> listDatabases() {
		try {
			return client.getAllDatabases();
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to list all databases in HiveCatalog %s", catalogName));
		}
	}

	@Override
	public CatalogDatabase getDatabase(String dbName) throws DatabaseNotExistException {
		Database hiveDb;

		try {
			hiveDb = client.getDatabase(dbName);
		} catch (NoSuchObjectException e) {
			throw new DatabaseNotExistException(catalogName, dbName);
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to get database %s from HiveCatalog %s", dbName, catalogName), e);
		}

		return HiveMetadataUtil.createCatalogDatabase(hiveDb);
	}

	@Override
	public boolean dbExists(String dbName) {
		try {
			return client.getDatabase(dbName) != null;
		} catch (NoSuchObjectException e) {
			return false;
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to get database %s", dbName), e);
		}
	}

	// ------ partitions ------

	@Override
	public void createPartition(ObjectPath path, CatalogPartition partition, boolean ignoreIfExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionAlreadyExistException {

		Table hiveTable = getHiveTable(path);

		if (hiveTable.getPartitionKeysSize() == 0) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			client.add_partition(
				HiveMetadataUtil.createHivePartition(hiveTable, partition));
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new PartitionAlreadyExistException(catalogName, path, partition.getPartitionSpec());
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to create partition %s of table %s", partition.getPartitionSpec(), path));
		}
	}

	@Override
	public void dropPartition(ObjectPath path, CatalogPartition.PartitionSpec partitionSpec, boolean ignoreIfNotExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {

		Table hiveTable = getHiveTable(path);

		if (!isTablePartitioned(hiveTable)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			client.dropPartition(path.getDbName(), path.getObjectName(), getOrderedPartitionValues(hiveTable, partitionSpec), true);
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new PartitionNotExistException(catalogName, path, partitionSpec);
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to drop partition %s of table %s", partitionSpec, path));
		}
	}

	@Override
	public void alterPartition(ObjectPath path, CatalogPartition newPartition, boolean ignoreIfNotExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {
		Table hiveTable = getHiveTable(path);

		if (hiveTable.getPartitionKeysSize() == 0) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		// Explicitly check if the parititon exists or not
		// because alter_partition() doesn't throw NoSuchObjectException like dropPartition() when the target doesn't exist
		if (partitionExists(path, newPartition.getPartitionSpec())) {
			try {
				client.alter_partition(
					path.getDbName(),
					path.getObjectName(),
					HiveMetadataUtil.createHivePartition(hiveTable, newPartition)
				);
			} catch (TException e) {
				throw new FlinkHiveException(
					String.format("Failed to alter existing partition with new partition %s of table %s", newPartition.getPartitionSpec(), path), e);
			}
		} else if (!ignoreIfNotExists) {
			throw new PartitionNotExistException(catalogName, path, newPartition.getPartitionSpec());
		}
	}

	@Override
	public List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath path)
		throws TableNotExistException, TableNotPartitionedException {

		Table hiveTable = getHiveTable(path);

		if (!isTablePartitioned(hiveTable)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			return client.listPartitionNames(path.getDbName(), path.getObjectName(), (short) -1).stream()
				.map(n -> HiveMetadataUtil.createPartitionSpec(n))
				.collect(Collectors.toList());
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to list partitions of table %s", path), e);
		}
	}

	@Override
	public List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath path, CatalogPartition.PartitionSpec partitionSpec)
		throws TableNotExistException, TableNotPartitionedException {

		Table hiveTable = getHiveTable(path);

		if (!isTablePartitioned(hiveTable)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			return client.listPartitionNames(path.getDbName(), path.getObjectName(), getOrderedPartitionValues(hiveTable, partitionSpec), (short) -1).stream()
				.map(n -> HiveMetadataUtil.createPartitionSpec(n))
				.collect(Collectors.toList());
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to list partitions of table %s", path), e);
		}
	}

	@Override
	public CatalogPartition getPartition(ObjectPath path, CatalogPartition.PartitionSpec partitionSpec)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {

		Table hiveTable = getHiveTable(path);

		if (!isTablePartitioned(hiveTable)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			return HiveMetadataUtil.createCatalogPartition(
				partitionSpec,
				client.getPartition(path.getDbName(), path.getObjectName(), getOrderedPartitionValues(hiveTable, partitionSpec)));
		} catch (NoSuchObjectException e) {
			throw new PartitionNotExistException(catalogName, path, partitionSpec);
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to get partition %s of table %s", partitionSpec, path), e);
		}
	}

	@Override
	public boolean partitionExists(ObjectPath path, CatalogPartition.PartitionSpec partitionSpec) {
		try {
			Table hiveTable = getHiveTable(path);
			return client.getPartition(path.getDbName(), path.getObjectName(), getOrderedPartitionValues(hiveTable, partitionSpec)) != null;
		} catch (NoSuchObjectException | TableNotExistException e) {
			return false;
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to get partition %s of table %s", partitionSpec, path), e);
		}
	}

	private boolean isTablePartitioned(Table hiveTable) throws TableNotExistException {
		return hiveTable.getPartitionKeysSize() != 0;
	}

	private List<String> getOrderedPartitionValues(Table hiveTable, CatalogPartition.PartitionSpec partitionSpec) {
		return partitionSpec.getOrderedValues(HiveMetadataUtil.getPartitionKeys(hiveTable.getPartitionKeys()));
	}
}
