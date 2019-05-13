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
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.GenericCatalogDatabase;
import org.apache.flink.table.catalog.GenericCatalogTable;
import org.apache.flink.table.catalog.GenericCatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A catalog that persists all Flink streaming and batch metadata by using Hive metastore as a persistent storage.
 */
public class GenericHiveMetastoreCatalog extends HiveCatalogBase {
	private static final Logger LOG = LoggerFactory.getLogger(GenericHiveMetastoreCatalog.class);

	// Prefix used to distinguish properties created by Hive and Flink,
	// as Hive metastore has its own properties created upon table creation and migration between different versions of metastore.
	private static final String FLINK_PROPERTY_PREFIX = "flink.";

	public GenericHiveMetastoreCatalog(String catalogName, String hivemetastoreURI) {
		super(catalogName, hivemetastoreURI);

		LOG.info("Created GenericHiveMetastoreCatalog '{}'", catalogName);
	}

	public GenericHiveMetastoreCatalog(String catalogName, HiveConf hiveConf) {
		super(catalogName, hiveConf);
	}

	public GenericHiveMetastoreCatalog(String catalogName, String defaultDatabase, HiveConf hiveConf) {
		super(catalogName, defaultDatabase, hiveConf);

		LOG.info("Created GenericHiveMetastoreCatalog '{}'", catalogName);
	}

	// ------ databases ------

	@Override
	protected CatalogDatabase createCatalogDatabase(Database hiveDatabase) {
		return new GenericCatalogDatabase(
			hiveDatabase.getParameters(),
			hiveDatabase.getDescription()
		);
	}

	@Override
	protected Database createHiveDatabase(String databaseName, CatalogDatabase catalogDatabase) {
		return new Database(
			databaseName,
			catalogDatabase.getComment(),
			// HDFS location URI which GenericCatalogDatabase shouldn't care
			null,
			catalogDatabase.getProperties());
	}

	// ------ tables and views------

	@Override
	protected void validateCatalogBaseTable(CatalogBaseTable table)
			throws CatalogException {
		if (!(table instanceof GenericCatalogTable) && !(table instanceof GenericCatalogView)) {
			throw new CatalogException(
				"GenericHiveMetastoreCatalog can only operate on GenericCatalogTable and GenericCatalogView.");
		}
	}

	@Override
	protected CatalogBaseTable createCatalogBaseTable(Table hiveTable) {
		// Table schema
		TableSchema tableSchema = HiveTableUtil.createTableSchema(
			hiveTable.getSd().getCols(), hiveTable.getPartitionKeys());

		// Table properties
		Map<String, String> properties = retrieveFlinkProperties(hiveTable.getParameters());

		// Table comment
		String comment = properties.remove(HiveTableConfig.TABLE_COMMENT);

		// Partition keys
		List<String> partitionKeys = new ArrayList<>();

		if (!hiveTable.getPartitionKeys().isEmpty()) {
			partitionKeys = hiveTable.getPartitionKeys().stream()
				.map(fs -> fs.getName())
				.collect(Collectors.toList());
		}

		if (TableType.valueOf(hiveTable.getTableType()) == TableType.VIRTUAL_VIEW) {
			return new GenericCatalogView(
				hiveTable.getViewOriginalText(),
				hiveTable.getViewExpandedText(),
				tableSchema,
				properties,
				comment
			);
		} else {
			return new GenericCatalogTable(
				tableSchema, partitionKeys, properties, comment);
		}
	}

	@Override
	protected Table createHiveTable(ObjectPath tablePath, CatalogBaseTable table) {
		Map<String, String> properties = new HashMap<>(table.getProperties());

		// Table comment
		properties.put(HiveTableConfig.TABLE_COMMENT, table.getComment());

		Table hiveTable = new Table();
		hiveTable.setDbName(tablePath.getDatabaseName());
		hiveTable.setTableName(tablePath.getObjectName());
		hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));

		// Table properties
		hiveTable.setParameters(maskFlinkProperties(properties));

		// Hive table's StorageDescriptor
		StorageDescriptor sd = new StorageDescriptor();
		sd.setSerdeInfo(new SerDeInfo(null, null, new HashMap<>()));

		List<FieldSchema> allColumns = HiveTableUtil.createHiveColumns(table.getSchema());

		// Table columns and partition keys
		if (table instanceof CatalogTable) {
			CatalogTable catalogTable = (CatalogTable) table;

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
		} else if (table instanceof CatalogView) {
			CatalogView view = (CatalogView) table;

			// TODO: [FLINK-12398] Support partitioned view in catalog API
			sd.setCols(allColumns);
			hiveTable.setPartitionKeys(new ArrayList<>());

			hiveTable.setViewOriginalText(view.getOriginalQuery());
			hiveTable.setViewExpandedText(view.getExpandedQuery());
			hiveTable.setTableType(TableType.VIRTUAL_VIEW.name());
		} else {
			throw new IllegalArgumentException(
				"GenericHiveMetastoreCatalog only supports CatalogTable and CatalogView");
		}

		hiveTable.setSd(sd);

		return hiveTable;
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
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws CatalogException {
		throw new UnsupportedOperationException();
	}

	// ------ functions ------

	@Override
	public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
			throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
			throws FunctionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
			throws FunctionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	// ------ statistics ------

	@Override
	public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics,
			boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics,
			boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
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
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	// ------ utils ------

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
}
