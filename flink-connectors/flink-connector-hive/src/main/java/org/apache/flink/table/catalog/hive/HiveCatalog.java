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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.hive.HiveTableFactory;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveDatabase;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.CatalogViewImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
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
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.catalog.hive.factories.HiveFunctionDefinitionFactory;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.catalog.hive.util.HiveStatsUtil;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.config.CatalogConfig.FLINK_PROPERTY_PREFIX;
import static org.apache.flink.table.catalog.hive.util.HiveStatsUtil.parsePositiveIntStat;
import static org.apache.flink.table.catalog.hive.util.HiveStatsUtil.parsePositiveLongStat;
import static org.apache.flink.table.utils.PartitionPathUtils.unescapePathName;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A catalog implementation for Hive.
 */
public class HiveCatalog extends AbstractCatalog {
	// Default database of Hive metastore
	public static final String DEFAULT_DB = "default";

	private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);
	private static final StorageFormatFactory storageFormatFactory = new StorageFormatFactory();
	private static final String DEFAULT_HIVE_TABLE_STORAGE_FORMAT = "TextFile";

	// Prefix used to distinguish Flink functions from Hive functions.
	// It's appended to Flink function's class name
	// because Hive's Function object doesn't have properties or other place to store the flag for Flink functions.
	private static final String FLINK_FUNCTION_PREFIX = "flink:";
	private static final String FLINK_PYTHON_FUNCTION_PREFIX = FLINK_FUNCTION_PREFIX + "python:";

	private final HiveConf hiveConf;
	private final String hiveVersion;
	private final HiveShim hiveShim;

	private HiveMetastoreClientWrapper client;

	public HiveCatalog(String catalogName, @Nullable String defaultDatabase, @Nullable String hiveConfDir) {
		this(catalogName, defaultDatabase, hiveConfDir, HiveShimLoader.getHiveVersion());
	}

	public HiveCatalog(String catalogName, @Nullable String defaultDatabase, @Nullable String hiveConfDir, String hiveVersion) {
		this(catalogName,
			defaultDatabase == null ? DEFAULT_DB : defaultDatabase,
			createHiveConf(hiveConfDir),
			hiveVersion,
			false);
	}

	@VisibleForTesting
	protected HiveCatalog(String catalogName, String defaultDatabase, @Nullable HiveConf hiveConf, String hiveVersion,
			boolean allowEmbedded) {
		super(catalogName, defaultDatabase == null ? DEFAULT_DB : defaultDatabase);

		this.hiveConf = hiveConf == null ? createHiveConf(null) : hiveConf;
		if (!allowEmbedded) {
			checkArgument(!StringUtils.isNullOrWhitespaceOnly(this.hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS)),
					"Embedded metastore is not allowed. Make sure you have set a valid value for " +
							HiveConf.ConfVars.METASTOREURIS.toString());
		}
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(hiveVersion), "hiveVersion cannot be null or empty");
		this.hiveVersion = hiveVersion;
		hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
		// add this to hiveConf to make sure table factory and source/sink see the same Hive version as HiveCatalog
		this.hiveConf.set(HiveCatalogValidator.CATALOG_HIVE_VERSION, hiveVersion);

		LOG.info("Created HiveCatalog '{}'", catalogName);
	}

	private static HiveConf createHiveConf(@Nullable String hiveConfDir) {
		LOG.info("Setting hive conf dir as {}", hiveConfDir);

		try {
			HiveConf.setHiveSiteLocation(
				hiveConfDir == null ?
					null : Paths.get(hiveConfDir, "hive-site.xml").toUri().toURL());
		} catch (MalformedURLException e) {
			throw new CatalogException(
				String.format("Failed to get hive-site.xml from %s", hiveConfDir), e);
		}

		// create HiveConf from hadoop configuration
		Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(new org.apache.flink.configuration.Configuration());

		// Add mapred-site.xml. We need to read configurations like compression codec.
		for (String possibleHadoopConfPath : HadoopUtils.possibleHadoopConfPaths(new org.apache.flink.configuration.Configuration())) {
			File mapredSite = new File(new File(possibleHadoopConfPath), "mapred-site.xml");
			if (mapredSite.exists()) {
				hadoopConf.addResource(new Path(mapredSite.getAbsolutePath()));
				break;
			}
		}
		return new HiveConf(hadoopConf, HiveConf.class);
	}

	@VisibleForTesting
	public HiveConf getHiveConf() {
		return hiveConf;
	}

	@VisibleForTesting
	public String getHiveVersion() {
		return hiveVersion;
	}

	@Override
	public void open() throws CatalogException {
		if (client == null) {
			client = HiveMetastoreClientFactory.create(hiveConf, hiveVersion);
			LOG.info("Connected to Hive metastore");
		}

		if (!databaseExists(getDefaultDatabase())) {
			throw new CatalogException(String.format("Configured default database %s doesn't exist in catalog %s.",
				getDefaultDatabase(), getName()));
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

	@Override
	public Optional<TableFactory> getTableFactory() {
		return Optional.of(new HiveTableFactory(hiveConf));
	}

	@Override
	public Optional<FunctionDefinitionFactory> getFunctionDefinitionFactory() {
		return Optional.of(new HiveFunctionDefinitionFactory(hiveShim));
	}

	// ------ databases ------

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		Database hiveDatabase = getHiveDatabase(databaseName);

		Map<String, String> properties = hiveDatabase.getParameters();

		boolean isGeneric = isGenericForGet(properties);
		if (!isGeneric) {
			properties.put(SqlCreateHiveDatabase.DATABASE_LOCATION_URI, hiveDatabase.getLocationUri());
		}

		return new CatalogDatabaseImpl(properties, hiveDatabase.getDescription());
	}

	@Override
	public void createDatabase(String databaseName, CatalogDatabase database, boolean ignoreIfExists)
			throws DatabaseAlreadyExistException, CatalogException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
		checkNotNull(database, "database cannot be null");

		Database hiveDatabase = HiveDatabaseUtil.instantiateHiveDatabase(databaseName, database);

		try {
			client.createDatabase(hiveDatabase);
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new DatabaseAlreadyExistException(getName(), hiveDatabase.getName());
			}
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to create database %s", hiveDatabase.getName()), e);
		}
	}

	@Override
	public void alterDatabase(String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
			throws DatabaseNotExistException, CatalogException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
		checkNotNull(newDatabase, "newDatabase cannot be null");

		// client.alterDatabase doesn't throw any exception if there is no existing database
		Database hiveDB;
		try {
			hiveDB = getHiveDatabase(databaseName);
		} catch (DatabaseNotExistException e) {
			if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(getName(), databaseName);
			}

			return;
		}

		try {
			client.alterDatabase(databaseName, HiveDatabaseUtil.alterDatabase(hiveDB, newDatabase));
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
				String.format("Failed to list all databases in %s", getName()), e);
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
	public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
			throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
		try {
			client.dropDatabase(name, true, ignoreIfNotExists, cascade);
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(getName(), name);
			}
		} catch (InvalidOperationException e) {
			throw new DatabaseNotEmptyException(getName(), name);
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to drop database %s", name), e);
		}
	}

	@VisibleForTesting
	public Database getHiveDatabase(String databaseName) throws DatabaseNotExistException {
		try {
			return client.getDatabase(databaseName);
		} catch (NoSuchObjectException e) {
			throw new DatabaseNotExistException(getName(), databaseName);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to get database %s from %s", databaseName, getName()), e);
		}
	}

	// ------ tables ------

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		checkNotNull(tablePath, "tablePath cannot be null");

		Table hiveTable = getHiveTable(tablePath);
		return instantiateCatalogTable(hiveTable, hiveConf);
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
			throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
		checkNotNull(tablePath, "tablePath cannot be null");
		checkNotNull(table, "table cannot be null");

		if (!databaseExists(tablePath.getDatabaseName())) {
			throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
		}

		Table hiveTable = instantiateHiveTable(tablePath, table);

		UniqueConstraint pkConstraint = null;
		List<String> notNullCols = new ArrayList<>();
		boolean isGeneric = isGenericForCreate(table.getOptions());
		if (!isGeneric) {
			pkConstraint = table.getSchema().getPrimaryKey().orElse(null);
			for (int i = 0; i < table.getSchema().getFieldDataTypes().length; i++) {
				if (!table.getSchema().getFieldDataTypes()[i].getLogicalType().isNullable()) {
					notNullCols.add(table.getSchema().getFieldNames()[i]);
				}
			}
		}

		try {
			if (pkConstraint != null || !notNullCols.isEmpty()) {
				// for now we just create constraints that are DISABLE, NOVALIDATE, RELY
				Byte[] pkTraits = new Byte[pkConstraint == null ? 0 : pkConstraint.getColumns().size()];
				Arrays.fill(pkTraits, HiveTableUtil.relyConstraint((byte) 0));
				Byte[] nnTraits = new Byte[notNullCols.size()];
				Arrays.fill(nnTraits, HiveTableUtil.relyConstraint((byte) 0));
				client.createTableWithConstraints(
						hiveTable,
						hiveConf,
						pkConstraint,
						Arrays.asList(pkTraits),
						notNullCols,
						Arrays.asList(nnTraits));
			} else {
				client.createTable(hiveTable);
			}
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new TableAlreadyExistException(getName(), tablePath, e);
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
					throw new TableAlreadyExistException(getName(), newPath);
				} else {
					Table table = getHiveTable(tablePath);
					table.setTableName(newTableName);
					client.alter_table(tablePath.getDatabaseName(), tablePath.getObjectName(), table);
				}
			} else if (!ignoreIfNotExists) {
				throw new TableNotExistException(getName(), tablePath);
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

		Table hiveTable;
		try {
			hiveTable = getHiveTable(tablePath);
		} catch (TableNotExistException e) {
			if (!ignoreIfNotExists) {
				throw e;
			}
			return;
		}

		CatalogBaseTable existingTable = instantiateCatalogTable(hiveTable, hiveConf);

		if (existingTable.getClass() != newCatalogTable.getClass()) {
			throw new CatalogException(
				String.format("Table types don't match. Existing table is '%s' and new table is '%s'.",
					existingTable.getClass().getName(), newCatalogTable.getClass().getName()));
		}

		Table newTable = instantiateHiveTable(tablePath, newCatalogTable);

		// client.alter_table() requires a valid location
		// thus, if new table doesn't have that, it reuses location of the old table
		if (!newTable.getSd().isSetLocation()) {
			newTable.getSd().setLocation(hiveTable.getSd().getLocation());
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
				throw new TableNotExistException(getName(), tablePath);
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
			throw new DatabaseNotExistException(getName(), databaseName);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to list tables in database %s", databaseName), e);
		}
	}

	@Override
	public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

		try {
			return client.getViews(databaseName);
		} catch (UnknownDBException e) {
			throw new DatabaseNotExistException(getName(), databaseName);
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

	@VisibleForTesting
	public Table getHiveTable(ObjectPath tablePath) throws TableNotExistException {
		try {
			return client.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
		} catch (NoSuchObjectException e) {
			throw new TableNotExistException(getName(), tablePath);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to get table %s from Hive metastore", tablePath.getFullName()), e);
		}
	}

	private CatalogBaseTable instantiateCatalogTable(Table hiveTable, HiveConf hiveConf) {
		boolean isView = TableType.valueOf(hiveTable.getTableType()) == TableType.VIRTUAL_VIEW;

		// Table properties
		Map<String, String> properties = hiveTable.getParameters();

		boolean isGeneric = isGenericForGet(hiveTable.getParameters());

		TableSchema tableSchema;
		// Partition keys
		List<String> partitionKeys = new ArrayList<>();

		if (isGeneric) {
			properties = retrieveFlinkProperties(properties);
			DescriptorProperties tableSchemaProps = new DescriptorProperties(true);
			tableSchemaProps.putProperties(properties);
			ObjectPath tablePath = new ObjectPath(hiveTable.getDbName(), hiveTable.getTableName());
			tableSchema = tableSchemaProps.getOptionalTableSchema(Schema.SCHEMA)
					.orElseThrow(() -> new CatalogException("Failed to get table schema from properties for generic table " + tablePath));
			partitionKeys = tableSchemaProps.getPartitionKeys();
			// remove the schema from properties
			properties = CatalogTableImpl.removeRedundant(properties, tableSchema, partitionKeys);
		} else {
			properties.put(CatalogConfig.IS_GENERIC, String.valueOf(false));
			// Table schema
			List<FieldSchema> fields = getNonPartitionFields(hiveConf, hiveTable);
			Set<String> notNullColumns = client.getNotNullColumns(hiveConf, hiveTable.getDbName(), hiveTable.getTableName());
			Optional<UniqueConstraint> primaryKey = isView ? Optional.empty() :
					client.getPrimaryKey(hiveTable.getDbName(), hiveTable.getTableName(), HiveTableUtil.relyConstraint((byte) 0));
			// PK columns cannot be null
			primaryKey.ifPresent(pk -> notNullColumns.addAll(pk.getColumns()));
			tableSchema = HiveTableUtil.createTableSchema(fields, hiveTable.getPartitionKeys(), notNullColumns, primaryKey.orElse(null));

			if (!hiveTable.getPartitionKeys().isEmpty()) {
				partitionKeys = getFieldNames(hiveTable.getPartitionKeys());
			}
		}

		String comment = properties.remove(HiveCatalogConfig.COMMENT);

		if (isView) {
			return new CatalogViewImpl(
					hiveTable.getViewOriginalText(),
					hiveTable.getViewExpandedText(),
					tableSchema,
					properties,
					comment);
		} else {
			return new CatalogTableImpl(tableSchema, partitionKeys, properties, comment);
		}
	}

	private List<FieldSchema> getNonPartitionFields(HiveConf hiveConf, Table hiveTable) {
		if (org.apache.hadoop.hive.ql.metadata.Table.hasMetastoreBasedSchema(hiveConf,
				hiveTable.getSd().getSerdeInfo().getSerializationLib())) {
			// get schema from metastore
			return hiveTable.getSd().getCols();
		} else {
			// get schema from deserializer
			return hiveShim.getFieldsFromDeserializer(hiveConf, hiveTable, true);
		}
	}

	@VisibleForTesting
	protected static Table instantiateHiveTable(ObjectPath tablePath, CatalogBaseTable table) {
		if (!(table instanceof CatalogTableImpl) && !(table instanceof CatalogViewImpl)) {
			throw new CatalogException(
					"HiveCatalog only supports CatalogTableImpl and CatalogViewImpl");
		}
		// let Hive set default parameters for us, e.g. serialization.format
		Table hiveTable = org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(tablePath.getDatabaseName(),
			tablePath.getObjectName());
		hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));

		Map<String, String> properties = new HashMap<>(table.getProperties());
		// Table comment
		if (table.getComment() != null) {
			properties.put(HiveCatalogConfig.COMMENT, table.getComment());
		}

		boolean isGeneric = isGenericForCreate(properties);

		// Hive table's StorageDescriptor
		StorageDescriptor sd = hiveTable.getSd();
		setStorageFormat(sd, properties);

		if (isGeneric) {
			DescriptorProperties tableSchemaProps = new DescriptorProperties(true);
			tableSchemaProps.putTableSchema(Schema.SCHEMA, table.getSchema());

			if (table instanceof CatalogTable) {
				tableSchemaProps.putPartitionKeys(((CatalogTable) table).getPartitionKeys());
			}

			properties.putAll(tableSchemaProps.asMap());
			properties = maskFlinkProperties(properties);
		} else {
			List<FieldSchema> allColumns = HiveTableUtil.createHiveColumns(table.getSchema());
			// Table columns and partition keys
			if (table instanceof CatalogTableImpl) {
				CatalogTable catalogTable = (CatalogTableImpl) table;

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
			} else {
				sd.setCols(allColumns);
			}
		}

		if (table instanceof CatalogViewImpl) {
			// TODO: [FLINK-12398] Support partitioned view in catalog API
			hiveTable.setPartitionKeys(new ArrayList<>());

			CatalogView view = (CatalogView) table;
			hiveTable.setViewOriginalText(view.getOriginalQuery());
			hiveTable.setViewExpandedText(view.getExpandedQuery());
			hiveTable.setTableType(TableType.VIRTUAL_VIEW.name());
		}

		// Table properties
		hiveTable.setParameters(properties);

		return hiveTable;
	}

	private static void setStorageFormat(StorageDescriptor sd, Map<String, String> properties) {
		// TODO: allow user to specify storage format. Simply use text format for now
		String storageFormatName = DEFAULT_HIVE_TABLE_STORAGE_FORMAT;
		StorageFormatDescriptor storageFormatDescriptor = storageFormatFactory.get(storageFormatName);
		checkArgument(storageFormatDescriptor != null, "Unknown storage format " + storageFormatName);
		sd.setInputFormat(storageFormatDescriptor.getInputFormat());
		sd.setOutputFormat(storageFormatDescriptor.getOutputFormat());
		String serdeLib = storageFormatDescriptor.getSerde();
		sd.getSerdeInfo().setSerializationLib(serdeLib != null ? serdeLib : LazySimpleSerDe.class.getName());
	}

	/**
	 * Filter out Hive-created properties, and return Flink-created properties.
	 * Note that 'is_generic' is a special key and this method will leave it as-is.
	 */
	private static Map<String, String> retrieveFlinkProperties(Map<String, String> hiveTableParams) {
		return hiveTableParams.entrySet().stream()
			.filter(e -> e.getKey().startsWith(FLINK_PROPERTY_PREFIX) || e.getKey().equals(CatalogConfig.IS_GENERIC))
			.collect(Collectors.toMap(e -> e.getKey().replace(FLINK_PROPERTY_PREFIX, ""), e -> e.getValue()));
	}

	/**
	 * Add a prefix to Flink-created properties to distinguish them from Hive-created properties.
	 * Note that 'is_generic' is a special key and this method will leave it as-is.
	 */
	private static Map<String, String> maskFlinkProperties(Map<String, String> properties) {
		return properties.entrySet().stream()
			.filter(e -> e.getKey() != null && e.getValue() != null)
			.map(e -> new Tuple2<>(
				e.getKey().equals(CatalogConfig.IS_GENERIC) ? e.getKey() : FLINK_PROPERTY_PREFIX + e.getKey(),
				e.getValue()))
			.collect(Collectors.toMap(t -> t.f0, t -> t.f1));
	}

	// ------ partitions ------

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws CatalogException {
		checkNotNull(tablePath, "Table path cannot be null");
		checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

		try {
			return getHivePartition(tablePath, partitionSpec) != null;
		} catch (NoSuchObjectException | TableNotExistException | PartitionSpecInvalidException e) {
			return false;
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to get partition %s of table %s", partitionSpec, tablePath), e);
		}
	}

	@Override
	public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists)
			throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
		checkNotNull(tablePath, "Table path cannot be null");
		checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");
		checkNotNull(partition, "Partition cannot be null");

		boolean isGeneric = Boolean.valueOf(partition.getProperties().get(CatalogConfig.IS_GENERIC));

		if (isGeneric) {
			throw new CatalogException("Currently only supports non-generic CatalogPartition");
		}

		Table hiveTable = getHiveTable(tablePath);

		ensureTableAndPartitionMatch(hiveTable, partition);

		ensurePartitionedTable(tablePath, hiveTable);

		try {
			client.add_partition(instantiateHivePartition(hiveTable, partitionSpec, partition));
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new PartitionAlreadyExistsException(getName(), tablePath, partitionSpec);
			}
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to create partition %s of table %s", partitionSpec, tablePath));
		}
	}

	@Override
	public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
			throws PartitionNotExistException, CatalogException {
		checkNotNull(tablePath, "Table path cannot be null");
		checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

		try {
			Table hiveTable = getHiveTable(tablePath);
			client.dropPartition(tablePath.getDatabaseName(), tablePath.getObjectName(),
				getOrderedFullPartitionValues(partitionSpec, getFieldNames(hiveTable.getPartitionKeys()), tablePath), true);
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
			}
		} catch (MetaException | TableNotExistException | PartitionSpecInvalidException e) {
			throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to drop partition %s of table %s", partitionSpec, tablePath));
		}
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
			throws TableNotExistException, TableNotPartitionedException, CatalogException {
		checkNotNull(tablePath, "Table path cannot be null");

		Table hiveTable = getHiveTable(tablePath);

		ensurePartitionedTable(tablePath, hiveTable);

		try {
			// pass -1 as max_parts to fetch all partitions
			return client.listPartitionNames(tablePath.getDatabaseName(), tablePath.getObjectName(), (short) -1).stream()
				.map(HiveCatalog::createPartitionSpec).collect(Collectors.toList());
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to list partitions of table %s", tablePath), e);
		}
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws TableNotExistException, TableNotPartitionedException, CatalogException {
		checkNotNull(tablePath, "Table path cannot be null");
		checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

		Table hiveTable = getHiveTable(tablePath);

		ensurePartitionedTable(tablePath, hiveTable);

		try {
			// partition spec can be partial
			List<String> partialVals = HiveReflectionUtils.getPvals(hiveShim, hiveTable.getPartitionKeys(),
				partitionSpec.getPartitionSpec());
			return client.listPartitionNames(tablePath.getDatabaseName(), tablePath.getObjectName(), partialVals,
				(short) -1).stream().map(HiveCatalog::createPartitionSpec).collect(Collectors.toList());
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to list partitions of table %s", tablePath), e);
		}
	}

	@Override
	public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> expressions)
			throws TableNotExistException, TableNotPartitionedException, CatalogException {
		Table hiveTable = getHiveTable(tablePath);
		ensurePartitionedTable(tablePath, hiveTable);
		List<String> partColNames = getFieldNames(hiveTable.getPartitionKeys());
		Optional<String> filter = HiveTableUtil.makePartitionFilter(
				getNonPartitionFields(hiveConf, hiveTable).size(), partColNames, expressions, hiveShim);
		if (!filter.isPresent()) {
			throw new UnsupportedOperationException(
					"HiveCatalog is unable to handle the partition filter expressions: " + expressions);
		}
		try {
			PartitionSpecProxy partitionSpec = client.listPartitionSpecsByFilter(
					tablePath.getDatabaseName(), tablePath.getObjectName(), filter.get(), (short) -1);
			List<CatalogPartitionSpec> res = new ArrayList<>(partitionSpec.size());
			PartitionSpecProxy.PartitionIterator partitions = partitionSpec.getPartitionIterator();
			while (partitions.hasNext()) {
				Partition partition = partitions.next();
				Map<String, String> spec = new HashMap<>();
				for (int i = 0; i < partColNames.size(); i++) {
					spec.put(partColNames.get(i), partition.getValues().get(i));
				}
				res.add(new CatalogPartitionSpec(spec));
			}
			return res;
		} catch (TException e) {
			throw new UnsupportedOperationException(
					"Failed to list partition by filter from HMS, filter expressions: " + expressions, e);
		}
	}

	@Override
	public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws PartitionNotExistException, CatalogException {
		checkNotNull(tablePath, "Table path cannot be null");
		checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

		try {
			Partition hivePartition = getHivePartition(tablePath, partitionSpec);

			Map<String, String> properties = hivePartition.getParameters();

			properties.put(HiveCatalogConfig.PARTITION_LOCATION, hivePartition.getSd().getLocation());

			String comment = properties.remove(HiveCatalogConfig.COMMENT);

			return new CatalogPartitionImpl(properties, comment);
		} catch (NoSuchObjectException | MetaException | TableNotExistException | PartitionSpecInvalidException e) {
			throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to get partition %s of table %s", partitionSpec, tablePath), e);
		}
	}

	@Override
	public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists)
			throws PartitionNotExistException, CatalogException {
		checkNotNull(tablePath, "Table path cannot be null");
		checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");
		checkNotNull(newPartition, "New partition cannot be null");

		boolean isGeneric = Boolean.valueOf(newPartition.getProperties().get(CatalogConfig.IS_GENERIC));

		if (isGeneric) {
			throw new CatalogException("Currently only supports non-generic CatalogPartition");
		}

		// Explicitly check if the partition exists or not
		// because alter_partition() doesn't throw NoSuchObjectException like dropPartition() when the target doesn't exist
		try {
			Table hiveTable = getHiveTable(tablePath);
			ensureTableAndPartitionMatch(hiveTable, newPartition);
			Partition oldHivePartition = getHivePartition(hiveTable, partitionSpec);
			if (oldHivePartition == null) {
				if (ignoreIfNotExists) {
					return;
				}
				throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
			}
			Partition newHivePartition = instantiateHivePartition(hiveTable, partitionSpec, newPartition);
			if (newHivePartition.getSd().getLocation() == null) {
				newHivePartition.getSd().setLocation(oldHivePartition.getSd().getLocation());
			}
			client.alter_partition(
				tablePath.getDatabaseName(),
				tablePath.getObjectName(),
				newHivePartition
			);
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
			}
		} catch (InvalidOperationException | MetaException | TableNotExistException | PartitionSpecInvalidException e) {
			throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to alter existing partition with new partition %s of table %s",
					partitionSpec, tablePath), e);
		}
	}

	// make sure both table and partition are generic, or neither is
	private static void ensureTableAndPartitionMatch(Table hiveTable, CatalogPartition catalogPartition) {
		boolean tableIsGeneric = Boolean.valueOf(hiveTable.getParameters().get(CatalogConfig.IS_GENERIC));
		boolean partitionIsGeneric = Boolean.valueOf(catalogPartition.getProperties().get(CatalogConfig.IS_GENERIC));

		if (tableIsGeneric != partitionIsGeneric) {
			throw new CatalogException(String.format("Cannot handle %s partition for %s table",
				catalogPartition.getClass().getName(), tableIsGeneric ? "generic" : "non-generic"));
		}
	}

	private Partition instantiateHivePartition(Table hiveTable, CatalogPartitionSpec partitionSpec, CatalogPartition catalogPartition)
			throws PartitionSpecInvalidException {
		List<String> partCols = getFieldNames(hiveTable.getPartitionKeys());
		List<String> partValues = getOrderedFullPartitionValues(
			partitionSpec, partCols, new ObjectPath(hiveTable.getDbName(), hiveTable.getTableName()));
		// validate partition values
		for (int i = 0; i < partCols.size(); i++) {
			if (StringUtils.isNullOrWhitespaceOnly(partValues.get(i))) {
				throw new PartitionSpecInvalidException(getName(), partCols,
					new ObjectPath(hiveTable.getDbName(), hiveTable.getTableName()), partitionSpec);
			}
		}
		// TODO: handle GenericCatalogPartition
		StorageDescriptor sd = hiveTable.getSd().deepCopy();
		sd.setLocation(catalogPartition.getProperties().remove(HiveCatalogConfig.PARTITION_LOCATION));

		Map<String, String> properties = new HashMap<>(catalogPartition.getProperties());
		properties.put(HiveCatalogConfig.COMMENT, catalogPartition.getComment());

		return HiveTableUtil.createHivePartition(
				hiveTable.getDbName(),
				hiveTable.getTableName(),
				partValues,
				sd,
				properties);
	}

	private void ensurePartitionedTable(ObjectPath tablePath, Table hiveTable) throws TableNotPartitionedException {
		if (!isTablePartitioned(hiveTable)) {
			throw new TableNotPartitionedException(getName(), tablePath);
		}
	}

	/**
	 * Get field names from field schemas.
	 */
	public static List<String> getFieldNames(List<FieldSchema> fieldSchemas) {
		List<String> names = new ArrayList<>(fieldSchemas.size());
		for (FieldSchema fs : fieldSchemas) {
			names.add(fs.getName());
		}
		return names;
	}

	/**
	 * Creates a {@link CatalogPartitionSpec} from a Hive partition name string.
	 * Example of Hive partition name string - "name=bob/year=2019"
	 */
	private static CatalogPartitionSpec createPartitionSpec(String hivePartitionName) {
		String[] partKeyVals = hivePartitionName.split("/");
		Map<String, String> spec = new HashMap<>(partKeyVals.length);
		for (String keyVal : partKeyVals) {
			String[] kv = keyVal.split("=");
			spec.put(unescapePathName(kv[0]), unescapePathName(kv[1]));
		}
		return new CatalogPartitionSpec(spec);
	}

	/**
	 * Get a list of ordered partition values by re-arranging them based on the given list of partition keys.
	 *
	 * @param partitionSpec a partition spec.
	 * @param partitionKeys a list of partition keys.
	 * @param tablePath path of the table to which the partition belongs.
	 * @return A list of partition values ordered according to partitionKeys.
	 * @throws PartitionSpecInvalidException thrown if partitionSpec and partitionKeys have different sizes,
	 *                                       or any key in partitionKeys doesn't exist in partitionSpec.
	 */
	private List<String> getOrderedFullPartitionValues(CatalogPartitionSpec partitionSpec, List<String> partitionKeys, ObjectPath tablePath)
			throws PartitionSpecInvalidException {
		Map<String, String> spec = partitionSpec.getPartitionSpec();
		if (spec.size() != partitionKeys.size()) {
			throw new PartitionSpecInvalidException(getName(), partitionKeys, tablePath, partitionSpec);
		}

		List<String> values = new ArrayList<>(spec.size());
		for (String key : partitionKeys) {
			if (!spec.containsKey(key)) {
				throw new PartitionSpecInvalidException(getName(), partitionKeys, tablePath, partitionSpec);
			} else {
				values.add(spec.get(key));
			}
		}

		return values;
	}

	private Partition getHivePartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws TableNotExistException, PartitionSpecInvalidException, TException {
		return getHivePartition(getHiveTable(tablePath), partitionSpec);
	}

	private Partition getHivePartition(Table hiveTable, CatalogPartitionSpec partitionSpec)
			throws PartitionSpecInvalidException, TException {
		return client.getPartition(hiveTable.getDbName(), hiveTable.getTableName(),
			getOrderedFullPartitionValues(partitionSpec, getFieldNames(hiveTable.getPartitionKeys()),
				new ObjectPath(hiveTable.getDbName(), hiveTable.getTableName())));
	}

	// ------ functions ------

	@Override
	public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
			throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
		checkNotNull(functionPath, "functionPath cannot be null");
		checkNotNull(function, "function cannot be null");

		Function hiveFunction;
		if (function instanceof CatalogFunctionImpl) {
			hiveFunction = instantiateHiveFunction(functionPath, function);
		} else {
			throw new CatalogException(
				String.format("Unsupported catalog function type %s", function.getClass().getName()));
		}

		try {
			client.createFunction(hiveFunction);
		} catch (NoSuchObjectException e) {
			throw new DatabaseNotExistException(getName(), functionPath.getDatabaseName(), e);
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new FunctionAlreadyExistException(getName(), functionPath, e);
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
			boolean existingType = existingFunction.isGeneric();
			boolean newType = newFunction.isGeneric();
			if (existingType != newType) {
				throw new CatalogException(
					String.format("Function types don't match. Existing function %s generic, and new function %s generic.",
						existingType ? "is" : "isn't",
						newType ? "is" : "isn't"));
			}

			Function hiveFunction;
			if (newFunction instanceof CatalogFunctionImpl) {
				hiveFunction = instantiateHiveFunction(functionPath, newFunction);
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
				throw new FunctionNotExistException(getName(), functionPath, e);
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
			throw new DatabaseNotExistException(getName(), databaseName);
		}

		try {
			// hive-1.x requires the pattern not being null, so pass a pattern that matches any name
			return client.getFunctions(databaseName, ".*");
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
				if (function.getClassName().startsWith(FLINK_PYTHON_FUNCTION_PREFIX)) {
					return new CatalogFunctionImpl(
						function.getClassName().substring(FLINK_PYTHON_FUNCTION_PREFIX.length()),
						FunctionLanguage.PYTHON);
				} else {
					return new CatalogFunctionImpl(function.getClassName().substring(FLINK_FUNCTION_PREFIX.length()));
				}
			} else {
				return new CatalogFunctionImpl(function.getClassName());
			}
		} catch (NoSuchObjectException e) {
			throw new FunctionNotExistException(getName(), functionPath, e);
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

	private static Function instantiateHiveFunction(ObjectPath functionPath, CatalogFunction function) {

		boolean isGeneric = function.isGeneric();

		// Hive Function does not have properties map
		// thus, use a prefix in class name to distinguish Flink and Hive functions
		String functionClassName;
		if (function.getFunctionLanguage().equals(FunctionLanguage.JAVA)) {
			functionClassName = isGeneric ?
				FLINK_FUNCTION_PREFIX + function.getClassName() :
				function.getClassName();
		} else if (function.getFunctionLanguage().equals(FunctionLanguage.PYTHON)) {
			functionClassName = FLINK_PYTHON_FUNCTION_PREFIX + function.getClassName();
		} else {
			throw new UnsupportedOperationException("HiveCatalog supports only creating" +
				" JAVA or PYTHON based function for now");
		}

		return new Function(
			// due to https://issues.apache.org/jira/browse/HIVE-22053, we have to normalize function name ourselves
			functionPath.getObjectName().trim().toLowerCase(),
			functionPath.getDatabaseName(),
			functionClassName,
			null,			// Owner name
			PrincipalType.GROUP,	// Temporarily set to GROUP type because it's required by Hive. May change later
			(int) (System.currentTimeMillis() / 1000),
			FunctionType.JAVA,		// FunctionType only has JAVA now
			new ArrayList<>()		// Resource URIs
		);
	}

	private boolean isTablePartitioned(Table hiveTable) {
		return hiveTable.getPartitionKeysSize() != 0;
	}

	// ------ stats ------

	@Override
	public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
		try {
			Table hiveTable = getHiveTable(tablePath);
			// the stats we put in table parameters will be overridden by HMS in older Hive versions, so error out
			if (!isTablePartitioned(hiveTable) && hiveVersion.compareTo("1.2.1") < 0) {
				throw new CatalogException("Alter table stats is not supported in Hive version " + hiveVersion);
			}
			// Set table stats
			if (statsChanged(tableStatistics, hiveTable.getParameters())) {
				updateStats(tableStatistics, hiveTable.getParameters());
				client.alter_table(tablePath.getDatabaseName(), tablePath.getObjectName(), hiveTable);
			}
		} catch (TableNotExistException e) {
			if (!ignoreIfNotExists) {
				throw e;
			}
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to alter table stats of table %s", tablePath.getFullName()), e);
		}
	}

	@Override
	public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {
		try {
			Table hiveTable = getHiveTable(tablePath);
			// Set table column stats. This only works for non-partitioned tables.
			if (!isTablePartitioned(hiveTable)) {
				client.updateTableColumnStatistics(HiveStatsUtil.createTableColumnStats(hiveTable, columnStatistics.getColumnStatisticsData(), hiveVersion));
			} else {
				throw new TablePartitionedException(getName(), tablePath);
			}
		} catch (TableNotExistException e) {
			if (!ignoreIfNotExists) {
				throw e;
			}
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to alter table column stats of table %s", tablePath.getFullName()), e);
		}
	}

	/**
	 * Determine if statistics need to be updated or not.
	 * @param newTableStats   new catalog table statistics.
	 * @param parameters      original hive table statistics parameters.
	 * @return                whether need to update stats.
	 */
	private boolean statsChanged(CatalogTableStatistics newTableStats, Map<String, String> parameters) {
		return newTableStats.getRowCount() != parsePositiveLongStat(parameters, StatsSetupConst.ROW_COUNT)
				|| newTableStats.getTotalSize() != parsePositiveLongStat(parameters, StatsSetupConst.TOTAL_SIZE)
				|| newTableStats.getFileCount() != parsePositiveIntStat(parameters, StatsSetupConst.NUM_FILES)
				|| newTableStats.getRawDataSize() != parsePositiveLongStat(parameters, StatsSetupConst.NUM_FILES);
	}

	/**
	 * Update original table statistics parameters.
	 * @param newTableStats   new catalog table statistics.
	 * @param parameters      original hive table statistics parameters.
	 */
	private void updateStats(CatalogTableStatistics newTableStats, Map<String, String> parameters) {
		parameters.put(StatsSetupConst.ROW_COUNT, String.valueOf(newTableStats.getRowCount()));
		parameters.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(newTableStats.getTotalSize()));
		parameters.put(StatsSetupConst.NUM_FILES, String.valueOf(newTableStats.getFileCount()));
		parameters.put(StatsSetupConst.RAW_DATA_SIZE, String.valueOf(newTableStats.getRawDataSize()));
	}

	private static CatalogTableStatistics createCatalogTableStatistics(Map<String, String> parameters) {
		return new CatalogTableStatistics(
				parsePositiveLongStat(parameters, StatsSetupConst.ROW_COUNT),
				parsePositiveIntStat(parameters, StatsSetupConst.NUM_FILES),
				parsePositiveLongStat(parameters, StatsSetupConst.TOTAL_SIZE),
				parsePositiveLongStat(parameters, StatsSetupConst.RAW_DATA_SIZE));
	}

	@Override
	public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		try {
			Partition hivePartition = getHivePartition(tablePath, partitionSpec);
			// Set table stats
			if (statsChanged(partitionStatistics, hivePartition.getParameters())) {
				updateStats(partitionStatistics, hivePartition.getParameters());
				client.alter_partition(tablePath.getDatabaseName(), tablePath.getObjectName(), hivePartition);
			}
		} catch (TableNotExistException | PartitionSpecInvalidException e) {
			throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to alter table stats of table %s 's partition %s", tablePath.getFullName(), String.valueOf(partitionSpec)), e);
		}
	}

	@Override
	public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		try {
			Partition hivePartition = getHivePartition(tablePath, partitionSpec);
			Table hiveTable = getHiveTable(tablePath);
			String partName = getPartitionName(tablePath, partitionSpec, hiveTable);
			client.updatePartitionColumnStatistics(HiveStatsUtil.createPartitionColumnStats(
					hivePartition, partName, columnStatistics.getColumnStatisticsData(), hiveVersion));
		} catch (TableNotExistException | PartitionSpecInvalidException e) {
			if (!ignoreIfNotExists) {
				throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
			}
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to alter table column stats of table %s 's partition %s",
													tablePath.getFullName(), String.valueOf(partitionSpec)), e);
		}
	}

	private String getPartitionName(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, Table hiveTable) throws PartitionSpecInvalidException {
		List<String> partitionCols = getFieldNames(hiveTable.getPartitionKeys());
		List<String> partitionVals = getOrderedFullPartitionValues(partitionSpec, partitionCols, tablePath);
		List<String> partKVs = new ArrayList<>();
		for (int i = 0; i < partitionCols.size(); i++) {
			partKVs.add(partitionCols.get(i) + "=" + partitionVals.get(i));
		}
		return String.join("/", partKVs);
	}

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException,
			CatalogException {
		Table hiveTable = getHiveTable(tablePath);
		if (!isTablePartitioned(hiveTable)) {
			return createCatalogTableStatistics(hiveTable.getParameters());
		} else {
			return CatalogTableStatistics.UNKNOWN;
		}
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		Table hiveTable = getHiveTable(tablePath);
		try {
			if (!isTablePartitioned(hiveTable)) {
				List<ColumnStatisticsObj> columnStatisticsObjs = client.getTableColumnStatistics(
						hiveTable.getDbName(), hiveTable.getTableName(), getFieldNames(hiveTable.getSd().getCols()));
				return new CatalogColumnStatistics(HiveStatsUtil.createCatalogColumnStats(columnStatisticsObjs, hiveVersion));
			} else {
				// TableColumnStats of partitioned table is unknown, the behavior is same as HIVE
				return CatalogColumnStatistics.UNKNOWN;
			}
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to get table column stats of table %s",
													tablePath.getFullName()), e);
		}
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		try {
			Partition partition = getHivePartition(tablePath, partitionSpec);
			return createCatalogTableStatistics(partition.getParameters());
		} catch (TableNotExistException | PartitionSpecInvalidException e) {
			throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to get partition stats of table %s 's partition %s",
													tablePath.getFullName(), String.valueOf(partitionSpec)), e);
		}
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		try {
			Partition partition = getHivePartition(tablePath, partitionSpec);
			Table hiveTable = getHiveTable(tablePath);
			String partName = getPartitionName(tablePath, partitionSpec, hiveTable);
			List<String> partNames = new ArrayList<>();
			partNames.add(partName);
			Map<String, List<ColumnStatisticsObj>> partitionColumnStatistics =
					client.getPartitionColumnStatistics(partition.getDbName(), partition.getTableName(), partNames,
														getFieldNames(partition.getSd().getCols()));
			List<ColumnStatisticsObj> columnStatisticsObjs = partitionColumnStatistics.get(partName);
			if (columnStatisticsObjs != null && !columnStatisticsObjs.isEmpty()) {
				return new CatalogColumnStatistics(HiveStatsUtil.createCatalogColumnStats(columnStatisticsObjs, hiveVersion));
			} else {
				return CatalogColumnStatistics.UNKNOWN;
			}
		} catch (TableNotExistException | PartitionSpecInvalidException e) {
			throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to get table stats of table %s 's partition %s",
													tablePath.getFullName(), String.valueOf(partitionSpec)), e);
		}
	}

	static boolean isGenericForCreate(Map<String, String> properties) {
		// When creating an object, a hive object needs explicitly have a key is_generic = false
		// otherwise, this is a generic object if 1) the key is missing 2) is_generic = true
		// this is opposite to reading an object. See getObjectIsGeneric().
		if (properties == null) {
			return true;
		}
		boolean isGeneric;
		if (!properties.containsKey(CatalogConfig.IS_GENERIC)) {
			// must be a generic object
			isGeneric = true;
			properties.put(CatalogConfig.IS_GENERIC, String.valueOf(true));
		} else {
			isGeneric = Boolean.parseBoolean(properties.get(CatalogConfig.IS_GENERIC));
		}
		return isGeneric;
	}

	static boolean isGenericForGet(Map<String, String> properties) {
		// When retrieving an object, a generic object needs explicitly have a key is_generic = true
		// otherwise, this is a Hive object if 1) the key is missing 2) is_generic = false
		// this is opposite to creating an object. See createObjectIsGeneric()
		return properties != null && Boolean.parseBoolean(properties.getOrDefault(CatalogConfig.IS_GENERIC, "false"));
	}

}
