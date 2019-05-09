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

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for catalogs backed by Hive metastore.
 */
public abstract class HiveCatalogBase implements Catalog {
	private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogBase.class);
	private static final String DEFAULT_DB = "default";

	protected final String catalogName;
	protected final HiveConf hiveConf;

	private final String defaultDatabase;
	protected IMetaStoreClient client;

	public HiveCatalogBase(String catalogName, String hivemetastoreURI) {
		this(catalogName, DEFAULT_DB, getHiveConf(hivemetastoreURI));
	}

	public HiveCatalogBase(String catalogName, HiveConf hiveConf) {
		this(catalogName, DEFAULT_DB, hiveConf);
	}

	public HiveCatalogBase(String catalogName, String defaultDatabase, HiveConf hiveConf) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(defaultDatabase), "defaultDatabase cannot be null or empty");
		this.catalogName = catalogName;
		this.defaultDatabase = defaultDatabase;
		this.hiveConf = checkNotNull(hiveConf, "hiveConf cannot be null");
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

	// ------ APIs ------

	/**
	 * Validate input base table.
	 *
	 * @param catalogBaseTable the base table to be validated
	 * @throws CatalogException thrown if the input base table is invalid.
	 */
	protected abstract void validateCatalogBaseTable(CatalogBaseTable catalogBaseTable)
		throws CatalogException;

	/**
	 * Create a CatalogBaseTable from a Hive table.
	 *
	 * @param hiveTable a Hive table
	 * @return a CatalogBaseTable
	 */
	protected abstract CatalogBaseTable createCatalogBaseTable(Table hiveTable);

	/**
	 * Create a Hive table from a CatalogBaseTable.
	 *
	 * @param tablePath path of the table
	 * @param table a CatalogBaseTable
	 * @return a Hive table
	 */
	protected abstract Table createHiveTable(ObjectPath tablePath, CatalogBaseTable table);

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

	@Override
	public String getDefaultDatabase() throws CatalogException {
		return defaultDatabase;
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
	public void dropDatabase(String name, boolean ignoreIfNotExists) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
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

	protected Database getHiveDatabase(String databaseName) throws DatabaseNotExistException {
		try {
			return client.getDatabase(databaseName);
		} catch (NoSuchObjectException e) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to get database %s from %s", databaseName, catalogName), e);
		}
	}

	protected void createHiveDatabase(Database hiveDatabase, boolean ignoreIfExists)
			throws DatabaseAlreadyExistException, CatalogException {
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

	protected void alterHiveDatabase(String name, Database newHiveDatabase, boolean ignoreIfNotExists)
			throws DatabaseNotExistException, CatalogException {
		try {
			if (databaseExists(name)) {
				client.alterDatabase(name, newHiveDatabase);
			} else if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(catalogName, name);
			}
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to alter database %s", name), e);
		}
	}

	// ------ tables ------

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath)
			throws TableNotExistException, CatalogException {
		return createCatalogBaseTable(getHiveTable(tablePath));
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
			throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
		validateCatalogBaseTable(table);

		if (!databaseExists(tablePath.getDatabaseName())) {
			throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
		} else {
			try {
				client.createTable(createHiveTable(tablePath, table));
			} catch (AlreadyExistsException e) {
				if (!ignoreIfExists) {
					throw new TableAlreadyExistException(catalogName, tablePath);
				}
			} catch (TException e) {
				throw new CatalogException(String.format("Failed to create table %s", tablePath.getFullName()), e);
			}
		}
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
			throws TableNotExistException, TableAlreadyExistException, CatalogException {
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
		validateCatalogBaseTable(newCatalogTable);

		try {
			if (!tableExists(tablePath)) {
				if (!ignoreIfNotExists) {
					throw new TableNotExistException(catalogName, tablePath);
				}
			} else {
				// TODO: [FLINK-12452] alterTable() in all catalogs should ensure existing base table and the new one are of the same type
				Table newTable = createHiveTable(tablePath, newCatalogTable);

				// client.alter_table() requires a valid location
				// thus, if new table doesn't have that, it reuses location of the old table
				if (!newTable.getSd().isSetLocation()) {
					Table oldTable = getHiveTable(tablePath);
					newTable.getSd().setLocation(oldTable.getSd().getLocation());
				}

				client.alter_table(tablePath.getDatabaseName(), tablePath.getObjectName(), newTable);
			}
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to rename table %s", tablePath.getFullName()), e);
		}
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException {
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
	public List<String> listTables(String databaseName)
			throws DatabaseNotExistException, CatalogException {
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
}
