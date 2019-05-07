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

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.GenericCatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.hive.util.GenericHiveMetastoreCatalogUtil;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A catalog that persists all Flink streaming and batch metadata by using Hive metastore as a persistent storage.
 */
public class GenericHiveMetastoreCatalog extends HiveCatalogBase {
	private static final Logger LOG = LoggerFactory.getLogger(GenericHiveMetastoreCatalog.class);

	public GenericHiveMetastoreCatalog(String catalogName, String hivemetastoreURI) {
		super(catalogName, hivemetastoreURI);

		LOG.info("Created GenericHiveMetastoreCatalog '{}'", catalogName);
	}

	public GenericHiveMetastoreCatalog(String catalogName, HiveConf hiveConf) {
		super(catalogName, hiveConf);

		LOG.info("Created GenericHiveMetastoreCatalog '{}'", catalogName);
	}

	// ------ databases ------

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		Database hiveDb = getHiveDatabase(databaseName);

		return new GenericCatalogDatabase(hiveDb.getParameters(), hiveDb.getDescription());
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
			throws DatabaseAlreadyExistException, CatalogException {
		createHiveDatabase(GenericHiveMetastoreCatalogUtil.createHiveDatabase(name, database), ignoreIfExists);
	}

	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
			throws DatabaseNotExistException, CatalogException {
		alterHiveDatabase(name, GenericHiveMetastoreCatalogUtil.createHiveDatabase(name, newDatabase), ignoreIfNotExists);
	}

	// ------ tables and views------

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
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
			throws TableNotExistException, TableAlreadyExistException, CatalogException {
		try {
			// alter_table() doesn't throw a clear exception when target table doesn't exist. Thus, check the table existence explicitly
			if (tableExists(tablePath)) {
				ObjectPath newPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
				// alter_table() doesn't throw a clear exception when new table already exists. Thus, check the table existence explicitly
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
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
			throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
		if (!databaseExists(tablePath.getDatabaseName())) {
			throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
		} else {
			try {
				client.createTable(GenericHiveMetastoreCatalogUtil.createHiveTable(tablePath, table));
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
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException {
		if (!tableExists(tablePath)) {
			if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, tablePath);
			}
		} else {
			// IMetastoreClient.alter_table() requires the table to have a valid location, which it doesn't in this case
			// Thus we have to translate alterTable() into (dropTable() + createTable())
			dropTable(tablePath, false);
			try {
				createTable(tablePath, newTable, false);
			} catch (TableAlreadyExistException | DatabaseNotExistException e) {
				// These exceptions wouldn't be thrown, unless a concurrent operation is triggered in Hive
				throw new CatalogException(
					String.format("Failed to alter table %s", tablePath), e);
			}
		}
	}

	@Override
	public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
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
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		Table hiveTable = getHiveTable(tablePath);

		return GenericHiveMetastoreCatalogUtil.createCatalogTable(hiveTable);
	}

	protected Table getHiveTable(ObjectPath tablePath) throws TableNotExistException {
		try {
			return client.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
		} catch (NoSuchObjectException e) {
			throw new TableNotExistException(catalogName, tablePath);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to get table %s from Hive metastore", tablePath.getFullName()), e);
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
}
