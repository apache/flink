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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A catalog implementation for Hive.
 */
public class HiveCatalog extends HiveCatalogBase {
	private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

	public HiveCatalog(String catalogName, String hivemetastoreURI) {
		super(catalogName, hivemetastoreURI);

		LOG.info("Created HiveCatalog '{}'", catalogName);
	}

	public HiveCatalog(String catalogName, HiveConf hiveConf) {
		super(catalogName, hiveConf);

		LOG.info("Created HiveCatalog '{}'", catalogName);
	}

	// ------ databases ------

	@Override
	public CatalogDatabase getDatabase(String databaseName)
			throws DatabaseNotExistException, CatalogException {
		Database hiveDb;

		try {
			hiveDb = client.getDatabase(databaseName);
		} catch (NoSuchObjectException e) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		} catch (TException e) {
			throw new CatalogException(
				String.format("Failed to get database %s from %s", databaseName, catalogName), e);
		}

		return new HiveCatalogDatabase(
			hiveDb.getParameters(), hiveDb.getLocationUri(), hiveDb.getDescription());
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
			throws DatabaseAlreadyExistException, CatalogException {
		try {
			client.createDatabase(HiveCatalogUtil.createHiveDatabase(name, database));
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new DatabaseAlreadyExistException(catalogName, name);
			}
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to create database %s", name), e);
		}
	}

	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
		try {
			if (databaseExists(name)) {
				client.alterDatabase(name, HiveCatalogUtil.createHiveDatabase(name, newDatabase));
			} else if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(catalogName, name);
			}
		} catch (TException e) {
			throw new CatalogException(String.format("Failed to alter database %s", name), e);
		}
	}

	// ------ tables and views------

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
			throws TableNotExistException, TableAlreadyExistException, DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
			throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTable(ObjectPath tableName, CatalogBaseTable newTable, boolean ignoreIfNotExists)
			throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> listTables(String databaseName)
			throws DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath objectPath) throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean tableExists(ObjectPath objectPath) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	// ------ partitions ------

	@Override
	public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists)
			throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
			throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists)
			throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
			throws TableNotExistException, TableNotPartitionedException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
			throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionNotExistException, CatalogException {
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
