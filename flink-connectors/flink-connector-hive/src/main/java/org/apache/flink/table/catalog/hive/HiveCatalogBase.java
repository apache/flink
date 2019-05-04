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

import org.apache.flink.table.catalog.ReadableWritableCatalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for catalogs backed by Hive metastore.
 */
public abstract class HiveCatalogBase implements ReadableWritableCatalog {
	private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogBase.class);

	public static final String DEFAULT_DB = "default";

	protected final String catalogName;
	protected final HiveConf hiveConf;

	protected String currentDatabase = DEFAULT_DB;
	protected IMetaStoreClient client;

	public HiveCatalogBase(String catalogName, String hivemetastoreURI) {
		this(catalogName, getHiveConf(hivemetastoreURI));
	}

	public HiveCatalogBase(String catalogName, HiveConf hiveConf) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		this.catalogName = catalogName;

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

	@Override
	public void open() throws CatalogException {
		if (client == null) {
			client = getMetastoreClient(hiveConf);
			LOG.info("Connected to Hive metastore");
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
	public String getCurrentDatabase() throws CatalogException {
		return currentDatabase;
	}

	@Override
	public void setCurrentDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(catalogName, databaseName);
		}

		currentDatabase = databaseName;
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
}
