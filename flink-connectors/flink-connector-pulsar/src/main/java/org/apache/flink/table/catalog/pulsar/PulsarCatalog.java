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

package org.apache.flink.table.catalog.pulsar;

import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
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
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.factories.TableFactory;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PulsarCatalog extends AbstractCatalog {

	private static final Logger LOG = LoggerFactory.getLogger(PulsarCatalog.class);

	private String adminUrl;
	private Map<String, String> properties;

	private PulsarMetadataReader metadataReader;

	public PulsarCatalog(String adminUrl, String catalogName, Map<String, String> properties, String defaultDatabase) {
		super(catalogName, defaultDatabase);

		this.adminUrl = adminUrl;
		this.properties = properties;

		LOG.info("Created PulsarCatalog '{}'", catalogName);
	}

	@Override
	public void open() throws CatalogException {
		if (metadataReader == null) {
			try {
				metadataReader = new PulsarMetadataReader(adminUrl);
			} catch (PulsarClientException e) {
				throw new CatalogException("Failed to create Pulsar admin using " + adminUrl, e);
			}
		}
	}

	@Override
	public void close() throws CatalogException {
		if (metadataReader != null) {
			metadataReader.close();
			metadataReader = null;
			LOG.info("Close connection to Pulsar");
		}
	}

	@Override
	public Optional<TableFactory> getTableFactory() {
		// TODO
		return Optional.empty();
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		try {
			return metadataReader.listNamespaces();
		} catch (PulsarAdminException e) {
			throw new CatalogException(String.format("Failed to list all databases in %s", getName()), e);
		}
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		Map<String, String> properties = new HashMap<>();
		return new CatalogDatabaseImpl(properties, databaseName);
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		try {
			return metadataReader.namespaceExists(databaseName);
		} catch (PulsarAdminException e) {
			throw new CatalogException(String.format("Failed to check existence of databases %s", databaseName), e);
		}
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
		try {
			metadataReader.createNamespace(name);
		} catch (PulsarAdminException.ConflictException e) {
			if (!ignoreIfExists) {
				throw new DatabaseAlreadyExistException(getName(), name, e);
			}
		} catch (PulsarAdminException e) {
			throw new CatalogException(String.format("Failed to create database %s", name), e);
		}
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
		try {
			metadataReader.deleteNamespace(name);
		} catch (PulsarAdminException.NotFoundException e) {
			if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(getName(), name);
			}
		} catch (PulsarAdminException e) {
			throw new CatalogException(String.format("Failed to drop database %s", name), e);
		}
	}

	@Override
	public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		try {
			return metadataReader.getTopics(databaseName);
		} catch (PulsarAdminException.NotFoundException e) {
			throw new DatabaseNotExistException(getName(), databaseName, e);
		} catch (PulsarAdminException e) {
			throw new CatalogException(String.format("Failed to list tables in database %s", databaseName), e);
		}
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		try {
			return new CatalogTableImpl(metadataReader.getTableSchema(tablePath), properties, "");
		} catch (PulsarAdminException.NotFoundException e) {
			throw new TableNotExistException(getName(), tablePath, e);
		} catch (PulsarAdminException e) {
			throw new CatalogException(String.format("Failed to get table %s schema", tablePath.getFullName()), e);
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		try {
			return metadataReader.topicExists(tablePath);
		} catch (PulsarAdminException.NotFoundException e) {
			return false;
		} catch (PulsarAdminException e) {
			throw new CatalogException(String.format("Failed to check table %s existence", tablePath.getFullName()), e);
		}
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
		try {
			metadataReader.deleteTopic(tablePath);
		} catch (PulsarAdminException.NotFoundException e) {
			if (!ignoreIfNotExists) {
				throw new TableNotExistException(getName(), tablePath, e);
			}
		} catch (PulsarAdminException e) {
			throw new CatalogException(String.format("Failed to drop table %s", tablePath.getFullName()), e);
		}
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
		int defaultNumPartitions = Integer.parseInt(properties.getOrDefault(PulsarOptions.DEFAULT_PARTITIONS, "5"));
		String databaseName = tablePath.getDatabaseName();
		Boolean databaseExists;
		try {
			databaseExists = metadataReader.namespaceExists(databaseName);
		} catch (PulsarAdminException e) {
			throw new CatalogException(String.format("Failed to check existence of databases %s", databaseName), e);
		}

		if (!databaseExists) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}

		try {
			metadataReader.createTopic(tablePath, defaultNumPartitions, table);
		} catch (PulsarAdminException e) {
			if (e.getStatusCode() == 409) {
				throw new TableAlreadyExistException(getName(), tablePath, e);
			} else {
				throw new CatalogException(String.format("Failed to create table %s", tablePath.getFullName()), e);
			}
		} catch (SchemaUtils.IncompatibleSchemaException e) {
			throw new CatalogException("Failed to translate Flink type to Pulsar", e);
		}
	}

	// ------------------------------------------------------------------------
	// Unsupported catalog operations for Pulsar
	// ------------------------------------------------------------------------

	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
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

	@Override
	public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
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
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}
}
