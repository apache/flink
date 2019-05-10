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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.operations.CatalogTableOperation;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A CatalogManager that encapsulates all available catalogs. It also implements the logic of
 * table path resolution. Supports both new API ({@link Catalog} as well as {@link ExternalCatalog}).
 */
@Internal
public class CatalogManager {
	private static final Logger LOG = LoggerFactory.getLogger(CatalogManager.class);

	// A map between names and catalogs.
	private Map<String, Catalog> catalogs;

	// TO BE REMOVED along with ExternalCatalog API
	private Map<String, ExternalCatalog>  externalCatalogs;

	// The name of the default catalog and schema
	private String currentCatalogName;

	private String currentDatabaseName;

	public CatalogManager(String defaultCatalogName, Catalog defaultCatalog) {
		checkArgument(
			!StringUtils.isNullOrWhitespaceOnly(defaultCatalogName),
			"Default catalog name cannot be null or empty");
		checkNotNull(defaultCatalog, "Default catalog cannot be null");
		catalogs = new LinkedHashMap<>();
		externalCatalogs = new LinkedHashMap<>();
		catalogs.put(defaultCatalogName, defaultCatalog);
		this.currentCatalogName = defaultCatalogName;
		this.currentDatabaseName = defaultCatalog.getDefaultDatabase();
	}

	/**
	 * Registers a catalog under the given name. The catalog name must be unique across both
	 * {@link Catalog}s and {@link ExternalCatalog}s.
	 *
	 * @param catalogName name under which to register the given catalog
	 * @param catalog catalog to register
	 * @throws CatalogException if the registration of the catalog under the given name failed
	 */
	public void registerCatalog(String catalogName, Catalog catalog) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "Catalog name cannot be null or empty.");
		checkNotNull(catalog, "Catalog cannot be null");

		if (catalogs.containsKey(catalogName) || externalCatalogs.containsKey(catalogName)) {
			throw new CatalogException(format("Catalog %s already exists.", catalogName));
		}

		catalogs.put(catalogName, catalog);
		catalog.open();
	}

	/**
	 * Gets a catalog by name.
	 *
	 * @param catalogName name of the catalog to retrieve
	 * @return the requested catalog or empty if it does not exist
	 * @see CatalogManager#getExternalCatalog(String)
	 */
	public Optional<Catalog> getCatalog(String catalogName) {
		return Optional.ofNullable(catalogs.get(catalogName));
	}

	/**
	 * Registers an external catalog under the given name. The catalog name must be unique across both
	 * {@link Catalog}s and {@link ExternalCatalog}s.
	 *
	 * @param catalogName name under which to register the given catalog
	 * @param catalog catalog to register
	 * @throws CatalogException thrown if the name is already taken
	 * @deprecated {@link ExternalCatalog} APIs will be dropped
	 */
	@Deprecated
	public void registerExternalCatalog(String catalogName, ExternalCatalog catalog) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "The catalog name cannot be null or empty.");
		checkNotNull(catalog, "The catalog cannot be null.");

		if (externalCatalogs.containsKey(catalogName) || catalogs.containsKey(catalogName)) {
			throw new CatalogException(format("An external catalog named [%s] already exists.", catalogName));
		}

		externalCatalogs.put(catalogName, catalog);
	}

	/**
	 * Gets an external catalog by name.
	 *
	 * @param externalCatalogName name of the catalog to retrieve
	 * @return the requested external catalog or empty if it does not exist
	 * @see CatalogManager#getCatalog(String)
	 * @deprecated {@link ExternalCatalog} APIs will be dropped
	 */
	@Deprecated
	public Optional<ExternalCatalog> getExternalCatalog(String externalCatalogName) {
		return Optional.ofNullable(externalCatalogs.get(externalCatalogName));
	}

	/**
	 * Retrieves names of all registered catalogs. It does not include {@link ExternalCatalog}s.
	 *
	 * @return a set of names of registered catalogs
	 * @see CatalogManager#getExternalCatalogs()
	 */
	public Set<String> getCatalogs() {
		return catalogs.keySet();
	}

	/**
	 * Retrieves names of all registered external catalogs. It does not include {@link Catalog}s.
	 *
	 * @return a set of names of registered catalogs
	 * @see CatalogManager#getCatalogs()
	 * @deprecated {@link ExternalCatalog} APIs will be dropped
	 */
	@Deprecated
	public Set<String> getExternalCatalogs() {
		return externalCatalogs.keySet();
	}

	/**
	 * Gets the current default catalog that will be used when resolving table path.
	 *
	 * @return the current default catalog
	 * @see CatalogManager#resolveTable(String...)
	 */
	public String getCurrentCatalog() {
		return currentCatalogName;
	}

	/**
	 * Sets the current default catalog name that will be used when resolving table path.
	 *
	 * @param catalogName catalog name to set as current default catalog
	 * @throws CatalogNotExistException thrown if the catalog doesn't exist
	 * @see CatalogManager#resolveTable(String...)
	 */
	public void setCurrentCatalog(String catalogName) throws CatalogNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "Catalog name cannot be null or empty.");

		if (externalCatalogs.containsKey(catalogName)) {
			throw new CatalogException("An external catalog cannot be set as the default one.");
		}

		Catalog potentialCurrentCatalog = catalogs.get(catalogName);
		if (potentialCurrentCatalog == null) {
			throw new CatalogException(format("A catalog with name [%s] does not exist.", catalogName));
		}

		if (!currentCatalogName.equals(catalogName)) {
			currentCatalogName = catalogName;
			currentDatabaseName = potentialCurrentCatalog.getDefaultDatabase();

			LOG.info(
				"Set the current default catalog as [{}] and the current default database as [{}].",
				currentCatalogName,
				currentDatabaseName);
		}
	}

	/**
	 * Gets the current default database name that will be used when resolving table path.
	 *
	 * @return the current default database
	 * @see CatalogManager#resolveTable(String...)
	 */
	public String getCurrentDatabase() {
		return currentDatabaseName;
	}

	/**
	 * Sets the current default database name that will be used when resolving a table path.
	 * The database has to exist in the current catalog.
	 *
	 * @param databaseName database name to set as current default database name
	 * @throws CatalogException thrown if the database doesn't exist in the current catalog
	 * @see CatalogManager#resolveTable(String...)
	 * @see CatalogManager#setCurrentCatalog(String)
	 */
	public void setCurrentDatabase(String databaseName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "The database name cannot be null or empty.");

		if (!catalogs.get(currentCatalogName).databaseExists(databaseName)) {
			throw new CatalogException(format(
				"A database with name [%s] does not exist in the catalog: [%s].",
				databaseName,
				currentCatalogName));
		}

		if (!currentDatabaseName.equals(databaseName)) {
			currentDatabaseName = databaseName;

			LOG.info(
				"Set the current default database as [{}] in the current default catalog [{}].",
				currentCatalogName,
				currentDatabaseName);
		}
	}

	/**
	 * Tries to resolve a table path to a {@link CatalogTableOperation}. The algorithm looks for requested table
	 * in following paths in that order:
	 * <ol>
	 *     <li>{@code [current-catalog].[current-database].[tablePath]}</li>
	 *     <li>{@code [current-catalog].[tablePath]}</li>
	 *     <li>{@code [tablePath]}</li>
	 * </ol>
	 *
	 * @param tablePath table path to look for
	 * @return {@link CatalogTableOperation} containing both fully qualified table identifier and its
	 * {@link TableSchema}.
	 */
	public Optional<CatalogTableOperation> resolveTable(String... tablePath) {
		checkArgument(tablePath != null && tablePath.length != 0, "Table path must not be null or empty.");

		List<String> userPath = asList(tablePath);

		List<List<String>> prefixes = asList(
			asList(currentCatalogName, currentDatabaseName),
			singletonList(currentCatalogName),
			emptyList()
		);

		for (List<String> prefix : prefixes) {
			Optional<CatalogTableOperation> potentialTable = lookupPath(prefix, userPath);
			if (potentialTable.isPresent()) {
				return potentialTable;
			}
		}

		return Optional.empty();
	}

	private Optional<CatalogTableOperation> lookupPath(List<String> prefix, List<String> userPath) {
		try {
			List<String> path = new ArrayList<>(prefix);
			path.addAll(userPath);

			Optional<CatalogTableOperation> potentialTable = lookupCatalogTable(path);

			if (!potentialTable.isPresent()) {
				potentialTable = lookupExternalTable(path);
			}
			return potentialTable;
		} catch (TableNotExistException e) {
			return Optional.empty();
		}
	}

	private Optional<CatalogTableOperation> lookupCatalogTable(List<String> path) throws TableNotExistException {
		if (path.size() == 3) {
			Catalog currentCatalog = catalogs.get(path.get(0));
			String currentDatabaseName = path.get(1);
			String tableName = String.join(".", path.subList(2, path.size()));
			ObjectPath objectPath = new ObjectPath(currentDatabaseName, tableName);

			if (currentCatalog != null && currentCatalog.tableExists(objectPath)) {
				TableSchema tableSchema = currentCatalog.getTable(objectPath).getSchema();
				return Optional.of(new CatalogTableOperation(
					asList(path.get(0), currentDatabaseName, tableName),
					tableSchema));
			}
		}

		return Optional.empty();
	}

	private Optional<CatalogTableOperation> lookupExternalTable(List<String> path) {
		ExternalCatalog currentCatalog = externalCatalogs.get(path.get(0));
		return Optional.ofNullable(currentCatalog)
			.flatMap(externalCatalog -> extractPath(externalCatalog, path.subList(1, path.size() - 1)))
			.map(finalCatalog -> finalCatalog.getTable(path.get(path.size() - 1)))
			.map(table -> new CatalogTableOperation(path, getTableSchema(table)));
	}

	private Optional<ExternalCatalog> extractPath(ExternalCatalog rootExternalCatalog, List<String> path) {
		ExternalCatalog schema = rootExternalCatalog;
		for (String pathPart : path) {
			schema = schema.getSubCatalog(pathPart);
			if (schema == null) {
				return Optional.empty();
			}
		}
		return Optional.of(schema);
	}

	private static TableSchema getTableSchema(ExternalCatalogTable externalTable) {
		if (externalTable.isTableSource()) {
			return TableFactoryUtil.findAndCreateTableSource(externalTable).getTableSchema();
		} else {
			TableSink tableSink = TableFactoryUtil.findAndCreateTableSink(externalTable);
			return new TableSchema(tableSink.getFieldNames(), tableSink.getFieldTypes());
		}
	}
}
