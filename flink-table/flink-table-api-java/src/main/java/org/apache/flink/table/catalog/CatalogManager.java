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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A CatalogManager that encapsulates all available catalogs. It also implements the logic of
 * table path resolution.
 */
@Internal
public class CatalogManager {
	private static final Logger LOG = LoggerFactory.getLogger(CatalogManager.class);

	// A map between names and catalogs.
	private Map<String, Catalog> catalogs;

	// The name of the current catalog and database
	private String currentCatalogName;

	private String currentDatabaseName;

	// The name of the built-in catalog
	private final String builtInCatalogName;

	public CatalogManager(String defaultCatalogName, Catalog defaultCatalog) {
		checkArgument(
			!StringUtils.isNullOrWhitespaceOnly(defaultCatalogName),
			"Default catalog name cannot be null or empty");
		checkNotNull(defaultCatalog, "Default catalog cannot be null");
		catalogs = new LinkedHashMap<>();
		catalogs.put(defaultCatalogName, defaultCatalog);
		this.currentCatalogName = defaultCatalogName;
		this.currentDatabaseName = defaultCatalog.getDefaultDatabase();

		// right now the default catalog is always the built-in one
		this.builtInCatalogName = defaultCatalogName;
	}

	/**
	 * Registers a catalog under the given name. The catalog name must be unique.
	 *
	 * @param catalogName name under which to register the given catalog
	 * @param catalog catalog to register
	 * @throws CatalogException if the registration of the catalog under the given name failed
	 */
	public void registerCatalog(String catalogName, Catalog catalog) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "Catalog name cannot be null or empty.");
		checkNotNull(catalog, "Catalog cannot be null");

		if (catalogs.containsKey(catalogName)) {
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
	 */
	public Optional<Catalog> getCatalog(String catalogName) {
		return Optional.ofNullable(catalogs.get(catalogName));
	}

	/**
	 * Retrieves names of all registered catalogs.
	 *
	 * @return a set of names of registered catalogs
	 */
	public Set<String> getCatalogs() {
		return catalogs.keySet();
	}

	/**
	 * Gets the current catalog that will be used when resolving table path.
	 *
	 * @return the current catalog
	 * @see CatalogManager#qualifyIdentifier(String...)
	 */
	public String getCurrentCatalog() {
		return currentCatalogName;
	}

	/**
	 * Sets the current catalog name that will be used when resolving table path.
	 *
	 * @param catalogName catalog name to set as current catalog
	 * @throws CatalogNotExistException thrown if the catalog doesn't exist
	 * @see CatalogManager#qualifyIdentifier(String...)
	 */
	public void setCurrentCatalog(String catalogName) throws CatalogNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "Catalog name cannot be null or empty.");

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
	 * Gets the current database name that will be used when resolving table path.
	 *
	 * @return the current database
	 * @see CatalogManager#qualifyIdentifier(String...)
	 */
	public String getCurrentDatabase() {
		return currentDatabaseName;
	}

	/**
	 * Sets the current database name that will be used when resolving a table path.
	 * The database has to exist in the current catalog.
	 *
	 * @param databaseName database name to set as current database name
	 * @throws CatalogException thrown if the database doesn't exist in the current catalog
	 * @see CatalogManager#qualifyIdentifier(String...)
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
				currentDatabaseName,
				currentCatalogName);
		}
	}

	/**
	 * Gets the built-in catalog name. The built-in catalog is used for storing all non-serializable
	 * transient meta-objects.
	 *
	 * @return the built-in catalog name
	 */
	public String getBuiltInCatalogName() {
		return builtInCatalogName;
	}

	/**
	 * Gets the built-in database name in the built-in catalog. The built-in database is used for storing
	 * all non-serializable transient meta-objects.
	 *
	 * @return the built-in database name
	 */
	public String getBuiltInDatabaseName() {
		// The default database of the built-in catalog is also the built-in database.
		return catalogs.get(getBuiltInCatalogName()).getDefaultDatabase();
	}

	/**
	 * Retrieves a fully qualified table. If the path is not yet fully qualified use
	 * {@link #qualifyIdentifier(String...)} first.
	 *
	 * @param objectIdentifier full path of the table to retrieve
	 * @return table that the path points to.
	 */
	public Optional<CatalogBaseTable> getTable(ObjectIdentifier objectIdentifier) {
		try {
			Catalog currentCatalog = catalogs.get(objectIdentifier.getCatalogName());
			ObjectPath objectPath = new ObjectPath(
				objectIdentifier.getDatabaseName(),
				objectIdentifier.getObjectName());

			if (currentCatalog != null && currentCatalog.tableExists(objectPath)) {
				return Optional.of(currentCatalog.getTable(objectPath));
			}
		} catch (TableNotExistException ignored) {
		}
		return Optional.empty();
	}

	/**
	 * Returns the full name of the given table path, this name may be padded
	 * with current catalog/database name based on the {@code paths} length.
	 *
	 * @param path Table path whose format can be "catalog.db.table", "db.table" or "table"
	 * @return An array of complete table path
	 */
	public ObjectIdentifier qualifyIdentifier(String... path) {
		if (path == null) {
			throw new ValidationException("Table paths can not be null!");
		}
		if (path.length < 1 || path.length > 3) {
			throw new ValidationException("Table paths length must be " +
				"between 1(inclusive) and 3(inclusive)");
		}
		if (Arrays.stream(path).anyMatch(StringUtils::isNullOrWhitespaceOnly)) {
			throw new ValidationException("Table paths contain null or " +
				"while-space-only string");
		}

		String catalogName;
		String dbName;
		String tableName;
		if (path.length == 3) {
			catalogName = path[0];
			dbName = path[1];
			tableName = path[2];
		} else if (path.length == 2) {
			catalogName = getCurrentCatalog();
			dbName = path[0];
			tableName = path[1];
		} else {
			catalogName = getCurrentCatalog();
			dbName = getCurrentDatabase();
			tableName = path[0];
		}

		return ObjectIdentifier.of(catalogName, dbName, tableName);
	}

	/**
	 * Creates a table in a given fully qualified path.
	 *
	 * @param table The table to put in the given path.
	 * @param objectIdentifier The fully qualified path where to put the table.
	 * @param ignoreIfExists If false exception will be thrown if a table exists in the given path.
	 */
	public void createTable(CatalogBaseTable table, ObjectIdentifier objectIdentifier, boolean ignoreIfExists) {
		execute(
			(catalog, path) -> catalog.createTable(path, table, ignoreIfExists),
			objectIdentifier,
			false,
			"CreateTable");
	}

	/**
	 * Alters a table in a given fully qualified path.
	 *
	 * @param table The table to put in the given path
	 * @param objectIdentifier The fully qualified path where to alter the table.
	 * @param ignoreIfNotExists If false exception will be thrown if the table or database or catalog to be altered
	 * does not exist.
	 */
	public void alterTable(CatalogBaseTable table, ObjectIdentifier objectIdentifier, boolean ignoreIfNotExists) {
		execute(
			(catalog, path) -> catalog.alterTable(path, table, ignoreIfNotExists),
			objectIdentifier,
			ignoreIfNotExists,
			"AlterTable");
	}

	/**
	 * Drops a table in a given fully qualified path.
	 *
	 * @param objectIdentifier The fully qualified path of the table to drop.
	 * @param ignoreIfNotExists If false exception will be thrown if the table or database or catalog to be altered
	 * does not exist.
	 */
	public void dropTable(ObjectIdentifier objectIdentifier, boolean ignoreIfNotExists) {
		execute(
			(catalog, path) -> catalog.dropTable(path, ignoreIfNotExists),
			objectIdentifier,
			ignoreIfNotExists,
			"DropTable");
	}

	/**
	 * A command that modifies given {@link Catalog} in an {@link ObjectPath}. This unifies error handling
	 * across different commands.
	 */
	private interface ModifyCatalog {
		void execute(Catalog catalog, ObjectPath path) throws Exception;
	}

	private void execute(
			ModifyCatalog command,
			ObjectIdentifier objectIdentifier,
			boolean ignoreNoCatalog,
			String commandName) {
		Optional<Catalog> catalog = getCatalog(objectIdentifier.getCatalogName());
		if (catalog.isPresent()) {
			try {
				command.execute(catalog.get(), objectIdentifier.toObjectPath());
			} catch (TableAlreadyExistException | TableNotExistException | DatabaseNotExistException e) {
				throw new ValidationException(getErrorMessage(objectIdentifier, commandName), e);
			} catch (Exception e) {
				throw new TableException(getErrorMessage(objectIdentifier, commandName), e);
			}
		} else if (!ignoreNoCatalog) {
			throw new ValidationException(String.format(
				"Catalog %s does not exist.",
				objectIdentifier.getCatalogName()));
		}
	}

	private String getErrorMessage(ObjectIdentifier objectIdentifier, String commandName) {
		return String.format("Could not execute %s in path %s", commandName, objectIdentifier);
	}
}
