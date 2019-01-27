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

import org.apache.flink.table.api.CatalogAlreadyExistException;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * CatalogManager manages all the registered ReadableCatalog instances in a table environment.
 * It also has a concept of default catalog, which will be selected when a catalog name isn’t given
 * in a meta-object reference.
 *
 * <p>CatalogManger also encapsulates Calcite’s schema framework such that no code outside CatalogManager
 * needs to interact with Calcite’s schema except the parser which needs all catalogs. (All catalogs will
 * be added to Calcite schema so that all external tables and tables can be resolved by Calcite during
 * query parsing and analysis.)
 */
public class CatalogManager {
	private static final Logger LOG = LoggerFactory.getLogger(CatalogManager.class);

	public static final String BUILTIN_CATALOG_NAME = "builtin";

	// The catalog to hold all registered and translated tables
	// We disable caching here to prevent side effects
	private CalciteSchema internalSchema = CalciteSchema.createRootSchema(true, false);
	private SchemaPlus rootSchema = internalSchema.plus();

	// A list of named catalogs.
	private Map<String, ReadableCatalog> catalogs;

	// The name of the default catalog and schema
	private String defaultCatalogName;
	private String defaultDbName;

	public CatalogManager() {
		LOG.info("Initializing CatalogManager");
		catalogs = new HashMap<>();

		FlinkInMemoryCatalog inMemoryCatalog = new FlinkInMemoryCatalog(BUILTIN_CATALOG_NAME);
		catalogs.put(BUILTIN_CATALOG_NAME, inMemoryCatalog);
		defaultCatalogName = BUILTIN_CATALOG_NAME;
		defaultDbName = inMemoryCatalog.getDefaultDatabaseName();

		CatalogCalciteSchema.registerCatalog(rootSchema, BUILTIN_CATALOG_NAME, inMemoryCatalog);
	}

	public void registerCatalog(String catalogName, ReadableCatalog catalog) throws CatalogAlreadyExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkNotNull(catalog, "catalog cannot be null");

		if (catalogs.containsKey(catalogName)) {
			throw new CatalogAlreadyExistException(catalogName);
		}

		catalogs.put(catalogName, catalog);
		catalog.open();
		CatalogCalciteSchema.registerCatalog(rootSchema, catalogName, catalog);
	}

	public ReadableCatalog getCatalog(String catalogName) throws CatalogNotExistException {
		if (!catalogs.keySet().contains(catalogName)) {
			throw new CatalogNotExistException(catalogName);
		}

		return catalogs.get(catalogName);
	}

	public Set<String> getCatalogs() {
		return catalogs.keySet();
	}

	public void setDefaultCatalog(String catalogName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkArgument(catalogs.keySet().contains(catalogName),
			String.format("Cannot find registered catalog %s", catalogName));

		if (!defaultCatalogName.equals(catalogName)) {
			defaultCatalogName = catalogName;
			defaultDbName = catalogs.get(catalogName).getDefaultDatabaseName();
			LOG.info("Set default catalog as '{}' and default database as '{}'", defaultCatalogName, defaultDbName);
		}
	}

	public ReadableCatalog getDefaultCatalog() {
		return catalogs.get(defaultCatalogName);
	}

	public String getDefaultCatalogName() {
		return defaultCatalogName;
	}

	public void setDefaultDatabase(String catalogName, String dbName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(dbName), "dbName cannot be null or empty");
		checkArgument(catalogs.containsKey(catalogName),
			String.format("Cannot find registered catalog %s", catalogName));
		checkArgument(catalogs.get(catalogName).listDatabases().contains(dbName),
			String.format("Cannot find registered database %s", dbName));

		defaultCatalogName = catalogName;
		defaultDbName = dbName;

		LOG.info("Set default catalog as '{}' and default database as '{}'", defaultCatalogName, defaultDbName);
	}

	public String getDefaultDatabaseName() {
		return defaultDbName;
	}

	public SchemaPlus getRootSchema() {
		return rootSchema;
	}

	/**
	 * Returns the full name of the given table name.
	 *
	 * @param paths Table paths whose format can be among "catalog.db.table", "db.table", or "table"
	 * @return An array of complete table path
	 */
	public String[] resolveTableName(String... paths) {
		return resolveTableName(Arrays.asList(paths));
	}

	/**
	 * Returns the full name of the given table name.
	 *
	 * @param paths Table paths whose format can be among "catalog.db.table", "db.table", or "table"
	 * @return An array of complete table path
	 */
	public String[] resolveTableName(List<String> paths) {
		checkNotNull(paths, "paths cannot be null");
		checkArgument(paths.size() >= 1 && paths.size() <= 3, "paths length has to be between 1 and 3");
		checkArgument(!paths.stream().anyMatch(p -> StringUtils.isNullOrWhitespaceOnly(p)),
			"Paths contains null or while-space-only string");

		if (paths.size() == 3) {
			return new String[] {paths.get(0), paths.get(1), paths.get(2)};
		}

		String catalogName;
		String dbName;
		String tableName;

		if (paths.size() == 1) {
			catalogName = getDefaultCatalogName();
			dbName = getDefaultDatabaseName();
			tableName = paths.get(0);
		} else {
			catalogName = getDefaultCatalogName();
			dbName = paths.get(0);
			tableName = paths.get(1);
		}

		return new String[]{catalogName, dbName, tableName};
	}

	/**
	 * Returns the full name of the given table name.
	 *
	 * @param paths Table paths whose format can be among "catalog.db.table", "db.table", or "table"
	 * @return A string of complete table path
	 */
	public String resolveTableNameAsString(String[] paths) {
		return String.join(".", resolveTableName(paths));
	}

	public List<List<String>> getCalciteReaderDefaultPaths(SchemaPlus defaultSchema) {
		List<List<String>> paths = new ArrayList<>();

		// Add both catalog and catalog.db, if there's a default db, as default schema paths
		paths.add(new ArrayList<>(CalciteSchema.from(defaultSchema).path(getDefaultCatalogName())));

		if (getDefaultDatabaseName() != null && defaultSchema.getSubSchema(getDefaultCatalogName()) != null) {
			paths.add(new ArrayList<>(
				CalciteSchema.from(defaultSchema.getSubSchema(getDefaultCatalogName())).path(getDefaultDatabaseName())));
		}

		return paths;
	}
}
