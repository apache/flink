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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

/**
 * Utility classes to construct a {@link CatalogManager} with a given structure.
 * It does create tables ({@link TestTable} and {@link ExternalTestTable} which
 * {@link Object#equals(Object)} method compares the fully qualified paths.
 *
 * <p>Example:
 * <pre>{@code
 * root()
 *  .builtin(
 *      database(
 *          "default",
 *          table("tab1"),
 *          table("tab2")
 *      )
 *  )
 *  .externalCatalog(
 *      "extCat1",
 *      table("tab1"),
 *      extCatalog(
 *          "extCat2",
 *          extCatalog("extCat3",
 *              table("tab1")
 *          ),
 *      table("tab1")
 *  )
 * ).build();
 * }</pre>
 */
public class CatalogStructureBuilder {

	public static final String BUILTIN_CATALOG_NAME = "builtin";
	private CatalogManager catalogManager = new CatalogManager(
		BUILTIN_CATALOG_NAME,
		new GenericInMemoryCatalog(BUILTIN_CATALOG_NAME));

	public static CatalogStructureBuilder root() {
		return new CatalogStructureBuilder();
	}

	public static DatabaseBuilder database(String name, TableBuilder... tables) {
		return new DatabaseBuilder(name, tables);
	}

	public static TableBuilder table(String name) {
		return new TableBuilder(name);
	}

	public static ExternalCatalogEntry extCatalog(String name, ExternalCatalogEntry... entries) {
		return new ExternalCatalogBuilder(name, entries);
	}

	public CatalogStructureBuilder builtin(DatabaseBuilder defaultDb, DatabaseBuilder... databases) throws Exception {
		GenericInMemoryCatalog catalog = buildCatalog(BUILTIN_CATALOG_NAME, defaultDb, databases);
		this.catalogManager = new CatalogManager(BUILTIN_CATALOG_NAME, catalog);

		return this;
	}

	public CatalogStructureBuilder catalog(
			String name,
			DatabaseBuilder defaultDatabase,
			DatabaseBuilder... databases) throws Exception {

		GenericInMemoryCatalog catalog = buildCatalog(name, defaultDatabase, databases);
		catalogManager.registerCatalog(name, catalog);

		return this;
	}

	private GenericInMemoryCatalog buildCatalog(
			String name,
			DatabaseBuilder defaultDatabase,
			DatabaseBuilder[] databases) throws Exception {
		GenericInMemoryCatalog catalog = new GenericInMemoryCatalog(
			name,
			defaultDatabase.getName(),
			new GenericCatalogDatabase(new HashMap<>()));
		defaultDatabase.build(catalog, name);
		registerDatabases(name, catalog, databases);
		return catalog;
	}

	private void registerDatabases(
			String name,
			Catalog catalog,
			DatabaseBuilder[] databases) throws Exception {
		for (DatabaseBuilder database : databases) {
			catalog.createDatabase(database.getName(), new GenericCatalogDatabase(new HashMap<>()), false);
			database.build(catalog, name);
		}
	}

	public CatalogStructureBuilder externalCatalog(String name, ExternalCatalogEntry... entries) throws Exception {
		new ExternalCatalogBuilder(name, entries).build(catalogManager);
		return this;
	}

	public CatalogManager build() {
		return catalogManager;
	}

	/**
	 * Helper class for creating mock {@link ExternalCatalog} in a {@link CatalogStructureBuilder}.
	 */
	public static class ExternalCatalogBuilder implements ExternalCatalogEntry {

		private final String name;
		private final ExternalCatalogEntry[] entries;

		private ExternalCatalogBuilder(String name, ExternalCatalogEntry[] entries) {
			this.entries = entries;
			this.name = name;
		}

		public void build(CrudExternalCatalog catalog, String path) throws Exception {
			catalog.createSubCatalog(name, buildCurrentCatalog(path), false);
		}

		private InMemoryExternalCatalog buildCurrentCatalog(String path) throws Exception {
			InMemoryExternalCatalog thisCatalog = new InMemoryExternalCatalog(name);
			final String currentPath;
			if (path != null) {
				currentPath = path + "." + name;
			} else {
				currentPath = name;
			}
			for (ExternalCatalogEntry entry : entries) {
				if (entry instanceof ExternalCatalogBuilder) {
					((ExternalCatalogBuilder) entry).build(thisCatalog, currentPath);
				} else if (entry instanceof TableBuilder){
					TableBuilder tableBuilder = (TableBuilder) entry;
					thisCatalog.createTable(
						tableBuilder.getName(),
						tableBuilder.buildExternalTable(currentPath),
						false);
				}
			}
			return thisCatalog;
		}

		public void build(CatalogManager catalogManager) throws Exception {
			catalogManager.registerExternalCatalog(name, buildCurrentCatalog(null));
		}
	}

	/**
	 * Helper class for creating mock {@link CatalogDatabase} in a {@link CatalogStructureBuilder}.
	 */
	public static class DatabaseBuilder {
		private final TableBuilder[] tables;
		private final String name;

		public DatabaseBuilder(String name, TableBuilder[] tables) {
			this.tables = tables;
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void build(Catalog catalog, String catalogName) throws Exception {
			for (TableBuilder tableBuilder : tables) {
				catalog.createTable(
					new ObjectPath(name, tableBuilder.getName()),
					tableBuilder.build(catalogName + "." + name),
					false);
			}
		}
	}

	/**
	 * Helper class for creating mock {@link CatalogTable} & {@link ExternalCatalogTable}
	 * in a {@link CatalogStructureBuilder}.
	 */
	public static class TableBuilder implements ExternalCatalogEntry {
		private final String name;

		TableBuilder(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public TestTable build(String path) {
			return new TestTable(path + "." + name);
		}

		public ExternalTestTable buildExternalTable(String path) {
			return new ExternalTestTable(path + "." + name);
		}
	}

	/**
	 * Marker interface to make {@link ExternalCatalogBuilder#extCatalog(String, ExternalCatalogEntry...)}
	 * accept both {@link ExternalCatalogBuilder} and {@link TableBuilder}.
	 */
	public interface ExternalCatalogEntry {
	}

	private static class ExternalTestTable extends ExternalCatalogTable {
		private final String fullyQualifiedName;

		public ExternalTestTable(String fullyQualifiedName) {
			super(false, true, true, true, new HashMap<>());
			this.fullyQualifiedName = fullyQualifiedName;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ExternalTestTable that = (ExternalTestTable) o;
			return Objects.equals(fullyQualifiedName, that.fullyQualifiedName);
		}

		@Override
		public Map<String, String> toProperties() {
			Map<String, String> properties = new HashMap<>();
			properties.put(CONNECTOR_TYPE, TestExternalTableSourceFactory.TEST_EXTERNAL_CONNECTOR_TYPE);
			return properties;
		}

		@Override
		public int hashCode() {
			return Objects.hash(fullyQualifiedName);
		}
	}

	private static class TestTable implements CatalogBaseTable {

		private final String fullyQualifiedPath;

		private TestTable(String fullyQualifiedPath) {
			this.fullyQualifiedPath = fullyQualifiedPath;
		}

		@Override
		public Map<String, String> getProperties() {
			return null;
		}

		@Override
		public TableSchema getSchema() {
			return new TableSchema(new String[] {}, new TypeInformation[] {});
		}

		@Override
		public String getComment() {
			return null;
		}

		@Override
		public CatalogBaseTable copy() {
			return this;
		}

		@Override
		public Optional<String> getDescription() {
			return Optional.empty();
		}

		@Override
		public Optional<String> getDetailedDescription() {
			return Optional.empty();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			TestTable testTable = (TestTable) o;
			return Objects.equals(fullyQualifiedPath, testTable.fullyQualifiedPath);
		}

		@Override
		public int hashCode() {
			return Objects.hash(fullyQualifiedPath);
		}
	}
}
