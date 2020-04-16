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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Utility classes to construct a {@link CatalogManager} with a given structure.
 * It does create tables ({@link TestTable} which {@link Object#equals(Object)} method
 * compares the fully qualified paths.
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
 *  .catalog(
 *      "cat1",
 *      database(
 *          "default",
 *          table("tab1"),
 *          table("tab2")
 *      )
 *  .temporaryTable(ObjectIdentifier.of("cat1", "default", "tab1"))
 *  ).build();
 * }</pre>
 */
public class CatalogStructureBuilder {

	public static final String BUILTIN_CATALOG_NAME = "builtin";
	private CatalogManager catalogManager = CatalogManagerMocks.preparedCatalogManager()
		.defaultCatalog(BUILTIN_CATALOG_NAME, new GenericInMemoryCatalog(BUILTIN_CATALOG_NAME))
		.build();

	public static CatalogStructureBuilder root() {
		return new CatalogStructureBuilder();
	}

	public static DatabaseBuilder database(String name, DatabaseEntryBuilder... tables) {
		return new DatabaseBuilder(name, tables);
	}

	public static TableBuilder table(String name) {
		return new TableBuilder(name);
	}

	public static ViewBuilder view(String name) {
		return new ViewBuilder(name);
	}

	public CatalogStructureBuilder builtin(DatabaseBuilder defaultDb, DatabaseBuilder... databases) throws Exception {
		GenericInMemoryCatalog catalog = buildCatalog(BUILTIN_CATALOG_NAME, defaultDb, databases);
		this.catalogManager = CatalogManagerMocks.preparedCatalogManager()
			.defaultCatalog(BUILTIN_CATALOG_NAME, catalog)
			.build();

		return this;
	}

	public CatalogStructureBuilder temporaryTable(ObjectIdentifier path) {
		this.catalogManager.createTemporaryTable(new TestTable(path.toString(), true), path, false);
		return this;
	}

	public CatalogStructureBuilder temporaryView(ObjectIdentifier path, String query) {
		this.catalogManager.createTemporaryTable(
			new TestView(
				query,
				query,
				TableSchema.builder().build(),
				Collections.emptyMap(),
				"",
				true,
				path.toString()),
			path,
			false);
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
			defaultDatabase.getName());
		defaultDatabase.build(catalog, name);
		registerDatabases(name, catalog, databases);
		return catalog;
	}

	private void registerDatabases(
			String name,
			Catalog catalog,
			DatabaseBuilder[] databases) throws Exception {
		for (DatabaseBuilder database : databases) {
			catalog.createDatabase(database.getName(), new CatalogDatabaseImpl(new HashMap<>(), ""), false);
			database.build(catalog, name);
		}
	}

	public CatalogManager build() {
		return catalogManager;
	}

	/**
	 * Helper class for creating mock {@link CatalogDatabase} in a {@link CatalogStructureBuilder}.
	 */
	public static class DatabaseBuilder {
		private final DatabaseEntryBuilder[] tables;
		private final String name;

		public DatabaseBuilder(String name, DatabaseEntryBuilder[] tables) {
			this.tables = tables;
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void build(Catalog catalog, String catalogName) throws Exception {
			for (DatabaseEntryBuilder tableBuilder : tables) {
				catalog.createTable(
					new ObjectPath(name, tableBuilder.getName()),
					tableBuilder.build(catalogName + "." + name),
					false);
			}
		}
	}

	/**
	 * Common interface for both {@link TableBuilder} & {@link ViewBuilder}.
	 */
	public interface DatabaseEntryBuilder {
		String getName();

		DatabaseEntryBuilder withTableSchema(TableSchema tableSchema);

		CatalogBaseTable build(String path);
	}

	/**
	 * Helper class for creating mock {@link CatalogTable} in a {@link CatalogStructureBuilder}.
	 */
	public static class TableBuilder implements DatabaseEntryBuilder {
		private final String name;
		private TableSchema tableSchema = TableSchema.builder().build();

		TableBuilder(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		@Override
		public TableBuilder withTableSchema(TableSchema tableSchema) {
			this.tableSchema = Objects.requireNonNull(tableSchema);
			return this;
		}

		public TestTable build(String path) {
			return new TestTable(
				path + "." + name,
				tableSchema,
				false);
		}
	}

	/**
	 * Helper class for creating mock {@link CatalogView} in a {@link CatalogStructureBuilder}.
	 */
	public static class ViewBuilder implements DatabaseEntryBuilder {
		private final String name;
		private TableSchema tableSchema = TableSchema.builder().build();
		private String query;

		ViewBuilder(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public ViewBuilder withQuery(String query) {
			this.query = query;
			return this;
		}

		@Override
		public ViewBuilder withTableSchema(TableSchema tableSchema) {
			this.tableSchema = Objects.requireNonNull(tableSchema);
			return this;
		}

		public TestView build(String path) {
			return new TestView(
				query,
				query,
				tableSchema,
				Collections.emptyMap(),
				"",
				true,
				path + "." + name);
		}
	}

	/**
	 * A test {@link CatalogTable}.
	 */
	public static class TestTable extends ConnectorCatalogTable<Row, Row> {
		private final String fullyQualifiedPath;
		private final boolean isTemporary;

		public boolean isTemporary() {
			return isTemporary;
		}

		private TestTable(
				String fullyQualifiedPath,
				TableSchema tableSchema,
				boolean isTemporary) {
			super(new StreamTableSource<Row>() {
				@Override
				public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
					return null;
				}

				@Override
				public DataType getProducedDataType() {
					return tableSchema.toRowDataType();
				}

				@Override
				public TableSchema getTableSchema() {
					throw new UnsupportedOperationException("Should not be called");
				}

				@Override
				public String explainSource() {
					return String.format("isTemporary=[%s]", isTemporary);
				}
			}, null, tableSchema, false);

			this.fullyQualifiedPath = fullyQualifiedPath;
			this.isTemporary = isTemporary;
		}

		private TestTable(String fullyQualifiedPath, boolean isTemporary) {
			this(fullyQualifiedPath, TableSchema.builder().build(), isTemporary);
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
			return Objects.equals(fullyQualifiedPath, testTable.fullyQualifiedPath) &&
				Objects.equals(isTemporary, testTable.isTemporary);
		}

		@Override
		public int hashCode() {
			return Objects.hash(fullyQualifiedPath, isTemporary);
		}
	}

	/**
	 * A test {@link CatalogView}.
	 */
	public static class TestView extends AbstractCatalogView {
		private final boolean isTemporary;
		private final String fullyQualifiedPath;

		public boolean isTemporary() {
			return isTemporary;
		}

		private TestView(
				String originalQuery,
				String expandedQuery,
				TableSchema schema,
				Map<String, String> properties,
				String comment,
				boolean isTemporary,
				String fullyQualifiedPath) {
			super(originalQuery, expandedQuery, schema, properties, comment);
			this.isTemporary = isTemporary;
			this.fullyQualifiedPath = fullyQualifiedPath;
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
			TestView testView = (TestView) o;
			return isTemporary == testView.isTemporary &&
				Objects.equals(fullyQualifiedPath, testView.fullyQualifiedPath);
		}

		@Override
		public int hashCode() {
			return Objects.hash(isTemporary, fullyQualifiedPath);
		}
	}
}
