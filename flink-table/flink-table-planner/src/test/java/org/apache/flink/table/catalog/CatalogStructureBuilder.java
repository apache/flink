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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Objects;

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
 *  ).build();
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
	 * Helper class for creating mock {@link CatalogTable} in a {@link CatalogStructureBuilder}.
	 */
	public static class TableBuilder {
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
	}

	private static class TestTable extends ConnectorCatalogTable<Row, Row> {
		private final String fullyQualifiedPath;

		private static final StreamTableSource<Row> tableSource = new StreamTableSource<Row>() {
			@Override
			public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
				return null;
			}

			@Override
			public TypeInformation<Row> getReturnType() {
				return Types.ROW();
			}

			@Override
			public TableSchema getTableSchema() {
				return TableSchema.builder().build();
			}
		};

		private TestTable(String fullyQualifiedPath) {
			super(tableSource, null, tableSource.getTableSchema(), false);
			this.fullyQualifiedPath = fullyQualifiedPath;
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
