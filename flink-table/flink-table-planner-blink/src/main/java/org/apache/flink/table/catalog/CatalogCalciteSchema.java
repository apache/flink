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
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A mapping between Flink's catalog and Calcite's schema. This enables to look up and access tables
 * in SQL queries without registering tables in advance. Databases are registered as sub-schemas in the schema.
 * This mapping is modeled as a strict two-level reference structure for Flink in Calcite,
 * the full path of tables and views is of format [catalog_name].[db_name].[meta-object_name].
 */
@Internal
public class CatalogCalciteSchema implements Schema {
	private static final Logger LOGGER = LoggerFactory.getLogger(CatalogCalciteSchema.class);

	private final String catalogName;
	private final ReadableCatalog catalog;

	public CatalogCalciteSchema(String catalogName, ReadableCatalog catalog) {
		this.catalogName = catalogName;
		this.catalog = catalog;
	}

	/**
	 * Look up a sub-schema (database) by the given sub-schema name.
	 *
	 * @param schemaName name of sub-schema to look up
	 * @return the sub-schema with a given dbName, or null
	 */
	@Override
	public Schema getSubSchema(String schemaName) {

		if (catalog.databaseExists(schemaName)) {
			return new DatabaseCalciteSchema(schemaName, catalog);
		} else {
			LOGGER.error(String.format("Schema %s does not exist in catalog %s", schemaName, catalogName));
			throw new CatalogException(
				new DatabaseNotExistException(catalogName, schemaName));
		}
	}

	@Override
	public Set<String> getSubSchemaNames() {
		return new HashSet<>(catalog.listDatabases());
	}

	@Override
	public Table getTable(String name) {
		return null;
	}

	@Override
	public Set<String> getTableNames() {
		return new HashSet<>();
	}

	@Override
	public RelProtoDataType getType(String name) {
		return null;
	}

	@Override
	public Set<String> getTypeNames() {
		return new HashSet<>();
	}

	@Override
	public Collection<Function> getFunctions(String s) {
		return new HashSet<>();
	}

	@Override
	public Set<String> getFunctionNames() {
		return new HashSet<>();
	}

	@Override
	public Expression getExpression(SchemaPlus parentSchema, String name) {
		return  Schemas.subSchemaExpression(parentSchema, name, getClass());
	}

	@Override
	public boolean isMutable() {
		return true;
	}

	@Override
	public Schema snapshot(SchemaVersion schemaVersion) {
		return this;
	}

	/**
	 * Register a ReadableCatalog to Calcite schema with given name.
	 *
	 * @param parentSchema the parent schema
	 * @param catalogName name of the catalog to register
	 * @param catalog the catalog to register
	 */
	public static void registerCatalog(
			SchemaPlus parentSchema,
			String catalogName,
			ReadableCatalog catalog) {
		CatalogCalciteSchema newCatalog = new CatalogCalciteSchema(catalogName, catalog);
		parentSchema.add(catalogName, newCatalog);

		LOGGER.info("Registered catalog '{}' to Calcite", catalogName);
	}

	/**
	 * A mapping between Flink catalog's database and Calcite's schema.
	 * Tables are registered as tables in the schema.
	 */
	private class DatabaseCalciteSchema implements Schema {

		private final String dbName;
		private final ReadableCatalog catalog;

		public DatabaseCalciteSchema(String dbName, ReadableCatalog catalog) {
			this.dbName = dbName;
			this.catalog = catalog;
		}

		@Override
		public Table getTable(String tableName) {
			LOGGER.info("Getting table '{}' from catalog '{}'", tableName, catalogName);

			ObjectPath tablePath = new ObjectPath(dbName, tableName);

			try {
				CatalogBaseTable table = catalog.getTable(tablePath);

				LOGGER.info("Successfully got table '{}' from catalog '{}'", tableName, catalogName);

				// TODO: [FLINK-12257] Convert CatalogBaseTable to org.apache.calcite.schema.Table
				//  so that planner can use unified catalog APIs
				return null;
			} catch (TableNotExistException e) {
				throw new CatalogException(e);
			}
		}

		@Override
		public Set<String> getTableNames() {
			try {
				return new HashSet<>(catalog.listTables(dbName));
			} catch (DatabaseNotExistException e) {
				throw new CatalogException(e);
			}
		}

		@Override
		public RelProtoDataType getType(String name) {
			return null;
		}

		@Override
		public Set<String> getTypeNames() {
			return new HashSet<>();
		}

		@Override
		public Collection<Function> getFunctions(String s) {
			return new HashSet<>();
		}

		@Override
		public Set<String> getFunctionNames() {
			return new HashSet<>();
		}

		@Override
		public Schema getSubSchema(String s) {
			return null;
		}

		@Override
		public Set<String> getSubSchemaNames() {
			return new HashSet<>();
		}

		@Override
		public Expression getExpression(SchemaPlus parentSchema, String name) {
			return Schemas.subSchemaExpression(parentSchema, name, getClass());
		}

		@Override
		public boolean isMutable() {
			return true;
		}

		@Override
		public Schema snapshot(SchemaVersion schemaVersion) {
			return this;
		}
	}
}
