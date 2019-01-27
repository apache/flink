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
import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.plan.schema.CatalogCalciteTable;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A mapping between Flink's catalog and Calcite's schema.
 * This enables to look up and access tables in SQL queries without registering tables in advance.
 * Databases are registered as sub-schemas in the schema.
 */
public class CatalogCalciteSchema implements Schema {
	private static final Logger LOGGER = LoggerFactory.getLogger(CatalogCalciteSchema.class);

	private final String catalogName;
	private final ReadableCatalog catalog;

	public CatalogCalciteSchema(String catalogName, ReadableCatalog catalog) {
		this.catalogName = catalogName;
		this.catalog = catalog;
	}

	/**
	 * Looks up a sub-schema (database) by the given sub-schema name.
	 *
	 * @param schemaName Name of sub-schema to look up.
	 * @return Sub-schema with a given dbName, or null.
	 */
	@Override
	public Schema getSubSchema(String schemaName) {
		try {
			return new DatabaseCalciteSchema(schemaName, catalog);
		} catch (DatabaseNotExistException e) {
			LOGGER.warn(String.format("Schema %s does not exist in catalog %s", schemaName, catalogName));
			return null;
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
		return new RelProtoDataType() {
			@Override
			public RelDataType apply(RelDataTypeFactory relDataTypeFactory) {
				return relDataTypeFactory.createSqlType(SqlTypeName.valueOf(name));
			}
		};
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

	public static void registerCatalog(
		SchemaPlus parentSchema,
		String catalogName,
		ReadableCatalog catalog) {

		LOGGER.info("Register catalog '{}' to Calcite", catalogName);
		SchemaPlus catalogSchema = parentSchema.getSubSchema(catalogName);

		if (catalogSchema != null) {
			throw new CatalogAlreadyExistException(catalogName);
		} else {
			CatalogCalciteSchema newCatalog = new CatalogCalciteSchema(catalogName, catalog);
			SchemaPlus schemaPlusOfNewCatalog = parentSchema.add(catalogName, newCatalog);
			newCatalog.registerSubSchemas(schemaPlusOfNewCatalog);
		}
	}

	public void registerSubSchemas(SchemaPlus schemaPlus) {
		for (String schemaName: catalog.listDatabases()) {
			schemaPlus.add(schemaName, getSubSchema(schemaName));
		}
	}

	/**
	 * A mapping between FlinK Catalog's database and Calcite's schema.
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
			try {
				LOGGER.info("Getting table '{}' from catalog '{}'", tableName, catalogName);
				CatalogTable table = catalog.getTable(new ObjectPath(dbName, tableName));

				LOGGER.info("Successfully got table '{}' from catalog '{}'", tableName, catalogName);

				if (table instanceof FlinkTempTable) {
					return ((FlinkTempTable) table).getAbstractTable();
				} else {
					return new CatalogCalciteTable(
						tableName, table, table.isStreaming());
				}
			} catch (TableNotExistException e) {
				LOGGER.warn(
					String.format("Table %s.%s does not exist in catalog %s", dbName, tableName, catalogName));
				return null;
			}
		}

		@Override
		public Set<String> getTableNames() {
			return catalog.listTables(dbName).stream()
				.map(op -> op.getObjectName())
				.collect(Collectors.toSet());
		}

		@Override
		public RelProtoDataType getType(String name) {
			return new RelProtoDataType() {
				@Override
				public RelDataType apply(RelDataTypeFactory relDataTypeFactory) {
					return relDataTypeFactory.createSqlType(SqlTypeName.valueOf(name));
				}
			};
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
