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
import org.apache.flink.table.api.TableConfig;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A mapping between Flink's catalog and Calcite's schema. This enables to look up and access objects(tables, views,
 * functions, types) in SQL queries without registering them in advance. Databases are registered as sub-schemas
 * in the schema.
 */
@Internal
public class CatalogCalciteSchema implements Schema {

	private final boolean isStreamingMode;
	private final String catalogName;
	private final CatalogManager catalogManager;
	private final TableConfig tableConfig;

	public CatalogCalciteSchema(
			boolean isStreamingMode,
			String catalogName,
			CatalogManager catalogManager,
			TableConfig tableConfig) {
		this.isStreamingMode = isStreamingMode;
		this.catalogName = catalogName;
		this.catalogManager = catalogManager;
		this.tableConfig = tableConfig;
	}

	/**
	 * Look up a sub-schema (database) by the given sub-schema name.
	 *
	 * @param schemaName name of sub-schema to look up
	 * @return the sub-schema with a given database name, or null
	 */
	@Override
	public Schema getSubSchema(String schemaName) {
		if (catalogManager.schemaExists(catalogName, schemaName)) {
			return new DatabaseCalciteSchema(isStreamingMode, schemaName, catalogName, catalogManager, tableConfig);
		} else {
			return null;
		}
	}

	@Override
	public Set<String> getSubSchemaNames() {
		return catalogManager.listSchemas(catalogName);
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
}
