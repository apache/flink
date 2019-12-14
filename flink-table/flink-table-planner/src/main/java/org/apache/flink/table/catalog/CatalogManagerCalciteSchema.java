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

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Bridge between the {@link CatalogManager} and the {@link Schema}. This way we can query Flink's specific catalogs
 * from Calcite.
 *
 * <p>The mapping for {@link Catalog}s is modeled as a strict two-level reference structure for Flink in Calcite,
 * the full path of objects is of format [catalog_name].[db_name].[meta-object_name].
 */
@Internal
public class CatalogManagerCalciteSchema implements Schema {

	private final CatalogManager catalogManager;
	private boolean isStreamingMode;

	public CatalogManagerCalciteSchema(CatalogManager catalogManager, boolean isStreamingMode) {
		this.catalogManager = catalogManager;
		this.isStreamingMode = isStreamingMode;
	}

	@Override
	public Table getTable(String name) {
		return null;
	}

	@Override
	public Set<String> getTableNames() {
		return Collections.emptySet();
	}

	@Override
	public RelProtoDataType getType(String name) {
		return null;
	}

	@Override
	public Set<String> getTypeNames() {
		return Collections.emptySet();
	}

	@Override
	public Collection<Function> getFunctions(String name) {
		return Collections.emptyList();
	}

	@Override
	public Set<String> getFunctionNames() {
		return Collections.emptySet();
	}

	@Override
	public Schema getSubSchema(String name) {
		if (catalogManager.schemaExists(name)) {
			return new CatalogCalciteSchema(isStreamingMode, name, catalogManager);
		} else {
			return null;
		}
	}

	@Override
	public Set<String> getSubSchemaNames() {
		return catalogManager.listSchemas();
	}

	@Override
	public Expression getExpression(SchemaPlus parentSchema, String name) {
		return null;
	}

	@Override
	public boolean isMutable() {
		return false;
	}

	@Override
	public Schema snapshot(SchemaVersion version) {
		return this;
	}
}
