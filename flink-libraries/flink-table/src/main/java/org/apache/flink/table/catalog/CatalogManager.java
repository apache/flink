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

import org.apache.flink.table.api.ExternalCatalogAlreadyExistException;
import org.apache.flink.table.api.ExternalCatalogNotExistException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The CatalogManager manages all registered {@link ExternalCatalog} instances, and keeps track of all
 * non-permanent meta objects such as temporary tables and functions.
 */
public class CatalogManager {
	// the catalog to hold all registered and translated tables
	// we disable caching here to prevent side effects
	private final CalciteSchema internalSchema = CalciteSchema.createRootSchema(false, false);
	private final SchemaPlus rootSchema = internalSchema.plus();

	// registered external catalog names -> catalog
	private final Map<String, ExternalCatalog> externalCatalogs = new HashMap<>();

	private final TableEnvironment tableEnv;

	public CatalogManager(TableEnvironment tableEnvironment) {
		this.tableEnv = Preconditions.checkNotNull(tableEnvironment);
	}

	/**
	 * Gets the rootSchema.
	 *
	 * @return The rootSchema
	 */
	public SchemaPlus getRootSchema() {
		return rootSchema;
	}

	/**
	 * Gets a schema by path.
	 *
	 * @param schemaPath The path of the schema
	 * @return The SchemaPlus in the path
	 */
	public SchemaPlus getSchema(String[] schemaPath) {
		SchemaPlus schema = rootSchema;
		for (String schemaName : schemaPath) {
			schema = schema.getSubSchema(schemaName);
			if (schema == null) {
				return schema;
			}
		}

		return schema;
	}

	/**
	 * Registers an {@link ExternalCatalog} under a unique name in the TableEnvironment's schema.
	 * All tables registered in the {@link ExternalCatalog} can be accessed.
	 *
	 * @param name            The name under which the externalCatalog will be registered
	 * @param externalCatalog The externalCatalog to register
	 */
	public void registerExternalCatalog(String name, ExternalCatalog externalCatalog)
		throws ExternalCatalogAlreadyExistException {
		if (rootSchema.getSubSchema(name) != null) {
			throw new ExternalCatalogAlreadyExistException(name);
		}
		externalCatalogs.put(name, externalCatalog);
		ExternalCatalogSchema.registerCatalog(tableEnv, rootSchema, name, externalCatalog);
	}

	/**
	 * Gets a registered {@link ExternalCatalog} by name.
	 *
	 * @param name The name to look up the {@link ExternalCatalog}
	 * @return The {@link ExternalCatalog}
	 */
	public ExternalCatalog getRegisteredExternalCatalog(String name)
		throws ExternalCatalogNotExistException {
		ExternalCatalog result = externalCatalogs.get(name);

		if (result != null) {
			return result;
		} else {
			throw new ExternalCatalogNotExistException(name);
		}
	}

	/**
	 * Gets the names of all tables registered in this environment.
	 *
	 * @return A list of the names of all registered tables.
	 */
	public List<String> listTables() {
		return new ArrayList(rootSchema.getTableNames());
	}

	/**
	 * Registers a Calcite {@link AbstractTable} in the TableEnvironment's catalog.
	 *
	 * @param name The name under which the table will be registered.
	 * @param table The table to register in the catalog
	 * @throws TableException if another table is registered under the provided name.
	 */
	public void registerTableInternal(String name, AbstractTable table) throws TableException {
		if (isRegistered(name)) {
			throw new TableException(
				String.format("Table %s already exists. Please, choose a different name.", name));
		} else {
			rootSchema.add(name, table);
		}
	}

	/**
	 * Replaces a registered Table with another Table under the same name.
	 * We use this method to replace a {@link org.apache.flink.table.plan.schema.DataStreamTable}
	 * with a {@link org.apache.calcite.schema.TranslatableTable}.
	 *
	 * @param name Name of the table to replace.
	 * @param table The table that replaces the previous table.
	 */
	public void replaceRegisteredTable(String name, AbstractTable table) throws TableException {
		if (isRegistered(name)) {
			rootSchema.add(name, table);
		} else {
			throw new TableException(String.format("Table %s is not registered.", name));
		}
	}

	/**
	 * Checks if a table is registered under the given name.
	 *
	 * @param name The table name to check.
	 * @return true, if a table is registered under the name, false otherwise.
	 */
	public boolean isRegistered(String name) {
		return rootSchema.getTableNames().contains(name);
	}

	/**
	 * Get a table from either internal or external catalogs.
	 *
	 * @param name The name of the table.
	 * @return The table registered either internally or externally, None otherwise.
	 */
	public Optional<Table> getTable(String name) {
		String[] pathNames = name.split("\\.");

		return getTableFromSchema(rootSchema, new LinkedList<>(Arrays.asList(pathNames)));
	}

	// recursively fetches a table from a schema.
	private Optional<Table> getTableFromSchema(SchemaPlus schema, List<String> paths) {
		Preconditions.checkArgument(paths != null && paths.size() >= 1);

		if (paths.size() == 1) {
			return Optional.ofNullable(schema.getTable(paths.get(0)));
		} else {
			Optional<SchemaPlus> subschema = Optional.ofNullable(schema.getSubSchema(paths.remove(0)));

			if (subschema.isPresent()) {
				return getTableFromSchema(subschema.get(), paths);
			} else {
				return Optional.empty();
			}
		}
	}

}
