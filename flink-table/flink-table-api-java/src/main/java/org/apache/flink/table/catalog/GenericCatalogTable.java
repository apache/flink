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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.plan.stats.TableStats;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A generic catalog table implementation.
 */
public class GenericCatalogTable implements CatalogTable {
	// Schema of the table (column names and types)
	private final TableSchema tableSchema;
	// Properties of the table
	private final Map<String, String> properties;
	// Statistics of the table
	private final TableStats tableStats;
	// Comment of the table
	private String comment = "This is a generic catalog table.";

	public GenericCatalogTable(TableSchema tableSchema, TableStats tableStats, Map<String, String> properties) {
		this.tableSchema = tableSchema;
		this.tableStats = tableStats;
		this.properties = checkNotNull(properties, "properties cannot be null");
	}

	public GenericCatalogTable(TableSchema tableSchema, TableStats tableStats, Map<String, String> properties,
		String comment) {
		this(tableSchema, tableStats, properties);
		this.comment = comment;
	}

	@Override
	public TableStats getStatistics() {
		return this.tableStats;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public TableSchema getSchema() {
		return this.tableSchema;
	}

	@Override
	public GenericCatalogTable copy() {
		return new GenericCatalogTable(this.tableSchema.copy(), this.tableStats.copy(),
			new HashMap<>(this.properties), comment);
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.of(comment);
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.of("This is a catalog table in an im-memory catalog");
	}

	public String getComment() {
		return this.comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

}
