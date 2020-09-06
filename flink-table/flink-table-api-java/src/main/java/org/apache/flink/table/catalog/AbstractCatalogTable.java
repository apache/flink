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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract catalog table.
 */
public abstract class AbstractCatalogTable implements CatalogTable {
	// Schema of the table (column names and types)
	private final TableSchema tableSchema;
	// Partition keys if this is a partitioned table. It's an empty set if the table is not partitioned
	private final List<String> partitionKeys;
	// Properties of the table
	private final Map<String, String> properties;
	// Comment of the table
	private final String comment;

	public AbstractCatalogTable(
			TableSchema tableSchema,
			Map<String, String> properties,
			String comment) {
		this(tableSchema, new ArrayList<>(), properties, comment);
	}

	public AbstractCatalogTable(
			TableSchema tableSchema,
			List<String> partitionKeys,
			Map<String, String> properties,
			String comment) {
		this.tableSchema = checkNotNull(tableSchema, "tableSchema cannot be null");
		this.partitionKeys = checkNotNull(partitionKeys, "partitionKeys cannot be null");
		this.properties = checkNotNull(properties, "properties cannot be null");

		checkArgument(
			properties.entrySet().stream().allMatch(e -> e.getKey() != null && e.getValue() != null),
			"properties cannot have null keys or values");

		this.comment = comment;
	}

	@Override
	public boolean isPartitioned() {
		return !partitionKeys.isEmpty();
	}

	@Override
	public List<String> getPartitionKeys() {
		return partitionKeys;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public TableSchema getSchema() {
		return tableSchema;
	}

	@Override
	public String getComment() {
		return comment;
	}
}
