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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Hive catalog view implementation.
 */
public class HiveCatalogView implements CatalogView {
	// Original text of the view definition.
	private final String originalQuery;

	// Expanded text of the original view definition
	// This is needed because the context such as current DB is
	// lost after the session, in which view is defined, is gone.
	// Expanded query text takes care of the this, as an example.
	private final String expandedQuery;

	// Schema of the view (column names and types)
	private final TableSchema tableSchema;
	// Properties of the view
	private final Map<String, String> properties;
	// Comment of the view
	private final String comment;

	public HiveCatalogView(
			String originalQuery,
			String expandedQuery,
			TableSchema tableSchema,
			Map<String, String> properties,
			String comment) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(originalQuery), "original query cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(expandedQuery), "expanded query cannot be null or empty");

		this.originalQuery = originalQuery;
		this.expandedQuery = expandedQuery;
		this.tableSchema = checkNotNull(tableSchema, "tableSchema cannot be null");
		this.properties = checkNotNull(properties, "properties cannot be null");
		this.comment = comment;
	}

	@Override
	public String getOriginalQuery() {
		return originalQuery;
	}

	@Override
	public String getExpandedQuery() {
		return expandedQuery;
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

	@Override
	public CatalogBaseTable copy() {
		return new HiveCatalogView(
			originalQuery, expandedQuery, tableSchema.copy(), new HashMap<>(properties), comment);
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.ofNullable(comment);
	}

	@Override
	public Optional<String> getDetailedDescription() {
		// TODO: return a detailed description
		return Optional.ofNullable(comment);
	}
}
