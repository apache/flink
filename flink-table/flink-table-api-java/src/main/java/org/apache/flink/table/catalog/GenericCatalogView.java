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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A generic catalog view implementation.
 */
public class GenericCatalogView implements CatalogView {
	// Original text of the view definition.
	private final String originalQuery;

	// Expanded text of the original view definition
	// This is needed because the context such as current DB is
	// lost after the session, in which view is defined, is gone.
	// Expanded query text takes care of the this, as an example.
	private final String expandedQuery;

	private final TableSchema schema;
	private final Map<String, String> properties;
	private String comment = "This is a generic catalog view";

	public GenericCatalogView(String originalQuery, String expandedQuery, TableSchema schema,
		Map<String, String> properties, String comment) {
		this(originalQuery, expandedQuery, schema, properties);
		this.comment = comment;
	}

	public GenericCatalogView(String originalQuery, String expandedQuery, TableSchema schema,
		Map<String, String> properties) {
		this.originalQuery = originalQuery;
		this.expandedQuery = expandedQuery;
		this.schema = schema;
		this.properties = properties;
	}

	@Override
	public String getOriginalQuery() {
		return this.originalQuery;
	}

	@Override
	public String getExpandedQuery() {
		return this.expandedQuery;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public TableSchema getSchema() {
		return schema;
	}

	@Override
	public GenericCatalogView copy() {
		return new GenericCatalogView(this.originalQuery, this.expandedQuery, schema.copy(),
			new HashMap<>(this.properties), comment);
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.of(comment);
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.of("This is a catalog view in an im-memory catalog");
	}

	public String getComment() {
		return this.comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

}
