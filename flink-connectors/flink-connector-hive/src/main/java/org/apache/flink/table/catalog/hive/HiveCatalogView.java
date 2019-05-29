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
import org.apache.flink.table.catalog.AbstractCatalogView;
import org.apache.flink.table.catalog.CatalogBaseTable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A Hive catalog view implementation.
 */
public class HiveCatalogView extends AbstractCatalogView {

	public HiveCatalogView(
			String originalQuery,
			String expandedQuery,
			TableSchema tableSchema,
			Map<String, String> properties,
			String comment) {
		super(originalQuery, expandedQuery, tableSchema, properties, comment);
	}

	@Override
	public CatalogBaseTable copy() {
		return new HiveCatalogView(
			this.getOriginalQuery(), this.getExpandedQuery(), this.getSchema().copy(), new HashMap<>(this.getProperties()), getComment());
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.ofNullable(getComment());
	}

	@Override
	public Optional<String> getDetailedDescription() {
		// TODO: return a detailed description
		return Optional.ofNullable(getComment());
	}

}
