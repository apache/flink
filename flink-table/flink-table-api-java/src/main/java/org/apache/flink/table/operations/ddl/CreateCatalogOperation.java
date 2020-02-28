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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Operation to describe a CREATE CATALOG statement.
 */
public class CreateCatalogOperation implements CreateOperation {
	private final String catalogName;
	private final Catalog catalog;

	public CreateCatalogOperation(String catalogName, Catalog catalog) {
		this.catalogName = checkNotNull(catalogName);
		this.catalog = checkNotNull(catalog);
	}

	public String getCatalogName() {
		return catalogName;
	}

	public Catalog getCatalog() {
		return catalog;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("catalogName", catalogName);

		return OperationUtils.formatWithChildren(
			"CREATE CATALOG",
			params,
			Collections.emptyList(),
			Operation::asSummaryString);
	}
}
