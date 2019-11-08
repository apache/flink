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
import org.apache.flink.table.operations.QueryOperation;

import java.util.HashMap;
import java.util.Optional;

/**
 * A view created from a {@link QueryOperation} via operations on {@link org.apache.flink.table.api.Table}.
 */
@Internal
public class QueryOperationCatalogView extends AbstractCatalogView {
	private final QueryOperation queryOperation;

	public QueryOperationCatalogView(QueryOperation queryOperation) {
		this(queryOperation, "");
	}

	public QueryOperationCatalogView(QueryOperation queryOperation, String comment) {
		super(
			queryOperation.asSummaryString(),
			queryOperation.asSummaryString(),
			queryOperation.getTableSchema(),
			new HashMap<>(),
			comment);
		this.queryOperation = queryOperation;
	}

	public QueryOperation getQueryOperation() {
		return queryOperation;
	}

	@Override
	public QueryOperationCatalogView copy() {
		return new QueryOperationCatalogView(this.queryOperation, getComment());
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.of(getComment());
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return getDescription();
	}
}
