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

import org.apache.flink.table.operations.TableOperation;

import java.util.HashMap;
import java.util.Optional;

/**
 * A view created from {@link TableOperation} via operations on {@link org.apache.flink.table.api.Table}.
 */
public class TableOperationCatalogView extends AbstractCatalogView {
	private final TableOperation tableOperation;

	public TableOperationCatalogView(TableOperation tableOperation) {
		this(tableOperation, "This is a catalog view backed by TableOperation");
	}

	private TableOperationCatalogView(TableOperation tableOperation, String comment) {
		super(
			tableOperation.toString(),
			tableOperation.toString(),
			tableOperation.getTableSchema(),
			new HashMap<>(),
			comment);
		this.tableOperation = tableOperation;
	}

	public TableOperation getTableOperation() {
		return tableOperation;
	}

	@Override
	public TableOperationCatalogView copy() {
		return new TableOperationCatalogView(this.tableOperation, getComment());
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
