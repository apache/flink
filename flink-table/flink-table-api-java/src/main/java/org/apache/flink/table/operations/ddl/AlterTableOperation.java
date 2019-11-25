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

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operation to describe a ALTER TABLE statement.
 */
public class AlterTableOperation implements AlterOperation {
	private final ObjectIdentifier tableIdentifier;
	private final ObjectIdentifier newTableIdentifier;
	private final CatalogTable catalogTable;
	private boolean isRename;

	public AlterTableOperation(
			ObjectIdentifier tableIdentifier,
			ObjectIdentifier newTableIdentifier,
			boolean isRename,
			CatalogTable catalogTable) {
		this.tableIdentifier = tableIdentifier;
		this.newTableIdentifier = newTableIdentifier;
		this.isRename = isRename;
		this.catalogTable = catalogTable;
	}

	public ObjectIdentifier getTableIdentifier() {
		return tableIdentifier;
	}

	public ObjectIdentifier getNewTableIdentifier() {
		return newTableIdentifier;
	}

	public CatalogTable getCatalogTable() {
		return catalogTable;
	}

	public boolean isRename() {
		return isRename;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("AlterTable", tableIdentifier.toString());
		params.put("identifier", tableIdentifier);
		if (isRename) {
			params.put("newIdentifier", newTableIdentifier.toString());
		} else {
			params.putAll(catalogTable.toProperties());
		}

		return OperationUtils.formatWithChildren(
			"Alter TABLE",
			params,
			Collections.emptyList(),
			Operation::asSummaryString);
	}
}
