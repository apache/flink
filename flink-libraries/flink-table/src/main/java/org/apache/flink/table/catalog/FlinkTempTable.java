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

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.plan.stats.TableStats;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Represents AbstractTable/RelTable in Flink catalog.
 */
public class FlinkTempTable extends CatalogTable {

	private final AbstractTable abstractTable;

	public FlinkTempTable(
		AbstractTable table,
		String tableType,
		TableSchema schema,
		Map<String, String> properties,
		RichTableSchema richTableSchema,
		TableStats stats,
		String comment,
		LinkedHashSet<String> partitionColumnNames,
		boolean isPartitioned,
		Map<String, RexNode> computedColumns,
		String rowTimeField,
		long watermarkOffset,
		long createTime,
		long lastAccessTime) {

		super(tableType,
			schema,
			properties,
			richTableSchema,
			stats,
			comment,
			partitionColumnNames,
			isPartitioned,
			computedColumns,
			rowTimeField,
			watermarkOffset,
			createTime,
			lastAccessTime,
			true);

		this.abstractTable = table;
	}

	public AbstractTable getAbstractTable() {
		return abstractTable;
	}
}
