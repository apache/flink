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

package org.apache.flink.table.utils;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * Mocking {@link TableSource} for tests.
 */
public class TableSourceMock implements TableSource<Row> {

	private final DataType producedDataType;

	private final TableSchema tableSchema;

	public TableSourceMock(TableSchema tableSchema) {
		this.tableSchema = TableSchemaUtils.checkNoGeneratedColumns(tableSchema);
		this.producedDataType = tableSchema.toRowDataType();
	}

	@Override
	public DataType getProducedDataType() {
		return producedDataType;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}
}
