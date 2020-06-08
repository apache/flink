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

package org.apache.flink.table.planner.sinks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;


/**
 * A {@link StreamTableSink} for batch select job to collect the result to local.
 */
public class BatchSelectTableSink extends SelectTableSinkBase<RowData> implements StreamTableSink<RowData> {

	public BatchSelectTableSink(TableSchema tableSchema) {
		super(tableSchema, createRowDataTypeInfo(tableSchema).createSerializer(new ExecutionConfig()));
	}

	@Override
	public TypeInformation<RowData> getOutputType() {
		return createRowDataTypeInfo(getTableSchema());
	}

	@Override
	protected Row convertToRow(RowData element) {
		// convert RowData to Row
		return converter.toExternal(element);
	}
}
