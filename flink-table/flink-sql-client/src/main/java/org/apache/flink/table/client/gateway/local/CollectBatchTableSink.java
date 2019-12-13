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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.OutputFormatTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

/**
 * Table sink for collecting the results locally all at once using accumulators.
 */
public class CollectBatchTableSink extends OutputFormatTableSink<Row> implements BatchTableSink<Row> {

	private final String accumulatorName;
	private final TypeSerializer<Row> serializer;
	private final TableSchema tableSchema;

	public CollectBatchTableSink(String accumulatorName, TypeSerializer<Row> serializer, TableSchema tableSchema) {
		this.accumulatorName = accumulatorName;
		this.serializer = serializer;
		this.tableSchema = TableSchemaUtils.checkNoGeneratedColumns(tableSchema);
	}

	/**
	 * Returns the serializer for deserializing the collected result.
	 */
	public TypeSerializer<Row> getSerializer() {
		return serializer;
	}

	@Override
	public DataType getConsumedDataType() {
		return getTableSchema().toRowDataType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public CollectBatchTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return new CollectBatchTableSink(accumulatorName, serializer, tableSchema);
	}

	@Override
	public void emitDataSet(DataSet<Row> dataSet) {
		dataSet
			.output(new Utils.CollectHelper<>(accumulatorName, serializer))
			.name("SQL Client Batch Collect Sink");
	}

	@Override
	public OutputFormat<Row> getOutputFormat() {
		return new Utils.CollectHelper<>(accumulatorName, serializer);
	}
}
