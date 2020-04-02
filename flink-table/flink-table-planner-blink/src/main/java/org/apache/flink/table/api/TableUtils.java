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

package org.apache.flink.table.api;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.Utils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;

import java.util.ArrayList;
import java.util.List;

/**
 * A collection of utilities for fetching table results.
 *
 * <p>NOTE: Methods in this utility class are experimental and can only be used for demonstration or testing
 * small table results. Please DO NOT use them in production or on large tables.
 *
 * <p>We ought to put this class in the api module, but as converting high precision timestamp data type
 * to type information is not possible in flink planner, we have to put this class in blink planner.
 */
@Experimental
public class TableUtils {

	/**
	 * Convert Flink table to Java list.
	 * This method is only applicable for small batch jobs and small finite append only stream jobs.
	 *
	 * @param table		Flink table to convert
	 * @return			Converted Java list
	 */
	public static List<Row> collectToList(Table table) throws Exception {
		TableEnvironment tEnv = ((TableImpl) table).getTableEnvironment();
		String id = new AbstractID().toString();
		TableSchema schema = buildNewTableSchema(table);
		DataType rowDataType = schema.toRowDataType();

		@SuppressWarnings("unchecked")
		TypeSerializer<Row> serializer = (TypeSerializer<Row>) TypeInfoDataTypeConverter
			.fromDataTypeToTypeInfo(rowDataType)
			.createSerializer(new ExecutionConfig());
		Utils.CollectHelper<Row> outputFormat = new Utils.CollectHelper<>(id, serializer);
		TableResultSink sink = new TableResultSink(schema, outputFormat);

		String tableName = table.toString();
		String sinkName = "tableResultSink_" + tableName + "_" + id;
		String jobName = "tableResultToList_" + tableName + "_" + id;

		List<Row> deserializedList;
		try {
			tEnv.registerTableSink(sinkName, sink);
			tEnv.insertInto(sinkName, table);
			JobExecutionResult executionResult = tEnv.execute(jobName);
			ArrayList<byte[]> accResult = executionResult.getAccumulatorResult(id);
			deserializedList = SerializedListAccumulator.deserializeList(accResult, serializer);
		} finally {
			tEnv.dropTemporaryTable(sinkName);
		}
		return deserializedList;
	}

	/**
	 * Table schema should only contain logical information. Users should specify conversion classes in sinks.
	 * As `tableResultToList` provides a default sink for users, we need to provide our conversion classes here,
	 * which are the default conversion classes.
	 *
	 * <p>Also, proc time / event time are internal time attributes, so we have to change them to normal timestamps
	 * for external output.
	 */
	private static TableSchema buildNewTableSchema(Table table) {
		TableSchema oldSchema = table.getSchema();
		DataType[] oldTypes = oldSchema.getFieldDataTypes();
		String[] oldNames = oldSchema.getFieldNames();

		TableSchema.Builder schemaBuilder = TableSchema.builder();
		for (int i = 0; i < oldSchema.getFieldCount(); i++) {
			// change to default conversion class
			DataType fieldType = LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(
				LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(oldTypes[i]));
			String fieldName = oldNames[i];
			if (fieldType.getLogicalType() instanceof TimestampType) {
				TimestampType timestampType = (TimestampType) fieldType.getLogicalType();
				if (!timestampType.getKind().equals(TimestampKind.REGULAR)) {
					// converts `TIME ATTRIBUTE(ROWTIME)`/`TIME ATTRIBUTE(PROCTIME)` to `TIMESTAMP(3)` for sink
					schemaBuilder.field(fieldName, DataTypes.TIMESTAMP(3));
					continue;
				}
			}
			schemaBuilder.field(fieldName, fieldType);
		}
		return schemaBuilder.build();
	}

	/**
	 * A {@link AppendStreamTableSink} which stores rows into {@link Accumulator}s.
	 */
	private static class TableResultSink implements AppendStreamTableSink<Row> {
		private final TableSchema schema;
		private final DataType rowType;
		private final Utils.CollectHelper<Row> outputFormat;

		TableResultSink(TableSchema schema, Utils.CollectHelper<Row> outputFormat) {
			this.schema = schema;
			this.rowType = schema.toRowDataType();
			this.outputFormat = outputFormat;
		}

		@Override
		public DataType getConsumedDataType() {
			return rowType;
		}

		@Override
		public TableSchema getTableSchema() {
			return schema;
		}

		@Override
		public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			throw new UnsupportedOperationException(
				"This sink is configured by passing a static schema when initiating");
		}

		@Override
		public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
			return dataStream.writeUsingOutputFormat(outputFormat).setParallelism(1).name("tableResult");
		}
	}
}
