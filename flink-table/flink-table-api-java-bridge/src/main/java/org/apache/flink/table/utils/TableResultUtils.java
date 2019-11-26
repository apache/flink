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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;

import java.util.ArrayList;
import java.util.List;

/**
 * A collection of utilities for fetching table results.
 *
 * <p>NOTE: Methods in this utility class are experimental and can only be used for demonstration or testing
 * small table results. Please DO NOT use them in production or on large tables.
 */
@Experimental
public class TableResultUtils {

	/**
	 * Convert Flink table to Java list.
	 *
	 * @param table		Flink table to convert
	 * @return			Converted Java list
	 */
	public static List<Row> tableResultToList(Table table) throws Exception {
		final TableEnvironment tEnv = ((TableImpl) table).getTableEnvironment();

		final String id = new AbstractID().toString();
		final TypeSerializer<Row> serializer = table.getSchema().toRowType().createSerializer(new ExecutionConfig());
		final Utils.CollectHelper<Row> outputFormat = new Utils.CollectHelper<>(id, serializer);
		final TableResultSink sink = new TableResultSink(table, outputFormat);

		final String sinkName = "tableResultSink_" + id;
		final String jobName = "tableResultToList_" + id;

		tEnv.registerTableSink(sinkName, sink);
		tEnv.insertInto(sinkName, table);

		JobExecutionResult executionResult = tEnv.execute(jobName);
		ArrayList<byte[]> accResult = executionResult.getAccumulatorResult(id);
		List<Row> deserializedList = SerializedListAccumulator.deserializeList(accResult, serializer);

		tEnv.dropTemporaryTable(sinkName);
		return deserializedList;
	}

	/**
	 * A {@link StreamTableSink} which stores rows into {@link Accumulator}s.
	 */
	private static class TableResultSink implements StreamTableSink<Row> {
		private TableSchema schema;
		private DataType rowType;
		private Utils.CollectHelper<Row> outputFormat;

		TableResultSink(Table table, Utils.CollectHelper<Row> outputFormat) {
			this.schema = table.getSchema();
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
		public void emitDataStream(DataStream<Row> dataStream) {
			throw new UnsupportedOperationException("Deprecated method, use consumeDataStream instead");
		}

		@Override
		public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
			return dataStream.writeUsingOutputFormat(outputFormat).setParallelism(1).name("tableResult");
		}
	}
}
