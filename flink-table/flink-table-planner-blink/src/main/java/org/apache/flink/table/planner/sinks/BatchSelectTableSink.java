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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.Utils;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.SelectTableSink;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * A {@link SelectTableSink} for batch select job.
 *
 * <p><strong>NOTES:</strong> This is a temporary solution,
 * once FLINK-14807 is finished, the implementation should be changed.
 */
public class BatchSelectTableSink implements StreamTableSink<Row>, SelectTableSink {
	private final TableSchema tableSchema;
	private final String accumulatorName;
	private final TypeSerializer<Row> typeSerializer;
	private JobClient jobClient;

	@SuppressWarnings("unchecked")
	public BatchSelectTableSink(TableSchema tableSchema) {
		this.tableSchema = SelectTableSinkSchemaConverter.convert(tableSchema);
		this.accumulatorName = new AbstractID().toString();
		this.typeSerializer = (TypeSerializer<Row>) TypeInfoDataTypeConverter
				.fromDataTypeToTypeInfo(this.tableSchema.toRowDataType())
				.createSerializer(new ExecutionConfig());
	}

	@Override
	public DataType getConsumedDataType() {
		return tableSchema.toRowDataType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return dataStream.writeUsingOutputFormat(
				new Utils.CollectHelper<>(accumulatorName, typeSerializer))
				.name("Batch select table sink")
				.setParallelism(1);
	}

	@Override
	public void setJobClient(JobClient jobClient) {
		this.jobClient = Preconditions.checkNotNull(jobClient, "jobClient should not be null");
	}

	@Override
	public Iterator<Row> getResultIterator() {
		Preconditions.checkNotNull(jobClient, "jobClient is null, please call setJobClient first.");
		JobExecutionResult jobExecutionResult;
		try {
			jobExecutionResult = jobClient.getJobExecutionResult(
					Thread.currentThread().getContextClassLoader())
					.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new TableException("Failed to get job execution result.", e);
		}
		ArrayList<byte[]> accResult = jobExecutionResult.getAccumulatorResult(accumulatorName);
		if (accResult == null) {
			throw new TableException("result is null.");
		}
		List<Row> rowList;
		try {
			rowList = SerializedListAccumulator.deserializeList(accResult, typeSerializer);
		} catch (IOException | ClassNotFoundException e) {
			throw new TableException("Failed to deserialize the result.", e);
		}
		return rowList.iterator();
	}
}
