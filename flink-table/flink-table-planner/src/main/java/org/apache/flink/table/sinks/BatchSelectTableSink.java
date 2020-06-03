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

package org.apache.flink.table.sinks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.SelectResultProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * A {@link BatchTableSink} for batch select job to collect the result to local.
 */
public class BatchSelectTableSink implements BatchTableSink<Row> {
	private final TableSchema tableSchema;
	private final String accumulatorName;
	private final TypeSerializer<Row> typeSerializer;

	public BatchSelectTableSink(TableSchema tableSchema) {
		this.tableSchema =
				SelectTableSinkSchemaConverter.convertTimeAttributeToRegularTimestamp(tableSchema);
		this.accumulatorName = new AbstractID().toString();
		this.typeSerializer = this.tableSchema.toRowType().createSerializer(new ExecutionConfig());
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
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DataSink<?> consumeDataSet(DataSet<Row> dataSet) {
		return dataSet.output(
				new Utils.CollectHelper<>(accumulatorName, typeSerializer))
				.name("Batch select table sink")
				.setParallelism(1);
	}

	public SelectResultProvider getSelectResultProvider() {
		return new SelectResultProvider() {
			private JobClient jobClient;

			@Override
			public void setJobClient(JobClient jobClient) {
				this.jobClient = Preconditions.checkNotNull(jobClient, "jobClient should not be null");
			}

			@Override
			public CloseableIterator<Row> getResultIterator() {
				Preconditions.checkNotNull(jobClient, "jobClient is null, please call setJobClient first.");
				return collectResult(jobClient);
			}
		};
	}

	private CloseableIterator<Row> collectResult(JobClient jobClient) {
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
		return CloseableIterator.adapterForIterator(rowList.iterator());
	}

}
