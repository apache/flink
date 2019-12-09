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

package org.apache.flink.table.client.gateway.local.result;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.local.CollectBatchTableSink;
import org.apache.flink.table.client.gateway.local.ProgramDeployer;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Collects results using accumulators and returns them as table snapshots.
 */
public class MaterializedCollectBatchResult<C> extends BasicResult<C> implements MaterializedResult<C> {

	private final TypeInformation<Row> outputType;
	private final String accumulatorName;
	private final CollectBatchTableSink tableSink;
	private final Object resultLock;
	private final ClassLoader classLoader;

	private int pageSize;
	private int pageCount;
	private AtomicReference<SqlExecutionException> executionException = new AtomicReference<>();
	private List<Row> resultTable;

	private volatile boolean snapshotted = false;

	public MaterializedCollectBatchResult(
			TableSchema tableSchema,
			RowTypeInfo outputType,
			ExecutionConfig config,
			ClassLoader classLoader) {
		this.outputType = outputType;

		accumulatorName = new AbstractID().toString();
		tableSink = new CollectBatchTableSink(accumulatorName, outputType.createSerializer(config), tableSchema);
		resultLock = new Object();
		this.classLoader = checkNotNull(classLoader);

		pageCount = 0;
	}

	@Override
	public boolean isMaterialized() {
		return true;
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return outputType;
	}

	@Override
	public void startRetrieval(ProgramDeployer deployer) {
		deployer
				.deploy()
				.thenCompose(jobClient -> jobClient.getJobExecutionResult(classLoader))
				.thenAccept(new ResultRetrievalHandler())
				.whenComplete((unused, throwable) -> {
					if (throwable != null) {
						executionException.compareAndSet(null,
								new SqlExecutionException(
										"Error while submitting job.",
										throwable));
					}
				});
	}

	@Override
	public TableSink<?> getTableSink() {
		return tableSink;
	}

	@Override
	public void close() {}

	@Override
	public List<Row> retrievePage(int page) {
		synchronized (resultLock) {
			if (page <= 0 || page > pageCount) {
				throw new SqlExecutionException("Invalid page '" + page + "'.");
			}
			return resultTable.subList(pageSize * (page - 1), Math.min(resultTable.size(), page * pageSize));
		}
	}

	@Override
	public TypedResult<Integer> snapshot(int pageSize) {
		synchronized (resultLock) {
			// the job finished with an exception
			SqlExecutionException e = executionException.get();
			if (e != null) {
				throw e;
			}

			// wait for a result
			if (null == resultTable) {
				return TypedResult.empty();
			}
			// we return a payload result the first time and EoS for the rest of times as if the results
			// are retrieved dynamically
			else if (!snapshotted) {
				snapshotted = true;
				this.pageSize = pageSize;
				pageCount = Math.max(1, (int) Math.ceil(((double) resultTable.size() / pageSize)));
				return TypedResult.payload(pageCount);
			} else {
				return TypedResult.endOfStream();
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private class ResultRetrievalHandler implements Consumer<JobExecutionResult> {

		@Override
		public void accept(JobExecutionResult jobExecutionResult) {
			try {
				final ArrayList<byte[]> accResult = jobExecutionResult.getAccumulatorResult(accumulatorName);
				if (accResult == null) {
					throw new SqlExecutionException("The accumulator could not retrieve the result.");
				}
				final List<Row> resultTable = SerializedListAccumulator.deserializeList(accResult, tableSink.getSerializer());
				// sets the result table all at once
				synchronized (resultLock) {
					MaterializedCollectBatchResult.this.resultTable = resultTable;
				}
			} catch (ClassNotFoundException | IOException e) {
				throw new SqlExecutionException("Serialization error while deserializing collected data.", e);
			}
		}
	}
}
