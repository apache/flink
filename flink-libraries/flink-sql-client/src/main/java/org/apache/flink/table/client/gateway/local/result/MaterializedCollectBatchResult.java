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

/**
 * Collects results using accumulators and returns them as table snapshots.
 */
public class MaterializedCollectBatchResult<C> extends BasicResult<C> implements MaterializedResult<C> {

	private final TypeInformation<Row> outputType;
	private final String accumulatorName;
	private final CollectBatchTableSink tableSink;
	private final Object resultLock;
	private final Thread retrievalThread;

	private ProgramDeployer<C> deployer;
	private int pageSize;
	private int pageCount;
	private SqlExecutionException executionException;
	private List<Row> resultTable;

	private volatile boolean snapshotted = false;

	public MaterializedCollectBatchResult(TypeInformation<Row> outputType, ExecutionConfig config) {
		this.outputType = outputType;

		accumulatorName = new AbstractID().toString();
		tableSink = new CollectBatchTableSink(accumulatorName, outputType.createSerializer(config));
		resultLock = new Object();
		retrievalThread = new ResultRetrievalThread();

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
	public void startRetrieval(ProgramDeployer<C> deployer) {
		this.deployer = deployer;
		retrievalThread.start();
	}

	@Override
	public TableSink<?> getTableSink() {
		return tableSink;
	}

	@Override
	public void close() {
		retrievalThread.interrupt();
	}

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
			// wait for a result
			if (retrievalThread.isAlive() && null == resultTable) {
				return TypedResult.empty();
			}
			// the job finished with an exception
			else if (executionException != null) {
				throw executionException;
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

	private class ResultRetrievalThread extends Thread {

		@Override
		public void run() {
			try {
				deployer.run();
				final JobExecutionResult result = deployer.fetchExecutionResult();
				final ArrayList<byte[]> accResult = result.getAccumulatorResult(accumulatorName);
				if (accResult == null) {
					throw new SqlExecutionException("The accumulator could not retrieve the result.");
				}
				final List<Row> resultTable = SerializedListAccumulator.deserializeList(accResult, tableSink.getSerializer());
				// sets the result table all at once
				synchronized (resultLock) {
					MaterializedCollectBatchResult.this.resultTable = resultTable;
				}
			} catch (ClassNotFoundException | IOException e) {
				executionException = new SqlExecutionException("Serialization error while deserializing collected data.", e);
			} catch (SqlExecutionException e) {
				executionException = e;
			}
		}
	}
}
