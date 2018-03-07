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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Result for batch queries.
 */
public class MaterializedCollectBatchResult<C> implements MaterializedResult<C> {

	private String accumulatorName;
	private CollectBatchTableSink tableSink;
	private int pageSize;
	private int pageCount;

	private List<Row> resultTable = null;
	private final TypeInformation<Row> outputType;
	private final Object resultLock;
	private Thread retrievingThread;
	private ProgramDeployer<C> deployer;

	private C clusterId;

	private volatile boolean snapshotted = false;

	public MaterializedCollectBatchResult(TypeInformation<Row> outputType) {
		this.outputType = outputType;

		accumulatorName = new AbstractID().toString();
		tableSink = new CollectBatchTableSink(accumulatorName);
		resultLock = new Object();
		pageCount = 0;
		retrievingThread = new Thread(() -> {
			deployer.run();
			JobExecutionResult result = deployer.fetchExecutionResult();
			ArrayList<byte[]> accResult = result.getAccumulatorResult(getAccumulatorName());
			if (accResult != null) {
				try {
					List<Row> resultTable = SerializedListAccumulator.deserializeList(accResult, getSerializer());
					setResultTable(resultTable);
				} catch (ClassNotFoundException e) {
					throw new SqlExecutionException("Cannot find type class of collected data type.", e);
				} catch (IOException e) {
					throw new SqlExecutionException("Serialization error while deserializing collected data", e);
				}
			} else {
				throw new SqlExecutionException("The call to collect() could not retrieve the DataSet.");
			}
		});
	}

	/**
	 * Sets the result table all at once.
	 */
	private void setResultTable(List<Row> resultTable) {
		synchronized (resultLock) {
			this.resultTable = resultTable;
		}
	}

	@Override
	public void setClusterId(C clusterId) {
		if (this.clusterId != null) {
			throw new IllegalStateException("Cluster id is already present.");
		}
		this.clusterId = clusterId;
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
	public void startRetrieval(ProgramDeployer<C> program) {
		this.deployer = program;
		retrievingThread.start();
	}

	@Override
	public TableSink<?> getTableSink() {
		return tableSink;
	}

	@Override
	public void close() {
		retrievingThread.interrupt();
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
			// We return a payload result the first time and EoS for the rest of times as if the results
			// are retrieved dynamically.
			if (null == resultTable) {
				return TypedResult.empty();
			} else if (!snapshotted) {
				snapshotted = true;
				this.pageSize = pageSize;
				pageCount = Math.max(1, (int) Math.ceil(((double) resultTable.size() / pageSize)));
				return TypedResult.payload(pageCount);
			} else {
				return TypedResult.endOfStream();
			}
		}
	}

	/**
	 * Returns the accumulator name for retrieving the results.
	 */
	public String getAccumulatorName() {
		return accumulatorName;
	}

	/**
	 * Returns the serializer for deserializing the collected result.
	 */
	public TypeSerializer<Row> getSerializer() {
		return tableSink.getSerializer();
	}
}
