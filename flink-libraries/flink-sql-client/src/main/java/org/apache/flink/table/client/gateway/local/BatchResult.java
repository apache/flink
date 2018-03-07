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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;

import java.util.List;

/**
 * Result for batch queries.
 */
public class BatchResult implements StaticResult {

	private String accumulatorName;
	private CollectBatchTableSink tableSink;
	private List<Row> resultTable;
	private int pageSize;
	private int pageCount;

	private volatile boolean snapshotted = false;

	public BatchResult() {
		accumulatorName = new AbstractID().toString();
		tableSink = new CollectBatchTableSink(accumulatorName);
		pageCount = 0;
	}

	@Override
	public TableSink<?> getTableSink() {
		return tableSink;
	}

	@Override
	public List<Row> retrievePage(int page) {
		if (page <= 0 || page > pageCount) {
			throw new SqlExecutionException("Invalid page '" + page + "'.");
		}
		return resultTable.subList(pageSize * (page - 1), Math.min(resultTable.size(), page * pageSize));
	}

	@Override
	public void setResultTable(List<Row> resultTable) {
		this.resultTable = resultTable;
	}

	@Override
	public TypedResult<Integer> snapshot(int pageSize) {
		// We return a payload result the first time and EoS for the rest of times as if the results
		// are retrieved dynamically.
		if (!snapshotted) {
			snapshotted = true;
			this.pageSize = pageSize;
			pageCount = Math.max(1, (int) Math.ceil(((double) resultTable.size() / pageSize)));
			return TypedResult.payload(pageCount);
		} else {
			return TypedResult.endOfStream();
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
