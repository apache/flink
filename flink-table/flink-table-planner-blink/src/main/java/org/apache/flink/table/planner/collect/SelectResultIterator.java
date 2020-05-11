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

package org.apache.flink.table.planner.collect;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;

/**
 * An iterator which iterates through the results of a SELECT query.
 */
public class SelectResultIterator implements CloseableIterator<Row> {

	private final SelectResultFetcher fetcher;
	private final LinkedList<Row> bufferedResults;

	public SelectResultIterator(
			CompletableFuture<OperatorID> operatorIdFuture,
			TypeSerializer<Row> serializer,
			String accumulatorName) {
		this.fetcher = new SelectResultFetcher(operatorIdFuture, serializer, accumulatorName);
		this.bufferedResults = new LinkedList<>();
	}

	@VisibleForTesting
	public SelectResultIterator(
			CompletableFuture<OperatorID> operatorIdFuture,
			TypeSerializer<Row> serializer,
			String accumulatorName,
			int retryMillis) {
		this.fetcher = new SelectResultFetcher(operatorIdFuture, serializer, accumulatorName, retryMillis);
		this.bufferedResults = new LinkedList<>();
	}

	@Override
	public boolean hasNext() {
		// we have to make sure that the next result exists
		// it is possible that there is no more result but the job is still running
		if (bufferedResults.isEmpty()) {
			bufferedResults.addAll(fetcher.nextBatch());
		}
		return !bufferedResults.isEmpty();
	}

	@Override
	public Row next() {
		if (bufferedResults.isEmpty()) {
			bufferedResults.addAll(fetcher.nextBatch());
		}
		if (bufferedResults.isEmpty()) {
			return null;
		} else {
			return bufferedResults.removeFirst();
		}
	}

	@Override
	public void close() throws Exception {
		fetcher.close();
	}

	public void setJobClient(JobClient jobClient) {
		fetcher.setJobClient(jobClient);
	}
}
