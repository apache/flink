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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * An iterator which iterates through the results of a query job.
 *
 * <p>NOTE: After using this iterator, the close method MUST be called in order to release job related resources.
 */
public class CollectResultIterator<T> implements Iterator<T>, AutoCloseable {

	private final CollectResultFetcher<T> fetcher;
	private T bufferedResult;

	public CollectResultIterator(
			CompletableFuture<OperatorID> operatorIdFuture,
			TypeSerializer<T> serializer,
			String accumulatorName) {
		this.fetcher = new CollectResultFetcher<>(operatorIdFuture, serializer, accumulatorName);
		this.bufferedResult = null;
	}

	@VisibleForTesting
	public CollectResultIterator(
			CompletableFuture<OperatorID> operatorIdFuture,
			TypeSerializer<T> serializer,
			String accumulatorName,
			int retryMillis) {
		this.fetcher = new CollectResultFetcher<>(operatorIdFuture, serializer, accumulatorName, retryMillis);
		this.bufferedResult = null;
	}

	@Override
	public boolean hasNext() {
		// we have to make sure that the next result exists
		// it is possible that there is no more result but the job is still running
		if (bufferedResult == null) {
			bufferedResult = nextResultFromFetcher();
		}
		return bufferedResult != null;
	}

	@Override
	public T next() {
		if (bufferedResult == null) {
			bufferedResult = nextResultFromFetcher();
		}
		T ret = bufferedResult;
		bufferedResult = null;
		return ret;
	}

	@Override
	public void close() throws Exception {
		fetcher.close();
	}

	public void setJobClient(JobClient jobClient) {
		fetcher.setJobClient(jobClient);
	}

	private T nextResultFromFetcher() {
		try {
			return fetcher.next();
		} catch (IOException e) {
			fetcher.close();
			throw new RuntimeException("Failed to fetch next result", e);
		}
	}
}
