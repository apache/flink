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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.experimental.SocketStreamIterator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.local.CollectStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A result that works similarly to {@link DataStreamUtils#collect(DataStream)}.
 *
 * @param <C> cluster id to which this result belongs to
 */
public abstract class CollectStreamResult<C> extends BasicResult<C> implements DynamicResult<C> {

	private final SocketStreamIterator<Tuple2<Boolean, Row>> iterator;
	private final CollectStreamTableSink collectTableSink;
	private final ResultRetrievalThread retrievalThread;
	private final ClassLoader classLoader;
	private CompletableFuture<JobExecutionResult> jobExecutionResultFuture;

	protected final Object resultLock;
	protected AtomicReference<SqlExecutionException> executionException = new AtomicReference<>();

	public CollectStreamResult(
			TableSchema tableSchema,
			ExecutionConfig config,
			InetAddress gatewayAddress,
			int gatewayPort,
			ClassLoader classLoader) {
		resultLock = new Object();

		// create socket stream iterator
		final TypeInformation<Tuple2<Boolean, Row>> socketType = Types.TUPLE(Types.BOOLEAN, tableSchema.toRowType());
		final TypeSerializer<Tuple2<Boolean, Row>> serializer = socketType.createSerializer(config);
		try {
			// pass gateway port and address such that iterator knows where to bind to
			iterator = new SocketStreamIterator<>(gatewayPort, gatewayAddress, serializer);
		} catch (IOException e) {
			throw new SqlClientException("Could not start socket for result retrieval.", e);
		}

		// create table sink
		// pass binding address and port such that sink knows where to send to
		collectTableSink = new CollectStreamTableSink(iterator.getBindAddress(), iterator.getPort(), serializer, tableSchema);
		retrievalThread = new ResultRetrievalThread();

		this.classLoader = checkNotNull(classLoader);
	}

	@Override
	public void startRetrieval(JobClient jobClient) {
		// start listener thread
		retrievalThread.start();

		jobExecutionResultFuture = jobClient.getJobExecutionResult(classLoader)
				.whenComplete((unused, throwable) -> {
					if (throwable != null) {
						executionException.compareAndSet(
								null,
								new SqlExecutionException("Error while retrieving result.", throwable));
					}
				});
	}

	@Override
	public TableSink<?> getTableSink() {
		return collectTableSink;
	}

	@Override
	public void close() {
		retrievalThread.isRunning = false;
		retrievalThread.interrupt();
		iterator.close();
	}

	// --------------------------------------------------------------------------------------------

	protected <T> TypedResult<T> handleMissingResult() {

		// check if the monitoring thread is still there
		// we need to wait until we know what is going on
		if (!jobExecutionResultFuture.isDone()) {
			return TypedResult.empty();
		}

		if (executionException.get() != null) {
			throw executionException.get();
		}

		// we assume that a bounded job finished
		return TypedResult.endOfStream();
	}

	protected boolean isRetrieving() {
		return retrievalThread.isRunning;
	}

	protected abstract void processRecord(Tuple2<Boolean, Row> change);

	// --------------------------------------------------------------------------------------------

	private class ResultRetrievalThread extends Thread {

		public volatile boolean isRunning = true;

		@Override
		public void run() {
			try {
				while (isRunning && iterator.hasNext()) {
					final Tuple2<Boolean, Row> change = iterator.next();
					processRecord(change);
				}
			} catch (RuntimeException e) {
				// ignore socket exceptions
			}

			// no result anymore
			// either the job is done or an error occurred
			isRunning = false;
		}
	}
}
