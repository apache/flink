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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.experimental.SocketStreamIterator;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.local.CollectStreamTableSink;
import org.apache.flink.table.client.gateway.local.ProgramDeployer;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Collects results and returns them as finalized table.
 *
 * @param <C> cluster id to which this result belongs to
 */
public class FinalizedCollectStreamResult<C> extends BasicResult<C> implements DynamicResult<C>, FinalizedResult<C> {

	private static final Logger LOG = LoggerFactory.getLogger(FinalizedCollectStreamResult.class);
	private final TypeInformation<Row> outputType;
	private final SocketStreamIterator<Tuple2<Boolean, Row>> iterator;
	private final CollectStreamTableSink collectTableSink;
	private final ResultRetrievalThread retrievalThread;
	private final CountDownLatch resultLatch;
	private final List<Row> finalizedTable;
	/**
	 * Caches the last row position for faster access. The position might not be exact (if rows
	 * with smaller position are deleted) nor complete (for deletes of duplicates). However, the
	 * cache narrows the search in the finalized table.
	 */
	private final Map<Row, Integer> rowPositionCache;

	private final ExecutorService executor;
	private final int queryTimeoutMS;

	private static final Signal SIGNAL_INT = new Signal("INT");

	public FinalizedCollectStreamResult(
			RowTypeInfo outputType,
			ExecutionConfig config,
			InetAddress gatewayAddress,
			int gatewayPort) {
		this(outputType, config, gatewayAddress, gatewayPort, -1);
	}

	public FinalizedCollectStreamResult(
			RowTypeInfo outputType,
			ExecutionConfig config,
			InetAddress gatewayAddress,
			int gatewayPort,
			int queryTimeoutMs) {
		this.outputType = outputType;
		this.resultLatch = new CountDownLatch(1);
		this.finalizedTable = new ArrayList<>();
		this.rowPositionCache = new HashMap<>();
		this.executor = Executors.newSingleThreadExecutor();
		this.queryTimeoutMS = queryTimeoutMs;

		// create socket stream iterator
		final TypeInformation<Tuple2<Boolean, Row>> socketType = Types.TUPLE(Types.BOOLEAN, outputType);
		final TypeSerializer<Tuple2<Boolean, Row>> serializer = socketType.createSerializer(config);
		try {
			// pass gateway port and address such that iterator knows where to bind to
			iterator = new SocketStreamIterator<>(gatewayPort, gatewayAddress, serializer);
		} catch (IOException e) {
			throw new SqlClientException("Could not start socket for result retrieval.", e);
		}

		// create table sink
		// pass binding address and port such that sink knows where to send to
		collectTableSink = new CollectStreamTableSink(iterator.getBindAddress(), iterator.getPort(), serializer)
			.configure(outputType.getFieldNames(), outputType.getFieldTypes());
		retrievalThread = new ResultRetrievalThread();
	}

	@Override
	public List<Row> retrieveResult() {
		try {
			// wait for the retrievalThread finished
			resultLatch.await();

			return finalizedTable;
		} catch (InterruptedException e) {
			throw new SqlExecutionException("Could not retrieve finalized result.", e);
		}
	}

	@Override
	public boolean isMaterialized() {
		return false;
	}

	@Override
	public boolean isFinalized() {
		return true;
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return outputType;
	}

	@Override
	public void startRetrieval(ProgramDeployer<C> deployer) {

		retrievalThread.start();

		final Future<?> deployFuture = executor.submit(deployer);

		final SignalHandler previousHandler = Signal.handle(SIGNAL_INT, new SignalHandler() {
			@Override
			public void handle(Signal signal) {
				deployFuture.cancel(true);
				retrievalThread.isRunning = false;
				retrievalThread.interrupt();
			}
		});

		try {
			if (queryTimeoutMS > 0) {
				LOG.info("Waiting for job finish or timeout in " + queryTimeoutMS + " ms.");
				try {
					deployFuture.get(queryTimeoutMS, TimeUnit.MILLISECONDS);
				} catch (final TimeoutException e) {
					deployFuture.cancel(true);
					retrievalThread.isRunning = false;
					retrievalThread.interrupt();
				}
			} else {
				LOG.info("Waiting for job finished.");
				deployFuture.get();
			}
		} catch (final InterruptedException | ExecutionException e) {
			close();
			throw new SqlExecutionException("Encounter unexpected error while deploying the job.", e);
		} catch (final CancellationException e) {
			LOG.info("Cancel job, fetch and display the finalized result");
			// query cancelled, ignore cancellation exception, fetch and display the finalized result.
		} finally {
			if (previousHandler != null) {
				Signal.handle(SIGNAL_INT, previousHandler);
			}
		}
	}

	@Override
	public TableSink<?> getTableSink() {
		return collectTableSink;
	}

	@Override
	public void close() {
		if (retrievalThread.isAlive()) {
			retrievalThread.isRunning = false;
			retrievalThread.interrupt();
		}
		iterator.close();
		executor.shutdown();
	}

	void processInsert(Row row) {
		finalizedTable.add(row);
		rowPositionCache.put(row, finalizedTable.size() - 1);
	}

	void processDelete(Row row) {
		final Integer cachedPos = rowPositionCache.get(row);
		final int startSearchPos;
		if (cachedPos != null) {
			startSearchPos = Math.min(cachedPos, finalizedTable.size() - 1);
		} else {
			startSearchPos = finalizedTable.size() - 1;
		}

		for (int i = startSearchPos; i >= 0; i--) {
			if (finalizedTable.get(i).equals(row)) {
				finalizedTable.remove(i);
				rowPositionCache.remove(row);
				break;
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private class ResultRetrievalThread extends Thread {

		public volatile boolean isRunning = true;

		@Override
		public void run() {
			try {
				while (isRunning && iterator.hasNext()) {
					final Tuple2<Boolean, Row> change = iterator.next();
					if (change.f0) {
						processInsert(change.f1);
					} else {
						processDelete(change.f1);
					}
				}
			} catch (RuntimeException e) {
				// ignore socket exceptions
			}

			// no result anymore
			// either the job is down or an error occurred.
			isRunning = false;
			resultLatch.countDown();
		}
	}
}
