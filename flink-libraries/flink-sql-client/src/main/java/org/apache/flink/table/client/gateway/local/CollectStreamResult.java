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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.experimental.SocketStreamIterator;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.net.InetAddress;

/**
 * A result that works similarly to {@link DataStreamUtils#collect(DataStream)}.
 *
 * @param <C> cluster id to which this result belongs to
 */
public abstract class CollectStreamResult<C> implements DynamicResult<C> {

	private final TypeInformation<Row> outputType;
	private final SocketStreamIterator<Tuple2<Boolean, Row>> iterator;
	private final CollectStreamTableSink collectTableSink;
	private final ResultRetrievalThread retrievalThread;
	private final JobMonitoringThread monitoringThread;
	private Runnable program;
	private C clusterId;

	protected final Object resultLock;
	protected SqlExecutionException executionException;

	public CollectStreamResult(TypeInformation<Row> outputType, ExecutionConfig config,
			InetAddress gatewayAddress, int gatewayPort) {
		this.outputType = outputType;

		resultLock = new Object();

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
		collectTableSink = new CollectStreamTableSink(iterator.getBindAddress(), iterator.getPort(), serializer);
		retrievalThread = new ResultRetrievalThread();
		monitoringThread = new JobMonitoringThread();
	}

	@Override
	public void setClusterId(C clusterId) {
		if (this.clusterId != null) {
			throw new IllegalStateException("Cluster id is already present.");
		}
		this.clusterId = clusterId;
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return outputType;
	}

	@Override
	public void startRetrieval(Runnable program) {
		// start listener thread
		retrievalThread.start();

		// start program
		this.program = program;
		monitoringThread.start();
	}

	@Override
	public TableSink<?> getTableSink() {
		return collectTableSink;
	}

	@Override
	public void close() {
		retrievalThread.isRunning = false;
		retrievalThread.interrupt();
		monitoringThread.interrupt();
		iterator.close();
	}

	// --------------------------------------------------------------------------------------------

	protected <T> TypedResult<T> handleMissingResult() {
		// check if the monitoring thread is still there
		// we need to wait until we know what is going on
		if (monitoringThread.isAlive()) {
			return TypedResult.empty();
		}
		// the job finished with an exception
		else if (executionException != null) {
			throw executionException;
		}
		// we assume that a bounded job finished
		else {
			return TypedResult.endOfStream();
		}
	}

	protected boolean isRetrieving() {
		return retrievalThread.isRunning;
	}

	protected abstract void processRecord(Tuple2<Boolean, Row> change);

	// --------------------------------------------------------------------------------------------

	private class JobMonitoringThread extends Thread {

		@Override
		public void run() {
			try {
				program.run();
			} catch (SqlExecutionException e) {
				executionException = e;
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
