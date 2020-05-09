/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A sink function that collects query results and sends them back to the client.
 *
 * <p>This sink works by limiting the number of results buffered in it (can be configured) so
 * that when the buffer is full, it back-pressures the job until the client consumes some results.
 *
 * <p>NOTE: When using this sink, make sure that its parallelism is 1, and make sure that it is used
 * in a {@link StreamTask}.
 *
 * @param <IN> type of results to be written into the sink.
 */
@Internal
public class CollectSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

	private static final Logger LOG = LoggerFactory.getLogger(CollectSinkFunction.class);

	private final TypeSerializer<IN> serializer;
	private final LinkedList<IN> bufferedResults;
	private final int maxResultsPerBatch;
	private final int maxResultsBuffered;
	private final String finalResultListAccumulatorName;
	private final String finalResultTokenAccumulatorName;

	private final ReentrantLock bufferedResultsLock;
	private final Condition bufferNotFullCondition;

	private OperatorEventGateway eventGateway;

	private String version;
	private long firstBufferedResultToken;
	private long lastCheckpointId;
	private ServerThread serverThread;

	private ListState<IN> bufferedResultsState;
	private ListState<Long> firstBufferedResultTokenState;
	private ListState<Long> lastCheckpointIdState;

	public CollectSinkFunction(
			TypeSerializer<IN> serializer,
			int maxResultsPerBatch,
			String finalResultListAccumulatorName,
			String finalResultTokenAccumulatorName) {
		this.serializer = serializer;
		this.bufferedResults = new LinkedList<>();
		this.maxResultsPerBatch = maxResultsPerBatch;
		this.maxResultsBuffered = maxResultsPerBatch * 2;
		this.finalResultListAccumulatorName = finalResultListAccumulatorName;
		this.finalResultTokenAccumulatorName = finalResultTokenAccumulatorName;

		this.bufferedResultsLock = new ReentrantLock();
		this.bufferNotFullCondition = bufferedResultsLock.newCondition();

		this.firstBufferedResultToken = 0;
		this.lastCheckpointId = Long.MIN_VALUE;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		bufferedResultsLock.lock();

		bufferedResultsState =
			context.getOperatorStateStore().getListState(
				new ListStateDescriptor<>("bufferedResultsState", serializer));
		bufferedResults.clear();
		for (IN result : bufferedResultsState.get()) {
			bufferedResults.add(result);
		}

		firstBufferedResultTokenState =
			context.getOperatorStateStore().getListState(
				new ListStateDescriptor<>("firstBufferedResultTokenState", Long.class));
		firstBufferedResultToken = 0;
		// there must be only 1 element in this state when restoring
		for (long token : firstBufferedResultTokenState.get()) {
			firstBufferedResultToken = token;
		}

		lastCheckpointIdState =
			context.getOperatorStateStore().getListState(
				new ListStateDescriptor<>("lastCheckpointIdState", Long.class));
		lastCheckpointId = Long.MIN_VALUE;
		// there must be only 1 element in this state when restoring
		for (long checkpointId : lastCheckpointIdState.get()) {
			lastCheckpointId = checkpointId;
		}

		bufferedResultsLock.unlock();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		bufferedResultsLock.lock();

		bufferedResultsState.clear();
		bufferedResultsState.addAll(bufferedResults);

		firstBufferedResultTokenState.clear();
		firstBufferedResultTokenState.add(firstBufferedResultToken);

		lastCheckpointId = context.getCheckpointId();
		lastCheckpointIdState.clear();
		lastCheckpointIdState.add(lastCheckpointId);

		bufferedResultsLock.unlock();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		Preconditions.checkState(
			getRuntimeContext().getNumberOfParallelSubtasks() == 1,
			"The parallelism of SocketServerSink must be 1");

		// generate a random uuid when the sink is opened
		// so that the client can know if the sink has been restarted
		version = UUID.randomUUID().toString();

		serverThread = new ServerThread();
		serverThread.start();

		// sending socket server address to coordinator
		Preconditions.checkNotNull(eventGateway, "Operator event gateway hasn't been set");
		InetSocketAddress address = serverThread.getServerSocketAddress();
		LOG.info("Collect sink server established, address = " + address);

		CollectSinkAddressEvent addressEvent = new CollectSinkAddressEvent(address);
		eventGateway.sendEventToCoordinator(addressEvent);
	}

	@Override
	public void invoke(IN value, Context context) throws Exception {
		try {
			bufferedResultsLock.lock();
			if (bufferedResults.size() >= maxResultsBuffered) {
				bufferNotFullCondition.await();
			}
			bufferedResults.add(value);
		} finally {
			bufferedResultsLock.unlock();
		}
	}

	@Override
	public void close() throws Exception {
		serverThread.close();
	}

	public void accumulateFinalResults() throws Exception {
		bufferedResultsLock.lock();
		// put results not consumed by the client into the accumulator
		// so that we do not block the closing procedure while not throwing results away
		SerializedListAccumulator<IN> listAccumulator = new SerializedListAccumulator<>();
		for (IN result : bufferedResults) {
			listAccumulator.add(result, serializer);
		}
		LongCounter tokenAccumulator = new LongCounter(firstBufferedResultToken);
		getRuntimeContext().addAccumulator(finalResultListAccumulatorName, listAccumulator);
		getRuntimeContext().addAccumulator(finalResultTokenAccumulatorName, tokenAccumulator);
		bufferedResultsLock.unlock();
	}

	public void setOperatorEventGateway(OperatorEventGateway eventGateway) {
		this.eventGateway = eventGateway;
	}

	/**
	 * The thread that runs the socket server.
	 */
	private class ServerThread extends Thread {

		private final ServerSocket serverSocket;

		private boolean running;

		private Socket connection;
		private DataInputViewStreamWrapper inStream;
		private DataOutputViewStreamWrapper outStream;

		ServerThread() throws Exception {
			this.serverSocket = new ServerSocket(0, 0, getBindAddress());
			this.running = true;
		}

		@Override
		public void run() {
			while (running) {
				try {
					if (connection == null) {
						// waiting for coordinator to connect
						connection = serverSocket.accept();
						LOG.debug("Coordinator connection received");

						inStream = new DataInputViewStreamWrapper(this.connection.getInputStream());
						outStream = new DataOutputViewStreamWrapper(this.connection.getOutputStream());
					}

					CollectCoordinationRequest.DeserializedRequest request =
						CollectCoordinationRequest.deserialize(inStream);
					String version = request.getVersion();
					long token = request.getToken();
					LOG.debug("Request received, version = " + version + ", token = " + token);

					String expectedVersion = CollectSinkFunction.this.version;
					long firstBufferedToken = CollectSinkFunction.this.firstBufferedResultToken;
					LOG.debug(
						"Expecting version = " + expectedVersion + ", firstBufferedToken = " + firstBufferedToken);

					if (!expectedVersion.equals(version) || token < firstBufferedResultToken) {
						// invalid request
						LOG.debug("Invalid request");
						sendBufferedResults(0, 0);
						continue;
					}

					// valid request, sending out results
					bufferedResultsLock.lock();

					int oldSize = bufferedResults.size();
					int start = Math.min((int) (token - firstBufferedResultToken), oldSize);
					int end = Math.min(start + maxResultsPerBatch, oldSize);
					LOG.debug("Sending back " + (end - start) + " results");
					sendBufferedResults(start, end - start);

					if (oldSize >= maxResultsBuffered && bufferedResults.size() < maxResultsBuffered) {
						bufferNotFullCondition.signal();
					}
					bufferedResultsLock.unlock();
				} catch (Exception e) {
					// exception occurs, just close current connection
					// client will come with the same token if it needs the same batch of results
					LOG.debug("Collect sink server encounters an exception", e);
					closeCurrentConnection();
				}
			}
		}

		public void close() {
			running = false;
			closeCurrentConnection();
			closeServer();
		}

		public InetSocketAddress getServerSocketAddress() {
			RuntimeContext context = getRuntimeContext();
			Preconditions.checkState(
				context instanceof StreamingRuntimeContext,
				"CollectSinkFunction can only be used in StreamTask");
			StreamingRuntimeContext streamingContext = (StreamingRuntimeContext) context;
			String taskManagerAddress = streamingContext.getTaskManagerRuntimeInfo().getTaskManagerAddress();
			return new InetSocketAddress(taskManagerAddress, serverSocket.getLocalPort());
		}

		private InetAddress getBindAddress() {
			RuntimeContext context = getRuntimeContext();
			Preconditions.checkState(
				context instanceof StreamingRuntimeContext,
				"CollectSinkFunction can only be used in StreamTask");
			StreamingRuntimeContext streamingContext = (StreamingRuntimeContext) context;
			String bindHost = streamingContext
				.getTaskManagerRuntimeInfo()
				.getConfiguration()
				.getString(TaskManagerOptions.BIND_HOST);

			if (bindHost != null) {
				try {
					return InetAddress.getByName(bindHost);
				} catch (UnknownHostException e) {
					return null;
				}
			}
			return null;
		}

		private void sendBufferedResults(int offset, int length) throws IOException {
			// skip `offset` results
			for (int i = 0; i < offset; i++) {
				bufferedResults.removeFirst();
				firstBufferedResultToken++;
			}

			// send out `length` results
			List<IN> results = new ArrayList<>(length);
			Iterator<IN> iterator = bufferedResults.iterator();
			for (int i = 0; i < length; i++) {
				results.add(iterator.next());
			}
			byte[] bytes = CollectCoordinationResponse.serialize(
				version, firstBufferedResultToken, lastCheckpointId, results, serializer);
			outStream.writeInt(bytes.length);
			outStream.write(bytes);
		}

		private void closeCurrentConnection() {
			try {
				if (connection != null) {
					connection.close();
					connection = null;
				}
			} catch (Exception e) {
				LOG.warn("Error occurs when closing client connections in SocketServerSink", e);
			}
		}

		private void closeServer() {
			try {
				serverSocket.close();
			} catch (Exception e) {
				LOG.warn("Error occurs when closing server in SocketServerSink", e);
			}
		}
	}
}
