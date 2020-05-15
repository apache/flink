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
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.CheckpointListener;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
 * <h2>Communication Protocol Explanation</h2>
 *
 * <p>We maintain the following variables in this communication protocol
 * <ol>
 *     <li><strong>version</strong>: This variable will be set to a random value when the sink opens.
 *         Client discovers that the sink has restarted if this variable is different.</li>
 *     <li><strong>offset</strong>: This indicates that client has successfully received the results
 *         before this offset. Sink can safely throw these results away.</li>
 *     <li><strong>lastCheckpointedOffset</strong>:
 *         This is the value of <code>offset</code> when the checkpoint happens. This value will be
 *         restored from the checkpoint and set back to <code>offset</code> when the sink restarts.</li>
 * </ol>
 *
 * <p>Client will put <code>version</code> and <code>offset</code> into the request, indicating that
 * it thinks what the current version is and it has received this much results.
 *
 * <p>Sink will check the validity of the request. If <code>version</code> mismatches or <code>offset</code>
 * is smaller than expected, sink will send back the current <code>version</code> and
 * <code>lastCheckpointedOffset</code> with an empty result list to indicate an invalid request.
 *
 * <p>If the request is valid, sink prepares some results starting from <code>offset</code> and sends them
 * back to the client with <code>lastCheckpointedOffset</code>.
 *
 * <p>For client who wants exactly-once semantics, when receiving the response, the client will check for
 * the following conditions:
 * <ol>
 *     <li>If the version mismatches, client knows that sink has restarted. It will throw away all uncheckpointed
 *         results after <code>lastCheckpointedOffset</code>.</li>
 *     <li>Otherwise the version matches. If <code>lastCheckpointedOffset</code> increases, client knows that
 *         a checkpoint happens. It can now move all results before this offset to a user-visible buffer. If
 *         the response also contains new results, client will now move these new results into uncheckpointed
 *         buffer.</li>
 * </ol>
 *
 * <p>Note that
 * <ol>
 *     <li>user can only see results before a <code>lastCheckpointedOffset</code>, and</li>
 *     <li>client will go back to the latest <code>lastCheckpointedOffset</code> when sink restarts,</li>
 * </ol>
 * client will never throw away results in user-visible buffer.
 * So this communication protocol achieves exactly-once semantics.
 *
 * @param <IN> type of results to be written into the sink.
 */
@Internal
public class CollectSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction, CheckpointListener {

	private static final Logger LOG = LoggerFactory.getLogger(CollectSinkFunction.class);

	private final TypeSerializer<IN> serializer;
	private final int maxResultsPerBatch;
	private final int maxResultsBuffered;
	private final String finalResultListAccumulatorName;
	private final String finalResultOffsetAccumulatorName;

	private transient OperatorEventGateway eventGateway;

	private transient LinkedList<IN> bufferedResults;
	private transient ReentrantLock bufferedResultsLock;
	private transient Condition bufferNotFullCondition;

	// this version indicates whether the sink has restarted or not
	private transient String version;
	// this offset acts as an acknowledgement,
	// results before this offset can be safely thrown away
	private transient long offset;
	private transient long lastCheckpointedOffset;

	private transient ServerThread serverThread;

	private transient ListState<IN> bufferedResultsState;
	private transient ListState<Long> offsetState;
	private transient Map<Long, Long> uncompletedCheckpointMap;

	public CollectSinkFunction(
			TypeSerializer<IN> serializer,
			int maxResultsPerBatch,
			String finalResultListAccumulatorName,
			String finalResultOffsetAccumulatorName) {
		this.serializer = serializer;
		this.maxResultsPerBatch = maxResultsPerBatch;
		this.maxResultsBuffered = maxResultsPerBatch * 2;
		this.finalResultListAccumulatorName = finalResultListAccumulatorName;
		this.finalResultOffsetAccumulatorName = finalResultOffsetAccumulatorName;
	}

	private void initBuffer() {
		if (bufferedResults != null) {
			return;
		}

		bufferedResults = new LinkedList<>();
		bufferedResultsLock = new ReentrantLock();
		bufferNotFullCondition = bufferedResultsLock.newCondition();

		offset = 0;
		lastCheckpointedOffset = offset;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		initBuffer();

		bufferedResultsState =
			context.getOperatorStateStore().getListState(
				new ListStateDescriptor<>("bufferedResultsState", serializer));
		bufferedResults.clear();
		for (IN result : bufferedResultsState.get()) {
			bufferedResults.add(result);
		}

		offsetState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>("offsetState", Long.class));
		offset = 0;
		// there must be only 1 element in this state when restoring
		for (long value : offsetState.get()) {
			offset = value;
		}
		lastCheckpointedOffset = offset;

		uncompletedCheckpointMap = new HashMap<>();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		bufferedResultsLock.lock();
		try {
			bufferedResultsState.clear();
			bufferedResultsState.addAll(bufferedResults);

			offsetState.clear();
			offsetState.add(offset);

			uncompletedCheckpointMap.put(context.getCheckpointId(), offset);
		} finally {
			bufferedResultsLock.unlock();
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		Preconditions.checkState(
			getRuntimeContext().getNumberOfParallelSubtasks() == 1,
			"The parallelism of CollectSinkFunction must be 1");

		initBuffer();

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
		bufferedResultsLock.lock();
		try {
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
		try {
			// put results not consumed by the client into the accumulator
			// so that we do not block the closing procedure while not throwing results away
			SerializedListAccumulator<IN> listAccumulator = new SerializedListAccumulator<>();
			for (IN result : bufferedResults) {
				listAccumulator.add(result, serializer);
			}
			LongCounter offsetAccumulator = new LongCounter(offset);
			getRuntimeContext().addAccumulator(finalResultListAccumulatorName, listAccumulator);
			getRuntimeContext().addAccumulator(finalResultOffsetAccumulatorName, offsetAccumulator);
		} finally {
			bufferedResultsLock.unlock();
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		lastCheckpointedOffset = uncompletedCheckpointMap.get(checkpointId);
		uncompletedCheckpointMap.remove(checkpointId);
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

		private ServerThread() throws Exception {
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
						inStream = new DataInputViewStreamWrapper(this.connection.getInputStream());
						outStream = new DataOutputViewStreamWrapper(this.connection.getOutputStream());
						LOG.info("Coordinator connection received");
					}

					CollectCoordinationRequest request = new CollectCoordinationRequest(inStream);
					String requestVersion = request.getVersion();
					// client acknowledges that it has successfully received results before this offset,
					// we can safely throw away results before this offset
					long requestOffset = request.getOffset();
					if (LOG.isDebugEnabled()) {
						LOG.debug(
							"Request received, version = " + requestVersion + ", offset = " + requestOffset);
						LOG.debug(
							"Expecting version = " + version + ", offset = " + offset);
					}

					if (!version.equals(requestVersion) || requestOffset < offset) {
						// invalid request
						LOG.warn("Invalid request. Received version = " + requestVersion +
							", offset = " + requestOffset + ", while expected version = "
							+ version + ", offset = " + offset);
						sendBackResults(Collections.emptyList());
						continue;
					}

					// valid request, sending out results
					List<IN> results;
					bufferedResultsLock.lock();
					try {
						int oldSize = bufferedResults.size();
						int ackedNum = Math.min((int) (requestOffset - offset), oldSize);
						int nextBatchSize = Math.min(ackedNum + maxResultsPerBatch, oldSize) - ackedNum;

						if (LOG.isDebugEnabled()) {
							LOG.debug("Preparing " + nextBatchSize + " results");
						}

						// drop acked results
						for (int i = 0; i < ackedNum; i++) {
							bufferedResults.removeFirst();
							offset++;
						}

						// prepare next result batch
						results = new ArrayList<>(bufferedResults.subList(0, nextBatchSize));

						if (oldSize >= maxResultsBuffered && bufferedResults.size() < maxResultsBuffered) {
							bufferNotFullCondition.signal();
						}
					} finally {
						bufferedResultsLock.unlock();
					}
					sendBackResults(results);
				} catch (IOException e) {
					// IOException occurs, just close current connection
					// client will come with the same offset if it needs the same batch of results
					if (LOG.isDebugEnabled()) {
						LOG.debug("Collect sink server encounters an IOException", e);
					}
					closeCurrentConnection();
				}
			}
		}

		private void close() {
			running = false;
			closeCurrentConnection();
			closeServer();
		}

		private InetSocketAddress getServerSocketAddress() {
			RuntimeContext context = getRuntimeContext();
			Preconditions.checkState(
				context instanceof StreamingRuntimeContext,
				"CollectSinkFunction can only be used in StreamTask");
			StreamingRuntimeContext streamingContext = (StreamingRuntimeContext) context;
			String taskManagerAddress = streamingContext.getTaskManagerRuntimeInfo().getTaskManagerExternalAddress();
			return new InetSocketAddress(taskManagerAddress, serverSocket.getLocalPort());
		}

		private InetAddress getBindAddress() {
			RuntimeContext context = getRuntimeContext();
			Preconditions.checkState(
				context instanceof StreamingRuntimeContext,
				"CollectSinkFunction can only be used in StreamTask");
			StreamingRuntimeContext streamingContext = (StreamingRuntimeContext) context;
			String bindAddress = streamingContext.getTaskManagerRuntimeInfo().getTaskManagerBindAddress();

			if (bindAddress != null) {
				try {
					return InetAddress.getByName(bindAddress);
				} catch (UnknownHostException e) {
					return null;
				}
			}
			return null;
		}

		private void sendBackResults(List<IN> results) throws IOException {
			CollectCoordinationResponse<IN> response = new CollectCoordinationResponse<>(
				version, lastCheckpointedOffset, results, serializer);
			response.serialize(outStream);
		}

		private void closeCurrentConnection() {
			try {
				if (connection != null) {
					connection.close();
					connection = null;
				}
			} catch (Exception e) {
				LOG.warn("Error occurs when closing client connections in CollectSinkFunction", e);
			}
		}

		private void closeServer() {
			try {
				serverSocket.close();
			} catch (Exception e) {
				LOG.warn("Error occurs when closing server in CollectSinkFunction", e);
			}
		}
	}
}
