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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
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
 *         restored from the checkpoint and set back to <code>offset</code> when the sink restarts.
 *         Clients who need exactly-once semantics need to rely on this value for the position to
 *         revert when a failover happens.</li>
 * </ol>
 *
 * <p>Client will put <code>version</code> and <code>offset</code> into the request, indicating that
 * it thinks what the current version is and it has received this much results.
 *
 * <p>Sink will check the validity of the request. If <code>version</code> mismatches or <code>offset</code>
 * is smaller than expected, sink will send back the current <code>version</code> and
 * <code>lastCheckpointedOffset</code> with an empty result list.
 *
 * <p>If the request is valid, sink prepares some results starting from <code>offset</code> and sends them
 * back to the client with <code>lastCheckpointedOffset</code>. If there is currently no results starting from
 * <code>offset</code>, sink will not wait but will instead send back an empty result list.
 *
 * <p>For client who wants exactly-once semantics, when receiving the response, the client will check for
 * the following conditions:
 * <ol>
 *     <li>If the version mismatches, client knows that sink has restarted. It will throw away all uncheckpointed
 *         results after <code>lastCheckpointedOffset</code>.</li>
 *     <li>If <code>lastCheckpointedOffset</code> increases, client knows that
 *         a checkpoint happens. It can now move all results before this offset to a user-visible buffer.</li>
 *     <li>If the response also contains new results, client will now move these new results into uncheckpointed
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
 * <p>In order not to block job finishing/cancelling, if there are still results in sink's buffer when job terminates,
 * these results will be sent back to client through accumulators.
 *
 * @param <IN> type of results to be written into the sink.
 */
@Internal
public class CollectSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction, CheckpointListener {

	private static final Logger LOG = LoggerFactory.getLogger(CollectSinkFunction.class);

	private final TypeSerializer<IN> serializer;
	private final long maxBytesPerBatch;
	private final long bufferSizeLimitBytes;
	private final String accumulatorName;

	private transient OperatorEventGateway eventGateway;

	private transient LinkedList<byte[]> buffer;
	private transient long currentBufferBytes;
	private transient ReentrantLock bufferLock;
	private transient Condition bufferCanAddNextResultCondition;
	private transient long invokingRecordBytes;

	// this version indicates whether the sink has restarted or not
	private transient String version;
	// this offset acts as an acknowledgement,
	// results before this offset can be safely thrown away
	private transient long offset;
	private transient long lastCheckpointedOffset;

	private transient ServerThread serverThread;

	private transient ListState<byte[]> bufferState;
	private transient ListState<Long> offsetState;
	private transient SortedMap<Long, Long> uncompletedCheckpointMap;

	public CollectSinkFunction(TypeSerializer<IN> serializer, long maxBytesPerBatch, String accumulatorName) {
		this.serializer = serializer;
		this.maxBytesPerBatch = maxBytesPerBatch;
		this.bufferSizeLimitBytes = maxBytesPerBatch * 2;
		this.accumulatorName = accumulatorName;
	}

	private void initBuffer() {
		if (buffer != null) {
			return;
		}

		buffer = new LinkedList<>();
		currentBufferBytes = 0;
		bufferLock = new ReentrantLock();
		bufferCanAddNextResultCondition = bufferLock.newCondition();

		offset = 0;
		lastCheckpointedOffset = offset;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		initBuffer();

		bufferState =
			context.getOperatorStateStore().getListState(
				new ListStateDescriptor<>("bufferState", BytePrimitiveArraySerializer.INSTANCE));
		for (byte[] value : bufferState.get()) {
			buffer.add(value);
			currentBufferBytes += value.length;
		}

		offsetState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>("offsetState", Long.class));
		// there must be only 1 element in this state when restoring
		for (long value : offsetState.get()) {
			offset = value;
		}
		lastCheckpointedOffset = offset;

		LOG.info("Initializing collect sink state with offset = " + lastCheckpointedOffset +
			", buffered results bytes = " + currentBufferBytes);

		uncompletedCheckpointMap = new TreeMap<>();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		bufferLock.lock();
		try {
			bufferState.clear();
			bufferState.addAll(buffer);

			offsetState.clear();
			offsetState.add(offset);

			uncompletedCheckpointMap.put(context.getCheckpointId(), offset);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Checkpoint begin with checkpointId = " + context.getCheckpointId() +
					", lastCheckpointedOffset = " + lastCheckpointedOffset +
					", buffered results bytes = " + currentBufferBytes);
			}
		} finally {
			bufferLock.unlock();
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

		serverThread = new ServerThread(serializer);
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
		bufferLock.lock();
		try {
			// TODO this implementation is not very effective,
			//  optimize this with MemorySegment if needed
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
			serializer.serialize(value, wrapper);
			invokingRecordBytes = baos.size();

			if (invokingRecordBytes > maxBytesPerBatch) {
				throw new RuntimeException(
					"Record size is too large for CollectSinkFunction. Record size is " + invokingRecordBytes + " bytes, " +
						"but max bytes per batch is only " + maxBytesPerBatch + " bytes. " +
						"Please consider increasing max bytes per batch value.");
			}

			if (currentBufferBytes + invokingRecordBytes > bufferSizeLimitBytes) {
				bufferCanAddNextResultCondition.await();
			}
			buffer.add(baos.toByteArray());
			currentBufferBytes += baos.size();
		} finally {
			bufferLock.unlock();
		}
	}

	@Override
	public void close() throws Exception {
		serverThread.close();
		serverThread.join();
	}

	public void accumulateFinalResults() throws Exception {
		bufferLock.lock();
		try {
			// put results not consumed by the client into the accumulator
			// so that we do not block the closing procedure while not throwing results away
			SerializedListAccumulator<byte[]> accumulator = new SerializedListAccumulator<>();
			accumulator.add(
				serializeAccumulatorResult(offset, version, lastCheckpointedOffset, buffer),
				BytePrimitiveArraySerializer.INSTANCE);
			getRuntimeContext().addAccumulator(accumulatorName, accumulator);
		} finally {
			bufferLock.unlock();
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		lastCheckpointedOffset = uncompletedCheckpointMap.get(checkpointId);
		uncompletedCheckpointMap.headMap(checkpointId + 1).clear();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Checkpoint complete with checkpointId = " + checkpointId +
				", lastCheckpointedOffset = " + lastCheckpointedOffset);
		}
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) {
	}

	public void setOperatorEventGateway(OperatorEventGateway eventGateway) {
		this.eventGateway = eventGateway;
	}

	@VisibleForTesting
	public static byte[] serializeAccumulatorResult(
			long offset,
			String version,
			long lastCheckpointedOffset,
			List<byte[]> buffer) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
		wrapper.writeLong(offset);
		CollectCoordinationResponse finalResponse =
			new CollectCoordinationResponse(version, lastCheckpointedOffset, buffer);
		finalResponse.serialize(wrapper);
		return baos.toByteArray();
	}

	public static Tuple2<Long, CollectCoordinationResponse> deserializeAccumulatorResult(
			byte[] serializedAccResults) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(serializedAccResults);
		DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
		long token = wrapper.readLong();
		CollectCoordinationResponse finalResponse = new CollectCoordinationResponse(wrapper);
		return Tuple2.of(token, finalResponse);
	}

	/**
	 * The thread that runs the socket server.
	 */
	private class ServerThread extends Thread {

		private final TypeSerializer<IN> serializer;
		private final ServerSocket serverSocket;

		private boolean running;

		private Socket connection;
		private DataInputViewStreamWrapper inStream;
		private DataOutputViewStreamWrapper outStream;

		private ServerThread(TypeSerializer<IN> serializer) throws Exception {
			this.serializer = serializer.duplicate();
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
						if (LOG.isDebugEnabled()) {
							// this is normal for the 1st request after the sink (re)starts, so we print debug log
							LOG.debug("Invalid request. Received version = " + requestVersion +
								", offset = " + requestOffset + ", while expected version = "
								+ version + ", offset = " + offset);
						}
						sendBackResults(Collections.emptyList());
						continue;
					}

					// valid request, sending out results
					List<byte[]> nextBatch = new ArrayList<>();
					bufferLock.lock();
					try {
						// drop acked results
						int ackedNum = (int) (requestOffset - offset);
						for (int i = 0; i < ackedNum && !buffer.isEmpty(); i++) {
							byte[] removed = buffer.removeFirst();
							currentBufferBytes -= removed.length;
							offset++;
						}

						// prepare next result batch
						long totalBytes = 0;
						for (byte[] value : buffer) {
							if (totalBytes + value.length <= maxBytesPerBatch) {
								nextBatch.add(value);
								totalBytes += value.length;
							} else {
								break;
							}
						}

						if (currentBufferBytes + invokingRecordBytes <= bufferSizeLimitBytes) {
							bufferCanAddNextResultCondition.signal();
						}
					} finally {
						bufferLock.unlock();
					}
					sendBackResults(nextBatch);
				} catch (Exception e) {
					// Exception occurs, just close current connection
					// client will come with the same offset if it needs the same batch of results
					if (LOG.isDebugEnabled()) {
						// this is normal when sink restarts or job ends, so we print a debug log
						LOG.debug("Collect sink server encounters an exception", e);
					}
					closeCurrentConnection();
				}
			}
		}

		private void close() {
			running = false;
			closeServerSocket();
			closeCurrentConnection();
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

		private void sendBackResults(List<byte[]> serializedResults) throws IOException {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Sending back " + serializedResults.size() + " results");
			}
			CollectCoordinationResponse response =
				new CollectCoordinationResponse(version, lastCheckpointedOffset, serializedResults);
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

		private void closeServerSocket() {
			try {
				serverSocket.close();
			} catch (Exception e) {
				LOG.warn("Error occurs when closing server in CollectSinkFunction", e);
			}
		}
	}
}
