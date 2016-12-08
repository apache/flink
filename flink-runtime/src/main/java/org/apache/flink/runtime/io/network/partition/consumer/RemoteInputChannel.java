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

package org.apache.flink.runtime.io.network.partition.consumer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.metrics.groups.IOMetricGroup;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input channel, which requests a remote partition queue.
 */
public class RemoteInputChannel extends InputChannel {

	/** ID to distinguish this channel from other channels sharing the same TCP connection. */
	private final InputChannelID id = new InputChannelID();

	/** The connection to use to request the remote partition. */
	private final ConnectionID connectionId;

	/** The connection manager to use connect to the remote partition provider. */
	private final ConnectionManager connectionManager;

	/**
	 * The received buffers. Received buffers are enqueued by the network I/O thread and the queue
	 * is consumed by the receiving task thread.
	 */
	private final ArrayDeque<Buffer> receivedBuffers = new ArrayDeque<Buffer>();

	/**
	 * Flag indicating whether this channel has been released. Either called by the receiving task
	 * thread or the task manager actor.
	 */
	private final AtomicBoolean isReleased = new AtomicBoolean();

	private final int capacityLimit;

	/** Client to establish a (possibly shared) TCP connection and request the partition. */
	private volatile PartitionRequestClient partitionRequestClient;

	/**
	 * Callback for capacity constrained input channels. Called when we fall
	 * below the limit constraint when consuming a buffer.
	 */
	private volatile CapacityAvailabilityListener capacityCallback;

	/**
	 * The next expected sequence number for the next buffer. This is modified by the network
	 * I/O thread only.
	 */
	private int expectedSequenceNumber = 0;

	public RemoteInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionID partitionId,
		ConnectionID connectionId,
		ConnectionManager connectionManager,
		Tuple2<Integer, Integer> initialAndMaxBackoff,
		IOMetricGroup metrics,
		int capacityLimit) {

		super(inputGate, channelIndex, partitionId, initialAndMaxBackoff, metrics.getNumBytesInRemoteCounter());

		checkArgument(capacityLimit >= 0);

		this.connectionId = checkNotNull(connectionId);
		this.connectionManager = checkNotNull(connectionManager);
		this.capacityLimit = capacityLimit;
	}

	@VisibleForTesting
	public int getCapacityLimit() {
		return capacityLimit;
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	/**
	 * Requests a remote subpartition.
	 */
	@Override
	public void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
		if (partitionRequestClient == null) {
			// Create a client and request the partition
			partitionRequestClient = connectionManager
				.createPartitionRequestClient(connectionId);

			partitionRequestClient.requestSubpartition(partitionId, subpartitionIndex, this, 0);
		}
	}

	/**
	 * Retriggers a remote subpartition request.
	 */
	void retriggerSubpartitionRequest(int subpartitionIndex) throws IOException, InterruptedException {
		checkState(partitionRequestClient != null, "Missing initial subpartition request.");

		if (increaseBackoff()) {
			partitionRequestClient.requestSubpartition(
				partitionId, subpartitionIndex, this, getCurrentBackoff());
		} else {
			failPartitionRequest();
		}
	}

	@Override
	public BufferAndAvailability getNextBuffer() throws IOException {
		checkState(!isReleased.get(), "Queried for a buffer after channel has been closed.");
		checkState(partitionRequestClient != null, "Queried for a buffer before requesting a queue.");

		checkError();

		final Buffer next;
		final int remaining;

		synchronized (receivedBuffers) {
			next = receivedBuffers.poll();
			remaining = receivedBuffers.size();

			CapacityAvailabilityListener listener;
			if (capacityLimit > 0 && capacityLimit - 1 == remaining && (listener = capacityCallback) != null) {
				// we just cleared up capacity, so notify the callback
				capacityCallback = null;
				listener.capacityAvailable();
			}
		}

		numBytesIn.inc(next.getSize());
		return new BufferAndAvailability(next, remaining > 0);
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	void sendTaskEvent(TaskEvent event) throws IOException {
		checkState(!isReleased.get(), "Tried to send task event to producer after channel has been released.");
		checkState(partitionRequestClient != null, "Tried to send task event to producer before requesting a queue.");

		checkError();

		partitionRequestClient.sendTaskEvent(partitionId, event, this);
	}

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	@Override
	boolean isReleased() {
		return isReleased.get();
	}

	@Override
	void notifySubpartitionConsumed() {
		// Nothing to do
	}

	/**
	 * Releases all received buffers and closes the partition request client.
	 */
	@Override
	void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {
			synchronized (receivedBuffers) {
				Buffer buffer;
				while ((buffer = receivedBuffers.poll()) != null) {
					buffer.recycle();
				}
			}

			// The released flag has to be set before closing the connection to ensure that
			// buffers received concurrently with closing are properly recycled.
			if (partitionRequestClient != null) {
				partitionRequestClient.close(this);
			} else {
				connectionManager.closeOpenChannelConnections(connectionId);
			}
		}
	}

	void failPartitionRequest() {
		setError(new PartitionNotFoundException(partitionId));
	}

	@Override
	public String toString() {
		return "RemoteInputChannel [" + partitionId + " at " + connectionId + "]";
	}

	// ------------------------------------------------------------------------
	// Network I/O notifications (called by network I/O thread)
	// ------------------------------------------------------------------------

	public int getNumberOfQueuedBuffers() {
		synchronized (receivedBuffers) {
			return receivedBuffers.size();
		}
	}

	public InputChannelID getInputChannelId() {
		return id;
	}

	public BufferProvider getBufferProvider() throws IOException {
		if (isReleased.get()) {
			return null;
		}

		return inputGate.getBufferProvider();
	}

	/**
	 * Queues a buffer from the network.
	 * 
	 * <p>If the channel is capacity constrained and the maximum capacity would be exceeded by this operation,
	 * the method returns {@code false} and installs the provided callback. The callback will be called
	 * as soon as capacity becomes available.
	 * 
	 * @param buffer The buffer to add to the remote input channel.
	 * @param sequenceNumber The sequence number of the buffer.
	 * @param callback The callback that will be installed when the channel is capacity constrained and
	 *                 the capacity is exceeded.
	 * 
	 * @return True, if the channel accepted and queued the buffer, false it it did not
	 *         accept the buffer due to back pressure and installed the callback.
	 */
	public boolean onBuffer(Buffer buffer, int sequenceNumber, @Nullable CapacityAvailabilityListener callback) {
		boolean success = false;

		try {
			synchronized (receivedBuffers) {
				if (!isReleased.get()) {
					// check if we would violate the capacity constraint and need to apply back pressure
					if (capacityLimit > 0 && receivedBuffers.size() >= capacityLimit && buffer.isBuffer()) {
						capacityCallback = callback;
						success = true;
						return false;
					}

					// check that the sequence numbers match (this is a check for corruption of the network stream)
					if (expectedSequenceNumber == sequenceNumber) {
						int available = receivedBuffers.size();
						receivedBuffers.add(buffer);
						expectedSequenceNumber++;

						if (available == 0) {
							notifyChannelNonEmpty();
						}

						success = true;
						return true;
					}
					else {
						onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
						return true; // this quasi accepted the buffer, no need for a callback
					}
				}
				else {
					// a released gate accepts everything and discards it
					return true;
				}
			}
		}
		finally {
			if (!success) {
				buffer.recycle();
			}
		}
	}

	public void onEmptyBuffer(int sequenceNumber) {
		synchronized (receivedBuffers) {
			if (!isReleased.get()) {
				if (expectedSequenceNumber == sequenceNumber) {
					expectedSequenceNumber++;
				} else {
					onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
				}
			}
		}
	}

	public void onFailedPartitionRequest() {
		inputGate.triggerPartitionStateCheck(partitionId);
	}

	public void onError(Throwable cause) {
		setError(cause);
	}

	// ------------------------------------------------------------------------

	public static class BufferReorderingException extends IOException {

		private static final long serialVersionUID = -888282210356266816L;

		private final int expectedSequenceNumber;

		private final int actualSequenceNumber;

		public BufferReorderingException(int expectedSequenceNumber, int actualSequenceNumber) {
			this.expectedSequenceNumber = expectedSequenceNumber;
			this.actualSequenceNumber = actualSequenceNumber;
		}

		@Override
		public String getMessage() {
			return String.format("Buffer re-ordering: expected buffer with sequence number %d, but received %d.",
				expectedSequenceNumber, actualSequenceNumber);
		}
	}

	// ------------------------------------------------------------------------

	public interface CapacityAvailabilityListener {

		void capacityAvailable();
	}
}
