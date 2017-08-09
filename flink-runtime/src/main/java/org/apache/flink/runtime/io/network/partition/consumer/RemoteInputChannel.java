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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input channel, which requests a remote partition queue.
 */
public class RemoteInputChannel extends InputChannel implements BufferRecycler, BufferListener {

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
	private final ArrayDeque<Buffer> receivedBuffers = new ArrayDeque<>();

	/**
	 * Flag indicating whether this channel has been released. Either called by the receiving task
	 * thread or the task manager actor.
	 */
	private final AtomicBoolean isReleased = new AtomicBoolean();

	/** Client to establish a (possibly shared) TCP connection and request the partition. */
	private volatile PartitionRequestClient partitionRequestClient;

	/**
	 * The next expected sequence number for the next buffer. This is modified by the network
	 * I/O thread only.
	 */
	private int expectedSequenceNumber = 0;

	/** The initial number of exclusive buffers assigned to this channel. */
	private int initialCredit;

	/** The current available buffers including both exclusive buffers and requested floating buffers. */
	private final ArrayDeque<Buffer> availableBuffers = new ArrayDeque<>();

	/** The number of available buffers that have not been announced to the producer yet. */
	private final AtomicInteger unannouncedCredit = new AtomicInteger(0);

	/** The number of unsent buffers in the producer's sub partition. */
	private final AtomicInteger senderBacklog = new AtomicInteger(0);

	/** The tag indicates whether this channel is waiting for additional floating buffers from the buffer pool. */
	private final AtomicBoolean isWaitingForFloatingBuffers = new AtomicBoolean(false);

	public RemoteInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionID partitionId,
		ConnectionID connectionId,
		ConnectionManager connectionManager,
		TaskIOMetricGroup metrics) {

		this(inputGate, channelIndex, partitionId, connectionId, connectionManager, 0, 0, metrics);
	}

	public RemoteInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionID partitionId,
		ConnectionID connectionId,
		ConnectionManager connectionManager,
		int initialBackOff,
		int maxBackoff,
		TaskIOMetricGroup metrics) {

		super(inputGate, channelIndex, partitionId, initialBackOff, maxBackoff, metrics.getNumBytesInRemoteCounter());

		this.connectionId = checkNotNull(connectionId);
		this.connectionManager = checkNotNull(connectionManager);
	}

	/**
	 * Assigns exclusive buffers to this input channel, and this method should be called only once
	 * after this input channel is created.
	 */
	void assignExclusiveSegments(List<MemorySegment> segments) {
		checkState(this.initialCredit == 0, "Bug in input channel setup logic: exclusive buffers have " +
			"already been set for this input channel.");

		checkNotNull(segments);
		checkArgument(segments.size() > 0, "The number of exclusive buffers per channel should be larger than 0.");

		this.initialCredit = segments.size();

		synchronized(availableBuffers) {
			for (MemorySegment segment : segments) {
				availableBuffers.add(new Buffer(segment, this));
			}
		}
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	/**
	 * Requests a remote subpartition.
	 */
	@Override
	void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
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
	BufferAndAvailability getNextBuffer() throws IOException {
		checkState(!isReleased.get(), "Queried for a buffer after channel has been closed.");
		checkState(partitionRequestClient != null, "Queried for a buffer before requesting a queue.");

		checkError();

		final Buffer next;
		final int remaining;

		synchronized (receivedBuffers) {
			next = receivedBuffers.poll();
			remaining = receivedBuffers.size();
		}

		numBytesIn.inc(next.getSizeUnsafe());
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
	 * Releases all exclusive and floating buffers, closes the partition request client.
	 */
	@Override
	void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {

			// Gather all exclusive buffers and recycle them to global pool in batch
			final List<MemorySegment> exclusiveRecyclingSegments = new ArrayList<>();

			synchronized (receivedBuffers) {
				Buffer buffer;
				while ((buffer = receivedBuffers.poll()) != null) {
					if (buffer.getRecycler() == this) {
						exclusiveRecyclingSegments.add(buffer.getMemorySegment());
					} else {
						buffer.recycle();
					}
				}
			}

			synchronized (availableBuffers) {
				Buffer buffer;
				while ((buffer = availableBuffers.poll()) != null) {
					if (buffer.getRecycler() == this) {
						exclusiveRecyclingSegments.add(buffer.getMemorySegment());
					} else {
						buffer.recycle();
					}
				}
			}

			if (exclusiveRecyclingSegments.size() > 0) {
				inputGate.returnExclusiveSegments(exclusiveRecyclingSegments);
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

	private void failPartitionRequest() {
		setError(new PartitionNotFoundException(partitionId));
	}

	@Override
	public String toString() {
		return "RemoteInputChannel [" + partitionId + " at " + connectionId + "]";
	}

	// ------------------------------------------------------------------------
	// Credit-based
	// ------------------------------------------------------------------------

	/**
	 * Enqueue this input channel in the pipeline for sending unannounced credits to producer.
	 */
	void notifyCreditAvailable() {
		//TODO in next PR
	}

	/**
	 * Exclusive buffer is recycled to this input channel directly and it may trigger notify
	 * credit to producer.
	 *
	 * @param segment The exclusive segment of this channel.
	 */
	@Override
	public void recycle(MemorySegment segment) {
		synchronized (availableBuffers) {
			// Important: the isReleased check should be inside the synchronized block.
			// that way the segment can also be returned to global pool after added into
			// the available queue during releasing all resources.
			if (isReleased.get()) {
				try {
					inputGate.returnExclusiveSegments(Arrays.asList(segment));
					return;
				} catch (Throwable t) {
					ExceptionUtils.rethrow(t);
				}
			}
			availableBuffers.add(new Buffer(segment, this));
		}

		if (unannouncedCredit.getAndAdd(1) == 0) {
			notifyCreditAvailable();
		}
	}

	public int getNumberOfAvailableBuffers() {
		synchronized (availableBuffers) {
			return availableBuffers.size();
		}
	}

	/**
	 * The Buffer pool notifies this channel of an available floating buffer. If the channel is released or
	 * currently does not need extra buffers, the buffer should be recycled to the buffer pool. Otherwise,
	 * the buffer will be added into the <tt>availableBuffers</tt> queue and the unannounced credit is
	 * increased by one.
	 *
	 * @param buffer Buffer that becomes available in buffer pool.
	 * @return True when this channel is waiting for more floating buffers, otherwise false.
	 */
	@Override
	public boolean notifyBufferAvailable(Buffer buffer) {
		checkState(isWaitingForFloatingBuffers.get(), "This channel should be waiting for floating buffers.");

		synchronized (availableBuffers) {
			// Important: the isReleased check should be inside the synchronized block.
			if (isReleased.get() || availableBuffers.size() >= senderBacklog.get()) {
				isWaitingForFloatingBuffers.set(false);
				buffer.recycle();

				return false;
			}

			availableBuffers.add(buffer);

			if (unannouncedCredit.getAndAdd(1) == 0) {
				notifyCreditAvailable();
			}

			if (availableBuffers.size() >= senderBacklog.get()) {
				isWaitingForFloatingBuffers.set(false);
				return false;
			} else {
				return true;
			}
		}
	}

	@Override
	public void notifyBufferDestroyed() {
		if (!isWaitingForFloatingBuffers.compareAndSet(true, false)) {
			throw new IllegalStateException("This channel should be waiting for floating buffers currently.");
		}
	}

	// ------------------------------------------------------------------------
	// Network I/O notifications (called by network I/O thread)
	// ------------------------------------------------------------------------

	public int getNumberOfQueuedBuffers() {
		synchronized (receivedBuffers) {
			return receivedBuffers.size();
		}
	}

	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return Math.max(0, receivedBuffers.size());
	}

	public InputChannelID getInputChannelId() {
		return id;
	}

	public int getInitialCredit() {
		return initialCredit;
	}

	public BufferProvider getBufferProvider() throws IOException {
		if (isReleased.get()) {
			return null;
		}

		return inputGate.getBufferProvider();
	}

	public void onBuffer(Buffer buffer, int sequenceNumber) {
		boolean success = false;

		try {
			synchronized (receivedBuffers) {
				if (!isReleased.get()) {
					if (expectedSequenceNumber == sequenceNumber) {
						int available = receivedBuffers.size();

						receivedBuffers.add(buffer);
						expectedSequenceNumber++;

						if (available == 0) {
							notifyChannelNonEmpty();
						}

						success = true;
					} else {
						onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
					}
				}
			}
		} finally {
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

	private static class BufferReorderingException extends IOException {

		private static final long serialVersionUID = -888282210356266816L;

		private final int expectedSequenceNumber;

		private final int actualSequenceNumber;

		BufferReorderingException(int expectedSequenceNumber, int actualSequenceNumber) {
			this.expectedSequenceNumber = expectedSequenceNumber;
			this.actualSequenceNumber = actualSequenceNumber;
		}

		@Override
		public String getMessage() {
			return String.format("Buffer re-ordering: expected buffer with sequence number %d, but received %d.",
				expectedSequenceNumber, actualSequenceNumber);
		}
	}
}
