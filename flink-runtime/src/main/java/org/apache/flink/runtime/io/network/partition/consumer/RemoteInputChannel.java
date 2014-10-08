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

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.RemoteAddress;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;

/**
 * An input channel, which requests a remote partition queue.
 */
public class RemoteInputChannel extends InputChannel {

	private static final Logger LOG = LoggerFactory.getLogger(RemoteInputChannel.class);

	private final InputChannelID id;

	private final RemoteAddress producerAddress;

	private final Queue<Buffer> receivedBuffers = new ArrayDeque<Buffer>();

	private final AtomicReference<IOException> ioError = new AtomicReference<IOException>();

	private final AtomicBoolean isReleased = new AtomicBoolean();

	private PartitionRequestClient partitionRequestClient;

	private int expectedSequenceNumber = 0;

	public RemoteInputChannel(
			int channelIndex,
			ExecutionAttemptID producerExecutionId,
			IntermediateResultPartitionID partitionId,
			BufferReader reader,
			RemoteAddress producerAddress) {

		super(channelIndex, producerExecutionId, partitionId, reader);

		/**
		 * This ID is used by the {@link PartitionRequestClient} to distinguish
		 * between receivers, which share the same TCP connection.
		 */
		this.id = new InputChannelID();
		this.producerAddress = producerAddress;
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	@Override
	public void requestIntermediateResultPartition(int queueIndex) throws IOException {
		if (partitionRequestClient == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Requesting queue {} from REMOTE partition {}.", partitionId, queueIndex);
			}

			partitionRequestClient = reader.getConnectionManager().createPartitionRequestClient(producerAddress);

			partitionRequestClient.requestIntermediateResultPartition(producerExecutionId, partitionId, queueIndex, this);
		}
	}

	@Override
	public Buffer getNextBuffer() throws IOException {
		checkState(!isReleased.get(), "Queried for a buffer after channel has been closed.");
		checkState(partitionRequestClient != null, "Queried for a buffer before requesting a queue.");

		checkIoError();

		synchronized (receivedBuffers) {
			Buffer buffer = receivedBuffers.poll();

			// Sanity check that channel is only queried after a notification
			if (buffer == null) {
				throw new IOException("Queried input channel for data although non is available.");
			}

			return buffer;
		}
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		checkState(!isReleased.get(), "Tried to send task event to producer after channel has been released.");
		checkState(partitionRequestClient != null, "Tried to send task event to producer before requesting a queue.");

		checkIoError();

		partitionRequestClient.sendTaskEvent(producerExecutionId, partitionId, event, this);
	}

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	@Override
	public boolean isReleased() {
		return isReleased.get();
	}

	/**
	 * Releases all received buffers and closes the partition request client.
	 */
	@Override
	public void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {
			synchronized (receivedBuffers) {
				Buffer buffer;
				while ((buffer = receivedBuffers.poll()) != null) {
					buffer.recycle();
				}
			}

			if (partitionRequestClient != null) {
				partitionRequestClient.close(this);
			}
		}
	}

	@Override
	public String toString() {
		return "REMOTE " + id + " " + super.toString();
	}

	// ------------------------------------------------------------------------
	// Network I/O notifications (called by network I/O thread)
	// ------------------------------------------------------------------------

	public InputChannelID getInputChannelId() {
		return id;
	}

	public BufferProvider getBufferProvider() throws IOException {
		if (isReleased.get()) {
			return null;
		}

		return reader.getBufferProvider();
	}

	public void onBuffer(Buffer buffer, int sequenceNumber) {
		boolean success = false;

		try {
			if (!isReleased.get()) {
				synchronized (receivedBuffers) {
					if (expectedSequenceNumber == sequenceNumber) {
						receivedBuffers.add(buffer);
						expectedSequenceNumber++;

						notifyReaderAboutAvailableBuffer();

						success = true;

						return;
					}
				}

				IOException error = new BufferReorderingException(expectedSequenceNumber, sequenceNumber);
				ioError.compareAndSet(null, error);
			}
		}
		finally {
			if (!success) {
				buffer.recycle();
			}
		}
	}

	public void onError(Throwable error) {
		if (ioError.compareAndSet(null, error instanceof IOException ? (IOException) error : new IOException(error))) {
			notifyReaderAboutAvailableBuffer();
		}
	}

	// ------------------------------------------------------------------------

	private void checkIoError() throws IOException {
		IOException error = ioError.get();

		if (error != null) {
			throw new IOException(String.format("%s at remote input channel of task '%s': %s].",
					error.getClass().getName(), reader.getTaskNameWithSubtasks(), error.getMessage()));
		}
	}

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
}
