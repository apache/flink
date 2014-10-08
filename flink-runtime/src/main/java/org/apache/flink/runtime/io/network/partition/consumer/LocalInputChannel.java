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

import com.google.common.base.Optional;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.queue.IntermediateResultPartitionQueueIterator;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

/**
 * An input channel, which requests a local partition queue.
 */
public class LocalInputChannel extends InputChannel implements NotificationListener {

	private static final Logger LOG = LoggerFactory.getLogger(LocalInputChannel.class);

	private IntermediateResultPartitionQueueIterator queueIterator;

	private boolean isReleased;

	private Buffer lookAhead;

	public LocalInputChannel(int channelIndex, ExecutionAttemptID producerExecutionId, IntermediateResultPartitionID partitionId, BufferReader reader) {
		super(channelIndex, producerExecutionId, partitionId, reader);
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	@Override
	public void requestIntermediateResultPartition(int queueIndex) throws IOException {
		if (queueIterator == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Requesting queue {} from LOCAL partition {}.", partitionId, queueIndex);
			}

			queueIterator = reader.getIntermediateResultPartitionProvider()
					.getIntermediateResultPartitionIterator(producerExecutionId, partitionId, queueIndex, Optional.of(reader.getBufferProvider()));

			getNextLookAhead();
		}
	}

	@Override
	public Buffer getNextBuffer() throws IOException {
		checkState(queueIterator != null, "Queried for a buffer before requesting a queue.");

		// After subscribe notification
		if (lookAhead == null) {
			lookAhead = queueIterator.getNextBuffer();
		}

		Buffer next = lookAhead;
		lookAhead = null;

		getNextLookAhead();

		return next;
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		checkState(queueIterator != null, "Tried to send task event to producer before requesting a queue.");

		if (!reader.getTaskEventDispatcher().publish(producerExecutionId, partitionId, event)) {
			throw new IOException("Error while publishing event " + event + " to producer. The producer could not be found.");
		}
	}

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	/**
	 * Releases the look ahead {@link Buffer} instance and discards the queue
	 * iterator.
	 */
	@Override
	public void releaseAllResources() throws IOException {
		if (!isReleased) {
			if (lookAhead != null) {
				lookAhead.recycle();
				lookAhead = null;
			}

			if (queueIterator != null) {
				queueIterator.discard();
				queueIterator = null;
			}

			isReleased = true;
		}
	}

	@Override
	public String toString() {
		return "LOCAL " + super.toString();
	}

	// ------------------------------------------------------------------------
	// Queue iterator listener (called by producing or disk I/O thread)
	// ------------------------------------------------------------------------

	@Override
	public void onNotification() {
		notifyReaderAboutAvailableBuffer();
	}

	// ------------------------------------------------------------------------

	private void getNextLookAhead() throws IOException {
		while (true) {
			lookAhead = queueIterator.getNextBuffer();

			if (lookAhead != null) {
				notifyReaderAboutAvailableBuffer();
				break;
			}

			if (queueIterator.subscribe(this) || queueIterator.isConsumed()) {
				return;
			}
		}
	}
}
