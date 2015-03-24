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
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * An input channel, which requests a local subpartition.
 */
public class LocalInputChannel extends InputChannel implements NotificationListener {

	private static final Logger LOG = LoggerFactory.getLogger(LocalInputChannel.class);

	private final ResultPartitionManager partitionManager;

	private final TaskEventDispatcher taskEventDispatcher;

	private ResultSubpartitionView queueIterator;

	private volatile boolean isReleased;

	private volatile Buffer lookAhead;

	LocalInputChannel(
			SingleInputGate gate,
			int channelIndex,
			ResultPartitionID partitionId,
			ResultPartitionManager partitionManager,
			TaskEventDispatcher taskEventDispatcher) {

		super(gate, channelIndex, partitionId);

		this.partitionManager = checkNotNull(partitionManager);
		this.taskEventDispatcher = checkNotNull(taskEventDispatcher);
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	@Override
	void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
		if (queueIterator == null) {
			LOG.debug("Requesting LOCAL queue {} of partition {}.", subpartitionIndex, partitionId);

			queueIterator = partitionManager
					.getSubpartition(partitionId, subpartitionIndex, Optional.of(inputGate.getBufferProvider()));

			if (queueIterator == null) {
				throw new IOException("Error requesting sub partition.");
			}

			getNextLookAhead();
		}
	}

	@Override
	Buffer getNextBuffer() throws IOException, InterruptedException {
		checkState(queueIterator != null, "Queried for a buffer before requesting a queue.");

		// After subscribe notification
		if (lookAhead == null) {
			lookAhead = queueIterator.getNextBuffer();
		}

		Buffer next = lookAhead;
		lookAhead = null;

		if (!next.isBuffer() && EventSerializer
				.fromBuffer(next, getClass().getClassLoader())
				.getClass() == EndOfPartitionEvent.class) {

				return next;
		}

		getNextLookAhead();

		return next;
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	void sendTaskEvent(TaskEvent event) throws IOException {
		checkState(queueIterator != null, "Tried to send task event to producer before requesting a queue.");

		if (!taskEventDispatcher.publish(partitionId, event)) {
			throw new IOException("Error while publishing event " + event + " to producer. The producer could not be found.");
		}
	}

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	@Override
	boolean isReleased() {
		return isReleased;
	}

	@Override
	void notifySubpartitionConsumed() throws IOException {
		if (queueIterator != null) {
			queueIterator.notifySubpartitionConsumed();
		}
	}

	/**
	 * Releases the look ahead {@link Buffer} instance and discards the queue
	 * iterator.
	 */
	@Override
	void releaseAllResources() throws IOException {
		if (!isReleased) {
			if (lookAhead != null) {
				lookAhead.recycle();
				lookAhead = null;
			}

			if (queueIterator != null) {
				queueIterator.releaseAllResources();
				queueIterator = null;
			}

			isReleased = true;
		}
	}

	@Override
	public String toString() {
		return "LocalInputChannel [" + partitionId + "]";
	}

	// ------------------------------------------------------------------------
	// Queue iterator listener (called by producing or disk I/O thread)
	// ------------------------------------------------------------------------

	@Override
	public void onNotification() {
		if (isReleased) {
			return;
		}

		try {
			getNextLookAhead();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	// ------------------------------------------------------------------------

	private void getNextLookAhead() throws IOException, InterruptedException {
		while (true) {
			lookAhead = queueIterator.getNextBuffer();

			if (lookAhead != null) {
				notifyAvailableBuffer();
				break;
			}

			if (queueIterator.registerListener(this) || queueIterator.isReleased()) {
				return;
			}
		}
	}
}
