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

	/** The local partition manager. */
	private final ResultPartitionManager partitionManager;

	/** Task event dispatcher for backwards events. */
	private final TaskEventDispatcher taskEventDispatcher;

	/** The consumed subpartition */
	private volatile ResultSubpartitionView subpartitionView;

	private volatile boolean isReleased;

	private volatile Buffer lookAhead;

	LocalInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			ResultPartitionManager partitionManager,
			TaskEventDispatcher taskEventDispatcher) {

		super(inputGate, channelIndex, partitionId);

		this.partitionManager = checkNotNull(partitionManager);
		this.taskEventDispatcher = checkNotNull(taskEventDispatcher);
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	@Override
	void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
		if (subpartitionView == null) {
			LOG.debug("{}: Requesting LOCAL subpartition {} of partition {}.",
					this, subpartitionIndex, partitionId);

			subpartitionView = partitionManager.createSubpartitionView(
					partitionId, subpartitionIndex, inputGate.getBufferProvider());

			if (subpartitionView == null) {
				throw new IOException("Error requesting subpartition.");
			}

			getNextLookAhead();
		}
	}

	@Override
	Buffer getNextBuffer() throws IOException, InterruptedException {
		checkState(subpartitionView != null, "Queried for a buffer before requesting the subpartition.");

		// After subscribe notification
		if (lookAhead == null) {
			lookAhead = subpartitionView.getNextBuffer();
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
		checkState(subpartitionView != null, "Tried to send task event to producer before requesting the subpartition.");

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
		if (subpartitionView != null) {
			subpartitionView.notifySubpartitionConsumed();
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

			if (subpartitionView != null) {
				subpartitionView.releaseAllResources();
				subpartitionView = null;
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

		final ResultSubpartitionView view = subpartitionView;

		if (view == null) {
			return;
		}

		while (true) {
			lookAhead = view.getNextBuffer();

			if (lookAhead != null) {
				notifyAvailableBuffer();
				break;
			}

			if (view.registerListener(this) || view.isReleased()) {
				return;
			}
		}
	}
}
