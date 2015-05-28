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
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * An input channel, which requests a local subpartition.
 */
public class LocalInputChannel extends InputChannel implements NotificationListener {

	private static final Logger LOG = LoggerFactory.getLogger(LocalInputChannel.class);

	private final Object requestLock = new Object();

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

		this(inputGate, channelIndex, partitionId, partitionManager, taskEventDispatcher,
				new Tuple2<Integer, Integer>(0, 0));
	}

	LocalInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			ResultPartitionManager partitionManager,
			TaskEventDispatcher taskEventDispatcher,
			Tuple2<Integer, Integer> initialAndMaxBackoff) {

		super(inputGate, channelIndex, partitionId, initialAndMaxBackoff);

		this.partitionManager = checkNotNull(partitionManager);
		this.taskEventDispatcher = checkNotNull(taskEventDispatcher);
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	@Override
	void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
		// The lock is required to request only once in the presence of retriggered requests.
		synchronized (requestLock) {
			if (subpartitionView == null) {
				LOG.debug("{}: Requesting LOCAL subpartition {} of partition {}.",
						this, subpartitionIndex, partitionId);

				try {
					subpartitionView = partitionManager.createSubpartitionView(
							partitionId, subpartitionIndex, inputGate.getBufferProvider());
				}
				catch (PartitionNotFoundException notFound) {
					if (increaseBackoff()) {
						inputGate.retriggerPartitionRequest(partitionId.getPartitionId());
						return;
					}
					else {
						throw notFound;
					}
				}

				if (subpartitionView == null) {
					throw new IOException("Error requesting subpartition.");
				}

				getNextLookAhead();
			}
		}
	}

	/**
	 * Retriggers a subpartition request.
	 */
	void retriggerSubpartitionRequest(Timer timer, final int subpartitionIndex) throws IOException, InterruptedException {
		synchronized (requestLock) {
			checkState(subpartitionView == null, "Already requested partition.");

			timer.schedule(new TimerTask() {
				@Override
				public void run() {
					try {
						requestSubpartition(subpartitionIndex);
					}
					catch (Throwable t) {
						setError(t);
					}
				}
			}, getCurrentBackoff());
		}
	}

	@Override
	Buffer getNextBuffer() throws IOException, InterruptedException {
		checkError();
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
		checkError();
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

			if (view.registerListener(this)) {
				return;
			}
			else if (view.isReleased()) {
				Throwable cause = view.getFailureCause();

				if (cause != null) {
					setError(new ProducerFailedException(cause));
				}

				return;
			}
		}
	}
}
