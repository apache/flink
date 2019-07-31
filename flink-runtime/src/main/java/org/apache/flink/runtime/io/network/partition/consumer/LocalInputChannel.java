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

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input channel, which requests a local subpartition.
 */
public class LocalInputChannel extends InputChannel implements BufferAvailabilityListener {

	private static final Logger LOG = LoggerFactory.getLogger(LocalInputChannel.class);

	// ------------------------------------------------------------------------

	private final Object requestLock = new Object();

	/** The local partition manager. */
	private final ResultPartitionManager partitionManager;

	/** Task event dispatcher for backwards events. */
	private final TaskEventPublisher taskEventPublisher;

	/** The consumed subpartition. */
	private volatile ResultSubpartitionView subpartitionView;

	private volatile boolean isReleased;

	public LocalInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionID partitionId,
		ResultPartitionManager partitionManager,
		TaskEventPublisher taskEventPublisher,
		InputChannelMetrics metrics) {

		this(inputGate, channelIndex, partitionId, partitionManager, taskEventPublisher,
			0, 0, metrics);
	}

	public LocalInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionID partitionId,
		ResultPartitionManager partitionManager,
		TaskEventPublisher taskEventPublisher,
		int initialBackoff,
		int maxBackoff,
		InputChannelMetrics metrics) {

		super(inputGate, channelIndex, partitionId, initialBackoff, maxBackoff, metrics.getNumBytesInLocalCounter(), metrics.getNumBuffersInLocalCounter());

		this.partitionManager = checkNotNull(partitionManager);
		this.taskEventPublisher = checkNotNull(taskEventPublisher);
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	@Override
	void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {

		boolean retriggerRequest = false;

		// The lock is required to request only once in the presence of retriggered requests.
		synchronized (requestLock) {
			checkState(!isReleased, "LocalInputChannel has been released already");

			if (subpartitionView == null) {
				LOG.debug("{}: Requesting LOCAL subpartition {} of partition {}.",
					this, subpartitionIndex, partitionId);

				try {
					ResultSubpartitionView subpartitionView = partitionManager.createSubpartitionView(
						partitionId, subpartitionIndex, this);

					if (subpartitionView == null) {
						throw new IOException("Error requesting subpartition.");
					}

					// make the subpartition view visible
					this.subpartitionView = subpartitionView;

					// check if the channel was released in the meantime
					if (isReleased) {
						subpartitionView.releaseAllResources();
						this.subpartitionView = null;
					}
				} catch (PartitionNotFoundException notFound) {
					if (increaseBackoff()) {
						retriggerRequest = true;
					} else {
						throw notFound;
					}
				}
			}
		}

		// Do this outside of the lock scope as this might lead to a
		// deadlock with a concurrent release of the channel via the
		// input gate.
		if (retriggerRequest) {
			inputGate.retriggerPartitionRequest(partitionId.getPartitionId());
		}
	}

	/**
	 * Retriggers a subpartition request.
	 */
	void retriggerSubpartitionRequest(Timer timer, final int subpartitionIndex) {
		synchronized (requestLock) {
			checkState(subpartitionView == null, "already requested partition");

			timer.schedule(new TimerTask() {
				@Override
				public void run() {
					try {
						requestSubpartition(subpartitionIndex);
					} catch (Throwable t) {
						setError(t);
					}
				}
			}, getCurrentBackoff());
		}
	}

	@Override
	Optional<BufferAndAvailability> getNextBuffer() throws IOException, InterruptedException {
		checkError();

		ResultSubpartitionView subpartitionView = this.subpartitionView;
		if (subpartitionView == null) {
			// There is a possible race condition between writing a EndOfPartitionEvent (1) and flushing (3) the Local
			// channel on the sender side, and reading EndOfPartitionEvent (2) and processing flush notification (4). When
			// they happen in that order (1 - 2 - 3 - 4), flush notification can re-enqueue LocalInputChannel after (or
			// during) it was released during reading the EndOfPartitionEvent (2).
			if (isReleased) {
				return Optional.empty();
			}

			// this can happen if the request for the partition was triggered asynchronously
			// by the time trigger
			// would be good to avoid that, by guaranteeing that the requestPartition() and
			// getNextBuffer() always come from the same thread
			// we could do that by letting the timer insert a special "requesting channel" into the input gate's queue
			subpartitionView = checkAndWaitForSubpartitionView();
		}

		BufferAndBacklog next = subpartitionView.getNextBuffer();

		if (next == null) {
			if (subpartitionView.isReleased()) {
				throw new CancelTaskException("Consumed partition " + subpartitionView + " has been released.");
			} else {
				return Optional.empty();
			}
		}

		numBytesIn.inc(next.buffer().getSize());
		numBuffersIn.inc();
		return Optional.of(new BufferAndAvailability(next.buffer(), next.isMoreAvailable(), next.buffersInBacklog()));
	}

	@Override
	public void notifyDataAvailable() {
		notifyChannelNonEmpty();
	}

	private ResultSubpartitionView checkAndWaitForSubpartitionView() {
		// synchronizing on the request lock means this blocks until the asynchronous request
		// for the partition view has been completed
		// by then the subpartition view is visible or the channel is released
		synchronized (requestLock) {
			checkState(!isReleased, "released");
			checkState(subpartitionView != null, "Queried for a buffer before requesting the subpartition.");
			return subpartitionView;
		}
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	void sendTaskEvent(TaskEvent event) throws IOException {
		checkError();
		checkState(subpartitionView != null, "Tried to send task event to producer before requesting the subpartition.");

		if (!taskEventPublisher.publish(partitionId, event)) {
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
	 * Releases the partition reader.
	 */
	@Override
	void releaseAllResources() throws IOException {
		if (!isReleased) {
			isReleased = true;

			ResultSubpartitionView view = subpartitionView;
			if (view != null) {
				view.releaseAllResources();
				subpartitionView = null;
			}
		}
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		ResultSubpartitionView view = subpartitionView;

		if (view != null) {
			return view.unsynchronizedGetNumberOfQueuedBuffers();
		}

		return 0;
	}

	@Override
	public String toString() {
		return "LocalInputChannel [" + partitionId + "]";
	}
}
