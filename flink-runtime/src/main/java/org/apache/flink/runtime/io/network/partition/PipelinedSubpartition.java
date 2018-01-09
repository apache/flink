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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayDeque;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 */
class PipelinedSubpartition extends ResultSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

	// ------------------------------------------------------------------------

	/** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
	private final ArrayDeque<Buffer> buffers = new ArrayDeque<>();

	/** The read view to consume this subpartition. */
	private PipelinedSubpartitionView readView;

	/** Flag indicating whether the subpartition has been finished. */
	private boolean isFinished;

	/** Flag indicating whether the subpartition has been released. */
	private volatile boolean isReleased;

	/** The number of non-event buffers currently in this subpartition. */
	@GuardedBy("buffers")
	private int buffersInBacklog;

	// ------------------------------------------------------------------------

	PipelinedSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	@Override
	public boolean add(Buffer buffer) throws IOException {
		checkNotNull(buffer);

		// view reference accessible outside the lock, but assigned inside the locked scope
		final PipelinedSubpartitionView reader;

		synchronized (buffers) {
			if (isFinished || isReleased) {
				buffer.recycleBuffer();
				return false;
			}

			// Add the buffer and update the stats
			buffers.add(buffer);
			reader = readView;
			updateStatistics(buffer);
			increaseBuffersInBacklog(buffer);
		}

		// Notify the listener outside of the synchronized block
		if (reader != null) {
			reader.notifyBuffersAvailable(1);
		}

		return true;
	}

	@Override
	public void finish() throws IOException {
		final Buffer buffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);

		// view reference accessible outside the lock, but assigned inside the locked scope
		final PipelinedSubpartitionView reader;

		synchronized (buffers) {
			if (isFinished || isReleased) {
				return;
			}

			buffers.add(buffer);
			reader = readView;
			updateStatistics(buffer);

			isFinished = true;
		}

		LOG.debug("Finished {}.", this);

		// Notify the listener outside of the synchronized block
		if (reader != null) {
			reader.notifyBuffersAvailable(1);
		}
	}

	@Override
	public void release() {
		// view reference accessible outside the lock, but assigned inside the locked scope
		final PipelinedSubpartitionView view;

		synchronized (buffers) {
			if (isReleased) {
				return;
			}

			// Release all available buffers
			Buffer buffer;
			while ((buffer = buffers.poll()) != null) {
				buffer.recycleBuffer();
			}

			view = readView;
			readView = null;

			// Make sure that no further buffers are added to the subpartition
			isReleased = true;
		}

		LOG.debug("Released {}.", this);

		if (view != null) {
			view.releaseAllResources();
		}
	}

	@Nullable
	BufferAndBacklog pollBuffer() {
		synchronized (buffers) {
			Buffer buffer = buffers.pollFirst();
			decreaseBuffersInBacklog(buffer);

			if (buffer != null) {
				return new BufferAndBacklog(buffer, buffersInBacklog);
			} else {
				return null;
			}
		}
	}

	@Override
	public int releaseMemory() {
		// The pipelined subpartition does not react to memory release requests.
		// The buffers will be recycled by the consuming task.
		return 0;
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	@VisibleForTesting
	public int getBuffersInBacklog() {
		return buffersInBacklog;
	}

	/**
	 * Decreases the number of non-event buffers by one after fetching a non-event
	 * buffer from this subpartition.
	 */
	private void decreaseBuffersInBacklog(Buffer buffer) {
		assert Thread.holdsLock(buffers);

		if (buffer != null && buffer.isBuffer()) {
			buffersInBacklog--;
		}
	}

	/**
	 * Increases the number of non-event buffers by one after adding a non-event
	 * buffer into this subpartition.
	 */
	private void increaseBuffersInBacklog(Buffer buffer) {
		assert Thread.holdsLock(buffers);

		if (buffer != null && buffer.isBuffer()) {
			buffersInBacklog++;
		}
	}

	@Override
	public PipelinedSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException {
		final int queueSize;

		synchronized (buffers) {
			checkState(!isReleased);
			checkState(readView == null,
					"Subpartition %s of is being (or already has been) consumed, " +
					"but pipelined subpartitions can only be consumed once.", index, parent.getPartitionId());

			LOG.debug("Creating read view for subpartition {} of partition {}.", index, parent.getPartitionId());

			queueSize = buffers.size();
			readView = new PipelinedSubpartitionView(this, availabilityListener);
		}

		readView.notifyBuffersAvailable(queueSize);

		return readView;
	}

	// ------------------------------------------------------------------------

	int getCurrentNumberOfBuffers() {
		return buffers.size();
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		final long numBuffers;
		final long numBytes;
		final boolean finished;
		final boolean hasReadView;

		synchronized (buffers) {
			numBuffers = getTotalNumberOfBuffers();
			numBytes = getTotalNumberOfBytes();
			finished = isFinished;
			hasReadView = readView != null;
		}

		return String.format(
			"PipelinedSubpartition [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished? %s, read view? %s]",
			numBuffers, numBytes, buffersInBacklog, finished, hasReadView);
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		// since we do not synchronize, the size may actually be lower than 0!
		return Math.max(buffers.size(), 0);
	}
}
