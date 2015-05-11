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

import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 */
class PipelinedSubpartition extends ResultSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

	/** Flag indicating whether the subpartition has been finished. */
	private boolean isFinished;

	/** Flag indicating whether the subpartition has been released. */
	private boolean isReleased;

	/**
	 * A data availability listener. Registered, when the consuming task is faster than the
	 * producing task.
	 */
	private NotificationListener registeredListener;

	/** The read view to consume this subpartition. */
	private PipelinedSubpartitionView readView;

	/** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
	final ArrayDeque<Buffer> buffers = new ArrayDeque<Buffer>();

	PipelinedSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	@Override
	public boolean add(Buffer buffer) {
		checkNotNull(buffer);

		final NotificationListener listener;

		synchronized (buffers) {
			if (isReleased || isFinished) {
				return false;
			}

			// Add the buffer and update the stats
			buffers.add(buffer);
			updateStatistics(buffer);

			// Get the listener...
			listener = registeredListener;
			registeredListener = null;
		}

		// Notify the listener outside of the synchronized block
		if (listener != null) {
			listener.onNotification();
		}

		return true;
	}

	@Override
	public void finish() {
		final NotificationListener listener;

		synchronized (buffers) {
			if (isReleased || isFinished) {
				return;
			}

			final Buffer buffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);

			buffers.add(buffer);
			updateStatistics(buffer);

			isFinished = true;

			LOG.debug("Finished {}.", this);

			// Get the listener...
			listener = registeredListener;
			registeredListener = null;
		}

		// Notify the listener outside of the synchronized block
		if (listener != null) {
			listener.onNotification();
		}
	}

	@Override
	public void release() {
		final NotificationListener listener;
		final PipelinedSubpartitionView view;

		synchronized (buffers) {
			if (isReleased) {
				return;
			}

			// Release all available buffers
			Buffer buffer;
			while ((buffer = buffers.poll()) != null) {
				if (!buffer.isRecycled()) {
					buffer.recycle();
				}
			}

			// Get the view...
			view = readView;
			readView = null;

			// Get the listener...
			listener = registeredListener;
			registeredListener = null;

			// Make sure that no further buffers are added to the subpartition
			isReleased = true;

			LOG.debug("Released {}.", this);
		}

		// Release all resources of the view
		if (view != null) {
			view.releaseAllResources();
		}

		// Notify the listener outside of the synchronized block
		if (listener != null) {
			listener.onNotification();
		}
	}

	@Override
	public int releaseMemory() {
		// The pipelined subpartition does not react to memory release requests. The buffers will be
		// recycled by the consuming task.
		return 0;
	}

	@Override
	public PipelinedSubpartitionView createReadView(BufferProvider bufferProvider) {
		synchronized (buffers) {
			if (readView != null) {
				throw new IllegalStateException("Subpartition " + index + " of "
						+ parent.getPartitionId() + " is being or already has been " +
						"consumed, but pipelined subpartitions can only be consumed once.");
			}

			readView = new PipelinedSubpartitionView(this);

			LOG.debug("Created read view for subpartition {} of partition {}.", index, parent.getPartitionId());

			return readView;
		}
	}

	@Override
	public String toString() {
		synchronized (buffers) {
			return String.format("PipelinedSubpartition [number of buffers: %d (%d bytes), " +
							"finished? %s, read view? %s]",
					getTotalNumberOfBuffers(), getTotalNumberOfBytes(), isFinished, readView != null);
		}
	}

	/**
	 * Registers a listener with this subpartition and returns whether the registration was
	 * successful.
	 *
	 * <p> A registered listener is notified when the state of the subpartition changes. After a
	 * notification, the listener is unregistered. Only a single listener is allowed to be
	 * registered.
	 */
	boolean registerListener(NotificationListener listener) {
		synchronized (buffers) {
			if (!buffers.isEmpty() || isReleased) {
				return false;
			}

			if (registeredListener == null) {
				registeredListener = listener;

				return true;
			}

			throw new IllegalStateException("Already registered listener.");
		}
	}
}
