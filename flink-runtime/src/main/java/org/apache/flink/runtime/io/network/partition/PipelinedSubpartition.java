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

import com.google.common.base.Optional;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.util.event.NotificationListener;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * An in-memory only subpartition, which can be consumed once by a single consumer.
 */
public class PipelinedSubpartition extends ResultSubpartition {

	final Queue<Buffer> queue = new ArrayDeque<Buffer>();

	boolean hasFinishedProduce;

	private NotificationListener listener;

	private boolean hasBeenDiscarded;

	private boolean hasConsumer;

	public PipelinedSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	@Override
	public void add(Buffer buffer) {
		synchronized (queue) {
			if (!hasFinishedProduce) {
				queue.add(buffer);

				updateStatistics(buffer);

				maybeNotifyListener();
			}
			else if (!buffer.isRecycled()) {
					buffer.recycle();
			}
		}
	}

	@Override
	public boolean isFinished() {
		synchronized (queue) {
			return hasFinishedProduce;
		}
	}

	@Override
	public void finish() {
		synchronized (queue) {
			if (hasFinishedProduce) {
				return;
			}

			queue.add(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE));

			hasFinishedProduce = true;

			maybeNotifyListener();
		}
	}

	@Override
	public int releaseMemory() {
		// Nothing to do here. Buffers are recycled when they are consumed.
		return 0;
	}

	@Override
	public void release() {
		synchronized (queue) {
			Buffer buffer;
			while ((buffer = queue.poll()) != null) {
				if (!buffer.isRecycled()) {
					buffer.recycle();
				}
			}

			hasFinishedProduce = true;
			hasBeenDiscarded = true;

			maybeNotifyListener();
		}
	}

	@Override
	public ResultSubpartitionView getReadView(Optional<BufferProvider> bufferProvider) throws IllegalSubpartitionRequestException {
		synchronized (queue) {
			if (hasBeenDiscarded) {
				throw new IllegalSubpartitionRequestException("Queue has been discarded during produce phase.");
			}

			if (hasConsumer) {
				throw new IllegalSubpartitionRequestException("Consumable once queue has been consumed/is being consumed.");
			}

			hasConsumer = true;

			return new PipelinedSubpartitionView(this);
		}
	}

	// Call in synchronized scope
	private void maybeNotifyListener() {
		NotificationListener consumer = listener;
		if (consumer != null) {
			listener = null;

			// TODO This is dangerous with the locks. Every listener needs to make sure not to query the queue again :S
			consumer.onNotification();
		}
	}

	private static class PipelinedSubpartitionView implements ResultSubpartitionView {

		private final PipelinedSubpartition partitionQueue;

		private boolean isDiscarded;

		private volatile boolean isConsumed;

		private PipelinedSubpartitionView(PipelinedSubpartition partitionQueue) {
			this.partitionQueue = partitionQueue;
		}

		@Override
		public boolean isConsumed() {
			if (isConsumed) {
				return true;
			}

			synchronized (partitionQueue.queue) {
				isConsumed = (partitionQueue.isFinished() && partitionQueue.queue.isEmpty()) || partitionQueue.hasBeenDiscarded;
			}

			if (isConsumed) {
				partitionQueue.notifyConsumed();

				return true;
			}
			else {
				return false;
			}
		}

		@Override
		public Buffer getNextBuffer() {
			synchronized (partitionQueue.queue) {
				return partitionQueue.queue.poll();
			}
		}

		@Override
		public void release() {
			synchronized (partitionQueue.queue) {
				if (!isDiscarded) {
					partitionQueue.release();

					isDiscarded = true;
				}
			}
		}

		@Override
		public boolean subscribe(NotificationListener listener) throws AlreadySubscribedException {
			synchronized (partitionQueue.queue) {
				if (isConsumed() || !partitionQueue.queue.isEmpty()) {
					return false;
				}

				if (partitionQueue.listener == null) {
					partitionQueue.listener = listener;
					return true;
				}

				throw new AlreadySubscribedException();
			}
		}
	}
}
