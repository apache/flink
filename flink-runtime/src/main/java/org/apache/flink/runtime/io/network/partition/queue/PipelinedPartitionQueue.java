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

package org.apache.flink.runtime.io.network.partition.queue;

import com.google.common.base.Optional;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.util.event.NotificationListener;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * An in-memory only queue, which can be consumed once by a single consumer.
 */
public class PipelinedPartitionQueue implements IntermediateResultPartitionQueue {

	final Queue<Buffer> queue = new ArrayDeque<Buffer>();

	boolean hasFinishedProduce;

	private NotificationListener listener;

	private boolean hasBeenDiscarded;

	private boolean hasConsumer;

	@Override
	public void add(Buffer buffer) {
		synchronized (queue) {
			if (!hasFinishedProduce) {
				queue.add(buffer);

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
	public int recycleBuffers() {
		// Nothing to do here. Buffers are recycled when they are consumed.
		return 0;
	}

	@Override
	public void discard() {
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
	public IntermediateResultPartitionQueueIterator getQueueIterator(Optional<BufferProvider> bufferProvider) throws IllegalQueueIteratorRequestException {
		synchronized (queue) {
			if (hasBeenDiscarded) {
				throw new IllegalQueueIteratorRequestException("Queue has been discarded during produce phase.");
			}

			if (hasConsumer) {
				throw new IllegalQueueIteratorRequestException("Consumable once queue has been consumed/is being consumed.");
			}

			hasConsumer = true;

			return new PipelinedPartitionQueueIterator(this);
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

	private static class PipelinedPartitionQueueIterator implements IntermediateResultPartitionQueueIterator {

		private final PipelinedPartitionQueue partitionQueue;

		private boolean isDiscarded;

		private PipelinedPartitionQueueIterator(PipelinedPartitionQueue partitionQueue) {
			this.partitionQueue = partitionQueue;
		}

		@Override
		public boolean isConsumed() {
			synchronized (partitionQueue.queue) {
				return (partitionQueue.isFinished() && partitionQueue.queue.isEmpty()) || partitionQueue.hasBeenDiscarded;
			}
		}

		@Override
		public Buffer getNextBuffer() {
			synchronized (partitionQueue.queue) {
				return partitionQueue.queue.poll();
			}
		}

		@Override
		public void discard() {
			synchronized (partitionQueue.queue) {
				if (!isDiscarded) {
					partitionQueue.discard();

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
