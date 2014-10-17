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

import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionQueueIterator;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionQueue;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumableOnceInMemoryOnlyQueue implements IntermediateResultPartitionQueue {

	private final Object iteratorMonitor = new Object();

	private ConcurrentLinkedQueue<BufferOrEvent> queue = new ConcurrentLinkedQueue<BufferOrEvent>();

	private AtomicBoolean isFinished = new AtomicBoolean(false);

	private AtomicBoolean isDiscarded = new AtomicBoolean(false);

	private int numConsumed;

	private int numActiveConsumers;

	@Override
	public void add(BufferOrEvent bufferOrEvent) {
		if (isFinished.get()) {
			if (bufferOrEvent.isBuffer()) {
				bufferOrEvent.getBuffer().recycle();
			}

			throw new IllegalStateException("Queue has already been finished.");
		}

		queue.add(bufferOrEvent);
	}

	@Override
	public void finish() {
		isFinished.compareAndSet(false, true);
	}

	@Override
	public void recycleBuffers() {
		// Nothing to do here. Buffers are recycled when they are consumed.
	}

	@Override
	public void discard() {
		if (isDiscarded.compareAndSet(false, true)) {
			BufferOrEvent bufferOrEvent;
			while ((bufferOrEvent = queue.poll()) != null) {
				if (bufferOrEvent.isBuffer()) {
					bufferOrEvent.getBuffer().recycle();
				}
			}
		}
	}

	@Override
	public int getNumConsumed() {
		synchronized (iteratorMonitor) {
			return numConsumed;
		}
	}

	@Override
	public int getNumActiveConsumers() {
		synchronized (iteratorMonitor) {
			return numActiveConsumers;
		}
	}

	@Override
	public IntermediateResultPartitionQueueIterator getLocalIterator(BufferProvider bufferProvider) {
		synchronized (iteratorMonitor) {
			if (numConsumed == 0 && numActiveConsumers == 0) {
				numActiveConsumers++;

				return new ConsumableOnceInMemoryOnlyQueueIterator();
			}
		}

		throw new IllegalStateException("Consumable once queue has been consumed or is being consumed.");
	}

	// ------------------------------------------------------------------------

	private class ConsumableOnceInMemoryOnlyQueueIterator implements IntermediateResultPartitionQueueIterator {

		private boolean hasBeenConsumed;

		@Override
		public boolean hasNext() {
			if (!isFinished.get() || !queue.isEmpty()) {
				return true;
			}

			if (!hasBeenConsumed) {
				synchronized (iteratorMonitor) {
					numActiveConsumers--;
					numConsumed++;
					hasBeenConsumed = true;
				}
			}

			return false;
		}

		@Override
		public BufferOrEvent getNextBufferOrEvent() {
			if (!hasNext()) {
				return null;
			}

			return queue.poll();
		}
	}
}
