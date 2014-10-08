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

import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.Channel.ID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionQueueIterator;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SpillableBlockingQueue implements IntermediateResultPartitionQueue {

	private final Object queueMonitor = new Object();

	private final Object iteratorMonitor = new Object();

	private final IOManager ioManager;

	private final ID ioChannel;

	private final List<BufferOrEvent> queue = new ArrayList<BufferOrEvent>();

	private AtomicBoolean isFinished = new AtomicBoolean(false);

	private boolean hasSpilled = false;

	private boolean isDiscarded = false;

	private int numConsumed;

	private int numActiveConsumers;

	private BufferFileWriter writer;

	public SpillableBlockingQueue(IOManager ioManager, ID ioChannel) {
		this.ioManager = ioManager;
		this.ioChannel = ioChannel;
	}

	@Override
	public void add(BufferOrEvent bufferOrEvent) throws IOException {
		if (isFinished.get()) {
			if (bufferOrEvent.isBuffer()) {
				bufferOrEvent.getBuffer().recycle();
			}

			throw new IllegalStateException("Queue has already been finished.");
		}

		synchronized (queueMonitor) {
			if (hasSpilled) {
				writer.writeBufferOrEvent(bufferOrEvent);
			}
			else {
				queue.add(bufferOrEvent);
			}
		}
	}

	@Override
	public void finish() throws IOException {
		isFinished.compareAndSet(false, true);

		synchronized (queueMonitor) {
			if (hasSpilled) {
				writer.close();
			}
		}
	}

	@Override
	public void recycleBuffers() throws IOException {
		synchronized (queueMonitor) {
			if (!hasSpilled) {
				writer = ioManager.createBufferFileWriter(ioChannel);

				while (!queue.isEmpty()) {
					writer.writeBufferOrEvent(queue.remove(0));
				}

				// TODO Is it a problem that we might wait synchronously here?
				// Alternative might be to check the pending requests in the
				// iterator.
				if (isFinished.get()) {
					writer.close();
				}

				hasSpilled = true;
			}
		}
	}

	@Override
	public void discard() throws IOException {
		synchronized (queueMonitor) {
			if (!isDiscarded) {
				while (!queue.isEmpty()) {
					BufferOrEvent boe = queue.remove(0);
					if (boe.isBuffer()) {
						boe.getBuffer().recycle();
					}
				}

				if (hasSpilled) {
					writer.deleteFile();
				}

				isDiscarded = true;
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
			if (isFinished.get()) {
				numActiveConsumers++;
				return new SpillableQueueIterator(bufferProvider);
			}
		}

		throw new IllegalStateException("Queue has not been finished yet.");
	}

	// ------------------------------------------------------------------------

	private class SpillableQueueIterator implements IntermediateResultPartitionQueueIterator {

		private final BufferProvider bufferProvider;

		private BufferFileReader reader;

		private boolean hasBeenConsumed;

		private int index = 0;

		private long position = 0;

		private SpillableQueueIterator(BufferProvider bufferProvider) {
			this.bufferProvider = bufferProvider;
		}

		@Override
		public boolean hasNext() {
			synchronized (queueMonitor) {
				boolean hasNext;

				if (isDiscarded) {
					hasNext = false;
				}
				else if (hasSpilled) {
					if (reader == null) {
						try {
							reader = ioManager.createBufferFileReader(ioChannel, bufferProvider, position);
						}
						catch (IOException e) {
							throw new RuntimeException("Unexpected exception during creating of reader.");
						}
					}

					hasNext = reader.hasNext();
				}
				else {
					hasNext = !isFinished.get() || index < queue.size();
				}

				if (!hasNext && !hasBeenConsumed) {
					synchronized (iteratorMonitor) {
						numActiveConsumers--;
						numConsumed++;
						hasBeenConsumed = true;
					}

					try {
						if (reader != null) {
							reader.close();
						}
					}
					catch (IOException e) {
						throw new RuntimeException("Unexpected exception during close of reader.");
					}
				}

				return hasNext;
			}
		}

		@Override
		public BufferOrEvent getNextBufferOrEvent() throws IOException {
			synchronized (queueMonitor) {
				if (!hasNext()) {
					return null;
				}

				if (hasSpilled) {
					return reader.getNextBufferOrEvent();
				}
				else {
					BufferOrEvent boe = queue.get(index++);

					if (boe.isBuffer()) {
						Buffer buffer = boe.getBuffer().retain();
						position += buffer.getSize() + 5;
					}
					else if (boe.isEvent()) {
						position += Envelope.toSerializedEvent(boe.getEvent()).remaining() + 5;
					}

					return boe;
				}
			}
		}
	}
}
