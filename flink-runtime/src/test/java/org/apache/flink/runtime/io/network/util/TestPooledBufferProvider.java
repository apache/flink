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

package org.apache.flink.runtime.io.network.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;

import org.apache.flink.shaded.guava18.com.google.common.collect.Queues;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.util.Preconditions.checkArgument;

public class TestPooledBufferProvider implements BufferProvider {

	private final Object bufferCreationLock = new Object();

	private final ArrayBlockingQueue<Buffer> buffers;

	private final TestBufferFactory bufferFactory;

	private final PooledBufferProviderRecycler bufferRecycler;

	private final int poolSize;

	public TestPooledBufferProvider(int poolSize) {
		checkArgument(poolSize > 0);
		this.poolSize = poolSize;

		this.buffers = new ArrayBlockingQueue<Buffer>(poolSize);
		this.bufferRecycler = new PooledBufferProviderRecycler(buffers);
		this.bufferFactory = new TestBufferFactory(32 * 1024, bufferRecycler);
	}

	@Override
	public Buffer requestBuffer() throws IOException {
		final Buffer buffer = buffers.poll();

		if (buffer != null) {
			return buffer;
		}
		else {
			synchronized (bufferCreationLock) {
				if (bufferFactory.getNumberOfCreatedBuffers() < poolSize) {
					return bufferFactory.create();
				}
			}

			return null;
		}
	}

	@Override
	public Buffer requestBufferBlocking() throws IOException, InterruptedException {
		final Buffer buffer = buffers.poll();

		if (buffer != null) {
			return buffer;
		}
		else {
			synchronized (bufferCreationLock) {
				if (bufferFactory.getNumberOfCreatedBuffers() < poolSize) {
					return bufferFactory.create();
				}
			}

			return buffers.take();
		}
	}

	@Override
	public boolean addBufferListener(BufferListener listener) {
		return bufferRecycler.registerListener(listener);
	}

	@Override
	public boolean isDestroyed() {
		return false;
	}

	@Override
	public int getMemorySegmentSize() {
		return bufferFactory.getBufferSize();
	}

	public int getNumberOfAvailableBuffers() {
		return buffers.size();
	}

	private static class PooledBufferProviderRecycler implements BufferRecycler {

		private final Object listenerRegistrationLock = new Object();

		private final Queue<Buffer> buffers;

		private final ConcurrentLinkedQueue<BufferListener> registeredListeners =
				Queues.newConcurrentLinkedQueue();

		public PooledBufferProviderRecycler(Queue<Buffer> buffers) {
			this.buffers = buffers;
		}

		@Override
		public void recycle(MemorySegment segment) {
			synchronized (listenerRegistrationLock) {
				final Buffer buffer = new Buffer(segment, this);

				BufferListener listener = registeredListeners.poll();

				if (listener == null) {
					buffers.add(buffer);
				}
				else {
					listener.notifyBufferAvailable(buffer);
				}
			}
		}

		boolean registerListener(BufferListener listener) {
			synchronized (listenerRegistrationLock) {
				if (buffers.isEmpty()) {
					registeredListeners.add(listener);

					return true;
				}

				return false;
			}
		}
	}
}
