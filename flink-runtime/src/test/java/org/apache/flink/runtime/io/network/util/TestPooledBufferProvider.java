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
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.apache.flink.shaded.guava18.com.google.common.collect.Queues;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static org.apache.flink.util.Preconditions.checkArgument;

public class TestPooledBufferProvider implements BufferProvider {

	private final BlockingQueue<Buffer> buffers = new LinkedBlockingDeque<>();

	private final TestBufferFactory bufferFactory;

	private final PooledBufferProviderRecycler bufferRecycler;

	public TestPooledBufferProvider(int poolSize) {
		this(poolSize, 32 * 1024);
	}

	public TestPooledBufferProvider(int poolSize, int bufferSize) {
		checkArgument(poolSize > 0);

		this.bufferRecycler = new PooledBufferProviderRecycler(buffers);
		this.bufferFactory = new TestBufferFactory(poolSize, bufferSize, bufferRecycler);
	}

	@Override
	public Buffer requestBuffer() throws IOException {
		final Buffer buffer = buffers.poll();
		if (buffer != null) {
			return buffer;
		}

		return bufferFactory.create();
	}

	private Buffer requestBufferBlocking() throws IOException, InterruptedException {
		Buffer buffer = buffers.poll();
		if (buffer != null) {
			return buffer;
		}

		buffer = bufferFactory.create();
		if (buffer != null) {
			return buffer;
		}

		return buffers.take();
	}

	@Override
	public BufferBuilder requestBufferBuilderBlocking() throws IOException, InterruptedException {
		Buffer buffer = requestBufferBlocking();
		return new BufferBuilder(buffer.getMemorySegment(), buffer.getRecycler());
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
	public CompletableFuture<?> getAvailableFuture() {
		return AVAILABLE;
	}

	public int getNumberOfAvailableBuffers() {
		return buffers.size();
	}

	public int getNumberOfCreatedBuffers() {
		return bufferFactory.getNumberOfCreatedBuffers();
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
				final Buffer buffer = new NetworkBuffer(segment, this);

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
