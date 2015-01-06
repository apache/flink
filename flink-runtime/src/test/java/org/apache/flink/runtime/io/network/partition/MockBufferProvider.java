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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A mock buffer provider.
 */
public class MockBufferProvider implements BufferProvider, BufferRecycler {

	private final int numBuffers;

	private final int bufferSize;

	private final Queue<Buffer> buffers;

	public MockBufferProvider(int bufferSize) {
		this.bufferSize = bufferSize;

		this.numBuffers = -1; // infinite number of buffers
		this.buffers = null; // don't pool buffers
	}

	public MockBufferProvider(int bufferSize, int numBuffers) {
		checkArgument(numBuffers > 0);
		checkArgument(bufferSize > 0);

		this.numBuffers = numBuffers;
		this.bufferSize = bufferSize;

		this.buffers = new ArrayDeque<Buffer>(numBuffers);
		for (int i = 0; i < numBuffers; i++) {
			buffers.add(new Buffer(new MemorySegment(new byte[bufferSize]), this));
		}
	}

	@Override
	public Buffer requestBuffer() throws IOException {
		if (numBuffers > 0) {
			synchronized (buffers) {
				return buffers.poll();
			}
		}

		return new Buffer(new MemorySegment(new byte[bufferSize]), this);
	}

	@Override
	public Buffer requestBufferBlocking() throws IOException, InterruptedException {
		if (numBuffers > 0) {
			synchronized (buffers) {
				Buffer buffer = buffers.poll();

				if (buffer != null) {
					return buffer;
				}

				while (buffer == null) {
					buffers.wait(1000);
					buffer = buffers.poll();
				}

				return buffer;
			}
		}

		return new Buffer(new MemorySegment(new byte[bufferSize]), this);
	}

	@Override
	public boolean addListener(EventListener<Buffer> listener) {
		throw new UnsupportedOperationException("addListener() not supported by mock buffer provider.");
	}

	@Override
	public void recycle(MemorySegment memorySegment) {
		if (numBuffers > 0) {
			synchronized (buffers) {
				buffers.add(new Buffer(memorySegment, this));
				buffers.notify();
			}
		}
	}

	// --------------------------------------------------------------------

	public boolean verifyAllBuffersAvailable() {
		return numBuffers == buffers.size();
	}
}