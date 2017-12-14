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

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TestInfiniteBufferProvider implements BufferProvider {

	private final ConcurrentLinkedQueue<Buffer> buffers = new ConcurrentLinkedQueue<Buffer>();

	private final TestBufferFactory bufferFactory = new TestBufferFactory(
			32 * 1024, new InfiniteBufferProviderRecycler(buffers));

	@Override
	public Buffer requestBuffer() throws IOException {
		Buffer buffer = buffers.poll();

		if (buffer != null) {
			return buffer;
		}

		return bufferFactory.create();
	}

	@Override
	public Buffer requestBufferBlocking() throws IOException, InterruptedException {
		return requestBuffer();
	}

	@Override
	public boolean addBufferListener(BufferListener listener) {
		return false;
	}

	@Override
	public boolean isDestroyed() {
		return false;
	}

	@Override
	public int getMemorySegmentSize() {
		return bufferFactory.getBufferSize();
	}

	private static class InfiniteBufferProviderRecycler implements BufferRecycler {

		private final ConcurrentLinkedQueue<Buffer> buffers;

		public InfiniteBufferProviderRecycler(ConcurrentLinkedQueue<Buffer> buffers) {
			this.buffers = buffers;
		}

		@Override
		public void recycle(MemorySegment segment) {
			buffers.add(new Buffer(segment, this));
		}
	}
}
