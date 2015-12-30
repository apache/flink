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

import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.testutils.DiscardingRecycler;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TestBufferFactory {

	public static final int BUFFER_SIZE = 32 * 1024;

	private static final BufferRecycler RECYCLER = new DiscardingRecycler();

	private static final Buffer MOCK_BUFFER = createBuffer();

	private final int bufferSize;

	private final BufferRecycler bufferRecycler;

	private AtomicInteger numberOfCreatedBuffers = new AtomicInteger();

	public TestBufferFactory() {
		this(BUFFER_SIZE, RECYCLER);
	}

	public TestBufferFactory(int bufferSize) {
		this(bufferSize, RECYCLER);
	}

	public TestBufferFactory(int bufferSize, BufferRecycler bufferRecycler) {
		checkArgument(bufferSize > 0);
		this.bufferSize = bufferSize;
		this.bufferRecycler = checkNotNull(bufferRecycler);
	}

	public Buffer create() {
		numberOfCreatedBuffers.incrementAndGet();

		return new Buffer(MemorySegmentFactory.allocateUnpooledSegment(bufferSize), bufferRecycler);
	}

	public Buffer createFrom(MemorySegment segment) {
		return new Buffer(segment, bufferRecycler);
	}

	public int getNumberOfCreatedBuffers() {
		return numberOfCreatedBuffers.get();
	}

	public int getBufferSize() {
		return bufferSize;
	}

	// ------------------------------------------------------------------------
	// Static test helpers
	// ------------------------------------------------------------------------

	public static Buffer createBuffer() {
		return createBuffer(BUFFER_SIZE);
	}

	public static Buffer createBuffer(int bufferSize) {
		checkArgument(bufferSize > 0);

		return new Buffer(MemorySegmentFactory.allocateUnpooledSegment(bufferSize), RECYCLER);
	}

	public static Buffer getMockBuffer() {
		return MOCK_BUFFER;
	}
}
