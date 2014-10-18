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

package org.apache.flink.runtime.testutils;

import java.io.IOException;
import java.util.Random;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.BufferRecycler;
import org.apache.flink.runtime.io.network.bufferprovider.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.bufferprovider.BufferProvider;

public class TestBufferProvider implements BufferProvider {
	
	private final BufferRecycler recycler = new DiscardingRecycler();
	
	private final Random rnd = new Random();
	
	private final int sizeOfMemorySegments;
	
	private final float probabilityForNoneAvailable;
	
	
	public TestBufferProvider(int sizeOfMemorySegments) {
		this(sizeOfMemorySegments, -1.0f);
	}
	
	public TestBufferProvider(int sizeOfMemorySegments, float probabilityForNoneAvailable) {
		this.sizeOfMemorySegments = sizeOfMemorySegments;
		this.probabilityForNoneAvailable = probabilityForNoneAvailable;
	}

	@Override
	public Buffer requestBuffer(int sizeOfBuffer) throws IOException {
		if (rnd.nextFloat() < this.probabilityForNoneAvailable) {
			return null;
		} else {
			MemorySegment segment = new MemorySegment(new byte[this.sizeOfMemorySegments]);
			return new Buffer(segment, sizeOfBuffer, this.recycler);
		}
	}

	@Override
	public Buffer requestBufferBlocking(int sizeOfBuffer) throws IOException, InterruptedException {
		MemorySegment segment = new MemorySegment(new byte[this.sizeOfMemorySegments]);
		return new Buffer(segment, sizeOfBuffer, this.recycler);
	}

	@Override
	public int getBufferSize() {
		return Integer.MAX_VALUE;
	}
	
	@Override
	public void reportAsynchronousEvent() {
		throw new UnsupportedOperationException();
	}

	@Override
	public BufferAvailabilityRegistration registerBufferAvailabilityListener(BufferAvailabilityListener bufferAvailabilityListener) {
		throw new UnsupportedOperationException();
	}
}
