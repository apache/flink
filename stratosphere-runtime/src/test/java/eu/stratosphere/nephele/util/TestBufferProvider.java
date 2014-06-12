/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.nephele.util;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.BufferRecycler;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider;

import java.io.IOException;
import java.util.Random;

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
