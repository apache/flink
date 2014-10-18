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


package org.apache.flink.runtime.io.network.bufferprovider;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.BufferRecycler;

public final class DiscardBufferPool implements BufferProvider, BufferRecycler {
	
	@Override
	public Buffer requestBuffer(int minBufferSize) {
		return null;
	}

	@Override
	public Buffer requestBufferBlocking(int minBufferSize) {
		return null;
	}

	@Override
	public int getBufferSize() {
		return 0;
	}

	@Override
	public void reportAsynchronousEvent() {
		throw new UnsupportedOperationException();
	}

	@Override
	public BufferAvailabilityRegistration registerBufferAvailabilityListener(BufferAvailabilityListener listener) {
		return BufferAvailabilityRegistration.FAILED_BUFFER_POOL_DESTROYED;
	}

	@Override
	public void recycle(MemorySegment buffer) {
		throw new UnsupportedOperationException();
	}
}
