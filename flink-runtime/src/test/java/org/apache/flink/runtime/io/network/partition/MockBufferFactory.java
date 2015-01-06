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
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.testutils.DiscardingRecycler;

public class MockBufferFactory {

	private static final int BUFFER_SIZE = 32 * 1024;

	private static final BufferRecycler BUFFER_RECYCLER = new DiscardingRecycler();

	private int currentCreateNumber;

	private int currentVerifyNumber;

	public Buffer create() {
		final MemorySegment segment = new MemorySegment(new byte[BUFFER_SIZE]);

		for (int i = 0; i < BUFFER_SIZE; i += 4) {
			segment.putInt(i, currentCreateNumber++);
		}

		return new Buffer(segment, BUFFER_RECYCLER);
	}

	public boolean verify(Buffer buffer) {
		final MemorySegment segment = buffer.getMemorySegment();

		for (int i = 0; i < BUFFER_SIZE; i += 4) {
			if (segment.getInt(i) != currentVerifyNumber++) {
				return false;
			}
		}

		return true;
	}

	public static Buffer createBuffer() {
		return new Buffer(new MemorySegment(new byte[BUFFER_SIZE]), BUFFER_RECYCLER);
	}

	public static int fillBufferWithAscendingNumbers(Buffer buffer, int currentNumber) {
		MemorySegment segment = buffer.getMemorySegment();

		final int size = buffer.getSize();

		for (int i = 0; i < size; i += 4) {
			segment.putInt(i, currentNumber++);
		}

		return currentNumber;
	}

	public static int verifyBufferFilledWithAscendingNumbers(Buffer buffer, int currentNumber) {
		MemorySegment segment = buffer.getMemorySegment();

		final int size = buffer.getSize();

		for (int i = 0; i < size; i += 4) {
			if (segment.getInt(i) != currentNumber++) {
				throw new IllegalStateException("Read unexpected number from buffer: was " + segment.getInt(i) + ", expected " + (currentNumber - 1));
			}
		}

		return currentNumber;
	}
}
