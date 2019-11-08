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

package org.apache.flink.core.memory;

import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.nio.ByteBuffer;

/**
 * Tests for the sanity checks of the memory segments.
 */
public class MemorySegmentChecksTest {

	@Test(expected = NullPointerException.class)
	public void testHeapNullBuffer1() {
		new HeapMemorySegment(null);
	}

	@Test(expected = NullPointerException.class)
	public void testHeapNullBuffer2() {
		new HeapMemorySegment(null, new Object());
	}

	@Test(expected = NullPointerException.class)
	public void testHybridHeapNullBuffer2() {
		new HybridMemorySegment((byte[]) null, new Object());
	}

	@Test(expected = NullPointerException.class)
	public void testHybridOffHeapNullBuffer2() {
		new HybridMemorySegment(null, new Object(), () -> {});
	}

	@Test(expected = IllegalArgumentException.class)
	public void testHybridNonDirectBuffer() {
		new HybridMemorySegment(ByteBuffer.allocate(1024), new Object(), () -> {});
	}

	@Test(expected = IllegalArgumentException.class)
	public void testZeroAddress(){
		new MockSegment(0L, 4 * 1024, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNegativeAddress(){
		new MockSegment(-1L, 4 * 1024, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTooLargeAddress(){
		new MockSegment(Long.MAX_VALUE - 8 * 1024, 4 * 1024, null);
	}

	// ------------------------------------------------------------------------

	final class MockSegment extends MemorySegment {

		MockSegment(long offHeapAddress, int size, Object owner) {
			super(offHeapAddress, size, owner);
		}

		@Override
		public ByteBuffer wrap(int offset, int length) {
			return null;
		}

		@Override
		public byte get(int index) {
			return 0;
		}

		@Override
		public void put(int index, byte b) {}

		@Override
		public void get(int index, byte[] dst) {}

		@Override
		public void put(int index, byte[] src) {}

		@Override
		public void get(int index, byte[] dst, int offset, int length) {}

		@Override
		public void put(int index, byte[] src, int offset, int length) {}

		@Override
		public boolean getBoolean(int index) {
			return false;
		}

		@Override
		public void putBoolean(int index, boolean value) {}

		@Override
		public void get(DataOutput out, int offset, int length) {}

		@Override
		public void put(DataInput in, int offset, int length) {}

		@Override
		public void get(int offset, ByteBuffer target, int numBytes) {}

		@Override
		public void put(int offset, ByteBuffer source, int numBytes) {}
	}
}
