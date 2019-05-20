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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Verifies interoperability between {@link HeapMemorySegment} and {@link HybridMemorySegment} (in
 * both heap and off-heap modes).
 */
public class CrossSegmentTypeTest {

	private static final long BYTE_ARRAY_BASE_OFFSET = MemoryUtils.UNSAFE.arrayBaseOffset(byte[].class);

	private final int pageSize = 32 * 1024;

	// ------------------------------------------------------------------------

	@Test
	public void testCompareBytesMixedSegments() {
		MemorySegment[] segs1 = {
				new HeapMemorySegment(new byte[pageSize]),
				new HybridMemorySegment(new byte[pageSize]),
				new HybridMemorySegment(ByteBuffer.allocateDirect(pageSize))
		};

		MemorySegment[] segs2 = {
				new HeapMemorySegment(new byte[pageSize]),
				new HybridMemorySegment(new byte[pageSize]),
				new HybridMemorySegment(ByteBuffer.allocateDirect(pageSize))
		};

		Random rnd = new Random();

		for (MemorySegment seg1 : segs1) {
			for (MemorySegment seg2 : segs2) {
				testCompare(seg1, seg2, rnd);
			}
		}
	}

	private void testCompare(MemorySegment seg1, MemorySegment seg2, Random random) {
		assertEquals(pageSize, seg1.size());
		assertEquals(pageSize, seg2.size());

		final byte[] bytes1 = new byte[pageSize];
		final byte[] bytes2 = new byte[pageSize];

		final int stride = pageSize / 255;
		final int shift = 16666;

		for (int i = 0; i < pageSize; i++) {
			byte val = (byte) ((i / stride) & 0xff);
			bytes1[i] = val;

			if (i + shift < bytes2.length) {
				bytes2[i + shift] = val;
			}
		}

		seg1.put(0, bytes1);
		seg2.put(0, bytes2);

		for (int i = 0; i < 1000; i++) {
			int pos1 = random.nextInt(bytes1.length);
			int pos2 = random.nextInt(bytes2.length);

			int len = Math.min(Math.min(bytes1.length - pos1, bytes2.length - pos2),
					random.nextInt(pageSize / 50));

			int cmp = seg1.compare(seg2, pos1, pos2, len);

			if (pos1 < pos2 - shift) {
				assertTrue(cmp <= 0);
			}
			else {
				assertTrue(cmp >= 0);
			}
		}
	}

	@Test
	public void testSwapBytesMixedSegments() {
		final int halfPageSize = pageSize / 2;

		MemorySegment[] segs1 = {
				new HeapMemorySegment(new byte[pageSize]),
				new HybridMemorySegment(new byte[pageSize]),
				new HybridMemorySegment(ByteBuffer.allocateDirect(pageSize))
		};

		MemorySegment[] segs2 = {
				new HeapMemorySegment(new byte[halfPageSize]),
				new HybridMemorySegment(new byte[halfPageSize]),
				new HybridMemorySegment(ByteBuffer.allocateDirect(halfPageSize))
		};

		Random rnd = new Random();

		for (MemorySegment seg1 : segs1) {
			for (MemorySegment seg2 : segs2) {
				testSwap(seg1, seg2, rnd, halfPageSize);
			}
		}
	}

	private void testSwap(MemorySegment seg1, MemorySegment seg2, Random random, int smallerSize) {
		assertEquals(pageSize, seg1.size());
		assertEquals(smallerSize, seg2.size());

		final byte[] bytes1 = new byte[pageSize];
		final byte[] bytes2 = new byte[smallerSize];

		Arrays.fill(bytes2, (byte) 1);

		seg1.put(0, bytes1);
		seg2.put(0, bytes2);

		// wap the second half of the first segment with the second segment

		int pos = 0;
		while (pos < smallerSize) {
			int len = random.nextInt(pageSize / 40);
			len = Math.min(len, smallerSize - pos);
			seg1.swapBytes(new byte[len], seg2, pos + smallerSize, pos, len);
			pos += len;
		}

		// the second segment should now be all zeros, the first segment should have one in its second half

		for (int i = 0; i < smallerSize; i++) {
			assertEquals((byte) 0, seg1.get(i));
			assertEquals((byte) 0, seg2.get(i));
			assertEquals((byte) 1, seg1.get(i + smallerSize));
		}
	}

	@Test
	public void testCopyMixedSegments() {
		MemorySegment[] segs1 = {
				new HeapMemorySegment(new byte[pageSize]),
				new HybridMemorySegment(new byte[pageSize]),
				new HybridMemorySegment(ByteBuffer.allocateDirect(pageSize))
		};

		MemorySegment[] segs2 = {
				new HeapMemorySegment(new byte[pageSize]),
				new HybridMemorySegment(new byte[pageSize]),
				new HybridMemorySegment(ByteBuffer.allocateDirect(pageSize))
		};

		Random rnd = new Random();

		for (MemorySegment seg1 : segs1) {
			for (MemorySegment seg2 : segs2) {
				testCopy(seg1, seg2, rnd);
			}
		}
	}

	private void testCopy(MemorySegment seg1, MemorySegment seg2, Random random) {
		assertEquals(pageSize, seg1.size());
		assertEquals(pageSize, seg2.size());

		byte[] expected = new byte[pageSize];
		byte[] actual = new byte[pageSize];
		byte[] unsafeCopy = new byte[pageSize];
		MemorySegment unsafeCopySeg = MemorySegmentFactory.allocateUnpooledSegment(pageSize);

		// zero out the memory
		seg1.put(0, expected);
		seg2.put(0, expected);

		for (int i = 0; i < 40; i++) {
			int numBytes = random.nextInt(pageSize / 20);
			byte[] bytes = new byte[numBytes];
			random.nextBytes(bytes);

			int thisPos = random.nextInt(pageSize - numBytes);
			int otherPos = random.nextInt(pageSize - numBytes);

			// track what we expect
			System.arraycopy(bytes, 0, expected, otherPos, numBytes);

			seg1.put(thisPos, bytes);
			seg1.copyTo(thisPos, seg2, otherPos, numBytes);
			seg1.copyToUnsafe(thisPos, unsafeCopy, (int) (otherPos + BYTE_ARRAY_BASE_OFFSET), numBytes);

			int otherPos2 = random.nextInt(pageSize - numBytes);
			unsafeCopySeg.copyFromUnsafe(otherPos2, unsafeCopy,
					(int) (otherPos + BYTE_ARRAY_BASE_OFFSET), numBytes);
			assertTrue(unsafeCopySeg.equalTo(seg2, otherPos2, otherPos, numBytes));
		}

		seg2.get(0, actual);
		assertArrayEquals(expected, actual);

		// test out of bound conditions

		final int[] validOffsets = { 0, 1, pageSize / 10 * 9 };
		final int[] invalidOffsets = { -1, pageSize + 1, -pageSize, Integer.MAX_VALUE, Integer.MIN_VALUE };

		final int[] validLengths = { 0, 1, pageSize / 10, pageSize };
		final int[] invalidLengths = { -1, -pageSize, pageSize + 1, Integer.MAX_VALUE, Integer.MIN_VALUE };

		for (int off1 : validOffsets) {
			for (int off2 : validOffsets) {
				for (int len : invalidLengths) {
					try {
						seg1.copyTo(off1, seg2, off2, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg1.copyTo(off2, seg2, off1, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg2.copyTo(off1, seg1, off2, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg2.copyTo(off2, seg1, off1, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}
				}
			}
		}

		for (int off1 : validOffsets) {
			for (int off2 : invalidOffsets) {
				for (int len : validLengths) {
					try {
						seg1.copyTo(off1, seg2, off2, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg1.copyTo(off2, seg2, off1, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg2.copyTo(off1, seg1, off2, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg2.copyTo(off2, seg1, off1, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}
				}
			}
		}

		for (int off1 : invalidOffsets) {
			for (int off2 : validOffsets) {
				for (int len : validLengths) {
					try {
						seg1.copyTo(off1, seg2, off2, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg1.copyTo(off2, seg2, off1, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg2.copyTo(off1, seg1, off2, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg2.copyTo(off2, seg1, off1, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}
				}
			}
		}

		for (int off1 : invalidOffsets) {
			for (int off2 : invalidOffsets) {
				for (int len : validLengths) {
					try {
						seg1.copyTo(off1, seg2, off2, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg1.copyTo(off2, seg2, off1, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg2.copyTo(off1, seg1, off2, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}

					try {
						seg2.copyTo(off2, seg1, off1, len);
						fail("should fail with an IndexOutOfBoundsException");
					}
					catch (IndexOutOfBoundsException ignored) {}
				}
			}
		}
	}
}
