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

package org.apache.flink.core.memory.benchmarks;

import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegment;

import java.nio.ByteBuffer;
import java.util.Random;

@SuppressWarnings("ConstantConditions")
public class MemorySegmentSpeedBenchmark {
	
	private static final long LONG_VALUE = 0x1234567890abcdefl;
	
	private static final boolean TEST_CORE_ON_HEAP = true;
	private static final boolean TEST_CORE_OFF_HEAP = false;
	
	// we keep this to make sure the JIT does not eliminate certain loops
	public static long sideEffect = 0L;
	
	
	public static void main(String[] args) {
		
		final int SMALL_SEGMENT_SIZE = 32 * 1024;
		final int LARGE_SEGMENT_SIZE = 1024 * 1024 * 1024;
		
		final int SMALL_SEGMENTS_ROUNDS = 100000;
		final int LARGE_SEGMENT_ROUNDS = 10;
		
		final byte[] largeSegment = new byte[LARGE_SEGMENT_SIZE];
		final byte[] smallSegment = new byte[SMALL_SEGMENT_SIZE];
		
		final ByteBuffer largeOffHeap = ByteBuffer.allocateDirect(LARGE_SEGMENT_SIZE);
		final ByteBuffer smallOffHeap = ByteBuffer.allocateDirect(SMALL_SEGMENT_SIZE);

		System.out.println("testing access of individual bytes");
		
		testPutBytes(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE, SMALL_SEGMENTS_ROUNDS);
		testGetBytes(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE, SMALL_SEGMENTS_ROUNDS);
		testPutBytes(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE, LARGE_SEGMENT_ROUNDS);
		testGetBytes(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE, LARGE_SEGMENT_ROUNDS);

		System.out.println("testing access of byte arrays");

		testPutByteArrays1024(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE / 1024, SMALL_SEGMENTS_ROUNDS);
		testGetByteArrays1024(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE / 1024, SMALL_SEGMENTS_ROUNDS);
		testPutByteArrays1024(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE / 1024, LARGE_SEGMENT_ROUNDS);
		testGetByteArrays1024(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE / 1024, LARGE_SEGMENT_ROUNDS);
		
		System.out.println("testing access of longs");
		
		testPutLongs(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
		testGetLongs(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
		testPutLongs(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);
		testGetLongs(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);

//		System.out.println("testing access of big endian longs");
//		
//		testPutLongsBigEndian(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
//		testGetLongsBigEndian(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
//		testPutLongsBigEndian(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);
//		testGetLongsBigEndian(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);
//
//		System.out.println("testing access of little endian longs");
//		
//		testPutLongsLittleEndian(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
//		testGetLongsLittleEndian(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
//		testPutLongsLittleEndian(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);
//		testGetLongsLittleEndian(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);

		System.out.println("testing access of ints");
		
		testPutInts(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE / 4, SMALL_SEGMENTS_ROUNDS);
		testGetInts(smallSegment, smallOffHeap, SMALL_SEGMENT_SIZE / 4, SMALL_SEGMENTS_ROUNDS);
		testPutInts(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE / 4, LARGE_SEGMENT_ROUNDS);
		testGetInts(largeSegment, largeOffHeap, LARGE_SEGMENT_SIZE / 4, LARGE_SEGMENT_ROUNDS);


	}

	// --------------------------------------------------------------------------------------------
	//                                  BYTEs
	// --------------------------------------------------------------------------------------------

	private static void testPutBytes(final byte[] heapMemory, final ByteBuffer offHeapMemory,
										final int numValues, final int rounds) {
		
		TestRunner pureHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHeapMemorySegment seg = new PureHeapMemorySegment(heapMemory);
				return timePutBytesOnHeap(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(heapMemory);
				return timePutBytesHybrid(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(offHeapMemory);
				return timePutBytesHybrid(seg, numValues, rounds);
			}
		};

		TestRunner coreHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HeapMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timePutBytesAbstract(seg, numValues, rounds);
			}
		};

		TestRunner coreHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timePutBytesAbstract(seg, numValues, rounds);
			}
		};
		
		TestRunner coreHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledOffHeapMemory(offHeapMemory, null);
				return timePutBytesAbstract(seg, numValues, rounds);
			}
		};
		
		TestRunner[] tests = {
				TEST_CORE_ON_HEAP ? coreHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridOffHeapRunner : null,
				pureHeapRunner, pureHybridHeapRunner, pureHybridOffHeapRunner
		};

		long[] results = runTestsInRandomOrder(tests, new Random(), 5, true);

		System.out.println(String.format(
				"Writing %d x %d bytes to %d bytes segment: " +
						"\n\theap=%,d msecs" +
						"\n\thybrid-on-heap=%,d msecs" +
						"\n\thybrid-off-heap=%,d msecs" +
						"\n\tspecialized heap=%,d msecs, " +
						"\n\tspecialized-hybrid-heap=%,d msecs, " +
						"\n\tspecialized-hybrid-off-heap=%,d msecs, ",
				rounds, numValues, heapMemory.length,
				(results[0] / 1000000), (results[1] / 1000000), (results[2] / 1000000),
				(results[3] / 1000000), (results[4] / 1000000), (results[5] / 1000000)));
	}

	private static void testGetBytes(final byte[] heapMemory, final ByteBuffer offHeapMemory,
										final int numValues, final int rounds) {

		TestRunner pureHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHeapMemorySegment seg = new PureHeapMemorySegment(heapMemory);
				return timeGetBytesOnHeap(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(heapMemory);
				return timeGetBytesHybrid(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(offHeapMemory);
				return timeGetBytesHybrid(seg, numValues, rounds);
			}
		};

		TestRunner coreHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HeapMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timeGetBytesAbstract(seg, numValues, rounds);
			}
		};

		TestRunner coreHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timeGetBytesAbstract(seg, numValues, rounds);
			}
		};

		TestRunner coreHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledOffHeapMemory(offHeapMemory, null);
				return timeGetBytesAbstract(seg, numValues, rounds);
			}
		};

		TestRunner[] tests = {
				TEST_CORE_ON_HEAP ? coreHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridOffHeapRunner : null,
				pureHeapRunner, pureHybridHeapRunner, pureHybridOffHeapRunner
		};

		long[] results = runTestsInRandomOrder(tests, new Random(), 5, true);

		System.out.println(String.format(
				"Reading %d x %d bytes from %d bytes segment: " +
						"\n\theap=%,d msecs" +
						"\n\thybrid-on-heap=%,d msecs" +
						"\n\thybrid-off-heap=%,d msecs" +
						"\n\tspecialized heap=%,d msecs, " +
						"\n\tspecialized-hybrid-heap=%,d msecs, " +
						"\n\tspecialized-hybrid-off-heap=%,d msecs, ",
				rounds, numValues, heapMemory.length,
				(results[0] / 1000000), (results[1] / 1000000), (results[2] / 1000000),
				(results[3] / 1000000), (results[4] / 1000000), (results[5] / 1000000)));
	}

	private static long timePutBytesOnHeap(final PureHeapMemorySegment segment, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.put(offset, (byte) i);
				offset++;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}

	private static long timePutBytesOffHeap(final PureOffHeapMemorySegment segment, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.put(offset, (byte) i);
				offset++;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}

	private static long timePutBytesHybrid(final PureHybridMemorySegment segment, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.put(offset, (byte) i);
				offset++;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}

	private static long timePutBytesAbstract(final MemorySegment segment, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.put(offset, (byte) i);
				offset++;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetBytesOnHeap(final PureHeapMemorySegment segment, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.get(offset);
				offset++;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}

	private static long timeGetBytesOffHeap(final PureOffHeapMemorySegment segment, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.get(offset);
				offset++;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}

	private static long timeGetBytesHybrid(final PureHybridMemorySegment segment, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.get(offset);
				offset++;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}


	private static long timeGetBytesAbstract(final MemorySegment segment, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.get(offset);
				offset++;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}

	// --------------------------------------------------------------------------------------------
	//                                  LONGs
	// --------------------------------------------------------------------------------------------
	
	private static void testPutLongs(final byte[] heapMemory, final ByteBuffer offHeapMemory,
										final int numValues, final int rounds) {

		TestRunner pureHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHeapMemorySegment seg = new PureHeapMemorySegment(heapMemory);
				return timePutLongsOnHeap(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(heapMemory);
				return timePutLongsHybrid(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(offHeapMemory);
				return timePutLongsHybrid(seg, numValues, rounds);
			}
		};

		TestRunner coreHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HeapMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timePutLongsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner coreHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timePutLongsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner coreHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledOffHeapMemory(offHeapMemory, null);
				return timePutLongsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner[] tests = {
				TEST_CORE_ON_HEAP ? coreHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridOffHeapRunner : null,
				pureHeapRunner, pureHybridHeapRunner, pureHybridOffHeapRunner
		};

		long[] results = runTestsInRandomOrder(tests, new Random(), 5, true);

		System.out.println(String.format(
				"Writing %d x %d longs to %d bytes segment: " +
						"\n\theap=%,d msecs" +
						"\n\thybrid-on-heap=%,d msecs" +
						"\n\thybrid-off-heap=%,d msecs" +
						"\n\tspecialized heap=%,d msecs, " +
						"\n\tspecialized-hybrid-heap=%,d msecs, " +
						"\n\tspecialized-hybrid-off-heap=%,d msecs, ",
				rounds, numValues, heapMemory.length,
				(results[0] / 1000000), (results[1] / 1000000), (results[2] / 1000000),
				(results[3] / 1000000), (results[4] / 1000000), (results[5] / 1000000)));
	}
	
	private static void testGetLongs(final byte[] heapMemory, final ByteBuffer offHeapMemory,
										final int numValues, final int rounds) {

		TestRunner pureHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHeapMemorySegment seg = new PureHeapMemorySegment(heapMemory);
				return timeGetLongsOnHeap(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(heapMemory);
				return timeGetLongsHybrid(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(offHeapMemory);
				return timeGetLongsHybrid(seg, numValues, rounds);
			}
		};

		TestRunner coreHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HeapMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timeGetLongsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner coreHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timeGetLongsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner coreHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledOffHeapMemory(offHeapMemory, null);
				return timeGetLongsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner[] tests = {
				TEST_CORE_ON_HEAP ? coreHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridOffHeapRunner : null,
				pureHeapRunner, pureHybridHeapRunner, pureHybridOffHeapRunner
		};

		long[] results = runTestsInRandomOrder(tests, new Random(), 5, true);

		System.out.println(String.format(
				"Reading %d x %d longs from %d bytes segment: " +
						"\n\theap=%,d msecs" +
						"\n\thybrid-on-heap=%,d msecs" +
						"\n\thybrid-off-heap=%,d msecs" +
						"\n\tspecialized heap=%,d msecs, " +
						"\n\tspecialized-hybrid-heap=%,d msecs, " +
						"\n\tspecialized-hybrid-off-heap=%,d msecs, ",
				rounds, numValues, heapMemory.length,
				(results[0] / 1000000), (results[1] / 1000000), (results[2] / 1000000),
				(results[3] / 1000000), (results[4] / 1000000), (results[5] / 1000000)));
	}
	
	private static long timePutLongsOnHeap(final PureHeapMemorySegment segment, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.putLong(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutLongsOffHeap(final PureOffHeapMemorySegment segment, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.putLong(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutLongsHybrid(final PureHybridMemorySegment segment, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.putLong(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}

	private static long timePutLongsAbstract(final MemorySegment segment, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.putLong(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetLongsOnHeap(final PureHeapMemorySegment segment, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.getLong(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetLongsOffHeap(final PureOffHeapMemorySegment segment, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.getLong(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetLongsHybrid(final PureHybridMemorySegment segment, final int num, final int rounds) {
		// checked segment
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.getLong(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}

	private static long timeGetLongsAbstract(final MemorySegment segment, final int num, final int rounds) {
		// checked segment
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.getLong(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  INTs
	// --------------------------------------------------------------------------------------------
	
	private static void testPutInts(final byte[] heapMemory, final ByteBuffer offHeapMemory,
									final int numValues, final int rounds) {

		TestRunner pureHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHeapMemorySegment seg = new PureHeapMemorySegment(heapMemory);
				return timePutIntsOnHeap(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(heapMemory);
				return timePutIntsHybrid(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(offHeapMemory);
				return timePutIntsHybrid(seg, numValues, rounds);
			}
		};

		TestRunner coreHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HeapMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timePutIntsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner coreHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timePutIntsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner coreHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledOffHeapMemory(offHeapMemory, null);
				return timePutIntsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner[] tests = {
				TEST_CORE_ON_HEAP ? coreHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridOffHeapRunner : null,
				pureHeapRunner, pureHybridHeapRunner, pureHybridOffHeapRunner
		};

		long[] results = runTestsInRandomOrder(tests, new Random(), 5, true);

		System.out.println(String.format(
				"Writing %d x %d ints to %d bytes segment: " +
						"\n\theap=%,d msecs" +
						"\n\thybrid-on-heap=%,d msecs" +
						"\n\thybrid-off-heap=%,d msecs" +
						"\n\tspecialized heap=%,d msecs, " +
						"\n\tspecialized-hybrid-heap=%,d msecs, " +
						"\n\tspecialized-hybrid-off-heap=%,d msecs, ",
				rounds, numValues, heapMemory.length,
				(results[0] / 1000000), (results[1] / 1000000), (results[2] / 1000000),
				(results[3] / 1000000), (results[4] / 1000000), (results[5] / 1000000)));
	}
	
	private static void testGetInts(final byte[] heapMemory, final ByteBuffer offHeapMemory,
									final int numValues, final int rounds) {

		TestRunner pureHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHeapMemorySegment seg = new PureHeapMemorySegment(heapMemory);
				return timeGetIntsOnHeap(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(heapMemory);
				return timeGetIntsHybrid(seg, numValues, rounds);
			}
		};

		TestRunner pureHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(offHeapMemory);
				return timeGetIntsHybrid(seg, numValues, rounds);
			}
		};

		TestRunner coreHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HeapMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timeGetIntsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner coreHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timeGetIntsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner coreHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledOffHeapMemory(offHeapMemory, null);
				return timeGetIntsAbstract(seg, numValues, rounds);
			}
		};

		TestRunner[] tests = {
				TEST_CORE_ON_HEAP ? coreHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridOffHeapRunner : null,
				pureHeapRunner, pureHybridHeapRunner, pureHybridOffHeapRunner
		};

		long[] results = runTestsInRandomOrder(tests, new Random(), 5, true);

		System.out.println(String.format(
				"Reading %d x %d ints from %d bytes segment: " +
						"\n\theap=%,d msecs" +
						"\n\thybrid-on-heap=%,d msecs" +
						"\n\thybrid-off-heap=%,d msecs" +
						"\n\tspecialized heap=%,d msecs, " +
						"\n\tspecialized-hybrid-heap=%,d msecs, " +
						"\n\tspecialized-hybrid-off-heap=%,d msecs, ",
				rounds, numValues, heapMemory.length,
				(results[0] / 1000000), (results[1] / 1000000), (results[2] / 1000000),
				(results[3] / 1000000), (results[4] / 1000000), (results[5] / 1000000)));
	}
	
	private static long timePutIntsOnHeap(final PureHeapMemorySegment segment, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.putInt(offset, i);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutIntsOffHeap(final PureOffHeapMemorySegment segment, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.putInt(offset, i);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutIntsHybrid(final PureHybridMemorySegment segment, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.putInt(offset, i);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}

	private static long timePutIntsAbstract(final MemorySegment segment, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.putInt(offset, i);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetIntsOnHeap(final PureHeapMemorySegment segment, final int num, final int rounds) {
		int l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.getInt(offset);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetIntsOffHeap(final PureOffHeapMemorySegment segment, final int num, final int rounds) {
		int l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.getInt(offset);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetIntsHybrid(final PureHybridMemorySegment segment, final int num, final int rounds) {
		int l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.getInt(offset);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}

	private static long timeGetIntsAbstract(final MemorySegment segment, final int num, final int rounds) {
		int l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += segment.getInt(offset);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  BYTE ARRAYs
	// --------------------------------------------------------------------------------------------
	
	private static void testPutByteArrays1024(final byte[] heapMemory, final ByteBuffer offHeapMemory, 
												final int numValues, final int rounds) {
		
		final byte[] sourceArray = new byte[1024];
		for (int i = 0; i < sourceArray.length; i++) {
			sourceArray[i] = (byte) i;
		}

		TestRunner pureHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHeapMemorySegment seg = new PureHeapMemorySegment(heapMemory);
				return timePutByteArrayOnHeap(seg, sourceArray, numValues, rounds);
			}
		};

		TestRunner pureHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(heapMemory);
				return timePutByteArrayHybrid(seg, sourceArray, numValues, rounds);
			}
		};

		TestRunner pureHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(offHeapMemory);
				return timePutByteArrayHybrid(seg, sourceArray, numValues, rounds);
			}
		};

		TestRunner coreHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HeapMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timePutByteArrayAbstract(seg, sourceArray, numValues, rounds);
			}
		};

		TestRunner coreHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timePutByteArrayAbstract(seg, sourceArray, numValues, rounds);
			}
		};

		TestRunner coreHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledOffHeapMemory(offHeapMemory, null);
				return timePutByteArrayAbstract(seg, sourceArray, numValues, rounds);
			}
		};

		TestRunner[] tests = {
				TEST_CORE_ON_HEAP ? coreHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridOffHeapRunner : null,
				pureHeapRunner, pureHybridHeapRunner, pureHybridOffHeapRunner
		};

		long[] results = runTestsInRandomOrder(tests, new Random(), 5, true);

		System.out.println(String.format(
				"Writing %d x %d byte[1024] to %d bytes segment: " +
						"\n\theap=%,d msecs" +
						"\n\thybrid-on-heap=%,d msecs" +
						"\n\thybrid-off-heap=%,d msecs" +
						"\n\tspecialized heap=%,d msecs, " +
						"\n\tspecialized-hybrid-heap=%,d msecs, " +
						"\n\tspecialized-hybrid-off-heap=%,d msecs, ",
				rounds, numValues, heapMemory.length,
				(results[0] / 1000000), (results[1] / 1000000), (results[2] / 1000000),
				(results[3] / 1000000), (results[4] / 1000000), (results[5] / 1000000)));
	}
	
	private static void testGetByteArrays1024(final byte[] heapMemory, final ByteBuffer offHeapMemory,
												final int numValues, final int rounds) {
		
		final byte[] targetArray = new byte[1024];

		TestRunner pureHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHeapMemorySegment seg = new PureHeapMemorySegment(heapMemory);
				return timeGetByteArrayOnHeap(seg, targetArray, numValues, rounds);
			}
		};

		TestRunner pureHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(heapMemory);
				return timeGetByteArrayHybrid(seg, targetArray, numValues, rounds);
			}
		};

		TestRunner pureHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				PureHybridMemorySegment seg = new PureHybridMemorySegment(offHeapMemory);
				return timeGetByteArrayHybrid(seg, targetArray, numValues, rounds);
			}
		};

		TestRunner coreHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HeapMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timeGetByteArrayAbstract(seg, targetArray, numValues, rounds);
			}
		};

		TestRunner coreHybridHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOnHeap(heapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledHeapMemory(heapMemory, null);
				return timeGetByteArrayAbstract(seg, targetArray, numValues, rounds);
			}
		};

		TestRunner coreHybridOffHeapRunner = new TestRunner() {
			@Override
			public long runTest() {
				fillOffHeap(offHeapMemory, (byte) 0);
				MemorySegment seg = HybridMemorySegment.FACTORY.wrapPooledOffHeapMemory(offHeapMemory, null);
				return timeGetByteArrayAbstract(seg, targetArray, numValues, rounds);
			}
		};

		TestRunner[] tests = {
				TEST_CORE_ON_HEAP ? coreHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridHeapRunner : null,
				TEST_CORE_OFF_HEAP ? coreHybridOffHeapRunner : null,
				pureHeapRunner, pureHybridHeapRunner, pureHybridOffHeapRunner
		};

		long[] results = runTestsInRandomOrder(tests, new Random(), 5, true);

		System.out.println(String.format(
				"Reading %d x %d byte[1024] from %d bytes segment: " +
						"\n\theap=%,d msecs" +
						"\n\thybrid-on-heap=%,d msecs" +
						"\n\thybrid-off-heap=%,d msecs" +
						"\n\tspecialized heap=%,d msecs, " +
						"\n\tspecialized-hybrid-heap=%,d msecs, " +
						"\n\tspecialized-hybrid-off-heap=%,d msecs, ",
				rounds, numValues, heapMemory.length,
				(results[0] / 1000000), (results[1] / 1000000), (results[2] / 1000000),
				(results[3] / 1000000), (results[4] / 1000000), (results[5] / 1000000)));
	}
	
	private static long timePutByteArrayOnHeap(final PureHeapMemorySegment segment, final byte[] source, final int num, final int rounds) {
		final int len = source.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.put(offset, source, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutByteArrayOffHeap(final PureOffHeapMemorySegment segment, final byte[] source, final int num, final int rounds) {
		final int len = source.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.put(offset, source, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutByteArrayHybrid(final PureHybridMemorySegment segment, final byte[] source, final int num, final int rounds) {
		final int len = source.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.put(offset, source, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}

	private static long timePutByteArrayAbstract(final MemorySegment segment, final byte[] source, final int num, final int rounds) {
		final int len = source.length;

		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.put(offset, source, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetByteArrayOnHeap(final PureHeapMemorySegment segment, final byte[] target, final int num, final int rounds) {
		final int len = target.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.get(offset, target, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetByteArrayOffHeap(final PureOffHeapMemorySegment segment, final byte[] target, final int num, final int rounds) {
		final int len = target.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.get(offset, target, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetByteArrayHybrid(final PureHybridMemorySegment segment, final byte[] target, final int num, final int rounds) {
		final int len = target.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.get(offset, target, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}

	private static long timeGetByteArrayAbstract(final MemorySegment segment, final byte[] target, final int num, final int rounds) {
		final int len = target.length;

		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				segment.get(offset, target, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}

//	// --------------------------------------------------------------------------------------------
//	//                                  LONG BIG ENDIAN
//	// --------------------------------------------------------------------------------------------
//
//	private static void testPutLongsBigEndian(byte[] heapMemory, ByteBuffer offHeapMemory, int numValues, int rounds) {
//		// test the pure heap memory 
//		fillOnHeap(heapMemory, (byte) 0);
//		PureHeapMemorySegment heapMemorySegment = new PureHeapMemorySegment(heapMemory);
//		long elapsedOnHeap = timePutLongsBigEndianOnHeap(heapMemorySegment, numValues, rounds);
//
//		// test the pure off-heap memory
//		fillOffHeap(offHeapMemory, (byte) 0);
//		PureOffHeapMemorySegment offHeapMemorySegment = new PureOffHeapMemorySegment(offHeapMemory);
//		long elapsedOffHeap = timePutLongsBigEndianOffHeap(offHeapMemorySegment, numValues, rounds);
//
//		// test hybrid on heap 
//		fillOnHeap(heapMemory, (byte) 0);
//		PureHybridMemorySegment hybridOnHeap = new PureHybridMemorySegment(heapMemory);
//		long elapsedHybridOnHeap = timePutLongsBigEndianHybrid(hybridOnHeap, numValues, rounds);
//
//		// test hybrid off heap 
//		fillOffHeap(offHeapMemory, (byte) 0);
//		PureHybridMemorySegment hybridOffeap = new PureHybridMemorySegment(offHeapMemory);
//		long elapsedHybridOffHeap = timePutLongsBigEndianHybrid(hybridOffeap, numValues, rounds);
//
//		System.out.println(String.format(
//				"Writing %d x %d big-endian longs to %d bytes segment: " +
//						"heap=%,d msecs, " +
//						"off-heap=%,d msecs, " +
//						"hybrid-on-heap=%,d msecs, " +
//						"hybrid-off-heap=%,d msecs",
//				rounds, numValues, heapMemory.length,
//				(elapsedOnHeap / 1000000), (elapsedOffHeap / 1000000),
//				(elapsedHybridOnHeap / 1000000), (elapsedHybridOffHeap / 1000000)));
//	}
//
//	private static void testGetLongsBigEndian(byte[] heapMemory, ByteBuffer offHeapMemory, int numValues, int rounds) {
//		// test the pure heap memory 
//		fillOnHeap(heapMemory, (byte) 0);
//		PureHeapMemorySegment heapMemorySegment = new PureHeapMemorySegment(heapMemory);
//		long elapsedOnHeap = timeGetLongsBigEndianOnHeap(heapMemorySegment, numValues, rounds);
//
//		// test the pure off-heap memory
//		fillOffHeap(offHeapMemory, (byte) 0);
//		PureOffHeapMemorySegment offHeapMemorySegment = new PureOffHeapMemorySegment(offHeapMemory);
//		long elapsedOffHeap = timeGetLongsBigEndianOffHeap(offHeapMemorySegment, numValues, rounds);
//
//		// test hybrid on heap 
//		fillOnHeap(heapMemory, (byte) 0);
//		PureHybridMemorySegment hybridOnHeap = new PureHybridMemorySegment(heapMemory);
//		long elapsedHybridOnHeap = timeGetLongsBigEndianHybrid(hybridOnHeap, numValues, rounds);
//
//		// test hybrid off heap 
//		fillOffHeap(offHeapMemory, (byte) 0);
//		PureHybridMemorySegment hybridOffeap = new PureHybridMemorySegment(offHeapMemory);
//		long elapsedHybridOffHeap = timeGetLongsBigEndianHybrid(hybridOffeap, numValues, rounds);
//
//		System.out.println(String.format(
//				"Reading %d x %d big-endian longs from %d bytes segment: " +
//						"heap=%,d msecs, " +
//						"off-heap=%,d msecs, " +
//						"hybrid-on-heap=%,d msecs, " +
//						"hybrid-off-heap=%,d msecs",
//				rounds, numValues, heapMemory.length,
//				(elapsedOnHeap / 1000000), (elapsedOffHeap / 1000000),
//				(elapsedHybridOnHeap / 1000000), (elapsedHybridOffHeap / 1000000)));
//	}
//
//	private static long timePutLongsBigEndianOnHeap(final PureHeapMemorySegment segment, final int num, final int rounds) {
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				segment.putLongBigEndian(offset, LONG_VALUE);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		return end - start;
//	}
//
//	private static long timePutLongsBigEndianOffHeap(final PureOffHeapMemorySegment segment, final int num, final int rounds) {
//		// checked segment
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				segment.putLongBigEndian(offset, LONG_VALUE);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		return end - start;
//	}
//
//	private static long timePutLongsBigEndianHybrid(final PureHybridMemorySegment segment, final int num, final int rounds) {
//		// checked segment
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				segment.putLongBigEndian(offset, LONG_VALUE);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		return end - start;
//	}
//
//	private static long timeGetLongsBigEndianOnHeap(final PureHeapMemorySegment segment, final int num, final int rounds) {
//		long l = 0;
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				l += segment.getLongBigEndian(offset);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		sideEffect += l;
//		return end - start;
//	}
//
//	private static long timeGetLongsBigEndianOffHeap(final PureOffHeapMemorySegment segment, final int num, final int rounds) {
//		long l = 0;
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				l += segment.getLongBigEndian(offset);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		sideEffect += l;
//		return end - start;
//	}
//
//	private static long timeGetLongsBigEndianHybrid(final PureHybridMemorySegment segment, final int num, final int rounds) {
//		// checked segment
//		long l = 0;
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				l += segment.getLongBigEndian(offset);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		sideEffect += l;
//		return end - start;
//	}
//
//	// --------------------------------------------------------------------------------------------
//	//                                  LONG LITTLE ENDIAN
//	// --------------------------------------------------------------------------------------------
//
//	private static void testPutLongsLittleEndian(byte[] heapMemory, ByteBuffer offHeapMemory, int numValues, int rounds) {
//		// test the pure heap memory 
//		fillOnHeap(heapMemory, (byte) 0);
//		PureHeapMemorySegment heapMemorySegment = new PureHeapMemorySegment(heapMemory);
//		long elapsedOnHeap = timePutLongsLittleEndianOnHeap(heapMemorySegment, numValues, rounds);
//
//		// test the pure off-heap memory
//		fillOffHeap(offHeapMemory, (byte) 0);
//		PureOffHeapMemorySegment offHeapMemorySegment = new PureOffHeapMemorySegment(offHeapMemory);
//		long elapsedOffHeap = timePutLongsLittleEndianOffHeap(offHeapMemorySegment, numValues, rounds);
//
//		// test hybrid on heap 
//		fillOnHeap(heapMemory, (byte) 0);
//		PureHybridMemorySegment hybridOnHeap = new PureHybridMemorySegment(heapMemory);
//		long elapsedHybridOnHeap = timePutLongsLittleEndianHybrid(hybridOnHeap, numValues, rounds);
//
//		// test hybrid off heap 
//		fillOffHeap(offHeapMemory, (byte) 0);
//		PureHybridMemorySegment hybridOffeap = new PureHybridMemorySegment(offHeapMemory);
//		long elapsedHybridOffHeap = timePutLongsLittleEndianHybrid(hybridOffeap, numValues, rounds);
//
//		System.out.println(String.format(
//				"Writing %d x %d little-endian longs to %d bytes segment: " +
//						"heap=%,d msecs, " +
//						"off-heap=%,d msecs, " +
//						"hybrid-on-heap=%,d msecs, " +
//						"hybrid-off-heap=%,d msecs",
//				rounds, numValues, heapMemory.length,
//				(elapsedOnHeap / 1000000), (elapsedOffHeap / 1000000),
//				(elapsedHybridOnHeap / 1000000), (elapsedHybridOffHeap / 1000000)));
//	}
//
//	private static void testGetLongsLittleEndian(byte[] heapMemory, ByteBuffer offHeapMemory, int numValues, int rounds) {
//		// test the pure heap memory 
//		fillOnHeap(heapMemory, (byte) 0);
//		PureHeapMemorySegment heapMemorySegment = new PureHeapMemorySegment(heapMemory);
//		long elapsedOnHeap = timeGetLongsLittleEndianOnHeap(heapMemorySegment, numValues, rounds);
//
//		// test the pure off-heap memory
//		fillOffHeap(offHeapMemory, (byte) 0);
//		PureOffHeapMemorySegment offHeapMemorySegment = new PureOffHeapMemorySegment(offHeapMemory);
//		long elapsedOffHeap = timeGetLongsLittleEndianOffHeap(offHeapMemorySegment, numValues, rounds);
//
//		// test hybrid on heap 
//		fillOnHeap(heapMemory, (byte) 0);
//		PureHybridMemorySegment hybridOnHeap = new PureHybridMemorySegment(heapMemory);
//		long elapsedHybridOnHeap = timeGetLongsLittleEndianHybrid(hybridOnHeap, numValues, rounds);
//
//		// test hybrid off heap 
//		fillOffHeap(offHeapMemory, (byte) 0);
//		PureHybridMemorySegment hybridOffeap = new PureHybridMemorySegment(offHeapMemory);
//		long elapsedHybridOffHeap = timeGetLongsLittleEndianHybrid(hybridOffeap, numValues, rounds);
//
//		System.out.println(String.format(
//				"Reading %d x %d little-endian longs from %d bytes segment: " +
//						"heap=%,d msecs, " +
//						"off-heap=%,d msecs, " +
//						"hybrid-on-heap=%,d msecs, " +
//						"hybrid-off-heap=%,d msecs",
//				rounds, numValues, heapMemory.length,
//				(elapsedOnHeap / 1000000), (elapsedOffHeap / 1000000),
//				(elapsedHybridOnHeap / 1000000), (elapsedHybridOffHeap / 1000000)));
//	}
//
//	private static long timePutLongsLittleEndianOnHeap(final PureHeapMemorySegment segment, final int num, final int rounds) {
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				segment.putLongLittleEndian(offset, LONG_VALUE);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		return end - start;
//	}
//
//	private static long timePutLongsLittleEndianOffHeap(final PureOffHeapMemorySegment segment, final int num, final int rounds) {
//		// checked segment
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				segment.putLongLittleEndian(offset, LONG_VALUE);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		return end - start;
//	}
//
//	private static long timePutLongsLittleEndianHybrid(final PureHybridMemorySegment segment, final int num, final int rounds) {
//		// checked segment
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				segment.putLongLittleEndian(offset, LONG_VALUE);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		return end - start;
//	}
//
//	private static long timeGetLongsLittleEndianOnHeap(final PureHeapMemorySegment segment, final int num, final int rounds) {
//		long l = 0;
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				l += segment.getLongLittleEndian(offset);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		sideEffect += l;
//		return end - start;
//	}
//
//	private static long timeGetLongsLittleEndianOffHeap(final PureOffHeapMemorySegment segment, final int num, final int rounds) {
//		long l = 0;
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				l += segment.getLongLittleEndian(offset);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		sideEffect += l;
//		return end - start;
//	}
//
//	private static long timeGetLongsLittleEndianHybrid(final PureHybridMemorySegment segment, final int num, final int rounds) {
//		// checked segment
//		long l = 0;
//		long start = System.nanoTime();
//		for (int round = 0; round < rounds; round++) {
//			int offset = 0;
//			for (int i = 0; i < num; i++) {
//				l += segment.getLongLittleEndian(offset);
//				offset += 8;
//			}
//		}
//		long end = System.nanoTime();
//		sideEffect += l;
//		return end - start;
//	}
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static void fillOnHeap(byte[] buffer, byte data) {
		for (int i = 0; i < buffer.length; i++) {
			buffer[i] = data;
		}
	}
	
	private static void fillOffHeap(ByteBuffer buffer, byte data) {
		final int len = buffer.capacity();
		for (int i = 0; i < len; i++) {
			buffer.put(i, data);
		}
	}
	
	private static long[] runTestsInRandomOrder(TestRunner[] runners, Random rnd, int numRuns, boolean printMeasures) {
		if (numRuns < 3) {
			throw new IllegalArgumentException("must do at least three runs");
		}
		
		// we run all runners in random order, to account for the JIT effects that specialize methods
		// The observation is that either earlier tests suffer from performance because the JIT needs to kick
		// in first, or that later tests suffer from performance, because the HIT optimized for the other case already
		
		long[][] measures = new long[runners.length][];
		for (int i = 0; i < measures.length; i++) {
			measures[i] = new long[numRuns];
		}
		
		for (int test = 0; test < numRuns; test++) {
			System.out.println("Round " + (test+1) + '/' + numRuns);
			
			// pick an order for the tests
			int[] order = new int[runners.length];
			for (int i = 0; i < order.length; i++) {
				order[i] = i;
			}
			for (int i = order.length; i > 1; i--) {
				int pos1 = i-1;
				int pos2 = rnd.nextInt(i);
				int tmp = order[pos1];
				order[pos1] = order[pos2];
				order[pos2] = tmp;
			}
			
			// run tests
			for (int pos : order) {
				TestRunner next = runners[pos];
				measures[pos][test] = next != null ? next.runTest() : 0L;
			}
		}
		
		if (printMeasures) {
			for (long[] series : measures) {
				StringBuilder bld = new StringBuilder();
				for (long measure : series) {
					bld.append(String.format("%,d", (measure / 1000000))).append(" | ");
				}
				System.out.println(bld.toString());
			}
		}
		
		// aggregate the measures
		long[] results = new long[runners.length];
		
		for (int i = 0; i < runners.length; i++) {
			// cancel out the min and max
			long max = Long.MIN_VALUE;
			long min = Long.MAX_VALUE;
			
			for (long val : measures[i]) {
				max = Math.max(max, val);
				min = Math.min(min, val);
			}
			
			long total = 0L;
			for (long val : measures[i]) {
				if (val != max && val != min) {
					total += val;
				}
			}
			
			results[i] = total / (numRuns - 2);
		}
		
		return results;
	}
	
	
	
	private static interface TestRunner {
		
		long runTest();
	}
}
