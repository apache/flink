/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.nephele.memory;

import java.util.Arrays;

import eu.stratosphere.nephele.services.memorymanager.CheckedMemorySegment;
import eu.stratosphere.nephele.services.memorymanager.DirectMemorySegment;
import eu.stratosphere.nephele.services.memorymanager.UnsafeMemorySegment;

/**
 *
 */
public class MemorySegmentSpeedBenchmark {
	
	private static final long LONG_VALUE = 0x1234567890abcdefl;
	
	private static final int INT_VALUE = 0x12345678;
	
	private static final byte BYTE_VALUE = 0x56;
	
	@SuppressWarnings("unused")
	private static long sideEffect = 0;
	
	
	public static void main(String[] args) {
		final int SMALL_SEGMENT_SIZE = 32 * 1024;
		final int LARGE_SEGMENT_SIZE = 1024 * 1024 * 1024;
		
		final int SMALL_SEGMENTS_ROUNDS = 50000;
		final int LARGE_SEGMENT_ROUNDS = 10;
		
		final byte[] largeSegment = new byte[LARGE_SEGMENT_SIZE];
		final byte[] smallSegment = new byte[SMALL_SEGMENT_SIZE];
		
		testPutLongs(smallSegment, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
		testGetLongs(smallSegment, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
		testPutLongs(largeSegment, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);
		testGetLongs(largeSegment, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);
		
		testPutLongsBigEndian(smallSegment, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
		testGetLongsBigEndian(smallSegment, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
		testPutLongsBigEndian(largeSegment, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);
		testGetLongsBigEndian(largeSegment, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);
		
		testPutLongsLittleEndian(smallSegment, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
		testGetLongsLittleEndian(smallSegment, SMALL_SEGMENT_SIZE / 8, SMALL_SEGMENTS_ROUNDS);
		testPutLongsLittleEndian(largeSegment, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);
		testGetLongsLittleEndian(largeSegment, LARGE_SEGMENT_SIZE / 8, LARGE_SEGMENT_ROUNDS);
		
		testPutInts(smallSegment, SMALL_SEGMENT_SIZE / 4, SMALL_SEGMENTS_ROUNDS);
		testGetInts(smallSegment, SMALL_SEGMENT_SIZE / 4, SMALL_SEGMENTS_ROUNDS);
		testPutInts(largeSegment, LARGE_SEGMENT_SIZE / 4, LARGE_SEGMENT_ROUNDS);
		testGetInts(largeSegment, LARGE_SEGMENT_SIZE / 4, LARGE_SEGMENT_ROUNDS);
		
		testPutBytes(smallSegment, SMALL_SEGMENT_SIZE, SMALL_SEGMENTS_ROUNDS);
		testGetBytes(smallSegment, SMALL_SEGMENT_SIZE, SMALL_SEGMENTS_ROUNDS);
		testPutBytes(largeSegment, LARGE_SEGMENT_SIZE, LARGE_SEGMENT_ROUNDS);
		testGetBytes(largeSegment, LARGE_SEGMENT_SIZE, LARGE_SEGMENT_ROUNDS);
		
		testPutByteArrays1024(smallSegment, SMALL_SEGMENT_SIZE / 1024, SMALL_SEGMENTS_ROUNDS);
		testGetByteArrays1024(smallSegment, SMALL_SEGMENT_SIZE / 1024, SMALL_SEGMENTS_ROUNDS);
		testPutByteArrays1024(largeSegment, LARGE_SEGMENT_SIZE / 1024, LARGE_SEGMENT_ROUNDS);
		testGetByteArrays1024(largeSegment, LARGE_SEGMENT_SIZE / 1024, LARGE_SEGMENT_ROUNDS);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  LONGs
	// --------------------------------------------------------------------------------------------
	
	private static final void testPutLongs(byte[] segmentArray, int numValues, int rounds) {
		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timePutLongsChecked(checkedSegment, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timePutLongsDirect(directSegment, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timePutLongsUnsafe(unsafeSegment, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Writing %d x %d longs to %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	
	private static final void testGetLongs(byte[] segmentArray, int numValues, int rounds) {
		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timeGetLongsChecked(checkedSegment, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timeGetLongsDirect(directSegment, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timeGetLongsUnsafe(unsafeSegment, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Reading %d x %d longs from %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	
	private static long timePutLongsChecked(final CheckedMemorySegment checked, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				checked.putLong(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutLongsDirect(final DirectMemorySegment direct, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				direct.putLong(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutLongsUnsafe(final UnsafeMemorySegment unsafe, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				unsafe.putLong(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetLongsChecked(final CheckedMemorySegment checked, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += checked.getLong(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetLongsDirect(final DirectMemorySegment direct, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += direct.getLong(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetLongsUnsafe(final UnsafeMemorySegment unsafe, final int num, final int rounds) {
		// checked segment
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += unsafe.getLong(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  LONG BIG ENDIAN
	// --------------------------------------------------------------------------------------------
	
	private static final void testPutLongsBigEndian(byte[] segmentArray, int numValues, int rounds) {
		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timePutLongsCheckedBigEndian(checkedSegment, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timePutLongsDirectBigEndian(directSegment, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timePutLongsUnsafeBigEndian(unsafeSegment, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Writing %d x %d big endian longs to %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	
	private static final void testGetLongsBigEndian(byte[] segmentArray, int numValues, int rounds) {
		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timeGetLongsCheckedBigEndian(checkedSegment, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timeGetLongsDirectBigEndian(directSegment, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timeGetLongsUnsafeBigEndian(unsafeSegment, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Reading %d x %d big endian longs from %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	
	private static long timePutLongsCheckedBigEndian(final CheckedMemorySegment checked, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				checked.putLongBigEndian(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutLongsDirectBigEndian(final DirectMemorySegment direct, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				direct.putLongBigEndian(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutLongsUnsafeBigEndian(final UnsafeMemorySegment unsafe, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				unsafe.putLongBigEndian(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetLongsCheckedBigEndian(final CheckedMemorySegment checked, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += checked.getLongBigEndian(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetLongsDirectBigEndian(final DirectMemorySegment direct, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += direct.getLongBigEndian(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetLongsUnsafeBigEndian(final UnsafeMemorySegment unsafe, final int num, final int rounds) {
		// checked segment
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += unsafe.getLongBigEndian(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  LONG LITTLE ENDIAN
	// --------------------------------------------------------------------------------------------
	
	private static final void testPutLongsLittleEndian(byte[] segmentArray, int numValues, int rounds) {
		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timePutLongsCheckedLittleEndian(checkedSegment, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timePutLongsDirectLittleEndian(directSegment, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timePutLongsUnsafeLittleEndian(unsafeSegment, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Writing %d x %d little endian longs to %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	
	private static final void testGetLongsLittleEndian(byte[] segmentArray, int numValues, int rounds) {
		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timeGetLongsCheckedLittleEndian(checkedSegment, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timeGetLongsDirectLittleEndian(directSegment, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timeGetLongsUnsafeLittleEndian(unsafeSegment, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Reading %d x %d little endian longs from %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	
	private static long timePutLongsCheckedLittleEndian(final CheckedMemorySegment checked, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				checked.putLongLittleEndian(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutLongsDirectLittleEndian(final DirectMemorySegment direct, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				direct.putLongLittleEndian(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutLongsUnsafeLittleEndian(final UnsafeMemorySegment unsafe, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				unsafe.putLongLittleEndian(offset, LONG_VALUE);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetLongsCheckedLittleEndian(final CheckedMemorySegment checked, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += checked.getLongLittleEndian(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetLongsDirectLittleEndian(final DirectMemorySegment direct, final int num, final int rounds) {
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += direct.getLongLittleEndian(offset);
				offset += 8;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetLongsUnsafeLittleEndian(final UnsafeMemorySegment unsafe, final int num, final int rounds) {
		// checked segment
		long l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += unsafe.getLongLittleEndian(offset);
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
	
	private static final void testPutInts(byte[] segmentArray, int numValues, int rounds) {
		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timePutIntsChecked(checkedSegment, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timePutIntsDirect(directSegment, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timePutIntsUnsafe(unsafeSegment, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Writing %d x %d ints to %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	
	private static final void testGetInts(byte[] segmentArray, int numValues, int rounds) {
		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timeGetIntsChecked(checkedSegment, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timeGetIntsDirect(directSegment, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timeGetIntsUnsafe(unsafeSegment, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Reading %d x %d ints from %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	
	private static long timePutIntsChecked(final CheckedMemorySegment checked, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				checked.putInt(offset, INT_VALUE);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutIntsDirect(final DirectMemorySegment direct, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				direct.putInt(offset, INT_VALUE);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutIntsUnsafe(final UnsafeMemorySegment unsafe, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				unsafe.putInt(offset, INT_VALUE);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetIntsChecked(final CheckedMemorySegment checked, final int num, final int rounds) {
		int l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += checked.getInt(offset);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetIntsDirect(final DirectMemorySegment direct, final int num, final int rounds) {
		int l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += direct.getInt(offset);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	private static long timeGetIntsUnsafe(final UnsafeMemorySegment unsafe, final int num, final int rounds) {
		int l = 0;
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				l += unsafe.getInt(offset);
				offset += 4;
			}
		}
		long end = System.nanoTime();
		sideEffect += l;
		return end - start;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  BYTEs
	// --------------------------------------------------------------------------------------------
	
	private static final void testPutBytes(byte[] segmentArray, int numValues, int rounds) {
		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timePutBytesChecked(checkedSegment, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timePutBytesDirect(directSegment, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timePutBytesUnsafe(unsafeSegment, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Writing %d x %d bytes to %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	
	private static final void testGetBytes(byte[] segmentArray, int numValues, int rounds) {
		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timeGetBytesChecked(checkedSegment, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timeGetBytesDirect(directSegment, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timeGetBytesUnsafe(unsafeSegment, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Reading %d x %d bytes from %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	
	private static long timePutBytesChecked(final CheckedMemorySegment checked, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				checked.put(offset, BYTE_VALUE);
				offset++;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutBytesDirect(final DirectMemorySegment direct, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				direct.put(offset, BYTE_VALUE);
				offset++;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutBytesUnsafe(final UnsafeMemorySegment unsafe, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				unsafe.put(offset, BYTE_VALUE);
				offset++;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetBytesChecked(final CheckedMemorySegment checked, final int num, final int rounds) {
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				checked.get(offset);
				offset++;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetBytesDirect(final DirectMemorySegment direct, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				direct.get(offset);
				offset++;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetBytesUnsafe(final UnsafeMemorySegment unsafe, final int num, final int rounds) {
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				unsafe.get(offset);
				offset++;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                  BYTE ARRAYs
	// --------------------------------------------------------------------------------------------
	
	private static final void testPutByteArrays1024(byte[] segmentArray, int numValues, int rounds) {
		byte[] sourceArray = new byte[1024];
		for (int i = 0; i < sourceArray.length; i++) {
			sourceArray[i] = (byte) (i % Byte.MAX_VALUE);
		}

		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timePutByteArrayChecked(checkedSegment, sourceArray, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timePutByteArrayDirect(directSegment, sourceArray, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timePutByteArrayUnsafe(unsafeSegment, sourceArray, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Writing %d x %d byte[1024] to %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	
	private static final void testGetByteArrays1024(byte[] segmentArray, int numValues, int rounds) {
		byte[] targetArray = new byte[1024];

		Arrays.fill(segmentArray, (byte) 0);
		CheckedMemorySegment checkedSegment = new CheckedMemorySegment(segmentArray);
		long elapsedChecked = timeGetByteArrayChecked(checkedSegment, targetArray, numValues, rounds);
		checkedSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		DirectMemorySegment directSegment = new DirectMemorySegment(segmentArray);
		long elapsedDirect = timeGetByteArrayDirect(directSegment, targetArray, numValues, rounds);
		directSegment = null;
		
		Arrays.fill(segmentArray, (byte) 0);
		UnsafeMemorySegment unsafeSegment = new UnsafeMemorySegment(segmentArray);
		long elapsedUnsafe = timeGetByteArrayUnsafe(unsafeSegment, targetArray, numValues, rounds);
		unsafeSegment = null;
		
		System.out.println(String.format("Reading %d x %d byte[1024] from %d segment: checked=%,d nsecs,  direct=%,d nsecs, unsafe=%,d nsecs.", rounds, numValues, segmentArray.length, elapsedChecked, elapsedDirect, elapsedUnsafe));
	}
	

	private static long timePutByteArrayChecked(final CheckedMemorySegment checked, final byte[] source, final int num, final int rounds) {
		final int len = source.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				checked.put(offset, source, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutByteArrayDirect(final DirectMemorySegment direct, final byte[] source, final int num, final int rounds) {
		final int len = source.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				direct.put(offset, source, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timePutByteArrayUnsafe(final UnsafeMemorySegment unsafe, final byte[] source, final int num, final int rounds) {
		final int len = source.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				unsafe.put(offset, source, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetByteArrayChecked(final CheckedMemorySegment checked, final byte[] target, final int num, final int rounds) {
		final int len = target.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				checked.get(offset, target, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetByteArrayDirect(final DirectMemorySegment direct, final byte[] target, final int num, final int rounds) {
		final int len = target.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				direct.get(offset, target, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
	
	private static long timeGetByteArrayUnsafe(final UnsafeMemorySegment unsafe, final byte[] target, final int num, final int rounds) {
		final int len = target.length;
		
		// checked segment
		long start = System.nanoTime();
		for (int round = 0; round < rounds; round++) {
			int offset = 0;
			for (int i = 0; i < num; i++) {
				unsafe.get(offset, target, 0, len);
				offset += len;
			}
		}
		long end = System.nanoTime();
		return end - start;
	}
}
