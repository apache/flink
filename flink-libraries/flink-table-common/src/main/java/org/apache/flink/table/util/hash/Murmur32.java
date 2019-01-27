/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.util.hash;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryUtils;

/**
 * 32-bit Murmur3 hasher.  This is based on Guava's Murmur3_32HashFunction.
 */
public final class Murmur32 {
	private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;
	private static final int C1 = 0xcc9e2d51;
	private static final int C2 = 0x1b873593;
	public static final int DEFAULT_SEED = 42;

	private static final int R1 = 31;
	private static final int R2 = 27;
	private static final int M = 5;
	private static final int N1 = 0x52dce729;

	public static int hashUnsafeWords(Object base, long offset, int lengthInBytes, int seed) {
		int h1 = hashUnsafeBytesByInt(base, offset, lengthInBytes, seed);
		return fmix(h1, lengthInBytes);
	}

	public static int hashUnsafeBytes(Object base, long offset, int lengthInBytes, int seed) {
		assert (lengthInBytes >= 0) : "lengthInBytes cannot be negative";
		int lengthAligned = lengthInBytes - lengthInBytes % 4;
		int h1 = hashUnsafeBytesByInt(base, offset, lengthAligned, seed);
		for (int i = lengthAligned; i < lengthInBytes; i++) {
			int halfWord = UNSAFE.getByte(base, offset + i);
			int k1 = mixK1(halfWord);
			h1 = mixH1(h1, k1);
		}
		return fmix(h1, lengthInBytes);
	}

	private static int hashUnsafeBytesByInt(Object base, long offset, int lengthInBytes, int seed) {
		assert (lengthInBytes % 4 == 0);
		int h1 = seed;
		for (int i = 0; i < lengthInBytes; i += 4) {
			int halfWord = UNSAFE.getInt(base, offset + i);
			int k1 = mixK1(halfWord);
			h1 = mixH1(h1, k1);
		}
		return h1;
	}

	public static int hashBytesByWords(MemorySegment segment, int offset, int lengthInBytes, int seed) {
		int h1 = hashBytesByInt(segment, offset, lengthInBytes, seed);
		return fmix(h1, lengthInBytes);
	}

	public static int hashBytes(MemorySegment segment, int offset, int lengthInBytes, int seed) {
		int lengthAligned = lengthInBytes - lengthInBytes % 4;
		int h1 = hashBytesByInt(segment, offset, lengthAligned, seed);
		for (int i = lengthAligned; i < lengthInBytes; i++) {
			int k1 = mixK1(segment.get(offset + i));
			h1 = mixH1(h1, k1);
		}
		return fmix(h1, lengthInBytes);
	}

	public static long hash64(MemorySegment data, int offset, int length) {
		return hash64(data, offset, length, DEFAULT_SEED);
	}

	private static long hashBytesByLong(
			MemorySegment segment, int offset, int lengthInBytes, int seed) {
		assert (lengthInBytes % 8 == 0);
		long hash = seed;
		for (int i = 0; i < lengthInBytes; i += 8) {
			long k = segment.getLong(offset + i);
			// mix functions
			k *= C1;
			k = Long.rotateLeft(k, R1);
			k *= C2;
			hash ^= k;
			hash = Long.rotateLeft(hash, R2) * M + N1;
		}
		return hash;
	}

	public static long hash64(MemorySegment segment, int offset, int lengthInBytes, int seed) {
		int lengthAligned = lengthInBytes - lengthInBytes % 8;
		long hash = hashBytesByLong(segment, offset, lengthAligned, seed);
		// tail
		long k1 = 0;
		for (int i = lengthInBytes - lengthAligned; i > 0; i--) {
			k1 ^= ((long) segment.get(offset + lengthAligned + i - 1) & 0xff) << ((i - 1) * 8);
			if (i == 1) {
				k1 *= C1;
				k1 = Long.rotateLeft(k1, R1);
				k1 *= C2;
				hash ^= k1;
			}
		}

		// finalization
		hash ^= lengthInBytes;
		return fmix(hash);
	}

	public static long hashUnsafe64(Object base, int offset, int lengthInBytes, int seed) {
		int lengthAligned = lengthInBytes - lengthInBytes % 8;
		long hash = hashUnsafe64ByLong(base, offset, lengthAligned, seed);
		// tail
		long k1 = 0;
		for (int i = lengthInBytes - lengthAligned; i > 0; i--) {
			k1 ^= ((long) UNSAFE.getByte(base, offset + lengthAligned + i - 1) & 0xff)
					<< ((i - 1) * 8);
			if (i == 1) {
				k1 *= C1;
				k1 = Long.rotateLeft(k1, R1);
				k1 *= C2;
				hash ^= k1;
			}
		}

		// finalization
		hash ^= lengthInBytes;
		return fmix(hash);
	}

	private static long hashUnsafe64ByLong(Object base, int offset, int lengthInBytes, int seed) {
		assert (lengthInBytes % 8 == 0);
		long hash = seed;
		for (int i = 0; i < lengthInBytes; i += 8) {
			long k = UNSAFE.getLong(base, offset + i);
			// mix functions
			k *= C1;
			k = Long.rotateLeft(k, R1);
			k *= C2;
			hash ^= k;
			hash = Long.rotateLeft(hash, R2) * M + N1;
		}
		return hash;
	}

	/**
	 * This is not a public method, please invoke {@link #hashBytesByWords} or {@link #hashBytes}.
	 */
	private static int hashBytesByInt(
			MemorySegment segment, int offset, int lengthInBytes, int seed) {
		assert (lengthInBytes % 4 == 0);
		int h1 = seed;
		for (int i = 0; i < lengthInBytes; i += 4) {
			int halfWord = segment.getInt(offset + i);
			int k1 = mixK1(halfWord);
			h1 = mixH1(h1, k1);
		}
		return h1;
	}

	private static int mixK1(int k1) {
		k1 *= C1;
		k1 = Integer.rotateLeft(k1, 15);
		k1 *= C2;
		return k1;
	}

	private static int mixH1(int h1, int k1) {
		h1 ^= k1;
		h1 = Integer.rotateLeft(h1, 13);
		h1 = h1 * 5 + 0xe6546b64;
		return h1;
	}

	// Finalization mix - force all bits of a hash block to avalanche
	private static int fmix(int h1, int length) {
		h1 ^= length;
		return fmix(h1);
	}

	public static int fmix(int h) {
		h ^= h >>> 16;
		h *= 0x85ebca6b;
		h ^= h >>> 13;
		h *= 0xc2b2ae35;
		h ^= h >>> 16;
		return h;
	}

	public static long fmix(long h) {
		h ^= (h >>> 33);
		h *= 0xff51afd7ed558ccdL;
		h ^= (h >>> 33);
		h *= 0xc4ceb9fe1a85ec53L;
		h ^= (h >>> 33);
		return h;
	}
}
