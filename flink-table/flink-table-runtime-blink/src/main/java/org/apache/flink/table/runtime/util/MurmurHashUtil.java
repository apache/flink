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

package org.apache.flink.table.runtime.util;

import org.apache.flink.core.memory.MemorySegment;

import static org.apache.flink.core.memory.MemoryUtils.UNSAFE;

/**
 * Murmur hasher.  This is based on Guava's Murmur3_32HashFunction.
 */
public final class MurmurHashUtil {

	private static final int C1 = 0xcc9e2d51;
	private static final int C2 = 0x1b873593;
	public static final int DEFAULT_SEED = 42;

	/**
	 * Hash unsafe bytes, length must be aligned to 4 bytes.
	 * @param base base unsafe object
	 * @param offset offset for unsafe object
	 * @param lengthInBytes length in bytes
	 * @return hash code
	 */
	public static int hashUnsafeBytesByWords(Object base, long offset, int lengthInBytes) {
		return hashUnsafeBytesByWords(base, offset, lengthInBytes, DEFAULT_SEED);
	}

	/**
	 * Hash unsafe bytes.
	 * @param base base unsafe object
	 * @param offset offset for unsafe object
	 * @param lengthInBytes length in bytes
	 * @return hash code
	 */
	public static int hashUnsafeBytes(Object base, long offset, int lengthInBytes) {
		return hashUnsafeBytes(base, offset, lengthInBytes, DEFAULT_SEED);
	}

	/**
	 * Hash bytes in MemorySegment, length must be aligned to 4 bytes.
	 * @param segment segment.
	 * @param offset offset for MemorySegment
	 * @param lengthInBytes length in MemorySegment
	 * @return hash code
	 */
	public static int hashBytesByWords(MemorySegment segment, int offset, int lengthInBytes) {
		return hashBytesByWords(segment, offset, lengthInBytes, DEFAULT_SEED);
	}

	/**
	 * Hash bytes in MemorySegment.
	 * @param segment segment.
	 * @param offset offset for MemorySegment
	 * @param lengthInBytes length in MemorySegment
	 * @return hash code
	 */
	public static int hashBytes(MemorySegment segment, int offset, int lengthInBytes) {
		return hashBytes(segment, offset, lengthInBytes, DEFAULT_SEED);
	}

	private static int hashUnsafeBytesByWords(Object base, long offset, int lengthInBytes, int seed) {
		int h1 = hashUnsafeBytesByInt(base, offset, lengthInBytes, seed);
		return fmix(h1, lengthInBytes);
	}

	private static int hashBytesByWords(MemorySegment segment, int offset, int lengthInBytes, int seed) {
		int h1 = hashBytesByInt(segment, offset, lengthInBytes, seed);
		return fmix(h1, lengthInBytes);
	}

	private static int hashBytes(MemorySegment segment, int offset, int lengthInBytes, int seed) {
		int lengthAligned = lengthInBytes - lengthInBytes % 4;
		int h1 = hashBytesByInt(segment, offset, lengthAligned, seed);
		for (int i = lengthAligned; i < lengthInBytes; i++) {
			int k1 = mixK1(segment.get(offset + i));
			h1 = mixH1(h1, k1);
		}
		return fmix(h1, lengthInBytes);
	}

	private static int hashUnsafeBytes(Object base, long offset, int lengthInBytes, int seed) {
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

	private static int hashBytesByInt(MemorySegment segment, int offset, int lengthInBytes, int seed) {
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
