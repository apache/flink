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

import org.apache.flink.core.memory.DataInputDeserializer;

import java.util.Arrays;

import static org.apache.flink.table.dataformat.util.BinaryRowUtil.BYTE_ARRAY_BASE_OFFSET;
import static org.apache.flink.table.dataformat.util.BinaryRowUtil.LONG_ARRAY_OFFSET;
import static org.apache.flink.table.dataformat.util.BinaryRowUtil.UNSAFE;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * BloomFilter based on a long array of Java heap, and serialization and merge based on Unsafe.
 *
 * <p>Part of this class refers to the implementation from Apache Hive project
 * https://github.com/apache/hive/blob/master/common/src/java/org/apache/hive/common/util/BloomFilter.java.
 */
public class BloomFilter {

	private static final long MIN_BLOOM_FILTER_ENTRIES = 500000L;

	/**
	 * Default false positive probability for BloomFilter.
	 */
	public static final double DEFAULT_FPP = 0.03f;

	private final int numBits;
	private final int numHashFunctions;
	private final BitSet bitSet;

	public BloomFilter(long maxNumEntries) {
		this(maxNumEntries, DEFAULT_FPP);
	}

	/**
	 * Constructor. MaxNumEntries and fpp together determine the size of bloomFilter.
	 * @param maxNumEntries max number entries in this bloomFilter.
	 * @param fpp false positive probability.
	 */
	public BloomFilter(long maxNumEntries, double fpp) {
		checkArgument(maxNumEntries > 0, "expectedEntries should be > 0");
		int nb = optimalNumOfBits(maxNumEntries, fpp);
		this.numBits = nb + (Long.SIZE - (nb % Long.SIZE));
		this.numHashFunctions = optimalNumOfHashFunctions(maxNumEntries, numBits);
		this.bitSet = new BitSet(this.numBits);
	}

	private BloomFilter(long[] bits, int numFuncs) {
		this.numBits = bits.length * Long.SIZE;
		this.numHashFunctions = numFuncs;
		this.bitSet = new BitSet(bits);
	}

	// Thomas Wang's integer hash function
	// http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
	public static long getLongHash(long key) {
		key = (~key) + (key << 21); // key = (key << 21) - key - 1;
		key = key ^ (key >> 24);
		key = (key + (key << 3)) + (key << 8); // key * 265
		key = key ^ (key >> 14);
		key = (key + (key << 2)) + (key << 4); // key * 21
		key = key ^ (key >> 28);
		key = key + (key << 31);
		return key;
	}

	public static long getLongHash(double key) {
		return getLongHash(Double.doubleToLongBits(key));
	}

	public void addHash(long hash64) {
		int hash1 = (int) hash64;
		int hash2 = (int) (hash64 >>> 32);

		for (int i = 1; i <= numHashFunctions; i++) {
			int combinedHash = hash1 + ((i + 1) * hash2);
			// hashcode should be positive, flip all the bits if it's negative
			if (combinedHash < 0) {
				combinedHash = ~combinedHash;
			}
			int pos = combinedHash % numBits;
			bitSet.set(pos);
		}
	}

	public boolean testHash(long hash64) {
		int hash1 = (int) hash64;
		int hash2 = (int) (hash64 >>> 32);

		for (int i = 1; i <= numHashFunctions; i++) {
			int combinedHash = hash1 + ((i + 1) * hash2);
			// hashcode should be positive, flip all the bits if it's negative
			if (combinedHash < 0) {
				combinedHash = ~combinedHash;
			}
			int pos = combinedHash % numBits;
			if (!bitSet.get(pos)) {
				return false;
			}
		}
		return true;
	}

	public long[] getBitSet() {
		return bitSet.getData();
	}

	/**
	 * Merge the specified bloom filter with current bloom filter.
	 *
	 * @param that - bloom filter to merge
	 */
	public void merge(BloomFilter that) {
		if (this != that && this.numBits == that.numBits && this.numHashFunctions == that.numHashFunctions) {
			this.bitSet.putAll(that.bitSet);
		} else {
			throw new IllegalArgumentException("BloomKFilters are not compatible for merging." +
					" this - " + this.toString() + " that - " + that.toString());
		}
	}

	public void reset() {
		this.bitSet.clear();
	}

	@Override
	public String toString() {
		return "numBits: " + numBits + " numHashFunctions: " + numHashFunctions;
	}

	/**
	 * This is a high performance to bytes.
	 * See {@link DataInputDeserializer#readLong()}, in LITTLE_ENDIAN, it will reverse long,
	 * that is low performance.
	 */
	public static byte[] toBytes(BloomFilter filter) {
		long[] bitSet = filter.getBitSet();
		int longLen = bitSet.length;
		byte[] bytes = new byte[1 + 4 + longLen * 8];
		UNSAFE.putByte(bytes, BYTE_ARRAY_BASE_OFFSET, (byte) filter.numHashFunctions);
		UNSAFE.putInt(bytes, BYTE_ARRAY_BASE_OFFSET + 1, longLen);
		UNSAFE.copyMemory(bitSet, LONG_ARRAY_OFFSET,
				bytes, BYTE_ARRAY_BASE_OFFSET + 5, longLen * 8);
		return bytes;
	}

	public static BloomFilter fromBytes(byte[] bytes) {
		byte numHashFunc = UNSAFE.getByte(bytes, BYTE_ARRAY_BASE_OFFSET);
		int longLen = UNSAFE.getInt(bytes, BYTE_ARRAY_BASE_OFFSET + 1);
		long[] data = new long[longLen];
		UNSAFE.copyMemory(bytes, BYTE_ARRAY_BASE_OFFSET + 5,
				data, LONG_ARRAY_OFFSET, longLen * 8);
		return new BloomFilter(data, numHashFunc);
	}

	/**
	 * For code gen.
	 */
	public static long suitableMaxNumEntries(long maxNumEntries) {
		return Math.max(maxNumEntries, BloomFilter.MIN_BLOOM_FILTER_ENTRIES);
	}

	private static int optimalNumOfHashFunctions(long n, long m) {
		return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
	}

	private static int optimalNumOfBits(long maxNumEntries, double fpp) {
		return (int) (-maxNumEntries * Math.log(fpp) / (Math.log(2) * Math.log(2)));
	}

	public static double findSuitableFpp(long entries, double maxNumOfBits) {
		for (double f = DEFAULT_FPP; f < 1.0f; f += 0.01f) {
			long bits = optimalNumOfBits(entries, f);
			if (bits < maxNumOfBits) {
				return f;
			}
		}
		return 1f;
	}

	public static void mergeBloomFilterBytes(byte[] bf1Bytes, byte[] bf2Bytes) {
		mergeBloomFilterBytes(bf1Bytes, 0, bf1Bytes.length, bf2Bytes, 0, bf2Bytes.length);
	}

	public static void mergeBloomFilterBytes(
			byte[] bf1Bytes, int bf1Start, int bf1Length,
			byte[] bf2Bytes, int bf2Start, int bf2Length) {
		if (bf1Length != bf2Length) {
			throw new IllegalArgumentException("bf1Length " + bf1Length + " does not match bf2Length " + bf2Length);
		}

		// Validation on the bitset size/3 hash functions.
		int longLen1 = UNSAFE.getInt(bf1Bytes, BYTE_ARRAY_BASE_OFFSET + bf1Start + 1);
		if (UNSAFE.getByte(bf1Bytes, BYTE_ARRAY_BASE_OFFSET + bf1Start) !=
				UNSAFE.getByte(bf2Bytes, BYTE_ARRAY_BASE_OFFSET + bf2Start) ||
				longLen1 != UNSAFE.getInt(bf2Bytes, BYTE_ARRAY_BASE_OFFSET + bf2Start + 1)) {
			throw new IllegalArgumentException("bf1 NumHashFunctions/NumBits does not match bf2");
		}

		for (int idx = 5 + BYTE_ARRAY_BASE_OFFSET; idx < bf1Length + BYTE_ARRAY_BASE_OFFSET; idx += 8) {
			long l1 = UNSAFE.getLong(bf1Bytes, bf1Start + idx);
			long l2 = UNSAFE.getLong(bf2Bytes, bf2Start + idx);
			UNSAFE.putLong(bf1Bytes, bf1Start + idx, l1 | l2);
		}
	}

	/**
	 * Bare metal bit set implementation. For performance reasons, this implementation does not
	 * check for index bounds nor expand the bit set size if the specified index is greater than
	 * the size.
	 */
	public static class BitSet {
		private final long[] data;

		BitSet(long bits) {
			this(new long[(int) Math.ceil((double) bits / (double) Long.SIZE)]);
		}

		/**
		 * Deserialize long array as bit set.
		 *
		 * @param data - bit array
		 */
		BitSet(long[] data) {
			assert data.length > 0 : "data length is zero!";
			this.data = data;
		}

		/**
		 * Sets the bit at specified index.
		 *
		 * @param index - position
		 */
		public void set(int index) {
			data[index >>> 6] |= (1L << index);
		}

		/**
		 * Returns true if the bit is set in the specified index.
		 *
		 * @param index - position
		 * @return - value at the bit position
		 */
		public boolean get(int index) {
			return (data[index >>> 6] & (1L << index)) != 0;
		}

		public long[] getData() {
			return data;
		}

		/**
		 * Combines the two BitArrays using bitwise OR.
		 */
		public void putAll(BloomFilter.BitSet array) {
			assert data.length == array.data.length :
					"BitArrays must be of equal length (" + data.length + "!= " + array.data.length + ")";
			for (int i = 0; i < data.length; i++) {
				data[i] |= array.data[i];
			}
		}

		/**
		 * Clear the bit set.
		 */
		public void clear() {
			Arrays.fill(data, 0);
		}
	}
}
