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

package org.apache.flink.table.runtime.functions.aggfunctions.cardinality;

/**
 * A basis structure for the HLL.
 */
public class RegisterSet {

	public static final int LOG2_BITS_PER_WORD = 6;
	public static final int REGISTER_SIZE = 5;

	public final int count;
	public final int size;

	private final int[] m;

	public static int getBits(int count) {
		return count / LOG2_BITS_PER_WORD;
	}

	public static int getSizeForCount(int count) {
		int bits = getBits(count);
		if (bits == 0) {
			return 1;
		} else if (bits % Integer.SIZE == 0) {
			return bits;
		} else {
			return bits + 1;
		}
	}

	public RegisterSet(int count) {
		this(count, null);
	}

	public RegisterSet(int count, int[] initialValues) {
		this.count = count;

		if (initialValues == null) {
			this.m = new int[getSizeForCount(count)];
		} else {
			this.m = initialValues;
		}
		this.size = this.m.length;
	}

	public void set(int position, int value) {
		int bucketPos = position / LOG2_BITS_PER_WORD;
		int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
		this.m[bucketPos] = (this.m[bucketPos] & ~(0x1f << shift)) | (value << shift);
	}

	public int get(int position) {
		int bucketPos = position / LOG2_BITS_PER_WORD;
		int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
		return (this.m[bucketPos] & (0x1f << shift)) >>> shift;
	}

	public boolean updateIfGreater(int position, int value) {
		int bucket = position / LOG2_BITS_PER_WORD;
		int shift = REGISTER_SIZE * (position - (bucket * LOG2_BITS_PER_WORD));
		int mask = 0x1f << shift;

		// Use long to avoid sign issues with the left-most shift
		long curVal = this.m[bucket] & mask;
		long newVal = value << shift;
		if (curVal < newVal) {
			this.m[bucket] = (int) ((this.m[bucket] & ~mask) | newVal);
			return true;
		} else {
			return false;
		}
	}

	public void merge(RegisterSet that) {
		for (int bucket = 0; bucket < m.length; bucket++) {
			int word = 0;
			for (int j = 0; j < LOG2_BITS_PER_WORD; j++) {
				int mask = 0x1f << (REGISTER_SIZE * j);

				int thisVal = (this.m[bucket] & mask);
				int thatVal = (that.m[bucket] & mask);
				word |= (thisVal < thatVal) ? thatVal : thisVal;
			}
			this.m[bucket] = word;
		}
	}

	int[] readOnlyBits() {
		return m;
	}

}
