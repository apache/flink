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

package org.apache.flink.api.common.typeutils.base;

import java.io.IOException;
import java.math.BigInteger;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;

/**
 * Comparator for comparing BigInteger values. Does not support null values.
 */
@Internal
public final class BigIntComparator extends BasicTypeComparator<BigInteger> {

	private static final long serialVersionUID = 1L;

	public BigIntComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		BigInteger bi1 = BigIntSerializer.readBigInteger(firstSource);
		BigInteger bi2 = BigIntSerializer.readBigInteger(secondSource);
		int comp = bi1.compareTo(bi2); // null is not supported
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return true;
	}

	/**
	 * Adds a normalized key containing the normalized number of bits and MSBs of the given record.
	 * 1 bit determines the sign (negative, zero/positive), 31 bit the bit length of the record.
	 * Remaining bytes contain the most significant bits of the record.
	 */
	@Override
	public void putNormalizedKey(BigInteger record, MemorySegment target, int offset, int len) {
		// add normalized bit length (the larger the length, the larger the value)
		int bitLen = 0;
		if (len > 0) {
			final int signum = record.signum();
			bitLen = record.bitLength();

			// normalize dependent on sign
			// from 0 to Integer.MAX
			// OR from Integer.MAX to 0
			int normBitLen = signum < 0 ? Integer.MAX_VALUE - bitLen : bitLen;

			// add sign
			if (signum >= 0) {
				normBitLen |= (1 << 31);
			}

			for (int i = 0; i < 4 && len > 0; i++, len--) {
				final byte b = (byte) (normBitLen >>> (8 * (3 - i)));
				target.put(offset++, b);
			}
		}

		// fill remaining bytes with most significant bits
		int bitPos = bitLen - 1;
		for (; len > 0; len--) {
			byte b = 0;
			for (int bytePos = 0; bytePos < 8 && bitPos >= 0; bytePos++, bitPos--) {
				b <<= 1;
				if (record.testBit(bitPos)) {
					b |= 1;
				}
			}
			// the last byte might be partially filled, but that's ok within an equal bit length.
			// no need for padding bits.
			target.put(offset++, b);
		}
	}

	@Override
	public BigIntComparator duplicate() {
		return new BigIntComparator(ascendingComparison);
	}
}
