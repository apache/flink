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
import java.math.BigDecimal;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;

/**
 * Comparator for comparing BigDecimal values. Does not support null values.
 */
@Internal
public final class BigDecComparator extends BasicTypeComparator<BigDecimal> {

	private static final long serialVersionUID = 1L;

	private static final long SMALLEST_MAGNITUDE = Integer.MAX_VALUE;

	private static final long LARGEST_MAGNITUDE = ((long) Integer.MIN_VALUE) - Integer.MAX_VALUE + 1;

	public BigDecComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		BigDecimal bd1 = BigDecSerializer.readBigDecimal(firstSource);
		BigDecimal bd2 = BigDecSerializer.readBigDecimal(secondSource);
		int comp = bd1.compareTo(bd2); // null is not supported
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
		return 5;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return true;
	}

	/**
	 * Adds a normalized key containing a normalized order of magnitude of the given record.
	 * 2 bits determine the sign (negative, zero, positive), 33 bits determine the magnitude.
	 * This method adds at most 5 bytes that contain information.
	 */
	@Override
	public void putNormalizedKey(BigDecimal record, MemorySegment target, int offset, int len) {
		final long signum = record.signum();

		// order of magnitude
		// smallest:
		// scale = Integer.MAX, precision = 1 => SMALLEST_MAGNITUDE
		// largest:
		// scale = Integer.MIN, precision = Integer.MAX => LARGEST_MAGNITUDE
		final long mag = ((long) record.scale()) - ((long) record.precision()) + 1;

		// normalize value range: from 0 to (SMALLEST_MAGNITUDE + -1*LARGEST_MAGNITUDE)
		final long normMag = -1L * LARGEST_MAGNITUDE + mag;

		// normalize value range dependent on sign:
		// 0 to (SMALLEST_MAGNITUDE + -1*LARGEST_MAGNITUDE)
		// OR (SMALLEST_MAGNITUDE + -1*LARGEST_MAGNITUDE) to 0
		// --> uses at most 33 bit (5 least-significant bytes)
		long signNormMag = signum < 0 ? normMag : (SMALLEST_MAGNITUDE + -1L * LARGEST_MAGNITUDE - normMag);

		// zero has no magnitude
		// set 34th bit to flag zero
		if (signum == 0) {
			signNormMag = 0L;
			signNormMag |= (1L << 34);
		}
		// set 35th bit to flag positive sign
		else if (signum > 0) {
			signNormMag |= (1L << 35);
		}

		// add 5 least-significant bytes that contain value to target
		for (int i = 0; i < 5 && len > 0; i++, len--) {
			final byte b = (byte) (signNormMag >>> (8 * (4 - i)));
			target.put(offset++, b);
		}
	}

	@Override
	public BigDecComparator duplicate() {
		return new BigDecComparator(ascendingComparison);
	}
}
