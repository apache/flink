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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;

@Internal
public final class DoubleComparator extends BasicTypeComparator<Double> {

	private static final long serialVersionUID = 1L;

	public DoubleComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public DoubleComparator duplicate() {
		return new DoubleComparator(ascendingComparison);
	}

	@Override 
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		double l1 = firstSource.readDouble(); 
		double l2 = secondSource.readDouble(); 
		int comp = (l1 < l2 ? -1 : (l1 > l2 ? 1 : 0)); 
		return ascendingComparison ? comp : -comp; 
	}

	// --------------------------------------------------------------------------------------------
	// key normalization
	// --------------------------------------------------------------------------------------------

	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 8;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < 8;
	}

	@Override
	public void putNormalizedKey(Double value, MemorySegment target, int offset, int numBytes) {
		// double representation is the same for positive and negative values
		// except for the leading sign bit; representations for positive values
		// are normalized and representations for negative values need inversion
		long bits = Double.doubleToLongBits(value);
		bits = (value < 0) ? ~bits : bits - Long.MIN_VALUE;

		// see LongValue for an explanation of the logic
		if (numBytes > 7) {
			target.putLongBigEndian(offset, bits);

			for (int i = 8; i < numBytes; i++) {
				target.put(offset + i, (byte) 0);
			}
		} else if (numBytes > 0) {
			for (int i = 0; numBytes > 0; numBytes--, i++) {
				target.put(offset + i, (byte) (bits >>> ((7-i)<<3)));
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	// serialization with key normalization
	// --------------------------------------------------------------------------------------------

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return true;
	}

	@Override
	public void writeWithKeyNormalization(Double record, DataOutputView target) throws IOException {
		target.writeDouble(-record);
	}

	@Override
	public Double readWithKeyDenormalization(Double reuse, DataInputView source) throws IOException {
		return -source.readDouble();
	}
}
