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
public final class FloatComparator extends BasicTypeComparator<Float> {

	private static final long serialVersionUID = 1L;

	public FloatComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public FloatComparator duplicate() {
		return new FloatComparator(ascendingComparison);
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		float l1 = firstSource.readFloat();
		float l2 = secondSource.readFloat();
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
		return 4;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < 4;
	}

	@Override
	public void putNormalizedKey(Float value, MemorySegment target, int offset, int numBytes) {
		// float representation is the same for positive and negative values
		// except for the leading sign bit; representations for positive values
		// are normalized and representations for negative values need inversion
		int bits = Float.floatToIntBits(value);
		bits = (value < 0) ? ~bits : bits - Integer.MIN_VALUE;

		// see IntValue for an explanation of the logic
		if (numBytes > 3) {
			target.putIntBigEndian(offset, bits);

			for (int i = 4; i < numBytes; i++) {
				target.put(offset + i, (byte) 0);
			}
		} else if (numBytes > 0) {
			for (int i = 0; numBytes > 0; numBytes--, i++) {
				target.put(offset + i, (byte) (bits >>> ((3-i)<<3)));
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
	public void writeWithKeyNormalization(Float record, DataOutputView target) throws IOException {
		target.writeFloat(-record);
	}

	@Override
	public Float readWithKeyDenormalization(Float reuse, DataInputView source) throws IOException {
		return -source.readFloat();
	}
}
