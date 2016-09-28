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
public final class ByteComparator extends BasicTypeComparator<Byte> {

	private static final long serialVersionUID = 1L;

	public ByteComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public ByteComparator duplicate() {
		return new ByteComparator(ascendingComparison);
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		byte b1 = firstSource.readByte();
		byte b2 = secondSource.readByte();
		int comp = (b1 < b2 ? -1 : (b1 == b2 ? 0 : 1)); 
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
		return 1;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < 1;
	}

	@Override
	public void putNormalizedKey(Byte value, MemorySegment target, int offset, int numBytes) {
		if (numBytes > 0) {
			target.put(offset, (byte) (value - Byte.MIN_VALUE));

			for (int i = 1; i < numBytes; i++) {
				target.put(offset + i, (byte) 0);
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
	public void writeWithKeyNormalization(Byte record, DataOutputView target) throws IOException {
		target.writeByte(record - Byte.MIN_VALUE);
	}

	@Override
	public Byte readWithKeyDenormalization(Byte reuse, DataInputView source) throws IOException {
		return (byte)(source.readByte() + Byte.MIN_VALUE);
	}
}
