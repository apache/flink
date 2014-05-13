/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.test.util.types;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;


public class IntPairComparator extends TypeComparator<IntPair> {

	private static final long serialVersionUID = 1L;
	
	private int reference;
	
	@Override
	public int hash(IntPair object) {
		return object.getKey() * 73;
	}
	
	@Override
	public void setReference(IntPair toCompare) {
		this.reference = toCompare.getKey();
	}
	
	@Override
	public boolean equalToReference(IntPair candidate) {
		return candidate.getKey() == this.reference;
	}
	
	@Override
	public int compareToReference(TypeComparator<IntPair> referencedAccessors) {
		final IntPairComparator comp = (IntPairComparator) referencedAccessors;
		return comp.reference - this.reference;
	}

	@Override
	public int compare(IntPair first, IntPair second) {
		return first.getKey() - second.getKey();
	}
	
	@Override
	public int compare(DataInputView source1, DataInputView source2) throws IOException {
		return source1.readInt() - source2.readInt();
	}

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
	public void putNormalizedKey(IntPair record, MemorySegment target, int offset, int len) {
		// see IntValue for a documentation of the logic
		final int value = record.getKey() - Integer.MIN_VALUE;
		
		if (len == 4) {
			target.putIntBigEndian(offset, value);
		}
		else if (len <= 0) {
		}
		else if (len < 4) {
			for (int i = 0; len > 0; len--, i++) {
				target.put(offset + i, (byte) ((value >>> ((3-i)<<3)) & 0xff));
			}
		}
		else {
			target.putIntBigEndian(offset, value);
			for (int i = 4; i < len; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}

	@Override
	public IntPairComparator duplicate() {
		return new IntPairComparator();
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return true;
	}
	
	@Override
	public void writeWithKeyNormalization(IntPair record, DataOutputView target) throws IOException {
		target.writeInt(record.getKey() - Integer.MIN_VALUE);
		target.writeInt(record.getValue());
	}

	@Override
	public IntPair readWithKeyDenormalization(IntPair reuse, DataInputView source) throws IOException {
		reuse.setKey(source.readInt() + Integer.MIN_VALUE);
		reuse.setValue(source.readInt());
		return reuse;
	}
}
