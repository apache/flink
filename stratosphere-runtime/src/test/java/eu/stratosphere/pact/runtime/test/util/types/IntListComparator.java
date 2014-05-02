/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.pact.runtime.test.util.types;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;

public class IntListComparator extends TypeComparator<IntList> {
	
	private static final long serialVersionUID = 1L;
	
	private int reference;

	@Override
	public int hash(IntList record) {
		return record.getKey() * 73;
	}

	@Override
	public void setReference(IntList toCompare) {
		this.reference = toCompare.getKey();
	}

	@Override
	public boolean equalToReference(IntList candidate) {
		return candidate.getKey() == this.reference;
	}

	@Override
	public int compareToReference(TypeComparator<IntList> referencedComparator) {
		final IntListComparator comp = (IntListComparator) referencedComparator;
		return comp.reference - this.reference;
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
	public boolean supportsSerializationWithKeyNormalization() {
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
	public void putNormalizedKey(IntList record, MemorySegment target, int offset, int len) {
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
	public void writeWithKeyNormalization(IntList record, DataOutputView target)
			throws IOException {
		target.writeInt(record.getKey() - Integer.MIN_VALUE);
		target.writeInt(record.getValue().length);
		for (int i = 0; i < record.getValue().length; i++) {
			target.writeInt(record.getValue()[i]);
		}
	}
	@Override
	public IntList readWithKeyDenormalization(IntList record, DataInputView source)
			throws IOException {
		record.setKey(source.readInt() + Integer.MIN_VALUE);
		int[] value = new int[source.readInt()];
		for (int i = 0; i < value.length; i++) {
			value[i] = source.readInt();
		}
		record.setValue(value);
		return record;
	}

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}

	@Override
	public TypeComparator<IntList> duplicate() {
		return new IntListComparator();
	}
}
