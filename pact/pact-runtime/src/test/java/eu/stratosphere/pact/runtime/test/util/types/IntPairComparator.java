/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.pact.generic.types.TypeComparator;


public class IntPairComparator implements TypeComparator<IntPair>
{
	private int reference;
	
	@Override
	public int hash(IntPair object) {
		return object.getKey();
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
	public void putNormalizedKey(IntPair record, byte[] target, int offset, int numBytes)
	{
		final int value = record.getKey();
		
		if (numBytes == 4) {
			// default case, full normalized key
			int highByte = ((value >>> 24) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target[offset    ] = (byte) highByte;
			target[offset + 1] = (byte) ((value >>> 16) & 0xff);
			target[offset + 2] = (byte) ((value >>>  8) & 0xff);
			target[offset + 3] = (byte) ((value       ) & 0xff);
		}
		else if (numBytes <= 0) {
		}
		else if (numBytes < 4) {
			int highByte = ((value >>> 24) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target[offset    ] = (byte) highByte;
			numBytes--;
			for (int i = 1; numBytes > 0; numBytes--, i++) {
				target[offset + i] = (byte) ((value >>> ((3-i)<<3)) & 0xff);
			}
		}
		else {
			int highByte = ((value >>> 24) & 0xff);
			highByte -= Byte.MIN_VALUE;
			target[offset    ] = (byte) highByte;
			target[offset + 1] = (byte) ((value >>> 16) & 0xff);
			target[offset + 2] = (byte) ((value >>>  8) & 0xff);
			target[offset + 3] = (byte) ((value       ) & 0xff);
			for (int i = 4; i < numBytes; i++) {
				target[offset + i] = 0;
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
	public void readWithKeyDenormalization(IntPair record, DataInputView source) throws IOException {
		record.setKey(source.readInt() + Integer.MIN_VALUE);
		record.setValue(source.readInt());
	}
}
