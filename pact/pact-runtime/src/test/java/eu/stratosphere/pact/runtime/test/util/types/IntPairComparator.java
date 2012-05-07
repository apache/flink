/***********************************************************************************************************************
*
* Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
import eu.stratosphere.pact.runtime.plugable.TypeComparator;


public class IntPairComparator implements TypeComparator<IntPair>
{
	private int reference;
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#hash(java.lang.Object)
	 */
	@Override
	public int hash(IntPair object) {
		return object.getKey();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#setReference(java.lang.Object)
	 */
	@Override
	public void setReference(IntPair toCompare) {
		this.reference = toCompare.getKey();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#equalToReference(java.lang.Object)
	 */
	@Override
	public boolean equalToReference(IntPair candidate) {
		return candidate.getKey() == this.reference;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#compareToReference(eu.stratosphere.pact.runtime.plugable.TypeComparator)
	 */
	@Override
	public int compareToReference(TypeComparator<IntPair> referencedAccessors)
	{
		final IntPairComparator comp = (IntPairComparator) referencedAccessors;
		return comp.reference - this.reference;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#compare(eu.stratosphere.nephele.services.memorymanager.DataInputView, eu.stratosphere.nephele.services.memorymanager.DataInputView)
	 */
	@Override
	public int compare(DataInputView source1, DataInputView source2) throws IOException {
		return source1.readInt() - source2.readInt();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#supportsNormalizedKey()
	 */
	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#getNormalizeKeyLen()
	 */
	@Override
	public int getNormalizeKeyLen() {
		return 4;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#isNormalizedKeyPrefixOnly(int)
	 */
	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return false;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#putNormalizedKey(java.lang.Object, byte[], int, int)
	 */
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

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#duplicate()
	 */
	@Override
	public IntPairComparator duplicate() {
		return new IntPairComparator();
	}
}
