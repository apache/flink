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

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.pact.runtime.plugable.TypeAccessors;


public class IntPairAccessors implements TypeAccessors<IntPair>
{
	private int reference;
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#createInstance()
	 */
	@Override
	public IntPair createInstance()
	{
		return new IntPair();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#createCopy(java.lang.Object)
	 */
	@Override
	public IntPair createCopy(IntPair from)
	{
		return new IntPair(from.getKey(), from.getValue());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copyTo(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void copyTo(IntPair from, IntPair to)
	{
		to.setKey(from.getKey());
		to.setValue(from.getValue());
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#getLength()
	 */
	@Override
	public int getLength()
	{
		return 8;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#serialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
	 */
	@Override
	public long serialize(IntPair record, DataOutputViewV2 target) throws IOException
	{
		target.writeInt(record.getKey());
		target.writeInt(record.getValue());
		return 8;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#deserialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataInputView)
	 */
	@Override
	public void deserialize(IntPair target, DataInputViewV2 source) throws IOException {
		target.setKey(source.readInt());
		target.setValue(source.readInt());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copy(eu.stratosphere.nephele.services.memorymanager.DataInputView, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
	 */
	@Override
	public void copy(DataInputViewV2 source, DataOutputViewV2 target) throws IOException
	{
		for (int i = 0; i < 8; i++) {
			target.writeByte(source.readUnsignedByte());
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#hash(java.lang.Object)
	 */
	@Override
	public int hash(IntPair object) {
		return object.getKey();
	}
	
	public void setReferenceForEquality(IntPair reference) {
		this.reference = reference.getKey();
	}
	
	public boolean equalToReference(IntPair candidate) {
		return candidate.getKey() == this.reference;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#compare(eu.stratosphere.nephele.services.memorymanager.DataInputView, eu.stratosphere.nephele.services.memorymanager.DataInputView)
	 */
	@Override
	public int compare(DataInputViewV2 source1, DataInputViewV2 source2) throws IOException {
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
	public IntPairAccessors duplicate() {
		return new IntPairAccessors();
	}
}
