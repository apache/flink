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

package eu.stratosphere.pact.runtime.plugable;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NormalizableKey;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class PactRecordAccessors implements TypeAccessors<PactRecord>
{
	private final int[] keyFields;
	
	private final Key[] keyHolders1;
	
	private final Key[] keyHolders2;
	
	private final int numLeadingNormalizableKeys;
	
	private final int normalizableKeyPrefixLen;
	
	/**
	 * @param keyFields
	 */
	public PactRecordAccessors(int[] keyFields, Class<? extends Key>[] keyTypes)
	{
		this.keyFields = keyFields;
		
		this.keyHolders1 = new Key[keyTypes.length];
		this.keyHolders2 = new Key[keyTypes.length];
		
		for (int i = 0; i < keyTypes.length; i++) {
			if (keyTypes[i] == null) {
				throw new NullPointerException("Key type " + i + " is null.");
			}
			this.keyHolders1[i] = InstantiationUtil.instantiate(keyTypes[i], Key.class);
			this.keyHolders2[i] = InstantiationUtil.instantiate(keyTypes[i], Key.class);
		}
		
		int nKeys = 0;
		int nKeyLen = 0;
		for (int i = 0; i < this.keyHolders1.length; i++) {
			Key k = this.keyHolders1[i];
			if (k instanceof NormalizableKey) {
				nKeys++;
				nKeyLen = ((NormalizableKey) k).getNormalizedKeyLen();
			}
			else break;
		}
		this.numLeadingNormalizableKeys = nKeys;
		this.normalizableKeyPrefixLen = nKeyLen;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#createInstance()
	 */
	@Override
	public PactRecord createInstance() {
		return new PactRecord(); 
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#createCopy(java.lang.Object)
	 */
	@Override
	public PactRecord createCopy(PactRecord from) {
		return from.createCopy();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#copyTo(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void copyTo(PactRecord from, PactRecord to) {
		from.copyTo(to);
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#serialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataOutputView, java.util.List, java.util.List)
	 */
	@Override
	public long serialize(PactRecord record, DataOutputView target, Iterator<MemorySegment> furtherBuffers,
			List<MemorySegment> targetForUsedFurther)
	throws IOException
	{
		return record.serialize(record, target, furtherBuffers, targetForUsedFurther);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#deserialize(java.lang.Object, java.util.List, int, int)
	 */
	@Override
	public void deserialize(PactRecord target, List<MemorySegment> sources, int firstSegment, int segmentOffset)
	throws IOException
	{
		target.deserialize(sources, firstSegment, segmentOffset);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#hash(java.lang.Object)
	 */
	@Override
	public int hash(PactRecord object) {
		object.getFieldsInto(this.keyFields, this.keyHolders1);
		int code = 0;
		for (int i = 0; i < this.keyHolders1.length; i++) {
			code ^= this.keyHolders1[i].hashCode();
		}
		return code;
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#compare(java.lang.Object, java.lang.Object, java.util.Comparator)
	 */
	@Override
	public int compare(PactRecord first, PactRecord second, Comparator<Key> comparator) {
		first.getFieldsInto(this.keyFields, this.keyHolders1);
		second.getFieldsInto(this.keyFields, this.keyHolders2);
		
		for (int i = 0; i < this.keyHolders1.length; i++) {
			int c = comparator.compare(this.keyHolders1[i], this.keyHolders2[i]);
			if (c != 0)
				return c;
		}
		return 0;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#compare(java.util.List, java.util.List, int, int, int, int)
	 */
	@Override
	public int compare(List<MemorySegment> sources1, List<MemorySegment> sources2, int firstSegment1,
			int firstSegment2, int offset1, int offset2) {
		// TODO Auto-generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#supportsNormalizedKey()
	 */
	@Override
	public boolean supportsNormalizedKey()
	{
		return this.numLeadingNormalizableKeys > 0;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#getNormalizeKeyLen()
	 */
	@Override
	public int getNormalizeKeyLen()
	{
		return this.normalizableKeyPrefixLen;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#putNormalizedKey(java.lang.Object, byte[], int, int)
	 */
	@Override
	public void putNormalizedKey(PactRecord record, byte[] target, int offset, int numBytes)
	{
		record.getFieldsInto(this.keyFields, this.keyHolders1);
		for (int i = 0; i < this.numLeadingNormalizableKeys & numBytes > 0; i++) {
			int bytes = ((NormalizableKey) this.keyHolders1[i]).copyNormalizedKey(target, offset, numBytes);
			numBytes -= bytes;
			offset += bytes;
		}
	}
}
