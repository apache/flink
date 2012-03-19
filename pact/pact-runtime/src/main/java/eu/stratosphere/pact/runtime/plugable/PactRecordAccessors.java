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

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NormalizableKey;
import eu.stratosphere.pact.common.type.NullKeyFieldException;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class PactRecordAccessors implements TypeAccessors<PactRecord>
{
	private static final int MAX_BIT = 0x80;					// byte where only the most significant bit is set
	
	// --------------------------------------------------------------------------------------------
	
	private final int[] keyFields;
	
	private final Key[] keyHolders, transientKeyHolders;
	
	private final PactRecord temp1, temp2;
	
	private final int[] normalizedKeyLengths;
	
	private final int numLeadingNormalizableKeys;
	
	private final int normalizableKeyPrefixLen;
	

	public PactRecordAccessors(int[] keyFields, Class<? extends Key>[] keyTypes)
	{
		this.keyFields = keyFields;
		
		// instantiate fields to extract keys into
		this.keyHolders = new Key[keyTypes.length];
		this.transientKeyHolders = new Key[keyTypes.length];
		for (int i = 0; i < keyTypes.length; i++) {
			if (keyTypes[i] == null) {
				throw new NullPointerException("Key type " + i + " is null.");
			}
			this.keyHolders[i] = InstantiationUtil.instantiate(keyTypes[i], Key.class);
			this.transientKeyHolders[i] = InstantiationUtil.instantiate(keyTypes[i], Key.class);
		}
		
		// set up auxiliary fields for normalized key support
		this.normalizedKeyLengths = new int[keyFields.length];
		int nKeys = 0;
		int nKeyLen = 0;
		for (int i = 0; i < this.keyHolders.length; i++) {
			Key k = this.keyHolders[i];
			if (k instanceof NormalizableKey) {
				nKeys++;
				final int len = ((NormalizableKey) k).getMaxNormalizedKeyLen();
				if (len < 0) {
					throw new RuntimeException("Data type " + k.getClass().getName() + 
						" specifies an invalid length for the normalized key: " + len);
				}
				this.normalizedKeyLengths[i] = len;
				nKeyLen += this.normalizedKeyLengths[i];
				if (nKeyLen < 0) {
					nKeyLen = Integer.MAX_VALUE;
				}
			}
			else break;
		}
		this.numLeadingNormalizableKeys = nKeys;
		this.normalizableKeyPrefixLen = nKeyLen;
		
		temp1 = new PactRecord();
		temp2 = new PactRecord();
	}
	
	/**
	 * Copy constructor.
	 * 
	 * @param keyFields
	 * @param keys
	 * @param normalKeyLengths
	 * @param leadingNormalKeys
	 * @param normalKeyPrefixLen
	 */
	private PactRecordAccessors(int[] keyFields, Key[] keys, int[] normalKeyLengths,
							int leadingNormalKeys, int normalKeyPrefixLen)
	{
		this.keyFields = keyFields;
		
		this.keyHolders = new Key[keys.length];
		this.transientKeyHolders = new Key[keys.length];
		for (int i = 0; i < keys.length; i++) {
			this.keyHolders[i] = InstantiationUtil.instantiate(keys[i].getClass(), Key.class);
			this.transientKeyHolders[i] = InstantiationUtil.instantiate(keys[i].getClass(), Key.class);
		}
		
		this.normalizedKeyLengths = normalKeyLengths;
		this.numLeadingNormalizableKeys = leadingNormalKeys;
		this.normalizableKeyPrefixLen = normalKeyPrefixLen;
		
		this.temp1 = new PactRecord();
		this.temp2 = new PactRecord();
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
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#getLength()
	 */
	@Override
	public int getLength() {
		return -1;
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#serialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2)
	 */
	@Override
	public long serialize(PactRecord record, DataOutputViewV2 target) throws IOException
	{
		return record.serialize(target);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#deserialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataInputViewV2)
	 */
	@Override
	public void deserialize(PactRecord target, DataInputViewV2 source) throws IOException
	{
		target.deserialize(source);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copy(eu.stratosphere.nephele.services.memorymanager.DataInputViewV2, eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2)
	 */
	@Override
	public void copy(DataInputViewV2 source, DataOutputViewV2 target) throws IOException
	{
		int val = source.readUnsignedByte();
		target.writeByte(val);
		
		if (val >= MAX_BIT) {
			int shift = 7;
			int curr;
			val = val & 0x7f;
			while ((curr = source.readUnsignedByte()) >= MAX_BIT) {
				target.writeByte(curr);
				val |= (curr & 0x7f) << shift;
				shift += 7;
			}
			target.writeByte(curr);
			val |= curr << shift;
		}
		
		target.write(source, val);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#hash(java.lang.Object)
	 */
	@Override
	public int hash(PactRecord object)
	{
		int i = 0;
		try {
			int code = 0;
			for (; i < this.keyFields.length; i++) {
				code ^= object.getField(this.keyFields[i], this.transientKeyHolders[i]).hashCode();
			}
			return code;
		}
		catch (NullPointerException npex) {
			throw new NullKeyFieldException(this.keyFields[i]);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#setReferenceForEquality(java.lang.Object)
	 */
	@Override
	public void setReferenceForEquality(PactRecord toCompare)
	{
		for (int i = 0; i < this.keyFields.length; i++) {
			if (!toCompare.getFieldInto(this.keyFields[i], this.keyHolders[i])) {
				throw new NullKeyFieldException(this.keyFields[i]);
			}
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#equalToReference(java.lang.Object)
	 */
	@Override
	public boolean equalToReference(PactRecord candidate)
	{
		for (int i = 0; i < this.keyFields.length; i++) {
			final Key k = candidate.getField(this.keyFields[i], this.transientKeyHolders[i]);
			if (k == null)
				throw new NullKeyFieldException(this.keyFields[i]);
			else if (!k.equals(this.keyHolders[i]))
				return false;
		}
		return true;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#compare(eu.stratosphere.nephele.services.memorymanager.DataInputViewV2, eu.stratosphere.nephele.services.memorymanager.DataInputViewV2)
	 */
	@Override
	public int compare(DataInputViewV2 source1, DataInputViewV2 source2) throws IOException
	{
		this.temp1.read(source1);
		this.temp2.read(source2);
		
		for (int i = 0; i < this.keyFields.length; i++) {
			final Key k1 = this.temp1.getField(this.keyFields[i], this.keyHolders[i]);
			final Key k2 = this.temp2.getField(this.keyFields[i], this.transientKeyHolders[i]);
			
			if (k1 == null || k2 == null)
				throw new NullKeyFieldException(this.keyFields[i]);
			
			final int comp = k1.compareTo(k2);
			if (comp != 0)
				return comp;
		}
		return 0;
	}

//	/* (non-Javadoc)
//	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#compare(java.lang.Object, java.lang.Object, java.util.Comparator)
//	 */
//	@Override
//	public int compare(PactRecord first, PactRecord second, Comparator<Key> comparator)
//	{
//		if (first.getFieldsInto(this.keyFields, this.keyHolders) &
//		    second.getFieldsInto(this.keyFields, this.transientKeyHolders))
//		{
//			for (int i = 0; i < this.keyHolders.length; i++) {
//				int c = comparator.compare(this.keyHolders[i], this.transientKeyHolders[i]);
//				if (c != 0)
//					return c;
//			}
//			return 0;
//		}
//		else throw new NullKeyFieldException();
//	}
	
	// --------------------------------------------------------------------------------------------

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
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#isNormalizedKeyPrefixOnly()
	 */
	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes)
	{
		return this.numLeadingNormalizableKeys < this.keyFields.length ||
		        this.normalizableKeyPrefixLen == Integer.MAX_VALUE ||
				this.normalizableKeyPrefixLen > keyBytes;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#putNormalizedKey(java.lang.Object, byte[], int, int)
	 */
	@Override
	public void putNormalizedKey(PactRecord record, byte[] target, int offset, int numBytes)
	{
		int i = 0;
		try {
			for (; i < this.numLeadingNormalizableKeys & numBytes > 0; i++)
			{
				int len = this.normalizedKeyLengths[i]; 
				len = numBytes >= len ? len : numBytes;
				((NormalizableKey) record.getField(this.keyFields[i], this.transientKeyHolders[i])).copyNormalizedKey(target, offset, len);
				numBytes -= len;
				offset += len;
			}
		}
		catch (NullPointerException npex) {
			throw new NullKeyFieldException(this.keyFields[i]);
		}
	}
	
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#duplicate()
	 */
	@Override
	public PactRecordAccessors duplicate() {
		return new PactRecordAccessors(this.keyFields, this.keyHolders, this.normalizedKeyLengths,
											this.numLeadingNormalizableKeys, this.normalizableKeyPrefixLen);
	}
}
