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

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyFieldOutOfBoundsException;
import eu.stratosphere.pact.common.type.NormalizableKey;
import eu.stratosphere.pact.common.type.NullKeyFieldException;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;


/**
 * Implementation of the {@link TypeComparator} interface for the pact record. Instances of this class
 * are parameterized with which fields are relevant to the comparison. 
 *
 * @author Stephan Ewen
 */
public final class PactRecordComparator implements TypeComparator<PactRecord>
{	
	private final int[] keyFields;
	
	private final Key[] keyHolders, transientKeyHolders;
	
	private final PactRecord temp1, temp2;
	
	private final int[] normalizedKeyLengths;
	
	private final int numLeadingNormalizableKeys;
	
	private final int normalizableKeyPrefixLen;
	

	/**
	 * Creates a new comparator that compares Pact Records by the subset of fields as described
	 * by the given key positions and types.
	 * 
	 * @param keyFields The positions of the key fields.
	 * @param keyTypes The types (classes) of the key fields.
	 */
	public PactRecordComparator(int[] keyFields, Class<? extends Key>[] keyTypes)
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
	
	private PactRecordComparator(int[] keyFields, Key[] keys, int[] normalKeyLengths,
							int leadingNormalKeys, int normalKeyPrefixLen)
	{
		this.keyFields = keyFields;
		
		this.keyHolders = new Key[keys.length];
		this.transientKeyHolders = new Key[keys.length];
		
		try {
			for (int i = 0; i < keys.length; i++) {
				this.keyHolders[i] = keys[i].getClass().newInstance();
				this.transientKeyHolders[i] = keys[i].getClass().newInstance();
			}
		} catch (Exception ex) {
			// this should never happen, because the classes have been instantiated before. Report for debugging.
			throw new RuntimeException("Could not instantiate key classes when duplicating PactRecordComparator.", ex);
		}
		
		this.normalizedKeyLengths = normalKeyLengths;
		this.numLeadingNormalizableKeys = leadingNormalKeys;
		this.normalizableKeyPrefixLen = normalKeyPrefixLen;
		
		this.temp1 = new PactRecord();
		this.temp2 = new PactRecord();
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
		catch (IndexOutOfBoundsException iobex) {
			throw new KeyFieldOutOfBoundsException(this.keyFields[i]);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#setReferenceForEquality(java.lang.Object)
	 */
	@Override
	public void setReference(PactRecord toCompare)
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
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#compareToReference(eu.stratosphere.pact.runtime.plugable.TypeAccessors)
	 */
	@Override
	public int compareToReference(TypeComparator<PactRecord> referencedAccessors)
	{
		final PactRecordComparator pra = (PactRecordComparator) referencedAccessors;
		
		for (int i = 0; i < this.keyFields.length; i++)
		{
			final int comp = pra.keyHolders[i].compareTo(this.keyHolders[i]);
			if (comp != 0)
				return comp;
		}
		return 0;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#compare(eu.stratosphere.nephele.services.memorymanager.DataInputViewV2, eu.stratosphere.nephele.services.memorymanager.DataInputViewV2)
	 */
	@Override
	public int compare(DataInputView source1, DataInputView source2) throws IOException
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
	public PactRecordComparator duplicate()
	{
		return new PactRecordComparator(this.keyFields, this.keyHolders, this.normalizedKeyLengths,
											this.numLeadingNormalizableKeys, this.normalizableKeyPrefixLen);
	}
	
	// --------------------------------------------------------------------------------------------
	//                           Non Standard Comparator Methods
	// --------------------------------------------------------------------------------------------
	
	public final int[] getKeyPositions()
	{
		return this.keyFields;
	}
	
	public final Class<? extends Key>[] getKeyTypes()
	{
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyTypes = new Class[this.keyHolders.length];
		for (int i = 0; i < keyTypes.length; i++) {
			keyTypes[i] = this.keyHolders[i].getClass();
		}
		return keyTypes;
	}
	
	public final Key[] getKeysAsCopy(PactRecord record)
	{
		try {
			final Key[] keys = new Key[this.keyFields.length];
			for (int i = 0; i < keys.length; i++) {
				keys[i] = this.keyHolders[i].getClass().newInstance();
			}
			record.getFieldsInto(this.keyFields, keys);
			return keys;
		} catch (Exception ex) {
			// this should never happen, because the classes have been instantiated before. Report for debugging.
			throw new RuntimeException("Could not instantiate key classes when duplicating PactRecordComparator.", ex);
		}
	}
	
	public final int compareAgainstReference(Key[] keys)
	{
		for (int i = 0; i < this.keyFields.length; i++)
		{
			final int comp = keys[i].compareTo(this.keyHolders[i]);
			if (comp != 0)
				return comp;
		}
		return 0;
	}
}
