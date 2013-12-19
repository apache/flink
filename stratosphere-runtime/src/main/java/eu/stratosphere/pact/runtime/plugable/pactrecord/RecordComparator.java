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

package eu.stratosphere.pact.runtime.plugable.pactrecord;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.KeyFieldOutOfBoundsException;
import eu.stratosphere.types.NormalizableKey;
import eu.stratosphere.types.NullKeyFieldException;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.InstantiationUtil;


/**
 * Implementation of the {@link TypeComparator} interface for the pact record. Instances of this class
 * are parameterized with which fields are relevant to the comparison. 
 */
public final class RecordComparator extends TypeComparator<Record> {
	
	/**
	 * A sequence of prime numbers to be used for salting the computed hash values.
	 * Based on some empirical evidence, we are using a 32-element subsequence of the  
	 * OEIS sequence #A068652 (numbers such that every cyclic permutation is a prime).
	 * 
	 * @see: http://en.wikipedia.org/wiki/List_of_prime_numbers
	 * @see: http://oeis.org/A068652
	 */
	private static final int[] HASH_SALT = new int[] { 
		73   , 79   , 97   , 113  , 131  , 197  , 199  , 311   , 
		337  , 373  , 719  , 733  , 919  , 971  , 991  , 1193  , 
		1931 , 3119 , 3779 , 7793 , 7937 , 9311 , 9377 , 11939 , 
		19391, 19937, 37199, 39119, 71993, 91193, 93719, 93911 };
	
	private final int[] keyFields;
	
	private final Key[] keyHolders, transientKeyHolders;
	
	private final Record temp1, temp2;
	
	private final boolean[] ascending;
	
	private final int[] normalizedKeyLengths;
	
	private final int numLeadingNormalizableKeys;
	
	private final int normalizableKeyPrefixLen;
	

	/**
	 * Creates a new comparator that compares Pact Records by the subset of fields as described
	 * by the given key positions and types. All order comparisons will assume ascending order on all fields.
	 * 
	 * @param keyFields The positions of the key fields.
	 * @param keyTypes The types (classes) of the key fields.
	 */
	public RecordComparator(int[] keyFields, Class<? extends Key>[] keyTypes) {
		this(keyFields, keyTypes, null);
	}
	
	/**
	 * Creates a new comparator that compares Pact Records by the subset of fields as described
	 * by the given key positions and types.
	 * 
	 * @param keyFields The positions of the key fields.
	 * @param keyTypes The types (classes) of the key fields.
	 * @param sortOrder The direction for sorting. A value of <i>true</i> indicates ascending for an attribute,
	 *                  a value of <i>false</i> indicated descending. If the parameter is <i>null</i>, then
	 *                  all order comparisons will assume ascending order on all fields.
	 */
	public RecordComparator(int[] keyFields, Class<? extends Key>[] keyTypes, boolean[] sortDirection) {
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
		boolean inverted = false;
		for (int i = 0; i < this.keyHolders.length; i++) {
			Key k = this.keyHolders[i];
			if (k instanceof NormalizableKey) {
				if (sortDirection != null) {
					if (sortDirection[i] && inverted) {
						break;
					} else if (i == 0 && !sortDirection[0]) {
						inverted = true;
					}
				}
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
					break;
				}
			}
			else break;
		}
		this.numLeadingNormalizableKeys = nKeys;
		this.normalizableKeyPrefixLen = nKeyLen;
		
		this.temp1 = new Record();
		this.temp2 = new Record();
		
		if (sortDirection != null) {
			this.ascending = sortDirection;
		} else {
			this.ascending = new boolean[keyFields.length];
			for (int i = 0; i < this.ascending.length; i++) {
				this.ascending[i] = true;
			}
		}
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
	private RecordComparator(RecordComparator toCopy) {
		this.keyFields = toCopy.keyFields;
		this.keyHolders = new Key[toCopy.keyHolders.length];
		this.transientKeyHolders = new Key[toCopy.keyHolders.length];
		
		try {
			for (int i = 0; i < this.keyHolders.length; i++) {
				this.keyHolders[i] = toCopy.keyHolders[i].getClass().newInstance();
				this.transientKeyHolders[i] = toCopy.keyHolders[i].getClass().newInstance();
			}
		} catch (Exception ex) {
			// this should never happen, because the classes have been instantiated before. Report for debugging.
			throw new RuntimeException("Could not instantiate key classes when duplicating RecordComparator.", ex);
		}
		
		this.normalizedKeyLengths = toCopy.normalizedKeyLengths;
		this.numLeadingNormalizableKeys = toCopy.numLeadingNormalizableKeys;
		this.normalizableKeyPrefixLen = toCopy.normalizableKeyPrefixLen;
		this.ascending = toCopy.ascending;
		
		this.temp1 = new Record();
		this.temp2 = new Record();
	}
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public int hash(Record object) {
		int i = 0;
		try {
			int code = 0;
			for (; i < this.keyFields.length; i++) {
				code ^= object.getField(this.keyFields[i], this.transientKeyHolders[i]).hashCode();
				code *= HASH_SALT[i & 0x1F]; // salt code with (i % HASH_SALT.length)-th salt component
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


	@Override
	public void setReference(Record toCompare) {
		for (int i = 0; i < this.keyFields.length; i++) {
			if (!toCompare.getFieldInto(this.keyFields[i], this.keyHolders[i])) {
				throw new NullKeyFieldException(this.keyFields[i]);
			}
		}
	}


	@Override
	public boolean equalToReference(Record candidate) {
		for (int i = 0; i < this.keyFields.length; i++) {
			final Key k = candidate.getField(this.keyFields[i], this.transientKeyHolders[i]);
			if (k == null)
				throw new NullKeyFieldException(this.keyFields[i]);
			else if (!k.equals(this.keyHolders[i]))
				return false;
		}
		return true;
	}
	

	@Override
	public int compareToReference(TypeComparator<Record> referencedAccessors) {
		final RecordComparator pra = (RecordComparator) referencedAccessors;
		
		for (int i = 0; i < this.keyFields.length; i++) {
			final int comp = pra.keyHolders[i].compareTo(this.keyHolders[i]);
			if (comp != 0)
				return this.ascending[i] ? comp : -comp;
		}
		return 0;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#compare(eu.stratosphere.nephele.services.memorymanager.DataInputView, eu.stratosphere.nephele.services.memorymanager.DataInputView)
	 */
	@Override
	public int compare(DataInputView source1, DataInputView source2) throws IOException {
		this.temp1.read(source1);
		this.temp2.read(source2);
		
		for (int i = 0; i < this.keyFields.length; i++) {
			final Key k1 = this.temp1.getField(this.keyFields[i], this.keyHolders[i]);
			final Key k2 = this.temp2.getField(this.keyFields[i], this.transientKeyHolders[i]);
			
			if (k1 == null || k2 == null)
				throw new NullKeyFieldException(this.keyFields[i]);
			
			final int comp = k1.compareTo(k2);
			if (comp != 0)
				return this.ascending[i] ? comp : -comp;
		}
		return 0;
	}
	
	// --------------------------------------------------------------------------------------------


	@Override
	public boolean supportsNormalizedKey() {
		return this.numLeadingNormalizableKeys > 0;
	}


	@Override
	public int getNormalizeKeyLen() {
		return this.normalizableKeyPrefixLen;
	}
	

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return this.numLeadingNormalizableKeys < this.keyFields.length ||
				this.normalizableKeyPrefixLen == Integer.MAX_VALUE ||
				this.normalizableKeyPrefixLen > keyBytes;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.generic.types.TypeComparator#putNormalizedKey(java.lang.Object, byte[], int, int)
	 */
	@Override
	public void putNormalizedKey(Record record, MemorySegment target, int offset, int numBytes) {
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
	

	@Override
	public boolean invertNormalizedKey() {
		return !this.ascending[0];
	}
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.generic.types.TypeComparator#writeWithKeyNormalization(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
	 */
	@Override
	public void writeWithKeyNormalization(Record record, DataOutputView target) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.generic.types.TypeComparator#readWithKeyDenormalization(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataInputView)
	 */
	@Override
	public void readWithKeyDenormalization(Record record, DataInputView source) {
		throw new UnsupportedOperationException();
	}
	
	// --------------------------------------------------------------------------------------------


	@Override
	public RecordComparator duplicate() {
		return new RecordComparator(this);
	}
	
	// --------------------------------------------------------------------------------------------
	//                           Non Standard Comparator Methods
	// --------------------------------------------------------------------------------------------
	
	public final int[] getKeyPositions() {
		return this.keyFields;
	}
	
	public final Class<? extends Key>[] getKeyTypes() {
		@SuppressWarnings("unchecked")
		final Class<? extends Key>[] keyTypes = new Class[this.keyHolders.length];
		for (int i = 0; i < keyTypes.length; i++) {
			keyTypes[i] = this.keyHolders[i].getClass();
		}
		return keyTypes;
	}
	
	public final Key[] getKeysAsCopy(Record record) {
		try {
			final Key[] keys = new Key[this.keyFields.length];
			for (int i = 0; i < keys.length; i++) {
				keys[i] = this.keyHolders[i].getClass().newInstance();
			}
			if(!record.getFieldsInto(this.keyFields, keys)) {
				throw new RuntimeException("Could not extract keys from record.");
			}
			return keys;
		} catch (Exception ex) {
			// this should never happen, because the classes have been instantiated before. Report for debugging.
			throw new RuntimeException("Could not instantiate key classes when duplicating RecordComparator.", ex);
		}
	}
	
	public final int compareAgainstReference(Key[] keys) {
		for (int i = 0; i < this.keyFields.length; i++)
		{
			final int comp = keys[i].compareTo(this.keyHolders[i]);
			if (comp != 0)
				return this.ascending[i] ? comp : -comp;
		}
		return 0;
	}
}
