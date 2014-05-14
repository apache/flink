/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils.runtime;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.types.KeyFieldOutOfBoundsException;
import eu.stratosphere.types.NullKeyFieldException;


public final class TupleComparator<T extends Tuple> extends TypeComparator<T> implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	/** key positions describe which fields are keys in what order */
	private final int[] keyPositions;
	
	/** comparators for the key fields, in the same order as the key fields */
	private final TypeComparator<Object>[] comparators;

	/** serializer factories to duplicate non thread-safe serializers */
	private final TypeSerializerFactory<Object>[] serializerFactories;
	
	
	private final int[] normalizedKeyLengths;
	
	private final int numLeadingNormalizableKeys;
	
	private final int normalizableKeyPrefixLen;
	
	private final boolean invertNormKey;
	
	
	/** serializers to deserialize the first n fields for comparison */
	private transient TypeSerializer<Object>[] serializers;
	
	// cache for the deserialized field objects
	private transient Object[] deserializedFields1;
	private transient Object[] deserializedFields2;
	
	
	@SuppressWarnings("unchecked")
	public TupleComparator(int[] keyPositions, TypeComparator<?>[] comparators, TypeSerializer<?>[] serializers) {
		// set the default utils
		this.keyPositions = keyPositions;
		this.comparators = (TypeComparator<Object>[]) comparators;
		this.serializers = (TypeSerializer<Object>[]) serializers;
	
		// set the serializer factories.
		this.serializerFactories = new TypeSerializerFactory[this.serializers.length];
		for (int i = 0; i < serializers.length; i++) {
			this.serializerFactories[i] = this.serializers[i].isStateful() ?
					new RuntimeStatefulSerializerFactory<Object>(this.serializers[i], Object.class) :
					new RuntimeStatelessSerializerFactory<Object>(this.serializers[i], Object.class);
		}
		
		// set up auxiliary fields for normalized key support
		this.normalizedKeyLengths = new int[keyPositions.length];
		int nKeys = 0;
		int nKeyLen = 0;
		boolean inverted = false;
		
		for (int i = 0; i < this.keyPositions.length; i++) {
			TypeComparator<?> k = this.comparators[i];
			
			// as long as the leading keys support normalized keys, we can build up the composite key
			if (k.supportsNormalizedKey()) {
				if (i == 0) {
					// the first comparator decides whether we need to invert the key direction
					inverted = k.invertNormalizedKey();
				}
				else if (k.invertNormalizedKey() != inverted) {
					// if a successor does not agree on the inversion direction, it cannot be part of the normalized key
					break;
				}
				
				nKeys++;
				final int len = k.getNormalizeKeyLen();
				if (len < 0) {
					throw new RuntimeException("Comparator " + k.getClass().getName() + " specifies an invalid length for the normalized key: " + len);
				}
				this.normalizedKeyLengths[i] = len;
				nKeyLen += len;
				
				if (nKeyLen < 0) {
					// overflow, which means we are out of budget for normalized key space anyways
					nKeyLen = Integer.MAX_VALUE;
					break;
				}
			} else {
				break;
			}
		}
		this.numLeadingNormalizableKeys = nKeys;
		this.normalizableKeyPrefixLen = nKeyLen;
		this.invertNormKey = inverted;
	}
	
	@SuppressWarnings("unchecked")
	private TupleComparator(TupleComparator<T> toClone) {
		// copy fields and serializer factories
		this.keyPositions = toClone.keyPositions;
		this.serializerFactories = toClone.serializerFactories;
		
		this.comparators = new TypeComparator[toClone.comparators.length];
		for (int i = 0; i < toClone.comparators.length; i++) {
			this.comparators[i] = toClone.comparators[i].duplicate();
		}
		
		this.normalizedKeyLengths = toClone.normalizedKeyLengths;
		this.numLeadingNormalizableKeys = toClone.numLeadingNormalizableKeys;
		this.normalizableKeyPrefixLen = toClone.normalizableKeyPrefixLen;
		this.invertNormKey = toClone.invertNormKey;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Comparator Methods
	// --------------------------------------------------------------------------------------------
	
	protected int[] getKeyPositions() {
		return this.keyPositions;
	}
	
	protected TypeComparator<Object>[] getComparators() {
		return this.comparators;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Comparator Methods
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hash(T value) {
		int i = 0;
		try {
			int code = 0;
			for (; i < this.keyPositions.length; i++) {
				code += this.comparators[i].hash(value.getField(keyPositions[i]));
				code *= HASH_SALT[i & 0x1F]; // salt code with (i % HASH_SALT.length)-th salt component
			}
			return code;
		}
		catch (NullPointerException npex) {
			throw new NullKeyFieldException(keyPositions[i]);
		}
		catch (IndexOutOfBoundsException iobex) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
	}

	@Override
	public void setReference(T toCompare) {
		int i = 0;
		try {
			for (; i < this.keyPositions.length; i++) {
				this.comparators[i].setReference(toCompare.getField(this.keyPositions[i]));
			}
		}
		catch (NullPointerException npex) {
			throw new NullKeyFieldException(keyPositions[i]);
		}
		catch (IndexOutOfBoundsException iobex) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
	}

	@Override
	public boolean equalToReference(T candidate) {
		int i = 0;
		try {
			for (; i < this.keyPositions.length; i++) {
				if (!this.comparators[i].equalToReference(candidate.getField(this.keyPositions[i]))) {
					return false;
				}
			}
			return true;
		}
		catch (NullPointerException npex) {
			throw new NullKeyFieldException(keyPositions[i]);
		}
		catch (IndexOutOfBoundsException iobex) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
	}

	@Override
	public int compareToReference(TypeComparator<T> referencedComparator) {
		TupleComparator<T> other = (TupleComparator<T>) referencedComparator;
		
		int i = 0;
		try {
			for (; i < this.keyPositions.length; i++) {
				int cmp = this.comparators[i].compareToReference(other.comparators[i]);
				if (cmp != 0) {
					return cmp;
				}
			}
			return 0;
		}
		catch (NullPointerException npex) {
			throw new NullKeyFieldException(keyPositions[i]);
		}
		catch (IndexOutOfBoundsException iobex) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
	}
	
	@Override
	public int compare(T first, T second) {
		int i = 0;
		try {
			for (; i < keyPositions.length; i++) {
				int keyPos = keyPositions[i];
				int cmp = comparators[i].compare(first.getField(keyPos), second.getField(keyPos));
				if (cmp != 0) {
					return cmp;
				}
			}
			
			return 0;
		} catch (NullPointerException npex) {
			throw new NullKeyFieldException(keyPositions[i]);
		} catch (IndexOutOfBoundsException iobex) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
	}

	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource) throws IOException {
		if (deserializedFields1 == null) {
			instantiateDeserializationUtils();
		}
		
		int i = 0;
		try {
			for (; i < serializers.length; i++) {
				deserializedFields1[i] = serializers[i].deserialize(deserializedFields1[i], firstSource);
				deserializedFields2[i] = serializers[i].deserialize(deserializedFields2[i], secondSource);
			}
			
			for (i = 0; i < keyPositions.length; i++) {
				int keyPos = keyPositions[i];
				int cmp = comparators[i].compare(deserializedFields1[keyPos], deserializedFields2[keyPos]);
				if (cmp != 0) {
					return cmp;
				}
			}
			
			return 0;
		} catch (NullPointerException npex) {
			throw new NullKeyFieldException(keyPositions[i]);
		} catch (IndexOutOfBoundsException iobex) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
	}
	
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
		return this.numLeadingNormalizableKeys < this.keyPositions.length ||
				this.normalizableKeyPrefixLen == Integer.MAX_VALUE ||
				this.normalizableKeyPrefixLen > keyBytes;
	}

	@Override
	public void putNormalizedKey(T value, MemorySegment target, int offset, int numBytes) {
		int i = 0;
		try {
			for (; i < this.numLeadingNormalizableKeys && numBytes > 0; i++)
			{
				int len = this.normalizedKeyLengths[i]; 
				len = numBytes >= len ? len : numBytes;
				this.comparators[i].putNormalizedKey(value.getField(this.keyPositions[i]), target, offset, len);
				numBytes -= len;
				offset += len;
			}
		}
		catch (NullPointerException npex) {
			throw new NullKeyFieldException(this.keyPositions[i]);
		}
	}

	@Override
	public boolean invertNormalizedKey() {
		return this.invertNormKey;
	}
	
	
	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}
	
	@Override
	public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public TupleComparator<T> duplicate() {
		return new TupleComparator<T>(this);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	private final void instantiateDeserializationUtils() {
		if (this.serializers == null) {
			this.serializers = new TypeSerializer[this.serializerFactories.length];
			for (int i = 0; i < this.serializers.length; i++) {
				this.serializers[i] = this.serializerFactories[i].getSerializer();
			}
		}
		
		this.deserializedFields1 = new Object[this.serializers.length];
		this.deserializedFields2 = new Object[this.serializers.length];
		
		for (int i = 0; i < this.serializers.length; i++) {
			this.deserializedFields1[i] = this.serializers[i].createInstance();
			this.deserializedFields2[i] = this.serializers[i].createInstance();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
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
}
