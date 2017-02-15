/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.java.typeutils.runtime;

import java.io.IOException;
import java.util.List;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.KeyFieldOutOfBoundsException;
import org.apache.flink.types.NullKeyFieldException;

@Internal
public abstract class TupleComparatorBase<T> extends CompositeTypeComparator<T> implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	/** key positions describe which fields are keys in what order */
	protected int[] keyPositions;

	/** comparators for the key fields, in the same order as the key fields */
	@SuppressWarnings("rawtypes")
	protected TypeComparator[] comparators;

	protected int[] normalizedKeyLengths;

	protected int numLeadingNormalizableKeys;

	protected int normalizableKeyPrefixLen;

	protected boolean invertNormKey;


	/** serializers to deserialize the first n fields for comparison */
	@SuppressWarnings("rawtypes")
	protected TypeSerializer[] serializers;

	// cache for the deserialized field objects
	protected transient Object[] deserializedFields1;
	protected transient Object[] deserializedFields2;


	@SuppressWarnings("unchecked")
	public TupleComparatorBase(int[] keyPositions, TypeComparator<?>[] comparators, TypeSerializer<?>[] serializers) {
		// set the default utils
		this.keyPositions = keyPositions;
		this.comparators = (TypeComparator<Object>[]) comparators;
		this.serializers = (TypeSerializer<Object>[]) serializers;

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

	protected TupleComparatorBase(TupleComparatorBase<T> toClone) {
		privateDuplicate(toClone);
	}

	// We need this because we cannot call the cloning constructor from the
	// ScalaTupleComparator
	protected void privateDuplicate(TupleComparatorBase<T> toClone) {
		// copy fields and serializer factories
		this.keyPositions = toClone.keyPositions;

		this.serializers = new TypeSerializer[toClone.serializers.length];
		for (int i = 0; i < toClone.serializers.length; i++) {
			this.serializers[i] = toClone.serializers[i].duplicate();
		}

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
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void getFlatComparator(List<TypeComparator> flatComparators) {
		for(int i = 0; i < comparators.length; i++) {
			if(comparators[i] instanceof CompositeTypeComparator) {
				((CompositeTypeComparator)comparators[i]).getFlatComparator(flatComparators);
			} else {
				flatComparators.add(comparators[i]);
			}
		}
	}	
	// --------------------------------------------------------------------------------------------
	//  Comparator Methods
	// --------------------------------------------------------------------------------------------


	@Override
	public int compareToReference(TypeComparator<T> referencedComparator) {
		TupleComparatorBase<T> other = (TupleComparatorBase<T>) referencedComparator;
		
		int i = 0;
		try {
			for (; i < this.keyPositions.length; i++) {
				@SuppressWarnings("unchecked")
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
	
	@SuppressWarnings("unchecked")
	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
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
			throw new KeyFieldOutOfBoundsException(keyPositions[i], iobex);
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
	
	// --------------------------------------------------------------------------------------------
	
	protected final void instantiateDeserializationUtils() {
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
	 * @see <a href="http://en.wikipedia.org/wiki/List_of_prime_numbers">http://en.wikipedia.org/wiki/List_of_prime_numbers</a>
	 * @see <a href="http://oeis.org/A068652">http://oeis.org/A068652</a>
	 */
	public static final int[] HASH_SALT = new int[] {
		73   , 79   , 97   , 113  , 131  , 197  , 199  , 311   , 
		337  , 373  , 719  , 733  , 919  , 971  , 991  , 1193  , 
		1931 , 3119 , 3779 , 7793 , 7937 , 9311 , 9377 , 11939 , 
		19391, 19937, 37199, 39119, 71993, 91193, 93719, 93911 };
}
