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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.KeyFieldOutOfBoundsException;
import org.apache.flink.types.NullFieldException;
import org.apache.flink.types.NullKeyFieldException;

@Internal
public final class TupleComparator<T extends Tuple> extends TupleComparatorBase<T> {

	private static final long serialVersionUID = 1L;
	
	public TupleComparator(int[] keyPositions, TypeComparator<?>[] comparators, TypeSerializer<?>[] serializers) {
		super(keyPositions, comparators, serializers);
	}
	
	private TupleComparator(TupleComparator<T> toClone) {
		super(toClone);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Comparator Methods
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	@Override
	public int hash(T value) {
		int i = 0;
		try {
			int code = this.comparators[0].hash(value.getFieldNotNull(keyPositions[0]));
			for (i = 1; i < this.keyPositions.length; i++) {
				code *= HASH_SALT[i & 0x1F]; // salt code with (i % HASH_SALT.length)-th salt component
				code += this.comparators[i].hash(value.getFieldNotNull(keyPositions[i]));
			}
			return code;
		}
		catch (NullFieldException nfex) {
			throw new NullKeyFieldException(nfex);
		}
		catch (IndexOutOfBoundsException iobex) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setReference(T toCompare) {
		int i = 0;
		try {
			for (; i < this.keyPositions.length; i++) {
				this.comparators[i].setReference(toCompare.getFieldNotNull(this.keyPositions[i]));
			}
		}
		catch (NullFieldException nfex) {
			throw new NullKeyFieldException(nfex);
		}
		catch (IndexOutOfBoundsException iobex) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equalToReference(T candidate) {
		int i = 0;
		try {
			for (; i < this.keyPositions.length; i++) {
				if (!this.comparators[i].equalToReference(candidate.getFieldNotNull(this.keyPositions[i]))) {
					return false;
				}
			}
			return true;
		}
		catch (NullFieldException nfex) {
			throw new NullKeyFieldException(nfex);
		}
		catch (IndexOutOfBoundsException iobex) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public int compare(T first, T second) {
		int i = 0;
		try {
			for (; i < keyPositions.length; i++) {
				int keyPos = keyPositions[i];
				int cmp = comparators[i].compare(first.getFieldNotNull(keyPos), second.getFieldNotNull(keyPos));

				if (cmp != 0) {
					return cmp;
				}
			}
			return 0;
		} 
		catch (NullFieldException nfex) {
			throw new NullKeyFieldException(nfex);
		}
		catch (IndexOutOfBoundsException iobex) {
			throw new KeyFieldOutOfBoundsException(keyPositions[i]);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void putNormalizedKey(T value, MemorySegment target, int offset, int numBytes) {
		int i = 0;
		try {
			for (; i < this.numLeadingNormalizableKeys && numBytes > 0; i++) {
				int len = this.normalizedKeyLengths[i];
				len = numBytes >= len ? len : numBytes;
				this.comparators[i].putNormalizedKey(value.getFieldNotNull(this.keyPositions[i]), target, offset, len);
				numBytes -= len;
				offset += len;
			}
		} catch (NullFieldException nfex) {
			throw new NullKeyFieldException(nfex);
		} catch (NullPointerException npex) {
			throw new NullKeyFieldException(this.keyPositions[i]);
		}
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		int localIndex = index;
		for(int i = 0; i < comparators.length; i++) {
			localIndex += comparators[i].extractKeys(((Tuple) record).getField(keyPositions[i]), target, localIndex);
		}
		return localIndex - index;
	}

	public TypeComparator<T> duplicate() {
		return new TupleComparator<T>(this);
	}
}
