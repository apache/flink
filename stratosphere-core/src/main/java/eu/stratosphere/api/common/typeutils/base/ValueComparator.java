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
package eu.stratosphere.api.common.typeutils.base;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.NormalizableKey;

/**
 * Comparator for all Value types that extend Key
 */
public class ValueComparator<T extends Key<T>> extends BasicTypeComparator<T> {
	
	private static final long serialVersionUID = 1L;
	
	private final Class<T> type;
	private final T instance;
	
	
	public ValueComparator(boolean ascending,  Class<T> type) {
		super(ascending);
		this.type = type;
		
		try {
			instance = type.newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Failed in creating the necessary default instance for comaprison of a Value type.");
		}
		
	}

	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource)
			throws IOException {
		
		T instance2;
		
		try {
			instance2 = type.newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Failed in creating the necessary second instance for comaprison of a Value type.");
		}
		
		instance.read(firstSource);
		instance2.read(secondSource);
		
		return instance.compareTo(instance2);
	}

	@Override
	public boolean supportsNormalizedKey() {
		
		return (instance instanceof NormalizableKey<?>);
	}

	@Override
	public int getNormalizeKeyLen() {
		NormalizableKey<?> key = (NormalizableKey<?>) instance;
		return key.getMaxNormalizedKeyLen();
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < getNormalizeKeyLen();
	}

	@Override
	public void putNormalizedKey(T record, MemorySegment target, int offset,
			int numBytes) {
		
		NormalizableKey<?> key = (NormalizableKey<?>) record;
		key.copyNormalizedKey(target, offset, numBytes);
	}

	@Override
	public TypeComparator<T> duplicate() {
		return new ValueComparator<T>(ascendingComparison, type);
	}
}
