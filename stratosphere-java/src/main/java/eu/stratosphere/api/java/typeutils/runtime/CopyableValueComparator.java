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
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.types.CopyableValue;
import eu.stratosphere.types.NormalizableKey;
import eu.stratosphere.util.InstantiationUtil;

/**
 * Comparator for all Value types that extend Key
 */
public class CopyableValueComparator<T extends CopyableValue<T> & Comparable<T>> extends TypeComparator<T> {
	
	private static final long serialVersionUID = 1L;
	
	private final Class<T> type;
	
	private final boolean ascendingComparison;
	
	private transient T reference;
	
	private transient T tempReference;
	
	
	public CopyableValueComparator(boolean ascending, Class<T> type) {
		this.type = type;
		this.ascendingComparison = ascending;
		this.reference = InstantiationUtil.instantiate(type, CopyableValue.class);
	}
	
	@Override
	public int hash(T record) {
		return record.hashCode();
	}

	@Override
	public void setReference(T toCompare) {
		toCompare.copyTo(reference);
	}

	@Override
	public boolean equalToReference(T candidate) {
		return candidate.equals(this.reference);
	}

	@Override
	public int compareToReference(TypeComparator<T> referencedComparator) {
		T otherRef = ((CopyableValueComparator<T>) referencedComparator).reference;
		int comp = otherRef.compareTo(reference);
		return ascendingComparison ? comp : -comp;
	}
	
	@Override
	public int compare(T first, T second) {
		int comp = first.compareTo(second);
		return ascendingComparison ? comp : -comp;
	}
	
	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource) throws IOException {
		if (tempReference == null) {
			tempReference = InstantiationUtil.instantiate(type, CopyableValue.class);
		}
		
		reference.read(firstSource);
		tempReference.read(secondSource);
		int comp = reference.compareTo(tempReference);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return NormalizableKey.class.isAssignableFrom(type);
	}

	@Override
	public int getNormalizeKeyLen() {
		NormalizableKey<?> key = (NormalizableKey<?>) reference;
		return key.getMaxNormalizedKeyLen();
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < getNormalizeKeyLen();
	}

	@Override
	public void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes) {
		NormalizableKey<?> key = (NormalizableKey<?>) record;
		key.copyNormalizedKey(target, offset, numBytes);
	}

	@Override
	public boolean invertNormalizedKey() {
		return !ascendingComparison;
	}
	
	@Override
	public TypeComparator<T> duplicate() {
		return new CopyableValueComparator<T>(ascendingComparison, type);
	}
	
	// --------------------------------------------------------------------------------------------
	// unsupported normalization
	// --------------------------------------------------------------------------------------------
	
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
	// serialization
	// --------------------------------------------------------------------------------------------
	
	private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
		// read basic object and the type
		s.defaultReadObject();
		
		this.reference = InstantiationUtil.instantiate(type, CopyableValue.class);
		this.tempReference = null;
	}
}
