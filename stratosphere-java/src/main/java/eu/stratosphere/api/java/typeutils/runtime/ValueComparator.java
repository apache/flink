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

import com.esotericsoftware.kryo.Kryo;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.types.NormalizableKey;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.InstantiationUtil;

/**
 * Comparator for all Value types that extend Key
 */
public class ValueComparator<T extends Value & Comparable<T>> extends TypeComparator<T> {
	
	private static final long serialVersionUID = 1L;
	
	private final Class<T> type;
	
	private final boolean ascendingComparison;
	
	private transient T reference;
	
	private transient T tempReference;
	
	private transient Kryo kryo;
	
	
	public ValueComparator(boolean ascending, Class<T> type) {
		this.type = type;
		this.ascendingComparison = ascending;
	}
	
	@Override
	public int hash(T record) {
		return record.hashCode();
	}

	@Override
	public void setReference(T toCompare) {
		checkKryoInitialized();
		reference = this.kryo.copy(toCompare);
	}

	@Override
	public boolean equalToReference(T candidate) {
		return candidate.equals(this.reference);
	}

	@Override
	public int compareToReference(TypeComparator<T> referencedComparator) {
		T otherRef = ((ValueComparator<T>) referencedComparator).reference;
		return otherRef.compareTo(reference);
	}
	
	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource) throws IOException {
		if (reference == null) {
			reference = InstantiationUtil.instantiate(type, Value.class);
		}
		if (tempReference == null) {
			tempReference = InstantiationUtil.instantiate(type, Value.class);
		}
		
		reference.read(firstSource);
		tempReference.read(secondSource);
		return reference.compareTo(tempReference);
	}

	@Override
	public boolean supportsNormalizedKey() {
		return NormalizableKey.class.isAssignableFrom(type);
	}

	@Override
	public int getNormalizeKeyLen() {
		if (reference == null) {
			reference = InstantiationUtil.instantiate(type, Value.class);
		}
		
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
		return new ValueComparator<T>(ascendingComparison, type);
	}
	
	private final void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();
			this.kryo.setAsmEnabled(true);
			this.kryo.register(type);
		}
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
}
