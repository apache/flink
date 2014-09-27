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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NormalizableKey;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;

import com.esotericsoftware.kryo.Kryo;

public class WritableComparator<T extends Writable & Comparable<T>> extends TypeComparator<T> {
	
	private static final long serialVersionUID = 1L;
	
	private Class<T> type;
	
	private final boolean ascendingComparison;
	
	private transient T reference;
	
	private transient T tempReference;
	
	private transient Kryo kryo;

	private final Comparable[] extractedKey = new Comparable[1];

	private final TypeComparator[] comparators = new TypeComparator[] {this};

	public WritableComparator(boolean ascending, Class<T> type) {
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
		return candidate.equals(reference);
	}
	
	@Override
	public int compareToReference(TypeComparator<T> referencedComparator) {
		T otherRef = ((WritableComparator<T>) referencedComparator).reference;
		int comp = otherRef.compareTo(reference);
		return ascendingComparison ? comp : -comp;
	}
	
	@Override
	public int compare(T first, T second) {
		int comp = first.compareTo(second);
		return ascendingComparison ? comp : -comp;
	}
	
	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		ensureReferenceInstantiated();
		ensureTempReferenceInstantiated();
		
		reference.readFields(firstSource);
		tempReference.readFields(secondSource);
		
		int comp = reference.compareTo(tempReference);
		return ascendingComparison ? comp : -comp;
	}
	
	@Override
	public boolean supportsNormalizedKey() {
		return NormalizableKey.class.isAssignableFrom(type);
	}
	
	@Override
	public int getNormalizeKeyLen() {
		ensureReferenceInstantiated();
		
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
		return new WritableComparator<T>(ascendingComparison, type);
	}

	@Override
	public Object[] extractKeys(T record) {
		extractedKey[0] = record;
		return extractedKey;
	}

	@Override public TypeComparator[] getComparators() {
		return comparators;
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
	
	private final void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();
			this.kryo.setAsmEnabled(true);
			this.kryo.register(type);
		}
	}
	
	private final void ensureReferenceInstantiated() {
		if (reference == null) {
			reference = InstantiationUtil.instantiate(type, Writable.class);
		}
	}
	
	private final void ensureTempReferenceInstantiated() {
		if (tempReference == null) {
			tempReference = InstantiationUtil.instantiate(type, Writable.class);
		}
	}
}
