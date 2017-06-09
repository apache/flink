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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NormalizableKey;
import org.apache.flink.util.InstantiationUtil;

import com.esotericsoftware.kryo.Kryo;
import org.apache.hadoop.io.Writable;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;

/**
 * A {@link TypeComparator} for {@link Writable}.
 * @param <T>
 */
public class WritableComparator<T extends Writable & Comparable<T>> extends TypeComparator<T> {

	private static final long serialVersionUID = 1L;

	private Class<T> type;

	private final boolean ascendingComparison;

	private transient T reference;

	private transient T tempReference;

	private transient Kryo kryo;

	@SuppressWarnings("rawtypes")
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

		reference = KryoUtils.copy(toCompare, kryo, new WritableSerializer<T>(type));
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
	public int extractKeys(Object record, Object[] target, int index) {
		target[index] = record;
		return 1;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public TypeComparator[] getFlatComparators() {
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

	private void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();

			Kryo.DefaultInstantiatorStrategy instantiatorStrategy = new Kryo.DefaultInstantiatorStrategy();
			instantiatorStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
			kryo.setInstantiatorStrategy(instantiatorStrategy);

			this.kryo.setAsmEnabled(true);
			this.kryo.register(type);
		}
	}

	private void ensureReferenceInstantiated() {
		if (reference == null) {
			reference = InstantiationUtil.instantiate(type, Writable.class);
		}
	}

	private void ensureTempReferenceInstantiated() {
		if (tempReference == null) {
			tempReference = InstantiationUtil.instantiate(type, Writable.class);
		}
	}
}
