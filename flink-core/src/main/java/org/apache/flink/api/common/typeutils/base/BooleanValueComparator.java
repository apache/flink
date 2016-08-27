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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.NormalizableKey;

import java.io.IOException;

/**
 * Specialized comparator for BooleanValue based on CopyableValueComparator.
 */
@Internal
public class BooleanValueComparator extends TypeComparator<BooleanValue> {

	private static final long serialVersionUID = 1L;

	private final boolean ascendingComparison;

	private final BooleanValue reference = new BooleanValue();

	private final BooleanValue tempReference = new BooleanValue();

	private final TypeComparator<?>[] comparators = new TypeComparator[] {this};

	public BooleanValueComparator(boolean ascending) {
		this.ascendingComparison = ascending;
	}

	@Override
	public int hash(BooleanValue record) {
		return record.hashCode();
	}

	@Override
	public void setReference(BooleanValue toCompare) {
		toCompare.copyTo(reference);
	}

	@Override
	public boolean equalToReference(BooleanValue candidate) {
		return candidate.equals(this.reference);
	}

	@Override
	public int compareToReference(TypeComparator<BooleanValue> referencedComparator) {
		BooleanValue otherRef = ((BooleanValueComparator) referencedComparator).reference;
		int comp = otherRef.compareTo(reference);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public int compare(BooleanValue first, BooleanValue second) {
		int comp = first.compareTo(second);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		reference.read(firstSource);
		tempReference.read(secondSource);
		int comp = reference.compareTo(tempReference);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return NormalizableKey.class.isAssignableFrom(BooleanValue.class);
	}

	@Override
	public int getNormalizeKeyLen() {
		return reference.getMaxNormalizedKeyLen();
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < getNormalizeKeyLen();
	}

	@Override
	public void putNormalizedKey(BooleanValue record, MemorySegment target, int offset, int numBytes) {
		record.copyNormalizedKey(target, offset, numBytes);
	}

	@Override
	public boolean invertNormalizedKey() {
		return !ascendingComparison;
	}

	@Override
	public TypeComparator<BooleanValue> duplicate() {
		return new BooleanValueComparator(ascendingComparison);
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		target[index] = record;
		return 1;
	}

	@Override
	public TypeComparator<?>[] getFlatComparators() {
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
	public void writeWithKeyNormalization(BooleanValue record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public BooleanValue readWithKeyDenormalization(BooleanValue reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}
}
