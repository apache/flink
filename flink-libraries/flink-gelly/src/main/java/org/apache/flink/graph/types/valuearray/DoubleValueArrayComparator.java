/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NormalizableKey;

import java.io.IOException;

/**
 * Specialized comparator for DoubleValueArray based on CopyableValueComparator.
 *
 * <p>This can be used for grouping keys but not for sorting keys.
 */
@Internal
public class DoubleValueArrayComparator extends TypeComparator<DoubleValueArray> {

	private static final long serialVersionUID = 1L;

	private final boolean ascendingComparison;

	private final DoubleValueArray reference = new DoubleValueArray();

	private final TypeComparator<?>[] comparators = new TypeComparator[] {this};

	public DoubleValueArrayComparator(boolean ascending) {
		this.ascendingComparison = ascending;
	}

	@Override
	public int hash(DoubleValueArray record) {
		return record.hashCode();
	}

	@Override
	public void setReference(DoubleValueArray toCompare) {
		toCompare.copyTo(reference);
	}

	@Override
	public boolean equalToReference(DoubleValueArray candidate) {
		return candidate.equals(this.reference);
	}

	@Override
	public int compareToReference(TypeComparator<DoubleValueArray> referencedComparator) {
		int comp = ((DoubleValueArrayComparator) referencedComparator).reference.compareTo(reference);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public int compare(DoubleValueArray first, DoubleValueArray second) {
		int comp = first.compareTo(second);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		int firstCount = firstSource.readInt();
		int secondCount = secondSource.readInt();

		int minCount = Math.min(firstCount, secondCount);
		while (minCount-- > 0) {
			double firstValue = firstSource.readDouble();
			double secondValue = secondSource.readDouble();

			int cmp = Double.compare(firstValue, secondValue);
			if (cmp != 0) {
				return ascendingComparison ? cmp : -cmp;
			}
		}

		int cmp = Integer.compare(firstCount, secondCount);
		return ascendingComparison ? cmp : -cmp;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return NormalizableKey.class.isAssignableFrom(DoubleValueArray.class);
	}

	@Override
	public int getNormalizeKeyLen() {
		return reference.getMaxNormalizedKeyLen();
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyDoubles) {
		return keyDoubles < getNormalizeKeyLen();
	}

	@Override
	public void putNormalizedKey(DoubleValueArray record, MemorySegment target, int offset, int numDoubles) {
		record.copyNormalizedKey(target, offset, numDoubles);
	}

	@Override
	public boolean invertNormalizedKey() {
		return !ascendingComparison;
	}

	@Override
	public TypeComparator<DoubleValueArray> duplicate() {
		return new DoubleValueArrayComparator(ascendingComparison);
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
	public void writeWithKeyNormalization(DoubleValueArray record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public DoubleValueArray readWithKeyDenormalization(DoubleValueArray reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}
}
