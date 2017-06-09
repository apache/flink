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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NormalizableKey;

import java.io.IOException;

import static org.apache.flink.graph.types.valuearray.StringValueArray.HIGH_BIT;

/**
 * Specialized comparator for StringValueArray based on CopyableValueComparator.
 *
 * <p>This can be used for grouping keys but not for sorting keys.
 */
@Internal
public class StringValueArrayComparator extends TypeComparator<StringValueArray> {

	private static final long serialVersionUID = 1L;

	private final boolean ascendingComparison;

	private final StringValueArray reference = new StringValueArray();

	private final TypeComparator<?>[] comparators = new TypeComparator[] {this};

	public StringValueArrayComparator(boolean ascending) {
		this.ascendingComparison = ascending;
	}

	@Override
	public int hash(StringValueArray record) {
		return record.hashCode();
	}

	@Override
	public void setReference(StringValueArray toCompare) {
		toCompare.copyTo(reference);
	}

	@Override
	public boolean equalToReference(StringValueArray candidate) {
		return candidate.equals(this.reference);
	}

	@Override
	public int compareToReference(TypeComparator<StringValueArray> referencedComparator) {
		int comp = ((StringValueArrayComparator) referencedComparator).reference.compareTo(reference);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public int compare(StringValueArray first, StringValueArray second) {
		int comp = first.compareTo(second);
		return ascendingComparison ? comp : -comp;
	}

	/**
	 * Read the length of the next serialized {@code StringValue}.
	 *
	 * @param source the input view containing the record
	 * @return the length of the next serialized {@code StringValue}
	 * @throws IOException if the input view raised an exception when reading the length
	 */
	private static int readStringLength(DataInputView source) throws IOException {
		int len = source.readByte() & 0xFF;

		if (len >= HIGH_BIT) {
			int shift = 7;
			int curr;
			len = len & 0x7F;
			while ((curr = source.readByte() & 0xFF) >= HIGH_BIT) {
				len |= (curr & 0x7F) << shift;
				shift += 7;
			}
			len |= curr << shift;
		}

		return len;
	}

	/**
	 * Read the next character from the serialized {@code StringValue}.
	 *
	 * @param source the input view containing the record
	 * @return the next {@code char} of the current serialized {@code StringValue}
	 * @throws IOException if the input view raised an exception when reading the length
	 */
	private static char readStringChar(DataInputView source) throws IOException {
		int c = source.readByte() & 0xFF;

		if (c >= HIGH_BIT) {
			int shift = 7;
			int curr;
			c = c & 0x7F;
			while ((curr = source.readByte() & 0xFF) >= HIGH_BIT) {
				c |= (curr & 0x7F) << shift;
				shift += 7;
			}
			c |= curr << shift;
		}

		return (char) c;
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		int firstCount = firstSource.readInt();
		int secondCount = secondSource.readInt();

		int minCount = Math.min(firstCount, secondCount);
		while (minCount-- > 0) {
			int firstLength = readStringLength(firstSource);
			int secondLength = readStringLength(secondSource);

			int minLength = Math.min(firstLength, secondLength);
			while (minLength-- > 0) {
				char firstChar = readStringChar(firstSource);
				char secondChar = readStringChar(secondSource);

				int cmp = Character.compare(firstChar, secondChar);
				if (cmp != 0) {
					return ascendingComparison ? cmp : -cmp;
				}
			}

			int cmp = Integer.compare(firstLength, secondLength);
			if (cmp != 0) {
				return ascendingComparison ? cmp : -cmp;
			}
		}

		int cmp = Integer.compare(firstCount, secondCount);
		return ascendingComparison ? cmp : -cmp;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return NormalizableKey.class.isAssignableFrom(StringValueArray.class);
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
	public void putNormalizedKey(StringValueArray record, MemorySegment target, int offset, int numBytes) {
		record.copyNormalizedKey(target, offset, numBytes);
	}

	@Override
	public boolean invertNormalizedKey() {
		return !ascendingComparison;
	}

	@Override
	public TypeComparator<StringValueArray> duplicate() {
		return new StringValueArrayComparator(ascendingComparison);
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
	public void writeWithKeyNormalization(StringValueArray record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public StringValueArray readWithKeyDenormalization(StringValueArray reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}
}
