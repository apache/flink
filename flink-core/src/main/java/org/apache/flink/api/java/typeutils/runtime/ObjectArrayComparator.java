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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;


public class ObjectArrayComparator<T,C> extends TypeComparator<T[]> implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private transient T[] reference;

	protected final boolean ascendingComparison;

	private final TypeSerializer<T[]> serializer;

	private TypeComparator<? super Object> comparatorInfo;

	// For use by getComparators
	@SuppressWarnings("rawtypes")
	private final TypeComparator[] comparators = new TypeComparator[] {this};

	public ObjectArrayComparator(boolean ascending, TypeSerializer<T[]> serializer, TypeComparator<? super Object> comparatorInfo) {
		this.ascendingComparison = ascending;
		this.serializer = serializer;
		this.comparatorInfo = comparatorInfo;
	}

	@Override
	public void setReference(T[] reference) {
		this.reference = reference;
	}

	@Override
	public boolean equalToReference(T[] candidate) {
		return compare(this.reference, candidate) == 0;
	}

	@Override
	public int compareToReference(TypeComparator<T[]> referencedComparator) {
		int comp = compare(((ObjectArrayComparator<T,C>) referencedComparator).reference, reference);
		return comp;
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		T[] firstArray = serializer.deserialize(firstSource);
		T[] secondArray = serializer.deserialize(secondSource);

		int comp = compare(firstArray, secondArray);
		return comp;
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		target[index] = record;
		return 1;
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		return comparators;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return false;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 0;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putNormalizedKey(T[] record, MemorySegment target, int offset, int numBytes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeWithKeyNormalization(T[] record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public T[] readWithKeyDenormalization(T[] reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean invertNormalizedKey() {
		return !ascendingComparison;
	}

	@Override
	public int hash(T[] record) {
		return Arrays.hashCode(record);
	}

	private int compareValues(Object first, Object second) {
		/**
		 * uses the chosen comparator ( of primitive or composite type ) & compares the provided objects as input
		 */
		return comparatorInfo.compare(first, second);
	}

	@SuppressWarnings("unchecked")
	private int parseGenericArray(Object firstArray, Object secondArray) {
		int compareResult = 0;

		/**
		 * logic to determine comparison result due to length difference.
		 * the length difference cannot fully determine the result of the comparison. Hence, result added to tempResult.
		 */
		int min = Array.getLength(firstArray);
		int tempResult = 0;
		if (min < Array.getLength(secondArray)) {
			tempResult = ascendingComparison ? -1: 1;
		}
		if (min > Array.getLength(secondArray)) {
			tempResult = ascendingComparison? 1: -1;
			min = Array.getLength(secondArray);
		}

		/**
		 * comparing the actual content of two arrays.
		 */
		for (int i=0;i < min;i++) {
			int val;

			if (!Array.get(firstArray, i).getClass().isArray() && !Array.get(secondArray, i).getClass().isArray()) {
				val = compareValues(Array.get(firstArray, i), Array.get(secondArray, i));
			}
			else {
				val = parseGenericArray(Array.get(firstArray, i), Array.get(secondArray, i));
			}

			if (val != 0) {
				compareResult = val;
				break;
			}
		}

		/**
		 * if the actual comparison cannot distinguish between two arrays, then length differences take preference.
		 */
		if (compareResult == 0) {
			compareResult = tempResult;
		}
		return compareResult;
	}

	@Override
	public int compare(T[] first, T[] second) {
		return parseGenericArray(first, second);
	}

	@Override
	public TypeComparator<T[]> duplicate() {
		ObjectArrayComparator<T,C> dupe = new ObjectArrayComparator<T,C>(ascendingComparison, serializer, comparatorInfo);
		dupe.setReference(this.reference);
		return dupe;
	}
}
