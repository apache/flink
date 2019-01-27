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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.types.DataType;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Represents internal structure of an Index, and may carry constant key(s) info during optimization.
 */
public class IndexKey implements Comparable<IndexKey>, Serializable {

	private static final long serialVersionUID = -7842311969684389493L;

	// BitSets are packed into arrays of "words."  Currently a word is
	// a long, which consists of 64 bits, requiring 6 address bits.
	// The choice of word size is determined purely by performance concerns.
	private static final int ADDRESS_BITS_PER_WORD = 6;

	private static final long[] EMPTY_LONGS = new long[0];

	private static final BitSet EMPTY = BitSet.valueOf(EMPTY_LONGS);

	private final List<Integer> definedColumns;

	private final Map<Integer, Tuple2<DataType, Object>> constantsMap = new HashMap<>();

	private final BitSet columnSet;

	private final boolean unique;

	private IndexKey(List<Integer> columns, boolean unique) {
		BitSet columnSet = createBitSet(columns);
		if (columnSet.isEmpty()) {
			throw new IllegalArgumentException("Index key must contains at least one column.");
		}
		this.definedColumns = columns;
		this.columnSet = columnSet;
		this.unique = unique;
	}

	/**
	 * Returns true if the composite columns contains a valid index.
	 */
	public boolean isIndex(int[] columns) {
		List<Integer> cols = Arrays.stream(columns).boxed().collect(Collectors.toList());
		BitSet other = createBitSet(cols);
		for (int i = columnSet.nextSetBit(0); i >= 0; i = columnSet.nextSetBit(i + 1)) {
			if (!other.get(i)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Create an IndexKey with column indices.
	 */
	public static IndexKey of(boolean unique, int... columns) {
		List<Integer> cols = Arrays.stream(columns).boxed().collect(Collectors.toList());
		return new IndexKey(cols, unique);
	}

	public static IndexKey of(boolean unique, Iterable<Integer> columns) {
		List<Integer> cols = StreamSupport.stream(columns.spliterator(), false).collect(Collectors.toList());
		return new IndexKey(cols, unique);
	}

	public void addConstantKey(int columnIndex, Tuple2<DataType, Object> constantValue) {
		if (!definedColumns.contains(columnIndex)) {
			throw new IllegalArgumentException("Given columnIndex:" + columnIndex + " is invalid of Index(" + StringUtils.join(definedColumns, ",") + ")");
		}
		this.constantsMap.put(columnIndex, constantValue);
	}

	public Map<Integer, Tuple2<DataType, Object>> getConstantsMap() {
		return constantsMap;
	}

	/**
	 * Returns column indexes(zero-based) in defined order of Index clause NOT column list.
	 * For example: a table Person has an Index(FirstName, LastName)
	 * <pre>
	 * CREATE TABLE Person (
	 *  ID bigint,
	 *  LastName varchar,
	 *  FirstName varchar,
	 *  Nick varchar,
	 *  Age int,
	 *  INDEX(FirstName, LastName)
	 *  )
	 * </pre>
	 * Then this method will return List(2, 1)
	 */
	public List<Integer> getDefinedColumns() {
		return definedColumns;
	}

	public boolean isUnique() {
		return unique;
	}

	/**
	 * Converts this unique key to an asc-ordered array.
	 * Or uses #getDefinedColumns() to get column indexes in defined order.
	 *
	 * <p>Each entry of the array is the ordinal of a column. The array is
	 * sorted by ascending order.
	 *
	 * @return Array of unique key
	 */
	@Internal
	public int[] toArray() {
		final int[] integers = new int[columnSet.cardinality()];
		int j = 0;
		for (int i = columnSet.nextSetBit(0); i >= 0; i = columnSet.nextSetBit(i + 1)) {
			integers[j++] = i;
		}
		return integers;
	}

	/**
	 * Returns a string representation of this unique key.
	 */
	public String toString() {
		return columnSet.toString();
	}

	private static BitSet createBitSet(Iterable<Integer> bits) {
		int max = -1;
		for (int bit : bits) {
			max = Math.max(bit, max);
		}
		if (max == -1) {
			return EMPTY;
		}
		long[] words = new long[wordIndex(max) + 1];
		for (int bit : bits) {
			int wordIndex = wordIndex(bit);
			words[wordIndex] |= 1L << bit;
		}
		return BitSet.valueOf(words);
	}


	/**
	 * Given a bit index, return word index containing it.
	 */
	private static int wordIndex(int bitIndex) {
		return bitIndex >> ADDRESS_BITS_PER_WORD;
	}

	@Override
	public int compareTo(IndexKey o) {
		// follow ascending order & null last
		int tp = this.unique ? 0 : 1;
		int op = null == o ? 2 : (o.unique ? 0 : 1);
		return tp - op;
	}
}
