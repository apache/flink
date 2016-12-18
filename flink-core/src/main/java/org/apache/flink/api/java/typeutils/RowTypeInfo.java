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
package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowComparator;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * TypeInformation for {@link Row}
 */
@PublicEvolving
public class RowTypeInfo extends TupleTypeInfoBase<Row> {

	private static final long serialVersionUID = 9158518989896601963L;

	protected final String[] fieldNames;
	/** Temporary variable for directly passing orders to comparators. */
	private boolean[] comparatorOrders = null;

	public RowTypeInfo(TypeInformation<?>... types) {
		super(Row.class, types);

		this.fieldNames = new String[types.length];

		for (int i = 0; i < types.length; i++) {
			fieldNames[i] = "f" + i;
		}
	}

	@Override
	public TypeComparator<Row> createComparator(
		int[] logicalKeyFields,
		boolean[] orders,
		int logicalFieldOffset,
		ExecutionConfig config) {

		comparatorOrders = orders;
		TypeComparator<Row> comparator = super.createComparator(
			logicalKeyFields,
			orders,
			logicalFieldOffset,
			config);
		comparatorOrders = null;
		return comparator;
	}

	@Override
	protected TypeComparatorBuilder<Row> createTypeComparatorBuilder() {
		if (comparatorOrders == null) {
			throw new IllegalStateException("Cannot create comparator builder without orders.");
		}
		return new RowTypeComparatorBuilder(comparatorOrders);
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public int getFieldIndex(String fieldName) {
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public TypeSerializer<Row> createSerializer(ExecutionConfig config) {
		int len = getArity();
		TypeSerializer<?>[] fieldSerializers = new TypeSerializer[len];
		for (int i = 0; i < len; i++) {
			fieldSerializers[i] = types[i].createSerializer(config);
		}
		return new RowSerializer(fieldSerializers);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof RowTypeInfo;
	}

	@Override
	public int hashCode() {
		return 31 * super.hashCode() + Arrays.hashCode(fieldNames);
	}

	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder("Row");
		if (types.length > 0) {
			bld.append('(').append(fieldNames[0]).append(": ").append(types[0]);

			for (int i = 1; i < types.length; i++) {
				bld.append(", ").append(fieldNames[i]).append(": ").append(types[i]);
			}

			bld.append(')');
		}
		return bld.toString();
	}

	private class RowTypeComparatorBuilder implements TypeComparatorBuilder<Row> {

		private final ArrayList<TypeComparator> fieldComparators = new ArrayList<TypeComparator>();
		private final ArrayList<Integer> logicalKeyFields = new ArrayList<Integer>();
		private final boolean[] comparatorOrders;

		public RowTypeComparatorBuilder(boolean[] comparatorOrders) {
			this.comparatorOrders = comparatorOrders;
		}

		@Override
		public void initializeTypeComparatorBuilder(int size) {
			fieldComparators.ensureCapacity(size);
			logicalKeyFields.ensureCapacity(size);
		}

		@Override
		public void addComparatorField(int fieldId, TypeComparator<?> comparator) {
			fieldComparators.add(comparator);
			logicalKeyFields.add(fieldId);
		}

		@Override
		public TypeComparator<Row> createTypeComparator(ExecutionConfig config) {
			checkState(
				fieldComparators.size() > 0,
				"No field comparators were defined for the TupleTypeComparatorBuilder."
			);

			checkState(
				logicalKeyFields.size() > 0,
				"No key fields were defined for the TupleTypeComparatorBuilder."
			);

			checkState(
				fieldComparators.size() == logicalKeyFields.size(),
				"The number of field comparators and key fields is not equal."
			);

			final int maxKey = Collections.max(logicalKeyFields);

			checkState(
				maxKey >= 0,
				"The maximum key field must be greater or equal than 0."
			);

			TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[maxKey + 1];

			for (int i = 0; i <= maxKey; i++) {
				fieldSerializers[i] = types[i].createSerializer(config);
			}

			int[] keyPositions = new int[logicalKeyFields.size()];
			for (int i = 0; i < keyPositions.length; i++) {
				keyPositions[i] = logicalKeyFields.get(i);
			}

			TypeComparator[] comparators = new TypeComparator[fieldComparators.size()];
			for (int i = 0; i < fieldComparators.size(); i++) {
				comparators[i] = fieldComparators.get(i);
			}

			//noinspection unchecked
			return new RowComparator(
				getArity(),
				keyPositions,
				comparators,
				(TypeSerializer<Object>[]) fieldSerializers,
				comparatorOrders);
		}
	}
}
