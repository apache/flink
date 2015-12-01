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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
//CHECKSTYLE.OFF: AvoidStarImport - Needed for TupleGenerator
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.runtime.Tuple0Serializer;
//CHECKSTYLE.ON: AvoidStarImport
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.types.Value;

/**
 * A {@link TypeInformation} for the tuple types of the Java API.
 *
 * @param <T> The type of the tuple.
 */
@Public
public final class TupleTypeInfo<T extends Tuple> extends TupleTypeInfoBase<T> {
	
	private static final long serialVersionUID = 1L;

	protected final String[] fieldNames;

	@SuppressWarnings("unchecked")
	@Experimental
	public TupleTypeInfo(TypeInformation<?>... types) {
		this((Class<T>) Tuple.getTupleClass(types.length), types);
	}

	@Experimental
	public TupleTypeInfo(Class<T> tupleType, TypeInformation<?>... types) {
		super(tupleType, types);

		Preconditions.checkArgument(
			types.length <= Tuple.MAX_ARITY,
			"The tuple type exceeds the maximum supported arity.");

		this.fieldNames = new String[types.length];

		for (int i = 0; i < types.length; i++) {
			fieldNames[i] = "f" + i;
		}
	}

	@Override
	@Experimental
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	@Experimental
	public int getFieldIndex(String fieldName) {
		int fieldIndex = Integer.parseInt(fieldName.substring(1));
		if (fieldIndex >= getArity()) {
			return -1;
		}
		return fieldIndex;
	}

	@SuppressWarnings("unchecked")
	@Override
	@Experimental
	public TupleSerializer<T> createSerializer(ExecutionConfig executionConfig) {
		if (getTypeClass() == Tuple0.class) {
			return (TupleSerializer<T>) Tuple0Serializer.INSTANCE;
		}

		TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[getArity()];
		for (int i = 0; i < types.length; i++) {
			fieldSerializers[i] = types[i].createSerializer(executionConfig);
		}
		
		Class<T> tupleClass = getTypeClass();
		
		return new TupleSerializer<T>(tupleClass, fieldSerializers);
	}

	@Override
	protected TypeComparatorBuilder<T> createTypeComparatorBuilder() {
		return new TupleTypeComparatorBuilder();
	}

	private class TupleTypeComparatorBuilder implements TypeComparatorBuilder<T> {

		private final ArrayList<TypeComparator> fieldComparators = new ArrayList<TypeComparator>();
		private final ArrayList<Integer> logicalKeyFields = new ArrayList<Integer>();

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
		public TypeComparator<T> createTypeComparator(ExecutionConfig config) {
			Preconditions.checkState(
				fieldComparators.size() > 0,
				"No field comparators were defined for the TupleTypeComparatorBuilder."
			);

			Preconditions.checkState(
				logicalKeyFields.size() > 0,
				"No key fields were defined for the TupleTypeComparatorBuilder."
			);

			Preconditions.checkState(
				fieldComparators.size() == logicalKeyFields.size(),
				"The number of field comparators and key fields is not equal."
			);

			final int maxKey = Collections.max(logicalKeyFields);

			Preconditions.checkState(
				maxKey >= 0,
				"The maximum key field must be greater or equal than 0."
			);

			TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[maxKey + 1];

			for (int i = 0; i <= maxKey; i++) {
				fieldSerializers[i] = types[i].createSerializer(config);
			}

			return new TupleComparator<T>(
				Ints.toArray(logicalKeyFields),
				fieldComparators.toArray(new TypeComparator[fieldComparators.size()]),
				fieldSerializers
			);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TupleTypeInfo) {
			@SuppressWarnings("unchecked")
			TupleTypeInfo<T> other = (TupleTypeInfo<T>) obj;
			return other.canEqual(this) &&
				super.equals(other) &&
				Arrays.equals(fieldNames, other.fieldNames);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof TupleTypeInfo;
	}
	
	@Override
	public int hashCode() {
		return 31 * super.hashCode() + Arrays.hashCode(fieldNames);
	}
	
	@Override
	public String toString() {
		return "Java " + super.toString();
	}

	// --------------------------------------------------------------------------------------------

	@Experimental
	public static <X extends Tuple> TupleTypeInfo<X> getBasicTupleTypeInfo(Class<?>... basicTypes) {
		if (basicTypes == null || basicTypes.length == 0) {
			throw new IllegalArgumentException();
		}
		
		TypeInformation<?>[] infos = new TypeInformation<?>[basicTypes.length];
		for (int i = 0; i < infos.length; i++) {
			Class<?> type = basicTypes[i];
			if (type == null) {
				throw new IllegalArgumentException("Type at position " + i + " is null.");
			}
			
			TypeInformation<?> info = BasicTypeInfo.getInfoFor(type);
			if (info == null) {
				throw new IllegalArgumentException("Type at position " + i + " is not a basic type.");
			}
			infos[i] = info;
		}

		@SuppressWarnings("unchecked")
		TupleTypeInfo<X> tupleInfo = (TupleTypeInfo<X>) new TupleTypeInfo<Tuple>(infos);
		return tupleInfo;
	}

	@SuppressWarnings("unchecked")
	@Experimental
	public static <X extends Tuple> TupleTypeInfo<X> getBasicAndBasicValueTupleTypeInfo(Class<?>... basicTypes) {
		if (basicTypes == null || basicTypes.length == 0) {
			throw new IllegalArgumentException();
		}

		TypeInformation<?>[] infos = new TypeInformation<?>[basicTypes.length];
		for (int i = 0; i < infos.length; i++) {
			Class<?> type = basicTypes[i];
			if (type == null) {
				throw new IllegalArgumentException("Type at position " + i + " is null.");
			}

			TypeInformation<?> info = BasicTypeInfo.getInfoFor(type);
			if (info == null) {
				try {
					info = ValueTypeInfo.getValueTypeInfo((Class<Value>) type);
					if (!((ValueTypeInfo<?>) info).isBasicValueType()) {
						throw new IllegalArgumentException("Type at position " + i + " is not a basic or value type.");
					}
				} catch (ClassCastException | InvalidTypesException e) {
					throw new IllegalArgumentException("Type at position " + i + " is not a basic or value type.", e);
				}
			}
			infos[i] = info;
		}


		return (TupleTypeInfo<X>) new TupleTypeInfo<>(infos);
	}
}
