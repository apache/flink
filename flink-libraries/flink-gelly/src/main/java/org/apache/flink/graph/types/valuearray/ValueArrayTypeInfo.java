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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link TypeInformation} for the {@link ValueArray} type.
 *
 * @param <T> the {@link Value} type
 */
public class ValueArrayTypeInfo<T> extends TypeInformation<ValueArray<T>> implements AtomicType<ValueArray<T>> {

	private static final long serialVersionUID = 1L;

	public static final ValueArrayTypeInfo<ByteValue> BYTE_VALUE_ARRAY_TYPE_INFO = new ValueArrayTypeInfo<>(ValueTypeInfo.BYTE_VALUE_TYPE_INFO);
	public static final ValueArrayTypeInfo<IntValue> INT_VALUE_ARRAY_TYPE_INFO = new ValueArrayTypeInfo<>(ValueTypeInfo.INT_VALUE_TYPE_INFO);
	public static final ValueArrayTypeInfo<LongValue> LONG_VALUE_ARRAY_TYPE_INFO = new ValueArrayTypeInfo<>(ValueTypeInfo.LONG_VALUE_TYPE_INFO);
	public static final ValueArrayTypeInfo<NullValue> NULL_VALUE_ARRAY_TYPE_INFO = new ValueArrayTypeInfo<>(ValueTypeInfo.NULL_VALUE_TYPE_INFO);
	public static final ValueArrayTypeInfo<StringValue> STRING_VALUE_ARRAY_TYPE_INFO = new ValueArrayTypeInfo<>(ValueTypeInfo.STRING_VALUE_TYPE_INFO);

	private final TypeInformation<T> valueType;

	private final Class<T> type;

	public ValueArrayTypeInfo(TypeInformation<T> valueType) {
		this.valueType = valueType;
		this.type = valueType == null ? null : valueType.getTypeClass();
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<ValueArray<T>> getTypeClass() {
		return (Class<ValueArray<T>>) (Class<?>) ValueArray.class;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public boolean isKeyType() {
		Preconditions.checkNotNull(type, "TypeInformation type class is required");

		return Comparable.class.isAssignableFrom(type);
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeSerializer<ValueArray<T>> createSerializer(ExecutionConfig executionConfig) {
		Preconditions.checkNotNull(type, "TypeInformation type class is required");

		if (ByteValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<ValueArray<T>>) (TypeSerializer<?>) new ByteValueArraySerializer();
		} else if (CharValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<ValueArray<T>>) (TypeSerializer<?>) new CharValueArraySerializer();
		} else if (DoubleValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<ValueArray<T>>) (TypeSerializer<?>) new DoubleValueArraySerializer();
		} else if (FloatValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<ValueArray<T>>) (TypeSerializer<?>) new FloatValueArraySerializer();
		} else if (IntValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<ValueArray<T>>) (TypeSerializer<?>) new IntValueArraySerializer();
		} else if (LongValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<ValueArray<T>>) (TypeSerializer<?>) new LongValueArraySerializer();
		} else if (NullValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<ValueArray<T>>) (TypeSerializer<?>) new NullValueArraySerializer();
		} else if (ShortValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<ValueArray<T>>) (TypeSerializer<?>) new ShortValueArraySerializer();
		} else if (StringValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<ValueArray<T>>) (TypeSerializer<?>) new StringValueArraySerializer();
		} else {
			throw new InvalidTypesException("No ValueArray class exists for " + type);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public TypeComparator<ValueArray<T>> createComparator(boolean sortOrderAscending, ExecutionConfig executionConfig) {
		Preconditions.checkNotNull(type, "TypeInformation type class is required");

		if (ByteValue.class.isAssignableFrom(type)) {
			return (TypeComparator<ValueArray<T>>) (TypeComparator<?>) new ByteValueArrayComparator(sortOrderAscending);
		} else if (CharValue.class.isAssignableFrom(type)) {
			return (TypeComparator<ValueArray<T>>) (TypeComparator<?>) new CharValueArrayComparator(sortOrderAscending);
		} else if (DoubleValue.class.isAssignableFrom(type)) {
			return (TypeComparator<ValueArray<T>>) (TypeComparator<?>) new DoubleValueArrayComparator(sortOrderAscending);
		} else if (FloatValue.class.isAssignableFrom(type)) {
			return (TypeComparator<ValueArray<T>>) (TypeComparator<?>) new FloatValueArrayComparator(sortOrderAscending);
		} else if (IntValue.class.isAssignableFrom(type)) {
			return (TypeComparator<ValueArray<T>>) (TypeComparator<?>) new IntValueArrayComparator(sortOrderAscending);
		} else if (LongValue.class.isAssignableFrom(type)) {
			return (TypeComparator<ValueArray<T>>) (TypeComparator<?>) new LongValueArrayComparator(sortOrderAscending);
		} else if (NullValue.class.isAssignableFrom(type)) {
			return (TypeComparator<ValueArray<T>>) (TypeComparator<?>) new NullValueArrayComparator(sortOrderAscending);
		} else if (ShortValue.class.isAssignableFrom(type)) {
			return (TypeComparator<ValueArray<T>>) (TypeComparator<?>) new ShortValueArrayComparator(sortOrderAscending);
		} else if (StringValue.class.isAssignableFrom(type)) {
			return (TypeComparator<ValueArray<T>>) (TypeComparator<?>) new StringValueArrayComparator(sortOrderAscending);
		} else {
			throw new InvalidTypesException("No ValueArray class exists for " + type);
		}
	}

	@Override
	public Map<String, TypeInformation<?>> getGenericParameters() {
		Map<String, TypeInformation<?>> m = new HashMap<>(1);
		m.put("T", valueType);
		return m;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		Preconditions.checkNotNull(type, "TypeInformation type class is required");

		return type.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ValueArrayTypeInfo) {
			@SuppressWarnings("unchecked")
			ValueArrayTypeInfo<T> valueArrayTypeInfo = (ValueArrayTypeInfo<T>) obj;

			return valueArrayTypeInfo.canEqual(this) &&
				type == valueArrayTypeInfo.type;
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ValueArrayTypeInfo;
	}

	@Override
	public String toString() {
		Preconditions.checkNotNull(type, "TypeInformation type class is required");

		return "ValueArrayType<" + type.getSimpleName() + ">";
	}
}
