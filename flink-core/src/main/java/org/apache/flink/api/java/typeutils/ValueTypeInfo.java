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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.CopyableValueComparator;
import org.apache.flink.api.java.typeutils.runtime.CopyableValueSerializer;
import org.apache.flink.api.java.typeutils.runtime.ValueComparator;
import org.apache.flink.api.java.typeutils.runtime.ValueSerializer;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Type information for data types that extend the {@link Value} interface. The value
 * interface allows types to define their custom serialization and deserialization routines.
 *
 * @param <T> The type of the class represented by this type information.
 */
@Public
public class ValueTypeInfo<T extends Value> extends TypeInformation<T> implements AtomicType<T> {

	private static final long serialVersionUID = 1L;

	public static final ValueTypeInfo<BooleanValue> BOOLEAN_VALUE_TYPE_INFO = new ValueTypeInfo<>(BooleanValue.class);
	public static final ValueTypeInfo<ByteValue> BYTE_VALUE_TYPE_INFO = new ValueTypeInfo<>(ByteValue.class);
	public static final ValueTypeInfo<CharValue> CHAR_VALUE_TYPE_INFO = new ValueTypeInfo<>(CharValue.class);
	public static final ValueTypeInfo<DoubleValue> DOUBLE_VALUE_TYPE_INFO = new ValueTypeInfo<>(DoubleValue.class);
	public static final ValueTypeInfo<FloatValue> FLOAT_VALUE_TYPE_INFO = new ValueTypeInfo<>(FloatValue.class);
	public static final ValueTypeInfo<IntValue> INT_VALUE_TYPE_INFO = new ValueTypeInfo<>(IntValue.class);
	public static final ValueTypeInfo<LongValue> LONG_VALUE_TYPE_INFO = new ValueTypeInfo<>(LongValue.class);
	public static final ValueTypeInfo<NullValue> NULL_VALUE_TYPE_INFO = new ValueTypeInfo<>(NullValue.class);
	public static final ValueTypeInfo<ShortValue> SHORT_VALUE_TYPE_INFO = new ValueTypeInfo<>(ShortValue.class);
	public static final ValueTypeInfo<StringValue> STRING_VALUE_TYPE_INFO = new ValueTypeInfo<>(StringValue.class);

	private final Class<T> type;

	@PublicEvolving
	public ValueTypeInfo(Class<T> type) {
		this.type = checkNotNull(type);

		checkArgument(
			Value.class.isAssignableFrom(type) || type.equals(Value.class),
			"ValueTypeInfo can only be used for subclasses of %s", Value.class.getName());
	}
	
	@Override
	@PublicEvolving
	public int getArity() {
		return 1;
	}

	@Override
	@PublicEvolving
	public int getTotalFields() {
		return 1;
	}
	
	@Override
	@PublicEvolving
	public Class<T> getTypeClass() {
		return this.type;
	}

	@Override
	@PublicEvolving
	public boolean isBasicType() {
		return false;
	}

	@PublicEvolving
	public boolean isBasicValueType() {
		return type.equals(StringValue.class) || type.equals(ByteValue.class) || type.equals(ShortValue.class) || type.equals(CharValue.class) ||
				type.equals(DoubleValue.class) || type.equals(FloatValue.class) || type.equals(IntValue.class) || type.equals(LongValue.class) ||
				type.equals(NullValue.class) || type.equals(BooleanValue.class);
	}

	@Override
	@PublicEvolving
	public boolean isTupleType() {
		return false;
	}
	
	@Override
	@PublicEvolving
	public boolean isKeyType() {
		return Comparable.class.isAssignableFrom(type);
	}

	@Override
	@SuppressWarnings("unchecked")
	@PublicEvolving
	public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
		if (CopyableValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<T>) createCopyableValueSerializer(type.asSubclass(CopyableValue.class));
		}
		else {
			return new ValueSerializer<T>(type);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	@PublicEvolving
	public TypeComparator<T> createComparator(boolean sortOrderAscending, ExecutionConfig executionConfig) {
		if (!isKeyType()) {
			throw new RuntimeException("The type " + type.getName() + " is not Comparable.");
		}
		
		if (CopyableValue.class.isAssignableFrom(type)) {
			return (TypeComparator<T>) new CopyableValueComparator(sortOrderAscending, type);
		}
		else {
			return (TypeComparator<T>) new ValueComparator(sortOrderAscending, type);
		}
	}
	
	// utility method to summon the necessary bound
	private static <X extends CopyableValue<X>> CopyableValueSerializer<X> createCopyableValueSerializer(Class<X> clazz) {
		return new CopyableValueSerializer<X>(clazz);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return this.type.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ValueTypeInfo) {
			@SuppressWarnings("unchecked")
			ValueTypeInfo<T> valueTypeInfo = (ValueTypeInfo<T>) obj;

			return valueTypeInfo.canEqual(this) &&
				type == valueTypeInfo.type;
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ValueTypeInfo;
	}
	
	@Override
	public String toString() {
		return "ValueType<" + type.getSimpleName() + ">";
	}
	
	// --------------------------------------------------------------------------------------------

	@PublicEvolving
	static <X extends Value> TypeInformation<X> getValueTypeInfo(Class<X> typeClass) {
		if (Value.class.isAssignableFrom(typeClass) && !typeClass.equals(Value.class)) {
			return new ValueTypeInfo<X>(typeClass);
		}
		else {
			throw new InvalidTypesException("The given class is no subclass of " + Value.class.getName());
		}
	}
}
