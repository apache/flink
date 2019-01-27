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

package org.apache.flink.table.typeutils.ordered;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BigIntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.typeutils.BinaryStringSerializer;
import org.apache.flink.table.util.StateUtil;

import java.math.BigInteger;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Type information for primitive types (int, long, double, byte, ...), String, Date,
 * BigInteger, and BigDecimal.
 */
@Internal
public class OrderedBasicTypeInfo<T> extends TypeInformation<T> {

	private static final long serialVersionUID = -430955220409131770L;

	public static final OrderedBasicTypeInfo<String> ASC_STRING_TYPE_INFO =
		new OrderedBasicTypeInfo<>(String.class, OrderedStringSerializer.ASC_INSTANCE, StringSerializer.INSTANCE);
	public static final OrderedBasicTypeInfo<String> DESC_STRING_TYPE_INFO =
		new OrderedBasicTypeInfo<>(String.class, OrderedStringSerializer.DESC_INSTANCE, StringSerializer.INSTANCE);
	public static final OrderedBasicTypeInfo<Boolean> ASC_BOOLEAN_TYPE_INFO =
		new OrderedBasicTypeInfo<>(Boolean.class, OrderedBooleanSerializer.ASC_INSTANCE);
	public static final OrderedBasicTypeInfo<Boolean> DESC_BOOLEAN_TYPE_INFO =
		new OrderedBasicTypeInfo<>(Boolean.class, OrderedBooleanSerializer.DESC_INSTANCE);
	public static final OrderedBasicTypeInfo<Byte> ASC_BYTE_TYPE_INFO =
		new OrderedIntegerTypeInfo<>(Byte.class, OrderedByteSerializer.ASC_INSTANCE);
	public static final OrderedBasicTypeInfo<Byte> DESC_BYTE_TYPE_INFO =
		new OrderedIntegerTypeInfo<>(Byte.class, OrderedByteSerializer.DESC_INSTANCE);
	public static final OrderedBasicTypeInfo<Short> ASC_SHORT_TYPE_INFO =
		new OrderedIntegerTypeInfo<>(Short.class, OrderedShortSerializer.ASC_INSTANCE);
	public static final OrderedBasicTypeInfo<Short> DESC_SHORT_TYPE_INFO =
		new OrderedIntegerTypeInfo<>(Short.class, OrderedShortSerializer.DESC_INSTANCE);
	public static final OrderedBasicTypeInfo<Integer> ASC_INT_TYPE_INFO =
		new OrderedIntegerTypeInfo<>(Integer.class, OrderedIntSerializer.ASC_INSTANCE);
	public static final OrderedBasicTypeInfo<Integer> DESC_INT_TYPE_INFO =
		new OrderedIntegerTypeInfo<>(Integer.class, OrderedIntSerializer.DESC_INSTANCE);
	public static final OrderedBasicTypeInfo<Long> ASC_LONG_TYPE_INFO =
		new OrderedIntegerTypeInfo<>(Long.class, OrderedLongSerializer.ASC_INSTANCE);
	public static final OrderedBasicTypeInfo<Long> DESC_LONG_TYPE_INFO =
		new OrderedIntegerTypeInfo<>(Long.class, OrderedLongSerializer.DESC_INSTANCE);
	public static final OrderedBasicTypeInfo<Float> ASC_FLOAT_TYPE_INFO =
		new OrderedFractionalTypeInfo<>(Float.class, OrderedFloatSerializer.ASC_INSTANCE);
	public static final OrderedBasicTypeInfo<Float> DESC_FLOAT_TYPE_INFO =
		new OrderedFractionalTypeInfo<>(Float.class, OrderedFloatSerializer.DESC_INSTANCE);
	public static final OrderedBasicTypeInfo<Double> ASC_DOUBLE_TYPE_INFO =
		new OrderedFractionalTypeInfo<>(Double.class, OrderedDoubleSerializer.ASC_INSTANCE);
	public static final OrderedBasicTypeInfo<Double> DESC_DOUBLE_TYPE_INFO =
		new OrderedFractionalTypeInfo<>(Double.class, OrderedDoubleSerializer.DESC_INSTANCE);
	public static final OrderedBasicTypeInfo<Character> ASC_CHAR_TYPE_INFO =
		new OrderedBasicTypeInfo<>(Character.class, OrderedCharSerializer.ASC_INSTANCE);
	public static final OrderedBasicTypeInfo<Character> DESC_CHAR_TYPE_INFO =
		new OrderedBasicTypeInfo<>(Character.class, OrderedCharSerializer.DESC_INSTANCE);
	public static final OrderedBasicTypeInfo<BigInteger> ASC_BIG_INT_TYPE_INFO =
		new OrderedBasicTypeInfo<>(BigInteger.class, OrderedBigIntSerializer.ASC_INSTANCE, BigIntSerializer.INSTANCE);
	public static final OrderedBasicTypeInfo<BigInteger> DESC_BIG_INT_TYPE_INFO =
		new OrderedBasicTypeInfo<>(BigInteger.class, OrderedBigIntSerializer.DESC_INSTANCE, BigIntSerializer.INSTANCE);
	public static final OrderedBasicTypeInfo<BinaryString> ASC_BINARY_STRING_TYPE_INFO =
		new OrderedBasicTypeInfo<>(BinaryString.class, OrderedBinaryStringSerializer.ASC_INSTANCE, BinaryStringSerializer.INSTANCE);
	public static final OrderedBasicTypeInfo<BinaryString> DESC_BINARY_STRING_TYPE_INFO =
		new OrderedBasicTypeInfo<>(BinaryString.class, OrderedBinaryStringSerializer.DESC_INSTANCE, BinaryStringSerializer.INSTANCE);

	// --------------------------------------------------------------------------------------------

	private final Class<T> clazz;

	private final TypeSerializer<T> serializer;

	private final TypeSerializer<T> heapSerializer;

	protected OrderedBasicTypeInfo(
			Class<T> clazz,
			TypeSerializer<T> serializer) {
		this.clazz = checkNotNull(clazz);
		this.serializer = checkNotNull(serializer);
		this.heapSerializer = null;
	}

	protected OrderedBasicTypeInfo(
		Class<T> clazz,
		TypeSerializer<T> serializer,
		TypeSerializer<T> heapSerializer) {
		this.clazz = checkNotNull(clazz);
		this.serializer = checkNotNull(serializer);
		this.heapSerializer = checkNotNull(heapSerializer);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean isBasicType() {
		return true;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<T> getTypeClass() {
		return this.clazz;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		if (config.getGlobalJobParameters() != null) {
			Configuration parameters = new Configuration();
			parameters.addAll(config.getGlobalJobParameters().toMap());
			if (parameters.getBoolean(StateUtil.STATE_BACKEND_ON_HEAP, true)) {
				// using heap statebackend
				// use non-ordered serialization here when String/BinaryString/byte[]
				if (heapSerializer != null) {
					return this.heapSerializer;
				}
			}
		}

		return this.serializer;
	}

	// ------------------------------------------W--------------------------------------------------

	@Override
	public int hashCode() {
		return Objects.hash(clazz, serializer);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof OrderedBasicTypeInfo;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof OrderedBasicTypeInfo) {
			@SuppressWarnings("unchecked")
			OrderedBasicTypeInfo<T>
				other = (OrderedBasicTypeInfo<T>) obj;

			return other.canEqual(this) &&
				this.clazz == other.clazz &&
				serializer.equals(other.serializer);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return clazz.getSimpleName();
	}
}
