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

package org.apache.flink.table.runtime.typeutils.coders.factory;

import org.apache.flink.table.runtime.typeutils.coders.CharTypeCoders;
import org.apache.flink.table.runtime.typeutils.coders.CoderFinder;
import org.apache.flink.table.runtime.typeutils.coders.DateTypeCoders;
import org.apache.flink.table.runtime.typeutils.coders.DecimalCoder;
import org.apache.flink.table.runtime.typeutils.coders.LegacyTypeInformationTypeCoders;
import org.apache.flink.table.runtime.typeutils.coders.TimeTypeCoders;
import org.apache.flink.table.runtime.typeutils.coders.TimestampTypeCoders;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.beam.sdk.coders.BigEndianShortCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.FloatCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;

import java.util.HashMap;
import java.util.Map;

/**
 * The coder factory for {@link AtomicDataType}.
 */
public class AtomicDataTypeCoderFactory implements DataTypeCoderFactory<AtomicDataType> {

	private static final BooleanCoder BOOLEAN_CODER = BooleanCoder.of();

	private static final ByteCoder BYTE_CODER = ByteCoder.of();

	private static final DoubleCoder DOUBLE_CODER = DoubleCoder.of();

	private static final FloatCoder FLOAT_CODER = FloatCoder.of();

	private static final VarIntCoder INT_CODER = VarIntCoder.of();

	private static final VarLongCoder LONG_CODER = VarLongCoder.of();

	private static final BigEndianShortCoder SHORT_CODER = BigEndianShortCoder.of();

	private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

	private Map<Class<? extends LogicalType>, Coder<?>> primitiveLogicalTypeCoderMap = new HashMap<>();

	private Map<Class<? extends LogicalType>, CoderFinder> complicatedLogicalTypeCoderMap = new HashMap<>();

	public static AtomicDataTypeCoderFactory of(boolean isBlinkPlanner) {
		if (isBlinkPlanner) {
			return BLINK_INSTANCE;
		} else {
			return FLINK_INSTANCE;
		}
	}

	private static final AtomicDataTypeCoderFactory FLINK_INSTANCE = new AtomicDataTypeCoderFactory(false);

	private static final AtomicDataTypeCoderFactory BLINK_INSTANCE = new AtomicDataTypeCoderFactory(true);

	private final boolean isBlinkPlanner;

	private AtomicDataTypeCoderFactory(boolean isBlinkPlanner) {
		this.isBlinkPlanner = isBlinkPlanner;
		// Coder for primitive coder.
		primitiveLogicalTypeCoderMap.put(BooleanType.class, BOOLEAN_CODER);
		primitiveLogicalTypeCoderMap.put(TinyIntType.class, BYTE_CODER);
		primitiveLogicalTypeCoderMap.put(SmallIntType.class, SHORT_CODER);
		primitiveLogicalTypeCoderMap.put(IntType.class, INT_CODER);
		primitiveLogicalTypeCoderMap.put(BigIntType.class, LONG_CODER);
		primitiveLogicalTypeCoderMap.put(FloatType.class, FLOAT_CODER);
		primitiveLogicalTypeCoderMap.put(DoubleType.class, DOUBLE_CODER);
		primitiveLogicalTypeCoderMap.put(NullType.class, null);
		primitiveLogicalTypeCoderMap.put(BinaryType.class, BYTE_ARRAY_CODER);
		primitiveLogicalTypeCoderMap.put(VarBinaryType.class, BYTE_ARRAY_CODER);

		// coder for complicated coder which may choose coder according conversion class.
		complicatedLogicalTypeCoderMap.put(CharType.class, CharTypeCoders.of());
		complicatedLogicalTypeCoderMap.put(VarCharType.class, CharTypeCoders.of());
		complicatedLogicalTypeCoderMap.put(DateType.class, DateTypeCoders.of());
		complicatedLogicalTypeCoderMap.put(TimeType.class, TimeTypeCoders.of());
		complicatedLogicalTypeCoderMap.put(TimestampType.class, TimestampTypeCoders.of());
		complicatedLogicalTypeCoderMap.put(LegacyTypeInformationType.class, LegacyTypeInformationTypeCoders.of());
	}

	@Override
	public Coder findCoder(AtomicDataType dataType) {
		LogicalType logicalType = dataType.getLogicalType();
		Class<?> conversionClass = dataType.getConversionClass();
		Coder foundCoder = primitiveLogicalTypeCoderMap.get(logicalType.getClass());
		if (foundCoder != null) {
			return foundCoder;
		}
		if (logicalType instanceof DecimalType) {
			DecimalType decimalType = (DecimalType) logicalType;
			return DecimalCoder.of(decimalType.getPrecision(), decimalType.getScale());
		} else {
			CoderFinder finder = complicatedLogicalTypeCoderMap.getOrDefault(logicalType.getClass(), NotFoundCoder.of());
			return finder.findMatchedCoder(isBlinkPlanner ?
				finder.toInternalType(conversionClass) : conversionClass);
		}
	}

	private static class NotFoundCoder implements CoderFinder {

		public static NotFoundCoder of() {
			return INSTANCE;
		}

		private static final NotFoundCoder INSTANCE = new NotFoundCoder();

		@Override
		public <T> Coder<T> findMatchedCoder(Class<T> conversionClass) {
			throw new IllegalArgumentException("No matched AtomicDataType Coder for " + conversionClass);
		}

		@Override
		public <IN, OUT> Class<OUT> toInternalType(Class<IN> externalType) {
			return null;
		}
	}
}
