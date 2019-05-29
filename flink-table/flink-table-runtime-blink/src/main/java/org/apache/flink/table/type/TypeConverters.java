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

package org.apache.flink.table.type;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.typeutils.BinaryArrayTypeInfo;
import org.apache.flink.table.typeutils.BinaryGenericTypeInfo;
import org.apache.flink.table.typeutils.BinaryMapTypeInfo;
import org.apache.flink.table.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.typeutils.DecimalTypeInfo;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Converters of {@link InternalType} and {@link TypeInformation}.
 */
public class TypeConverters {

	private static final Map<TypeInformation, InternalType> TYPE_INFO_TO_INTERNAL_TYPE;
	private static final Map<InternalType, TypeInformation> INTERNAL_TYPE_TO_INTERNAL_TYPE_INFO;
	private static final Map<InternalType, TypeInformation> INTERNAL_TYPE_TO_EXTERNAL_TYPE_INFO;
	static {
		Map<TypeInformation, InternalType> tiToType = new HashMap<>();
		tiToType.put(BasicTypeInfo.STRING_TYPE_INFO, InternalTypes.STRING);
		tiToType.put(BasicTypeInfo.BOOLEAN_TYPE_INFO, InternalTypes.BOOLEAN);
		tiToType.put(BasicTypeInfo.DOUBLE_TYPE_INFO, InternalTypes.DOUBLE);
		tiToType.put(BasicTypeInfo.FLOAT_TYPE_INFO, InternalTypes.FLOAT);
		tiToType.put(BasicTypeInfo.BYTE_TYPE_INFO, InternalTypes.BYTE);
		tiToType.put(BasicTypeInfo.INT_TYPE_INFO, InternalTypes.INT);
		tiToType.put(BasicTypeInfo.LONG_TYPE_INFO, InternalTypes.LONG);
		tiToType.put(BasicTypeInfo.SHORT_TYPE_INFO, InternalTypes.SHORT);
		tiToType.put(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, InternalTypes.BINARY);
		tiToType.put(SqlTimeTypeInfo.DATE, InternalTypes.DATE);
		tiToType.put(SqlTimeTypeInfo.TIMESTAMP, InternalTypes.TIMESTAMP);
		tiToType.put(SqlTimeTypeInfo.TIME, InternalTypes.TIME);
		tiToType.put(TimeIntervalTypeInfo.INTERVAL_MONTHS, InternalTypes.INTERVAL_MONTHS);
		tiToType.put(TimeIntervalTypeInfo.INTERVAL_MILLIS, InternalTypes.INTERVAL_MILLIS);

		// BigDecimal have infinity precision and scale, but we converted it into a limited
		// Decimal(38, 18). If the user's BigDecimal is more precision than this, we will
		// throw Exception to remind user to use GenericType in real data conversion.
		tiToType.put(BasicTypeInfo.BIG_DEC_TYPE_INFO, InternalTypes.SYSTEM_DEFAULT_DECIMAL);

		// Internal type info
		tiToType.put(BinaryStringTypeInfo.INSTANCE, InternalTypes.STRING);

		TYPE_INFO_TO_INTERNAL_TYPE = Collections.unmodifiableMap(tiToType);

		Map<InternalType, TypeInformation> internalTypeToInfo = new HashMap<>();
		internalTypeToInfo.put(InternalTypes.STRING, BinaryStringTypeInfo.INSTANCE);
		internalTypeToInfo.put(InternalTypes.BOOLEAN, BasicTypeInfo.BOOLEAN_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.DOUBLE, BasicTypeInfo.DOUBLE_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.FLOAT, BasicTypeInfo.FLOAT_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.BYTE, BasicTypeInfo.BYTE_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.INT, BasicTypeInfo.INT_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.LONG, BasicTypeInfo.LONG_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.SHORT, BasicTypeInfo.SHORT_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.DATE, BasicTypeInfo.INT_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.TIMESTAMP, BasicTypeInfo.LONG_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.PROCTIME_INDICATOR, BasicTypeInfo.LONG_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.ROWTIME_INDICATOR, BasicTypeInfo.LONG_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.TIME, BasicTypeInfo.INT_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.BINARY, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.INTERVAL_MONTHS, BasicTypeInfo.INT_TYPE_INFO);
		internalTypeToInfo.put(InternalTypes.INTERVAL_MILLIS, BasicTypeInfo.LONG_TYPE_INFO);
		INTERNAL_TYPE_TO_INTERNAL_TYPE_INFO = Collections.unmodifiableMap(internalTypeToInfo);

		Map<InternalType, TypeInformation> itToEti = new HashMap<>();
		itToEti.put(InternalTypes.STRING, BasicTypeInfo.STRING_TYPE_INFO);
		itToEti.put(InternalTypes.BOOLEAN, BasicTypeInfo.BOOLEAN_TYPE_INFO);
		itToEti.put(InternalTypes.DOUBLE, BasicTypeInfo.DOUBLE_TYPE_INFO);
		itToEti.put(InternalTypes.FLOAT, BasicTypeInfo.FLOAT_TYPE_INFO);
		itToEti.put(InternalTypes.BYTE, BasicTypeInfo.BYTE_TYPE_INFO);
		itToEti.put(InternalTypes.INT, BasicTypeInfo.INT_TYPE_INFO);
		itToEti.put(InternalTypes.LONG, BasicTypeInfo.LONG_TYPE_INFO);
		itToEti.put(InternalTypes.SHORT, BasicTypeInfo.SHORT_TYPE_INFO);
		itToEti.put(InternalTypes.DATE, SqlTimeTypeInfo.DATE);
		itToEti.put(InternalTypes.TIMESTAMP, SqlTimeTypeInfo.TIMESTAMP);
		itToEti.put(InternalTypes.PROCTIME_INDICATOR, TimeIndicatorTypeInfo.PROCTIME_INDICATOR);
		itToEti.put(InternalTypes.ROWTIME_INDICATOR, TimeIndicatorTypeInfo.ROWTIME_INDICATOR);
		itToEti.put(InternalTypes.TIME, SqlTimeTypeInfo.TIME);
		itToEti.put(InternalTypes.BINARY, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
		INTERNAL_TYPE_TO_EXTERNAL_TYPE_INFO = Collections.unmodifiableMap(itToEti);
	}

	/**
	 * Create a {@link InternalType} from a {@link TypeInformation}.
	 *
	 * <p>Note: Information may be lost. For example, after Pojo is converted to InternalType,
	 * we no longer know that it is a Pojo and only think it is a Row.
	 *
	 * <p>Eg:
	 * {@link BasicTypeInfo#STRING_TYPE_INFO} => {@link InternalTypes#STRING}.
	 * {@link BasicTypeInfo#BIG_DEC_TYPE_INFO} => {@link DecimalType}.
	 * {@link RowTypeInfo} => {@link RowType}.
	 * {@link PojoTypeInfo} (CompositeType) => {@link RowType}.
	 * {@link TupleTypeInfo} (CompositeType) => {@link RowType}.
	 */
	public static InternalType createInternalTypeFromTypeInfo(TypeInformation typeInfo) {
		if (typeInfo instanceof TimeIndicatorTypeInfo) {
			TimeIndicatorTypeInfo timeIndicatorTypeInfo = (TimeIndicatorTypeInfo) typeInfo;
			if (timeIndicatorTypeInfo.isEventTime()) {
				return InternalTypes.ROWTIME_INDICATOR;
			} else {
				return InternalTypes.PROCTIME_INDICATOR;
			}
		}

		InternalType type = TYPE_INFO_TO_INTERNAL_TYPE.get(typeInfo);
		if (type != null) {
			return type;
		}

		if (typeInfo instanceof CompositeType) {
			CompositeType compositeType = (CompositeType) typeInfo;
			return InternalTypes.createRowType(
					Stream.iterate(0, x -> x + 1).limit(compositeType.getArity())
							.map((Function<Integer, TypeInformation>) compositeType::getTypeAt)
							.map(TypeConverters::createInternalTypeFromTypeInfo)
							.toArray(InternalType[]::new),
					compositeType.getFieldNames()
			);
		} else if (typeInfo instanceof DecimalTypeInfo) {
			DecimalTypeInfo decimalType = (DecimalTypeInfo) typeInfo;
			return InternalTypes.createDecimalType(decimalType.precision(), decimalType.scale());
		} else if (typeInfo instanceof PrimitiveArrayTypeInfo) {
			PrimitiveArrayTypeInfo arrayType = (PrimitiveArrayTypeInfo) typeInfo;
			return InternalTypes.createArrayType(
					createInternalTypeFromTypeInfo(arrayType.getComponentType()));
		} else if (typeInfo instanceof BasicArrayTypeInfo) {
			BasicArrayTypeInfo arrayType = (BasicArrayTypeInfo) typeInfo;
			return InternalTypes.createArrayType(
					createInternalTypeFromTypeInfo(arrayType.getComponentInfo()));
		} else if (typeInfo instanceof ObjectArrayTypeInfo) {
			ObjectArrayTypeInfo arrayType = (ObjectArrayTypeInfo) typeInfo;
			return InternalTypes.createArrayType(
					createInternalTypeFromTypeInfo(arrayType.getComponentInfo()));
		} else if (typeInfo instanceof MultisetTypeInfo) {
			MultisetTypeInfo multisetType = (MultisetTypeInfo) typeInfo;
			return InternalTypes.createMultisetType(
				createInternalTypeFromTypeInfo(multisetType.getElementTypeInfo()));
		} else if (typeInfo instanceof MapTypeInfo) {
			MapTypeInfo mapType = (MapTypeInfo) typeInfo;
			return InternalTypes.createMapType(
					createInternalTypeFromTypeInfo(mapType.getKeyTypeInfo()),
					createInternalTypeFromTypeInfo(mapType.getValueTypeInfo()));
		} else if (typeInfo instanceof BinaryMapTypeInfo) {
			BinaryMapTypeInfo mapType = (BinaryMapTypeInfo) typeInfo;
			return InternalTypes.createMapType(
					mapType.getKeyType(), mapType.getValueType());
		} else if (typeInfo instanceof BinaryArrayTypeInfo) {
			BinaryArrayTypeInfo arrayType = (BinaryArrayTypeInfo) typeInfo;
			return InternalTypes.createArrayType(arrayType.getElementType());
		} else if (typeInfo instanceof BigDecimalTypeInfo) {
			BigDecimalTypeInfo decimalType = (BigDecimalTypeInfo) typeInfo;
			return new DecimalType(decimalType.precision(), decimalType.scale());
		} else {
			return InternalTypes.createGenericType(typeInfo);
		}
	}

	/**
	 * Create a internal {@link TypeInformation} from a {@link InternalType}.
	 *
	 * <p>eg:
	 * {@link InternalTypes#STRING} => {@link BinaryStringTypeInfo}.
	 * {@link RowType} => {@link BaseRowTypeInfo}.
	 */
	public static TypeInformation createInternalTypeInfoFromInternalType(InternalType type) {
		TypeInformation typeInfo = INTERNAL_TYPE_TO_INTERNAL_TYPE_INFO.get(type);
		if (typeInfo != null) {
			return typeInfo;
		}

		if (type instanceof RowType) {
			RowType rowType = (RowType) type;
			return new BaseRowTypeInfo(rowType.getFieldTypes(), rowType.getFieldNames());
		} else if (type instanceof ArrayType) {
			return new BinaryArrayTypeInfo(((ArrayType) type).getElementType());
		} else if (type instanceof MapType) {
			MapType mapType = (MapType) type;
			return new BinaryMapTypeInfo(mapType.getKeyType(), mapType.getValueType());
		} else if (type instanceof DecimalType) {
			DecimalType decimalType = (DecimalType) type;
			return new DecimalTypeInfo(decimalType.precision(), decimalType.scale());
		}  else if (type instanceof GenericType) {
			GenericType<?> genericType = (GenericType<?>) type;
			return new BinaryGenericTypeInfo<>(genericType);
		} else {
			throw new UnsupportedOperationException("Not support yet: " + type);
		}
	}

	/**
	 * Create a external {@link TypeInformation} from a {@link InternalType}.
	 *
	 * <p>eg:
	 * {@link InternalTypes#STRING} => {@link BasicTypeInfo#STRING_TYPE_INFO}.
	 * {@link RowType} => {@link RowTypeInfo}.
	 */
	@SuppressWarnings("unchecked")
	public static TypeInformation createExternalTypeInfoFromInternalType(InternalType type) {
		TypeInformation typeInfo = INTERNAL_TYPE_TO_EXTERNAL_TYPE_INFO.get(type);
		if (typeInfo != null) {
			return typeInfo;
		}

		if (type instanceof RowType) {
			RowType rowType = (RowType) type;
			return new RowTypeInfo(Arrays.stream(rowType.getFieldTypes())
					.map(TypeConverters::createExternalTypeInfoFromInternalType)
					.toArray(TypeInformation[]::new),
					rowType.getFieldNames());
		} else if (type instanceof ArrayType) {
			return ObjectArrayTypeInfo.getInfoFor(
					createExternalTypeInfoFromInternalType(((ArrayType) type).getElementType()));
		} else if (type instanceof MultisetType) {
			MultisetType multisetType = (MultisetType) type;
			return MultisetTypeInfo.getInfoFor(
				createExternalTypeInfoFromInternalType(multisetType.getElementType()));
		} else if (type instanceof MapType) {
			MapType mapType = (MapType) type;
			return new MapTypeInfo(
					createExternalTypeInfoFromInternalType(mapType.getKeyType()),
					createExternalTypeInfoFromInternalType(mapType.getValueType()));
		} else if (type instanceof DecimalType) {
			DecimalType decimalType = (DecimalType) type;
			return new BigDecimalTypeInfo(decimalType.precision(), decimalType.scale());
		} else if (type instanceof GenericType) {
			GenericType genericType = (GenericType) type;
			return genericType.getTypeInfo();
		} else {
			throw new UnsupportedOperationException("Not support yet: " + type);
		}
	}
}
