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

package org.apache.flink.table.runtime.types;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.dataview.MapViewTypeInfo;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyInstantTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyLocalDateTimeTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyTimestampTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.TimestampDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.WrapperTypeInfo;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;
import static org.apache.flink.table.runtime.types.PlannerTypeUtils.isPrimitive;

/**
 * Converter between {@link TypeInformation} and {@link DataType}.
 *
 * <p>Don't override {@link TypeConversions#fromLegacyInfoToDataType}. It is a user interface.
 *
 * <p>The different with {@link TypeConversions#fromDataTypeToLegacyInfo}:
 * 1.Deal with VARCHAR and VARBINARY with precision.
 * 2.Deal with RowData.
 * 3.Deal with DecimalData.
 *
 * <p>This class is for:
 * 1.See {@link TableFunctionDefinition#getResultType()}.
 * 2.See {@link AggregateFunctionDefinition#getAccumulatorTypeInfo()}.
 * 3.See {@link MapViewTypeInfo#getKeyType()}.
 */
@Deprecated
public class TypeInfoDataTypeConverter {
	private static final Map<String, TypeInformation<?>> primitiveDataTypeTypeInfoMap = new HashMap<>();

	static {
		addDefaultTypeInfo(boolean.class, Types.BOOLEAN);
		addDefaultTypeInfo(byte.class, Types.BYTE);
		addDefaultTypeInfo(short.class, Types.SHORT);
		addDefaultTypeInfo(int.class, Types.INT);
		addDefaultTypeInfo(long.class, Types.LONG);
		addDefaultTypeInfo(float.class, Types.FLOAT);
		addDefaultTypeInfo(double.class, Types.DOUBLE);
	}

	private static void addDefaultTypeInfo(Class<?> clazz, TypeInformation<?> typeInformation) {
		Preconditions.checkArgument(clazz.isPrimitive());
		primitiveDataTypeTypeInfoMap.put(clazz.getName(), typeInformation);
	}

	public static TypeInformation<?> fromDataTypeToTypeInfo(DataType dataType) {
		Class<?> clazz = dataType.getConversionClass();
		if (clazz.isPrimitive()) {
			final TypeInformation<?> foundTypeInfo = primitiveDataTypeTypeInfoMap.get(clazz.getName());
			if (foundTypeInfo != null) {
				return foundTypeInfo;
			}
		}
		LogicalType logicalType = fromDataTypeToLogicalType(dataType);
		switch (logicalType.getTypeRoot()) {
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				TimestampType timestampType = (TimestampType) logicalType;
				int precision = timestampType.getPrecision();
				if (timestampType.getKind() == TimestampKind.REGULAR) {
					return clazz == TimestampData.class ?
						new TimestampDataTypeInfo(precision) :
						(clazz == LocalDateTime.class ?
							((3 == precision) ?
								Types.LOCAL_DATE_TIME : new LegacyLocalDateTimeTypeInfo(precision)) :
							((3 == precision) ?
								Types.SQL_TIMESTAMP : new LegacyTimestampTypeInfo(precision)));
				} else {
					return TypeConversions.fromDataTypeToLegacyInfo(dataType);
				}
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				LocalZonedTimestampType lzTs = (LocalZonedTimestampType) logicalType;
				int precisionLzTs = lzTs.getPrecision();
				return clazz == TimestampData.class ?
					new TimestampDataTypeInfo(precisionLzTs) :
					(clazz == Instant.class ?
						((3 == precisionLzTs) ? Types.INSTANT : new LegacyInstantTypeInfo(precisionLzTs)) :
						TypeConversions.fromDataTypeToLegacyInfo(dataType));

			case DECIMAL:
				DecimalType decimalType = (DecimalType) logicalType;
				return clazz == DecimalData.class ?
						new DecimalDataTypeInfo(decimalType.getPrecision(), decimalType.getScale()) :
						new BigDecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
			case CHAR:
			case VARCHAR: // ignore precision
				return clazz == StringData.class ?
						StringDataTypeInfo.INSTANCE :
						BasicTypeInfo.STRING_TYPE_INFO;
			case BINARY:
			case VARBINARY: // ignore precision
				return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
			case INTERVAL_YEAR_MONTH:
				return TimeIntervalTypeInfo.INTERVAL_MONTHS;
			case INTERVAL_DAY_TIME:
				return TimeIntervalTypeInfo.INTERVAL_MILLIS;
			case ARRAY:
				if (dataType instanceof CollectionDataType &&
						!isPrimitive(((CollectionDataType) dataType).getElementDataType().getLogicalType())) {
					return ObjectArrayTypeInfo.getInfoFor(
							fromDataTypeToTypeInfo(((CollectionDataType) dataType).getElementDataType()));
				} else {
					return TypeConversions.fromDataTypeToLegacyInfo(dataType);
				}
			case MAP:
				KeyValueDataType mapType = (KeyValueDataType) dataType;
				return new MapTypeInfo(
						fromDataTypeToTypeInfo(mapType.getKeyDataType()),
						fromDataTypeToTypeInfo(mapType.getValueDataType()));
			case MULTISET:
				return MultisetTypeInfo.getInfoFor(
						fromDataTypeToTypeInfo(((CollectionDataType) dataType).getElementDataType()));
			case ROW:
				if (RowData.class.isAssignableFrom(dataType.getConversionClass())) {
					return RowDataTypeInfo.of((RowType) fromDataTypeToLogicalType(dataType));
				} else if (Row.class == dataType.getConversionClass()) {
					RowType logicalRowType = (RowType) logicalType;
					return new RowTypeInfo(
						dataType.getChildren()
							.stream()
							.map(TypeInfoDataTypeConverter::fromDataTypeToTypeInfo)
							.toArray(TypeInformation[]::new),
						logicalRowType.getFieldNames().toArray(new String[0]));
				} else {
					return TypeConversions.fromDataTypeToLegacyInfo(dataType);
				}
			case RAW:
				if (logicalType instanceof RawType) {
					final RawType<?> rawType = (RawType<?>) logicalType;
					return createWrapperTypeInfo(rawType);
				}
				return TypeConversions.fromDataTypeToLegacyInfo(dataType);
			default:
				return TypeConversions.fromDataTypeToLegacyInfo(dataType);
		}
	}

	private static <T> WrapperTypeInfo<T> createWrapperTypeInfo(RawType<T> rawType) {
		return new WrapperTypeInfo<>(rawType.getOriginatingClass(), rawType.getTypeSerializer());
	}
}
