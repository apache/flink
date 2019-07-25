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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataview.MapViewTypeInfo;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.runtime.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

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
 * 2.Deal with BaseRow.
 * 3.Deal with Decimal.
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
			case DECIMAL:
				DecimalType decimalType = (DecimalType) logicalType;
				return clazz == Decimal.class ?
						new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()) :
						new BigDecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
			case CHAR:
			case VARCHAR: // ignore precision
				return clazz == BinaryString.class ?
						BinaryStringTypeInfo.INSTANCE :
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
				if (BaseRow.class.isAssignableFrom(dataType.getConversionClass())) {
					return BaseRowTypeInfo.of((RowType) fromDataTypeToLogicalType(dataType));
				} else if (Row.class == dataType.getConversionClass()) {
					FieldsDataType rowType = (FieldsDataType) dataType;
					RowType logicalRowType = (RowType) logicalType;
					return new RowTypeInfo(
							logicalRowType.getFieldNames().stream()
									.map(name -> rowType.getFieldDataTypes().get(name))
									.map(TypeInfoDataTypeConverter::fromDataTypeToTypeInfo)
									.toArray(TypeInformation[]::new),
							logicalRowType.getFieldNames().toArray(new String[0]));
				} else {
					return TypeConversions.fromDataTypeToLegacyInfo(dataType);
				}
			default:
				return TypeConversions.fromDataTypeToLegacyInfo(dataType);
		}
	}
}
