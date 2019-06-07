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

package org.apache.flink.table.types;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.typeutils.DecimalTypeInfo;
import org.apache.flink.table.typeutils.MapViewTypeInfo;
import org.apache.flink.types.Row;

import static org.apache.flink.table.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;

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

	public static TypeInformation<?> fromDataTypeToTypeInfo(DataType dataType) {
		LogicalType logicalType = fromDataTypeToLogicalType(dataType);
		Class<?> clazz = dataType.getConversionClass();
		switch (logicalType.getTypeRoot()) {
			case DECIMAL:
				DecimalType decimalType = (DecimalType) logicalType;
				return clazz == Decimal.class ?
						new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()) :
						new BigDecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
			case VARCHAR: // ignore precision
				return clazz == BinaryString.class ?
						BinaryStringTypeInfo.INSTANCE :
						BasicTypeInfo.STRING_TYPE_INFO;
			case VARBINARY: // ignore precision
				return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
			case ARRAY:
				if (dataType instanceof CollectionDataType) {
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
