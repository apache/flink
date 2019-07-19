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

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.runtime.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TypeInformationAnyType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType;

/**
 * Converter between {@link DataType} and {@link LogicalType}.
 *
 * <p>This class is for:
 * 1.Source, Sink.
 * 2.UDF, UDTF, UDAF.
 * 3.TableEnv.
 */
@Deprecated
public class LogicalTypeDataTypeConverter {

	public static DataType fromLogicalTypeToDataType(LogicalType logicalType) {
		return TypeConversions.fromLogicalToDataType(logicalType);
	}

	/**
	 * It convert {@link LegacyTypeInformationType} to planner types.
	 */
	public static LogicalType fromDataTypeToLogicalType(DataType dataType) {
		return dataType.getLogicalType().accept(new LegacyTypeToPlannerTypeConverter());
	}

	private static class LegacyTypeToPlannerTypeConverter extends LogicalTypeDefaultVisitor<LogicalType> {

		@Override
		protected LogicalType defaultMethod(LogicalType logicalType) {
			if (logicalType instanceof LegacyTypeInformationType) {
				TypeInformation typeInfo = ((LegacyTypeInformationType) logicalType).getTypeInformation();
				if (typeInfo.equals(BasicTypeInfo.BIG_DEC_TYPE_INFO)) {
					// BigDecimal have infinity precision and scale, but we converted it into a limited
					// Decimal(38, 18). If the user's BigDecimal is more precision than this, we will
					// throw Exception to remind user to use GenericType in real data conversion.
					return Decimal.DECIMAL_SYSTEM_DEFAULT;
				} else if (typeInfo.equals(BinaryStringTypeInfo.INSTANCE)) {
					return DataTypes.STRING().getLogicalType();
				} else if (typeInfo instanceof BasicArrayTypeInfo) {
					return new ArrayType(
							fromTypeInfoToLogicalType(((BasicArrayTypeInfo) typeInfo).getComponentInfo()));
				} else if (typeInfo instanceof CompositeType) {
					CompositeType compositeType = (CompositeType) typeInfo;
					return RowType.of(
							Stream.iterate(0, x -> x + 1).limit(compositeType.getArity())
									.map((Function<Integer, TypeInformation>) compositeType::getTypeAt)
									.map(TypeInfoLogicalTypeConverter::fromTypeInfoToLogicalType)
									.toArray(LogicalType[]::new),
							compositeType.getFieldNames()
					);
				} else if (typeInfo instanceof DecimalTypeInfo) {
					DecimalTypeInfo decimalType = (DecimalTypeInfo) typeInfo;
					return new DecimalType(decimalType.precision(), decimalType.scale());
				} else if (typeInfo instanceof BigDecimalTypeInfo) {
					BigDecimalTypeInfo decimalType = (BigDecimalTypeInfo) typeInfo;
					return new DecimalType(decimalType.precision(), decimalType.scale());
				} else {
					return new TypeInformationAnyType<>(typeInfo);
				}
			} else {
				return logicalType;
			}
		}

		@Override
		public LogicalType visit(ArrayType arrayType) {
			return new ArrayType(
					arrayType.isNullable(),
					arrayType.getElementType().accept(this));
		}

		@Override
		public LogicalType visit(MultisetType multisetType) {
			return new MultisetType(
					multisetType.isNullable(),
					multisetType.getElementType().accept(this));
		}

		@Override
		public LogicalType visit(MapType mapType) {
			return new MapType(
					mapType.isNullable(),
					mapType.getKeyType().accept(this),
					mapType.getValueType().accept(this));
		}

		@Override
		public LogicalType visit(RowType rowType) {
			return new RowType(
					rowType.isNullable(),
					rowType.getFields().stream().map(field ->
							new RowType.RowField(
									field.getName(),
									field.getType().accept(LegacyTypeToPlannerTypeConverter.this)))
							.collect(Collectors.toList()));
		}
	}
}
