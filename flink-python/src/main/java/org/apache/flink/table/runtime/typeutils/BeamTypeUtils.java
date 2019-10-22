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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.runtime.typeutils.coders.factory.AtomicDataTypeCoderFactory;
import org.apache.flink.table.runtime.typeutils.coders.factory.CollectionDataTypeCoderFactory;
import org.apache.flink.table.runtime.typeutils.coders.factory.FieldsDataTypeCoderFactory;
import org.apache.flink.table.runtime.typeutils.coders.factory.KeyValueDataTypeCoderFactory;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.AnyType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import org.apache.beam.sdk.coders.Coder;

import java.math.BigDecimal;

/**
 * Utilities for converting Flink data types to Beam data types.
 */
@Internal
public final class BeamTypeUtils {

	private static final String EMPTY_STRING = "";

	public static Coder toCoder(DataType dataType) {
		return dataType.accept(DataTypeToCoderConverter.INSTANCE);
	}

	public static Coder toBlinkCoder(DataType dataType) {
		return dataType.accept(DataTypeToBlinkCoderConverter.INSTANCE);
	}

	public static FlinkFnApi.Schema.FieldType toProtoType(LogicalType logicalType) {
		return logicalType.accept(LogicalTypeToProtoTypeConverter.INSTANCE);
	}

	private static class DataTypeToCoderConverter implements DataTypeVisitor<Coder> {

		private static final DataTypeToCoderConverter INSTANCE = new DataTypeToCoderConverter();

		@Override
		public Coder visit(AtomicDataType atomicDataType) {
			return AtomicDataTypeCoderFactory.of(false).findCoder(atomicDataType);
		}

		@Override
		public Coder visit(CollectionDataType collectionDataType) {
			return CollectionDataTypeCoderFactory.of(this, false).findCoder(collectionDataType);
		}

		@Override
		public Coder visit(FieldsDataType fieldsDataType) {
			return FieldsDataTypeCoderFactory.of(this, false).findCoder(fieldsDataType);
		}

		@Override
		public Coder visit(KeyValueDataType keyValueDataType) {
			return KeyValueDataTypeCoderFactory.of(this, false).findCoder(keyValueDataType);
		}
	}

	private static class DataTypeToBlinkCoderConverter implements DataTypeVisitor<Coder> {

		private static final DataTypeToBlinkCoderConverter INSTANCE = new DataTypeToBlinkCoderConverter();

		@Override
		public Coder visit(AtomicDataType atomicDataType) {
			return AtomicDataTypeCoderFactory.of(true).findCoder(atomicDataType);
		}

		@Override
		public Coder visit(CollectionDataType collectionDataType) {
			return CollectionDataTypeCoderFactory.of(this, true).findCoder(collectionDataType);
		}

		@Override
		public Coder visit(KeyValueDataType keyValueDataType) {
			return KeyValueDataTypeCoderFactory.of(this, true).findCoder(keyValueDataType);
		}

		@Override
		public Coder visit(FieldsDataType fieldsDataType) {
			return FieldsDataTypeCoderFactory.of(this, true).findCoder(fieldsDataType);
		}
	}

	private static class LogicalTypeToProtoTypeConverter implements LogicalTypeVisitor<FlinkFnApi.Schema.FieldType> {

		private static final LogicalTypeToProtoTypeConverter INSTANCE = new LogicalTypeToProtoTypeConverter();

		@Override
		public FlinkFnApi.Schema.FieldType visit(CharType charType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.CHAR)
				.setNullable(charType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(VarCharType varCharType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.VARCHAR)
				.setNullable(varCharType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(BooleanType booleanType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.BOOLEAN)
				.setNullable(booleanType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(BinaryType binaryType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.BINARY)
				.setNullable(binaryType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(VarBinaryType varBinaryType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.VARBINARY)
				.setNullable(varBinaryType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(DecimalType decimalType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.DECIMAL)
				.setNullable(decimalType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(TinyIntType tinyIntType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.TINYINT)
				.setNullable(tinyIntType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(SmallIntType smallIntType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.SMALLINT)
				.setNullable(smallIntType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(IntType intType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.INT)
				.setNullable(intType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(BigIntType bigIntType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.BIGINT)
				.setNullable(bigIntType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(FloatType floatType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.FLOAT)
				.setNullable(floatType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(DoubleType doubleType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.DOUBLE)
				.setNullable(doubleType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(DateType dateType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.DATE)
				.setNullable(dateType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(TimeType timeType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.TIME)
				.setNullable(timeType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(TimestampType timestampType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.DATETIME)
				.setNullable(timestampType.isNullable())
				.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(ZonedTimestampType zonedTimestampType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(LocalZonedTimestampType localZonedTimestampType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(YearMonthIntervalType yearMonthIntervalType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(DayTimeIntervalType dayTimeIntervalType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(ArrayType arrayType) {
			FlinkFnApi.Schema.FieldType.Builder builder =
				FlinkFnApi.Schema.FieldType.newBuilder()
					.setTypeName(FlinkFnApi.Schema.TypeName.ARRAY)
					.setNullable(arrayType.isNullable());

			FlinkFnApi.Schema.FieldType elementFieldType = arrayType.getElementType().accept(this);
			builder.setCollectionElementType(elementFieldType);
			return builder.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(MultisetType multisetType) {
			FlinkFnApi.Schema.FieldType.Builder builder =
				FlinkFnApi.Schema.FieldType.newBuilder()
					.setTypeName(FlinkFnApi.Schema.TypeName.MULTISET)
					.setNullable(multisetType.isNullable());

			FlinkFnApi.Schema.FieldType elementFieldType = multisetType.getElementType().accept(this);
			builder.setCollectionElementType(elementFieldType);
			return builder.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(MapType mapType) {
			FlinkFnApi.Schema.FieldType.Builder builder =
				FlinkFnApi.Schema.FieldType.newBuilder()
					.setTypeName(FlinkFnApi.Schema.TypeName.MAP)
					.setNullable(mapType.isNullable());

			FlinkFnApi.Schema.MapType.Builder mapBuilder =
				FlinkFnApi.Schema.MapType.newBuilder()
					.setKeyType(mapType.getKeyType().accept(this))
					.setValueType(mapType.getValueType().accept(this));
			builder.setMapType(mapBuilder.build());
			return builder.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(RowType rowType) {
			FlinkFnApi.Schema.FieldType.Builder builder =
				FlinkFnApi.Schema.FieldType.newBuilder()
					.setTypeName(FlinkFnApi.Schema.TypeName.ROW)
					.setNullable(rowType.isNullable());

			FlinkFnApi.Schema.Builder schemaBuilder = FlinkFnApi.Schema.newBuilder();
			for (RowType.RowField field : rowType.getFields()) {
				schemaBuilder.addFields(
					FlinkFnApi.Schema.Field.newBuilder()
						.setName(field.getName())
						.setDescription(field.getDescription().orElse(EMPTY_STRING))
						.setType(field.getType().accept(this))
						.build());
			}
			builder.setRowSchema(schemaBuilder.build());
			return builder.build();
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(DistinctType distinctType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(StructuredType structuredType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(NullType nullType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(AnyType<?> anyType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(SymbolType<?> symbolType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(LogicalType other) {
			if (other instanceof LegacyTypeInformationType) {
				Class<?> typeClass = ((LegacyTypeInformationType) other).getTypeInformation().getTypeClass();
				if (typeClass == BigDecimal.class) {
					return FlinkFnApi.Schema.FieldType.newBuilder()
						.setTypeName(FlinkFnApi.Schema.TypeName.DECIMAL)
						.setNullable(other.isNullable())
						.build();
				}
			}
			return null;
		}
	}
}
