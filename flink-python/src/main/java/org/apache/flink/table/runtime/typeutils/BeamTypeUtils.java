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
import org.apache.flink.table.runtime.typeutils.coders.BaseRowCoder;
import org.apache.flink.table.runtime.typeutils.coders.RowCoder;
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
import org.apache.beam.sdk.coders.VarLongCoder;

/**
 * Utilities for converting Flink data types to Beam data types.
 */
@Internal
public final class BeamTypeUtils {

	private static final String EMPTY_STRING = "";

	public static Coder toCoder(LogicalType logicalType) {
		return logicalType.accept(new LogicalTypeToCoderConverter());
	}

	public static Coder toBlinkCoder(LogicalType logicalType) {
		return logicalType.accept(new LogicalTypeToBlinkCoderConverter());
	}

	public static FlinkFnApi.Schema.FieldType toProtoType(LogicalType logicalType) {
		return logicalType.accept(new LogicalTypeToProtoTypeConverter());
	}

	private static class LogicalTypeToCoderConverter implements LogicalTypeVisitor<Coder> {

		@Override
		public Coder visit(CharType charType) {
			return null;
		}

		@Override
		public Coder visit(VarCharType varCharType) {
			return null;
		}

		@Override
		public Coder visit(BooleanType booleanType) {
			return null;
		}

		@Override
		public Coder visit(BinaryType binaryType) {
			return null;
		}

		@Override
		public Coder visit(VarBinaryType varBinaryType) {
			return null;
		}

		@Override
		public Coder visit(DecimalType decimalType) {
			return null;
		}

		@Override
		public Coder visit(TinyIntType tinyIntType) {
			return null;
		}

		@Override
		public Coder visit(SmallIntType smallIntType) {
			return null;
		}

		@Override
		public Coder visit(IntType intType) {
			return null;
		}

		@Override
		public Coder visit(BigIntType bigIntType) {
			return VarLongCoder.of();
		}

		@Override
		public Coder visit(FloatType floatType) {
			return null;
		}

		@Override
		public Coder visit(DoubleType doubleType) {
			return null;
		}

		@Override
		public Coder visit(DateType dateType) {
			return null;
		}

		@Override
		public Coder visit(TimeType timeType) {
			return null;
		}

		@Override
		public Coder visit(TimestampType timestampType) {
			return null;
		}

		@Override
		public Coder visit(ZonedTimestampType zonedTimestampType) {
			return null;
		}

		@Override
		public Coder visit(LocalZonedTimestampType localZonedTimestampType) {
			return null;
		}

		@Override
		public Coder visit(YearMonthIntervalType yearMonthIntervalType) {
			return null;
		}

		@Override
		public Coder visit(DayTimeIntervalType dayTimeIntervalType) {
			return null;
		}

		@Override
		public Coder visit(ArrayType arrayType) {
			return null;
		}

		@Override
		public Coder visit(MultisetType multisetType) {
			return null;
		}

		@Override
		public Coder visit(MapType mapType) {
			return null;
		}

		@Override
		public Coder visit(RowType rowType) {
			final Coder[] fieldCoders = rowType.getFields()
				.stream()
				.map(f -> f.getType().accept(this))
				.toArray(Coder[]::new);
			return new RowCoder(fieldCoders);
		}

		@Override
		public Coder visit(DistinctType distinctType) {
			return null;
		}

		@Override
		public Coder visit(StructuredType structuredType) {
			return null;
		}

		@Override
		public Coder visit(NullType nullType) {
			return null;
		}

		@Override
		public Coder visit(AnyType<?> anyType) {
			return null;
		}

		@Override
		public Coder visit(SymbolType<?> symbolType) {
			return null;
		}

		@Override
		public Coder visit(LogicalType other) {
			return null;
		}
	}

	private static class LogicalTypeToBlinkCoderConverter extends LogicalTypeToCoderConverter {

		@Override
		public Coder visit(RowType rowType) {
			final Coder[] fieldCoders = rowType.getFields()
				.stream()
				.map(f -> f.getType().accept(this))
				.toArray(Coder[]::new);
			return new BaseRowCoder(fieldCoders, rowType.getChildren().toArray(new LogicalType[0]));
		}
	}

	private static class LogicalTypeToProtoTypeConverter implements LogicalTypeVisitor<FlinkFnApi.Schema.FieldType> {

		@Override
		public FlinkFnApi.Schema.FieldType visit(CharType charType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(VarCharType varCharType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(BooleanType booleanType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(BinaryType binaryType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(VarBinaryType varBinaryType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(DecimalType decimalType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(TinyIntType tinyIntType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(SmallIntType smallIntType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(IntType intType) {
			return null;
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
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(DoubleType doubleType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(DateType dateType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(TimeType timeType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(TimestampType timestampType) {
			return null;
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
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(MultisetType multisetType) {
			return null;
		}

		@Override
		public FlinkFnApi.Schema.FieldType visit(MapType mapType) {
			return null;
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
			return null;
		}
	}
}
