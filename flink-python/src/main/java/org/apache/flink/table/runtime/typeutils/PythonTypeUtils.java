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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.runtime.typeutils.serializers.python.BaseRowSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.DateSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.StringSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

/**
 * Utilities for converting Flink logical types, such as convert it to the related
 * TypeSerializer or ProtoType.
 */
@Internal
public final class PythonTypeUtils {

	private static final String EMPTY_STRING = "";

	public static TypeSerializer toFlinkTypeSerializer(LogicalType logicalType) {
		return logicalType.accept(new LogicalTypeToTypeSerializerConverter());
	}

	public static TypeSerializer toBlinkTypeSerializer(LogicalType logicalType) {
		return logicalType.accept(new LogicalTypeToBlinkTypeSerializerConverter());
	}

	public static FlinkFnApi.Schema.FieldType toProtoType(LogicalType logicalType) {
		return logicalType.accept(new LogicalTypeToProtoTypeConverter());
	}

	private static class LogicalTypeToTypeSerializerConverter extends LogicalTypeDefaultVisitor<TypeSerializer> {
		@Override
		public TypeSerializer visit(BooleanType booleanType) {
			return BooleanSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(TinyIntType tinyIntType) {
			return ByteSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(SmallIntType smallIntType) {
			return ShortSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(IntType intType) {
			return IntSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(BigIntType bigIntType) {
			return LongSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(FloatType floatType) {
			return FloatSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(DoubleType doubleType) {
			return DoubleSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(BinaryType binaryType) {
			return BytePrimitiveArraySerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(VarBinaryType varBinaryType) {
			return BytePrimitiveArraySerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(VarCharType varCharType) {
			return StringSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(CharType charType) {
			return StringSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(DateType dateType) {
			return DateSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(RowType rowType) {
			final TypeSerializer[] fieldTypeSerializers = rowType.getFields()
				.stream()
				.map(f -> f.getType().accept(this))
				.toArray(TypeSerializer[]::new);
			return new RowSerializer(fieldTypeSerializers);
		}

		@Override
		protected TypeSerializer defaultMethod(LogicalType logicalType) {
			throw new UnsupportedOperationException(String.format(
				"Python UDF doesn't support logical type %s currently.", logicalType.asSummaryString()));
		}
	}

	private static class LogicalTypeToBlinkTypeSerializerConverter extends LogicalTypeToTypeSerializerConverter {

		@Override
		public TypeSerializer visit(RowType rowType) {
			final TypeSerializer[] fieldTypeSerializers = rowType.getFields()
				.stream()
				.map(f -> f.getType().accept(this))
				.toArray(TypeSerializer[]::new);
			return new BaseRowSerializer(rowType.getChildren().toArray(new LogicalType[0]), fieldTypeSerializers);
		}

		@Override
		public TypeSerializer visit(VarCharType varCharType) {
			return BinaryStringSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(CharType charType) {
			return BinaryStringSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer visit(DateType dateType) {
			return IntSerializer.INSTANCE;
		}
	}

	private static class LogicalTypeToProtoTypeConverter extends LogicalTypeDefaultVisitor<FlinkFnApi.Schema.FieldType> {
		@Override
		public FlinkFnApi.Schema.FieldType visit(BooleanType booleanType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.BOOLEAN)
				.setNullable(booleanType.isNullable())
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
		public FlinkFnApi.Schema.FieldType visit(DateType dateType) {
			return FlinkFnApi.Schema.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.Schema.TypeName.DATE)
				.setNullable(dateType.isNullable())
				.build();
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
		protected FlinkFnApi.Schema.FieldType defaultMethod(LogicalType logicalType) {
			throw new UnsupportedOperationException(String.format(
				"Python UDF doesn't support logical type %s currently.", logicalType.asSummaryString()));
		}
	}
}
