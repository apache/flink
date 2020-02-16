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

package org.apache.flink.table.runtime.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.vector.ColumnVector;
import org.apache.flink.table.runtime.arrow.readers.BigIntFieldReader;
import org.apache.flink.table.runtime.arrow.readers.IntFieldReader;
import org.apache.flink.table.runtime.arrow.readers.SmallIntFieldReader;
import org.apache.flink.table.runtime.arrow.readers.TinyIntFieldReader;
import org.apache.flink.table.runtime.arrow.vectors.ArrowBigIntColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowIntColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowSmallIntColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowTinyIntColumnVector;
import org.apache.flink.table.runtime.arrow.writers.BaseRowBigIntWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowIntWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowSmallIntWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowTinyIntWriter;
import org.apache.flink.table.runtime.arrow.writers.BigIntWriter;
import org.apache.flink.table.runtime.arrow.writers.IntWriter;
import org.apache.flink.table.runtime.arrow.writers.SmallIntWriter;
import org.apache.flink.table.runtime.arrow.writers.TinyIntWriter;
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
import org.apache.flink.table.types.logical.RawType;
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
import org.apache.flink.types.Row;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for Arrow.
 */
@Internal
public final class ArrowUtils {

	public static final RootAllocator ROOT_ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

	/**
	 * Returns the Arrow schema of the specified type.
	 */
	public static Schema toArrowSchema(RowType rowType) {
		Collection<Field> fields = rowType.getFields().stream()
			.map(ArrowUtils::toArrowField)
			.collect(Collectors.toCollection(ArrayList::new));
		return new Schema(fields);
	}

	private static Field toArrowField(RowType.RowField rowField) {
		FieldType fieldType = new FieldType(
			rowField.getType().isNullable(),
			rowField.getType().accept(LogicalTypeToArrowTypeConverter.INSTANCE),
			null);
		return new Field(rowField.getName(), fieldType, null);
	}

	/**
	 * Creates an {@link ArrowWriter} for the specified {@link VectorSchemaRoot}.
	 */
	public static ArrowWriter<Row> createArrowWriter(VectorSchemaRoot root) {
		ArrowFieldWriter<Row>[] fieldWriters = new ArrowFieldWriter[root.getFieldVectors().size()];
		List<FieldVector> vectors = root.getFieldVectors();
		for (int i = 0; i < vectors.size(); i++) {
			FieldVector vector = vectors.get(i);
			vector.allocateNew();
			fieldWriters[i] = createArrowFieldWriter(vector);
		}

		return new ArrowWriter<>(root, fieldWriters);
	}

	private static ArrowFieldWriter<Row> createArrowFieldWriter(FieldVector vector) {
		if (vector instanceof TinyIntVector) {
			return new TinyIntWriter((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return new SmallIntWriter((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return new IntWriter((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return new BigIntWriter((BigIntVector) vector);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fromArrowField(vector.getField())));
		}
	}

	/**
	 * Creates an {@link ArrowWriter} for blink planner for the specified {@link VectorSchemaRoot}.
	 */
	public static ArrowWriter<BaseRow> createBlinkArrowWriter(VectorSchemaRoot root) {
		ArrowFieldWriter<BaseRow>[] fieldWriters = new ArrowFieldWriter[root.getFieldVectors().size()];
		List<FieldVector> vectors = root.getFieldVectors();
		for (int i = 0; i < vectors.size(); i++) {
			FieldVector vector = vectors.get(i);
			vector.allocateNew();
			fieldWriters[i] = createBlinkArrowFieldWriter(vector);
		}

		return new ArrowWriter<>(root, fieldWriters);
	}

	private static ArrowFieldWriter<BaseRow> createBlinkArrowFieldWriter(FieldVector vector) {
		if (vector instanceof TinyIntVector) {
			return new BaseRowTinyIntWriter((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return new BaseRowSmallIntWriter((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return new BaseRowIntWriter((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return new BaseRowBigIntWriter((BigIntVector) vector);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fromArrowField(vector.getField())));
		}
	}

	/**
	 * Creates an {@link ArrowReader} for the specified {@link VectorSchemaRoot}.
	 */
	public static RowArrowReader createArrowReader(VectorSchemaRoot root) {
		List<ArrowFieldReader> fieldReaders = new ArrayList<>();
		for (FieldVector vector : root.getFieldVectors()) {
			fieldReaders.add(createFieldReader(vector));
		}

		return new RowArrowReader(fieldReaders.toArray(new ArrowFieldReader[0]));
	}

	private static ArrowFieldReader createFieldReader(FieldVector vector) {
		if (vector instanceof TinyIntVector) {
			return new TinyIntFieldReader((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return new SmallIntFieldReader((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return new IntFieldReader((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return new BigIntFieldReader((BigIntVector) vector);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fromArrowField(vector.getField())));
		}
	}

	/**
	 * Creates an {@link ArrowReader} for blink planner for the specified {@link VectorSchemaRoot}.
	 */
	public static BaseRowArrowReader createBlinkArrowReader(VectorSchemaRoot root) {
		List<ColumnVector> columnVectors = new ArrayList<>();
		for (FieldVector vector : root.getFieldVectors()) {
			columnVectors.add(createColumnVector(vector));
		}

		return new BaseRowArrowReader(columnVectors.toArray(new ColumnVector[0]));
	}

	private static ColumnVector createColumnVector(FieldVector vector) {
		if (vector instanceof TinyIntVector) {
			return new ArrowTinyIntColumnVector((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return new ArrowSmallIntColumnVector((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return new ArrowIntColumnVector((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return new ArrowBigIntColumnVector((BigIntVector) vector);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fromArrowField(vector.getField())));
		}
	}

	public static LogicalType fromArrowField(Field field) {
		if (field.getType() == ArrowType.List.INSTANCE) {
			LogicalType elementType = fromArrowField(field.getChildren().get(0));
			return new ArrayType(field.isNullable(), elementType);
		} else if (field.getType() == ArrowType.Struct.INSTANCE) {
			List<RowType.RowField> fields = field.getChildren().stream().map(child -> {
				LogicalType type = fromArrowField(child);
				return new RowType.RowField(child.getName(), type, null);
			}).collect(Collectors.toList());
			return new RowType(field.isNullable(), fields);
		} else if (field.getType() instanceof ArrowType.Map) {
			Field elementField = field.getChildren().get(0);
			LogicalType keyType = fromArrowField(elementField.getChildren().get(0));
			LogicalType valueType = fromArrowField(elementField.getChildren().get(1));
			return new MapType(field.isNullable(), keyType, valueType);
		} else {
			return fromArrowType(field.isNullable(), field.getType());
		}
	}

	private static LogicalType fromArrowType(boolean isNullable, ArrowType arrowType) {
		if (arrowType instanceof ArrowType.Int && ((ArrowType.Int) arrowType).getIsSigned()) {
			ArrowType.Int intType = (ArrowType.Int) arrowType;
			if (intType.getBitWidth() == 8) {
				return new TinyIntType(isNullable);
			} else if (intType.getBitWidth() == 8 * 2) {
				return new SmallIntType(isNullable);
			} else if (intType.getBitWidth() == 8 * 4) {
				return new IntType(isNullable);
			} else if (intType.getBitWidth() == 8 * 8) {
				return new BigIntType(isNullable);
			}
		}
		throw new UnsupportedOperationException(
			String.format("Unexpected arrow type: %s.", arrowType.toString()));
	}

	private static class LogicalTypeToArrowTypeConverter implements LogicalTypeVisitor<ArrowType> {

		private static final LogicalTypeToArrowTypeConverter INSTANCE = new LogicalTypeToArrowTypeConverter();

		@Override
		public ArrowType visit(CharType charType) {
			return null;
		}

		@Override
		public ArrowType visit(VarCharType varCharType) {
			return null;
		}

		@Override
		public ArrowType visit(BooleanType booleanType) {
			return null;
		}

		@Override
		public ArrowType visit(BinaryType binaryType) {
			return null;
		}

		@Override
		public ArrowType visit(VarBinaryType varBinaryType) {
			return null;
		}

		@Override
		public ArrowType visit(DecimalType decimalType) {
			return null;
		}

		@Override
		public ArrowType visit(TinyIntType tinyIntType) {
			return new ArrowType.Int(8, true);
		}

		@Override
		public ArrowType visit(SmallIntType smallIntType) {
			return new ArrowType.Int(2 * 8, true);
		}

		@Override
		public ArrowType visit(IntType intType) {
			return new ArrowType.Int(4 * 8, true);
		}

		@Override
		public ArrowType visit(BigIntType bigIntType) {
			return new ArrowType.Int(8 * 8, true);
		}

		@Override
		public ArrowType visit(FloatType floatType) {
			return null;
		}

		@Override
		public ArrowType visit(DoubleType doubleType) {
			return null;
		}

		@Override
		public ArrowType visit(DateType dateType) {
			return null;
		}

		@Override
		public ArrowType visit(TimeType timeType) {
			return null;
		}

		@Override
		public ArrowType visit(TimestampType timestampType) {
			return null;
		}

		@Override
		public ArrowType visit(ZonedTimestampType zonedTimestampType) {
			return null;
		}

		@Override
		public ArrowType visit(LocalZonedTimestampType localZonedTimestampType) {
			return null;
		}

		@Override
		public ArrowType visit(YearMonthIntervalType yearMonthIntervalType) {
			return null;
		}

		@Override
		public ArrowType visit(DayTimeIntervalType dayTimeIntervalType) {
			return null;
		}

		@Override
		public ArrowType visit(ArrayType arrayType) {
			return null;
		}

		@Override
		public ArrowType visit(MultisetType multisetType) {
			return null;
		}

		@Override
		public ArrowType visit(MapType mapType) {
			return null;
		}

		@Override
		public ArrowType visit(RowType rowType) {
			return null;
		}

		@Override
		public ArrowType visit(DistinctType distinctType) {
			return null;
		}

		@Override
		public ArrowType visit(StructuredType structuredType) {
			return null;
		}

		@Override
		public ArrowType visit(NullType nullType) {
			return null;
		}

		@Override
		public ArrowType visit(RawType<?> rawType) {
			return null;
		}

		@Override
		public ArrowType visit(SymbolType<?> symbolType) {
			return null;
		}

		@Override
		public ArrowType visit(LogicalType other) {
			return null;
		}
	}
}
