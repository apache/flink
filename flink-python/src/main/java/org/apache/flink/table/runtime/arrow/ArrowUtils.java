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
import org.apache.flink.table.runtime.arrow.readers.ArrowFieldReader;
import org.apache.flink.table.runtime.arrow.readers.BigIntFieldReader;
import org.apache.flink.table.runtime.arrow.readers.BooleanFieldReader;
import org.apache.flink.table.runtime.arrow.readers.DateFieldReader;
import org.apache.flink.table.runtime.arrow.readers.DecimalFieldReader;
import org.apache.flink.table.runtime.arrow.readers.DoubleFieldReader;
import org.apache.flink.table.runtime.arrow.readers.FloatFieldReader;
import org.apache.flink.table.runtime.arrow.readers.IntFieldReader;
import org.apache.flink.table.runtime.arrow.readers.RowArrowReader;
import org.apache.flink.table.runtime.arrow.readers.SmallIntFieldReader;
import org.apache.flink.table.runtime.arrow.readers.TimeFieldReader;
import org.apache.flink.table.runtime.arrow.readers.TinyIntFieldReader;
import org.apache.flink.table.runtime.arrow.readers.VarBinaryFieldReader;
import org.apache.flink.table.runtime.arrow.readers.VarCharFieldReader;
import org.apache.flink.table.runtime.arrow.vectors.ArrowBigIntColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowBooleanColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowDateColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowDecimalColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowDoubleColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowFloatColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowIntColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowSmallIntColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowTimeColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowTinyIntColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowVarBinaryColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowVarCharColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.BaseRowArrowReader;
import org.apache.flink.table.runtime.arrow.writers.ArrowFieldWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowBigIntWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowBooleanWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowDateWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowDecimalWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowDoubleWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowFloatWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowIntWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowSmallIntWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowTimeWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowTinyIntWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowVarBinaryWriter;
import org.apache.flink.table.runtime.arrow.writers.BaseRowVarCharWriter;
import org.apache.flink.table.runtime.arrow.writers.BigIntWriter;
import org.apache.flink.table.runtime.arrow.writers.BooleanWriter;
import org.apache.flink.table.runtime.arrow.writers.DateWriter;
import org.apache.flink.table.runtime.arrow.writers.DecimalWriter;
import org.apache.flink.table.runtime.arrow.writers.DoubleWriter;
import org.apache.flink.table.runtime.arrow.writers.FloatWriter;
import org.apache.flink.table.runtime.arrow.writers.IntWriter;
import org.apache.flink.table.runtime.arrow.writers.SmallIntWriter;
import org.apache.flink.table.runtime.arrow.writers.TimeWriter;
import org.apache.flink.table.runtime.arrow.writers.TinyIntWriter;
import org.apache.flink.table.runtime.arrow.writers.VarBinaryWriter;
import org.apache.flink.table.runtime.arrow.writers.VarCharWriter;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.types.Row;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
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
	public static ArrowWriter<Row> createRowArrowWriter(VectorSchemaRoot root, RowType rowType) {
		ArrowFieldWriter<Row>[] fieldWriters = new ArrowFieldWriter[root.getFieldVectors().size()];
		List<FieldVector> vectors = root.getFieldVectors();
		for (int i = 0; i < vectors.size(); i++) {
			FieldVector vector = vectors.get(i);
			vector.allocateNew();
			fieldWriters[i] = createRowArrowFieldWriter(vector, rowType.getTypeAt(i));
		}

		return new ArrowWriter<>(root, fieldWriters);
	}

	private static ArrowFieldWriter<Row> createRowArrowFieldWriter(FieldVector vector, LogicalType fieldType) {
		if (vector instanceof TinyIntVector) {
			return new TinyIntWriter((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return new SmallIntWriter((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return new IntWriter((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return new BigIntWriter((BigIntVector) vector);
		} else if (vector instanceof BitVector) {
			return new BooleanWriter((BitVector) vector);
		} else if (vector instanceof Float4Vector) {
			return new FloatWriter((Float4Vector) vector);
		} else if (vector instanceof Float8Vector) {
			return new DoubleWriter((Float8Vector) vector);
		} else if (vector instanceof VarCharVector) {
			return new VarCharWriter((VarCharVector) vector);
		} else if (vector instanceof VarBinaryVector) {
			return new VarBinaryWriter((VarBinaryVector) vector);
		} else if (vector instanceof DecimalVector) {
			DecimalVector decimalVector = (DecimalVector) vector;
			return new DecimalWriter(decimalVector, getPrecision(decimalVector), decimalVector.getScale());
		} else if (vector instanceof DateDayVector) {
			return new DateWriter((DateDayVector) vector);
		} else if (vector instanceof TimeSecVector || vector instanceof TimeMilliVector ||
			vector instanceof TimeMicroVector || vector instanceof TimeNanoVector) {
			return new TimeWriter(vector);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fieldType));
		}
	}

	/**
	 * Creates an {@link ArrowWriter} for blink planner for the specified {@link VectorSchemaRoot}.
	 */
	public static ArrowWriter<BaseRow> createBaseRowArrowWriter(VectorSchemaRoot root, RowType rowType) {
		ArrowFieldWriter<BaseRow>[] fieldWriters = new ArrowFieldWriter[root.getFieldVectors().size()];
		List<FieldVector> vectors = root.getFieldVectors();
		for (int i = 0; i < vectors.size(); i++) {
			FieldVector vector = vectors.get(i);
			vector.allocateNew();
			fieldWriters[i] = createBaseRowArrowFieldWriter(vector, rowType.getTypeAt(i));
		}

		return new ArrowWriter<>(root, fieldWriters);
	}

	private static ArrowFieldWriter<BaseRow> createBaseRowArrowFieldWriter(FieldVector vector, LogicalType fieldType) {
		if (vector instanceof TinyIntVector) {
			return new BaseRowTinyIntWriter((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return new BaseRowSmallIntWriter((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return new BaseRowIntWriter((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return new BaseRowBigIntWriter((BigIntVector) vector);
		} else if (vector instanceof BitVector) {
			return new BaseRowBooleanWriter((BitVector) vector);
		} else if (vector instanceof Float4Vector) {
			return new BaseRowFloatWriter((Float4Vector) vector);
		} else if (vector instanceof Float8Vector) {
			return new BaseRowDoubleWriter((Float8Vector) vector);
		} else if (vector instanceof VarCharVector) {
			return new BaseRowVarCharWriter((VarCharVector) vector);
		} else if (vector instanceof VarBinaryVector) {
			return new BaseRowVarBinaryWriter((VarBinaryVector) vector);
		} else if (vector instanceof DecimalVector) {
			DecimalVector decimalVector = (DecimalVector) vector;
			return new BaseRowDecimalWriter(decimalVector, getPrecision(decimalVector), decimalVector.getScale());
		} else if (vector instanceof DateDayVector) {
			return new BaseRowDateWriter((DateDayVector) vector);
		}  else if (vector instanceof TimeSecVector || vector instanceof TimeMilliVector ||
			vector instanceof TimeMicroVector || vector instanceof TimeNanoVector) {
			return new BaseRowTimeWriter(vector);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fieldType));
		}
	}

	/**
	 * Creates an {@link ArrowReader} for the specified {@link VectorSchemaRoot}.
	 */
	public static RowArrowReader createRowArrowReader(VectorSchemaRoot root, RowType rowType) {
		List<ArrowFieldReader> fieldReaders = new ArrayList<>();
		List<FieldVector> fieldVectors = root.getFieldVectors();
		for (int i = 0; i < fieldVectors.size(); i++) {
			fieldReaders.add(createRowArrowFieldReader(fieldVectors.get(i), rowType.getTypeAt(i)));
		}

		return new RowArrowReader(fieldReaders.toArray(new ArrowFieldReader[0]));
	}

	private static ArrowFieldReader createRowArrowFieldReader(FieldVector vector, LogicalType fieldType) {
		if (vector instanceof TinyIntVector) {
			return new TinyIntFieldReader((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return new SmallIntFieldReader((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return new IntFieldReader((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return new BigIntFieldReader((BigIntVector) vector);
		} else if (vector instanceof BitVector) {
			return new BooleanFieldReader((BitVector) vector);
		} else if (vector instanceof Float4Vector) {
			return new FloatFieldReader((Float4Vector) vector);
		} else if (vector instanceof Float8Vector) {
			return new DoubleFieldReader((Float8Vector) vector);
		} else if (vector instanceof VarCharVector) {
			return new VarCharFieldReader((VarCharVector) vector);
		} else if (vector instanceof VarBinaryVector) {
			return new VarBinaryFieldReader((VarBinaryVector) vector);
		} else if (vector instanceof DecimalVector) {
			return new DecimalFieldReader((DecimalVector) vector);
		} else if (vector instanceof DateDayVector) {
			return new DateFieldReader((DateDayVector) vector);
		} else if (vector instanceof TimeSecVector || vector instanceof TimeMilliVector ||
			vector instanceof TimeMicroVector || vector instanceof TimeNanoVector) {
			return new TimeFieldReader(vector);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fieldType));
		}
	}

	/**
	 * Creates an {@link ArrowReader} for blink planner for the specified {@link VectorSchemaRoot}.
	 */
	public static BaseRowArrowReader createBaseRowArrowReader(VectorSchemaRoot root, RowType rowType) {
		List<ColumnVector> columnVectors = new ArrayList<>();
		List<FieldVector> fieldVectors = root.getFieldVectors();
		for (int i = 0; i < fieldVectors.size(); i++) {
			columnVectors.add(createColumnVector(fieldVectors.get(i), rowType.getTypeAt(i)));
		}

		return new BaseRowArrowReader(columnVectors.toArray(new ColumnVector[0]));
	}

	private static ColumnVector createColumnVector(FieldVector vector, LogicalType fieldType) {
		if (vector instanceof TinyIntVector) {
			return new ArrowTinyIntColumnVector((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return new ArrowSmallIntColumnVector((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return new ArrowIntColumnVector((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return new ArrowBigIntColumnVector((BigIntVector) vector);
		} else if (vector instanceof BitVector) {
			return new ArrowBooleanColumnVector((BitVector) vector);
		} else if (vector instanceof Float4Vector) {
			return new ArrowFloatColumnVector((Float4Vector) vector);
		} else if (vector instanceof Float8Vector) {
			return new ArrowDoubleColumnVector((Float8Vector) vector);
		} else if (vector instanceof VarCharVector) {
			return new ArrowVarCharColumnVector((VarCharVector) vector);
		} else if (vector instanceof VarBinaryVector) {
			return new ArrowVarBinaryColumnVector((VarBinaryVector) vector);
		} else if (vector instanceof DecimalVector) {
			return new ArrowDecimalColumnVector((DecimalVector) vector);
		} else if (vector instanceof DateDayVector) {
			return new ArrowDateColumnVector((DateDayVector) vector);
		} else if (vector instanceof TimeSecVector || vector instanceof TimeMilliVector ||
			vector instanceof TimeMicroVector || vector instanceof TimeNanoVector) {
			return new ArrowTimeColumnVector(vector);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fieldType));
		}
	}

	private static class LogicalTypeToArrowTypeConverter extends LogicalTypeDefaultVisitor<ArrowType> {

		private static final LogicalTypeToArrowTypeConverter INSTANCE = new LogicalTypeToArrowTypeConverter();

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
		public ArrowType visit(BooleanType booleanType) {
			return ArrowType.Bool.INSTANCE;
		}

		@Override
		public ArrowType visit(FloatType floatType) {
			return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
		}

		@Override
		public ArrowType visit(DoubleType doubleType) {
			return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
		}

		@Override
		public ArrowType visit(VarCharType varCharType) {
			return ArrowType.Utf8.INSTANCE;
		}

		@Override
		public ArrowType visit(VarBinaryType varCharType) {
			return ArrowType.Binary.INSTANCE;
		}

		@Override
		public ArrowType visit(DecimalType decimalType) {
			return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale());
		}

		@Override
		public ArrowType visit(DateType dateType) {
			return new ArrowType.Date(DateUnit.DAY);
		}

		@Override
		public ArrowType visit(TimeType timeType) {
			if (timeType.getPrecision() == 0) {
				return new ArrowType.Time(TimeUnit.SECOND, 32);
			} else if (timeType.getPrecision() >= 1 && timeType.getPrecision() <= 3) {
				return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
			} else if (timeType.getPrecision() >= 4 && timeType.getPrecision() <= 6) {
				return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
			} else {
				return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
			}
		}

		@Override
		protected ArrowType defaultMethod(LogicalType logicalType) {
			if (logicalType instanceof LegacyTypeInformationType) {
				Class<?> typeClass = ((LegacyTypeInformationType) logicalType).getTypeInformation().getTypeClass();
				if (typeClass == BigDecimal.class) {
					// Because we can't get precision and scale from legacy BIG_DEC_TYPE_INFO,
					// we set the precision and scale to default value compatible with python.
					return new ArrowType.Decimal(38, 18);
				}
			}
			throw new UnsupportedOperationException(String.format(
				"Python vectorized UDF doesn't support logical type %s currently.", logicalType.asSummaryString()));
		}
	}

	private static int getPrecision(DecimalVector decimalVector) {
		int precision = -1;
		try {
			java.lang.reflect.Field precisionField = decimalVector.getClass().getDeclaredField("precision");
			precisionField.setAccessible(true);
			precision = (int) precisionField.get(decimalVector);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			// should not happen, ignore
		}
		return precision;
	}
}
