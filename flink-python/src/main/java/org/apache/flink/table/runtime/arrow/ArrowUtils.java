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
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableEnvImpl;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.sinks.SelectTableSinkSchemaConverter;
import org.apache.flink.table.runtime.arrow.readers.ArrayFieldReader;
import org.apache.flink.table.runtime.arrow.readers.ArrowFieldReader;
import org.apache.flink.table.runtime.arrow.readers.BigIntFieldReader;
import org.apache.flink.table.runtime.arrow.readers.BooleanFieldReader;
import org.apache.flink.table.runtime.arrow.readers.DateFieldReader;
import org.apache.flink.table.runtime.arrow.readers.DecimalFieldReader;
import org.apache.flink.table.runtime.arrow.readers.DoubleFieldReader;
import org.apache.flink.table.runtime.arrow.readers.FloatFieldReader;
import org.apache.flink.table.runtime.arrow.readers.IntFieldReader;
import org.apache.flink.table.runtime.arrow.readers.RowArrowReader;
import org.apache.flink.table.runtime.arrow.readers.RowFieldReader;
import org.apache.flink.table.runtime.arrow.readers.SmallIntFieldReader;
import org.apache.flink.table.runtime.arrow.readers.TimeFieldReader;
import org.apache.flink.table.runtime.arrow.readers.TimestampFieldReader;
import org.apache.flink.table.runtime.arrow.readers.TinyIntFieldReader;
import org.apache.flink.table.runtime.arrow.readers.VarBinaryFieldReader;
import org.apache.flink.table.runtime.arrow.readers.VarCharFieldReader;
import org.apache.flink.table.runtime.arrow.sources.AbstractArrowTableSource;
import org.apache.flink.table.runtime.arrow.sources.ArrowTableSource;
import org.apache.flink.table.runtime.arrow.sources.RowArrowTableSource;
import org.apache.flink.table.runtime.arrow.vectors.ArrowArrayColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowBigIntColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowBooleanColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowDateColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowDecimalColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowDoubleColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowFloatColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowIntColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowRowColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowSmallIntColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowTimeColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowTimestampColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowTinyIntColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowVarBinaryColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.ArrowVarCharColumnVector;
import org.apache.flink.table.runtime.arrow.vectors.RowDataArrowReader;
import org.apache.flink.table.runtime.arrow.writers.ArrayWriter;
import org.apache.flink.table.runtime.arrow.writers.ArrowFieldWriter;
import org.apache.flink.table.runtime.arrow.writers.BigIntWriter;
import org.apache.flink.table.runtime.arrow.writers.BooleanWriter;
import org.apache.flink.table.runtime.arrow.writers.DateWriter;
import org.apache.flink.table.runtime.arrow.writers.DecimalWriter;
import org.apache.flink.table.runtime.arrow.writers.DoubleWriter;
import org.apache.flink.table.runtime.arrow.writers.FloatWriter;
import org.apache.flink.table.runtime.arrow.writers.IntWriter;
import org.apache.flink.table.runtime.arrow.writers.RowArrayWriter;
import org.apache.flink.table.runtime.arrow.writers.RowBigIntWriter;
import org.apache.flink.table.runtime.arrow.writers.RowBooleanWriter;
import org.apache.flink.table.runtime.arrow.writers.RowDateWriter;
import org.apache.flink.table.runtime.arrow.writers.RowDecimalWriter;
import org.apache.flink.table.runtime.arrow.writers.RowDoubleWriter;
import org.apache.flink.table.runtime.arrow.writers.RowFloatWriter;
import org.apache.flink.table.runtime.arrow.writers.RowIntWriter;
import org.apache.flink.table.runtime.arrow.writers.RowRowWriter;
import org.apache.flink.table.runtime.arrow.writers.RowSmallIntWriter;
import org.apache.flink.table.runtime.arrow.writers.RowTimeWriter;
import org.apache.flink.table.runtime.arrow.writers.RowTimestampWriter;
import org.apache.flink.table.runtime.arrow.writers.RowTinyIntWriter;
import org.apache.flink.table.runtime.arrow.writers.RowVarBinaryWriter;
import org.apache.flink.table.runtime.arrow.writers.RowVarCharWriter;
import org.apache.flink.table.runtime.arrow.writers.RowWriter;
import org.apache.flink.table.runtime.arrow.writers.SmallIntWriter;
import org.apache.flink.table.runtime.arrow.writers.TimeWriter;
import org.apache.flink.table.runtime.arrow.writers.TimestampWriter;
import org.apache.flink.table.runtime.arrow.writers.TinyIntWriter;
import org.apache.flink.table.runtime.arrow.writers.VarBinaryWriter;
import org.apache.flink.table.runtime.arrow.writers.VarCharWriter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.types.Row;

import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for Arrow.
 */
@Internal
public final class ArrowUtils {

	private static final Logger LOG = LoggerFactory.getLogger(ArrowUtils.class);

	private static RootAllocator rootAllocator;

	public static synchronized RootAllocator getRootAllocator() {
		if (rootAllocator == null) {
			rootAllocator = new RootAllocator(Long.MAX_VALUE);
		}
		return rootAllocator;
	}

	public static void checkArrowUsable() {
		// Arrow requires the property io.netty.tryReflectionSetAccessible to
		// be set to true for JDK >= 9. Please refer to ARROW-5412 for more details.
		if (System.getProperty("io.netty.tryReflectionSetAccessible") == null) {
			System.setProperty("io.netty.tryReflectionSetAccessible", "true");
		} else if (!io.netty.util.internal.PlatformDependent.hasDirectBufferNoCleanerConstructor()) {
			throw new RuntimeException("Arrow depends on " +
				"DirectByteBuffer.<init>(long, int) which is not available. Please set the " +
				"system property 'io.netty.tryReflectionSetAccessible' to 'true'.");
		}
	}

	/**
	 * Returns the Arrow schema of the specified type.
	 */
	public static Schema toArrowSchema(RowType rowType) {
		Collection<Field> fields = rowType.getFields().stream()
			.map(f -> ArrowUtils.toArrowField(f.getName(), f.getType()))
			.collect(Collectors.toCollection(ArrayList::new));
		return new Schema(fields);
	}

	private static Field toArrowField(String fieldName, LogicalType logicalType) {
		FieldType fieldType = new FieldType(
			logicalType.isNullable(),
			logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE),
			null);
		List<Field> children = null;
		if (logicalType instanceof ArrayType) {
			children = Collections.singletonList(toArrowField(
				"element", ((ArrayType) logicalType).getElementType()));
		} else if (logicalType instanceof RowType) {
			RowType rowType = (RowType) logicalType;
			children = new ArrayList<>(rowType.getFieldCount());
			for (RowType.RowField field : rowType.getFields()) {
				children.add(toArrowField(field.getName(), field.getType()));
			}
		}
		return new Field(fieldName, fieldType, children);
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

	private static ArrowFieldWriter<Row> createRowArrowFieldWriter(ValueVector vector, LogicalType fieldType) {
		if (vector instanceof TinyIntVector) {
			return new RowTinyIntWriter((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return new RowSmallIntWriter((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return new RowIntWriter((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return new RowBigIntWriter((BigIntVector) vector);
		} else if (vector instanceof BitVector) {
			return new RowBooleanWriter((BitVector) vector);
		} else if (vector instanceof Float4Vector) {
			return new RowFloatWriter((Float4Vector) vector);
		} else if (vector instanceof Float8Vector) {
			return new RowDoubleWriter((Float8Vector) vector);
		} else if (vector instanceof VarCharVector) {
			return new RowVarCharWriter((VarCharVector) vector);
		} else if (vector instanceof VarBinaryVector) {
			return new RowVarBinaryWriter((VarBinaryVector) vector);
		} else if (vector instanceof DecimalVector) {
			DecimalVector decimalVector = (DecimalVector) vector;
			return new RowDecimalWriter(decimalVector, getPrecision(decimalVector), decimalVector.getScale());
		} else if (vector instanceof DateDayVector) {
			return new RowDateWriter((DateDayVector) vector);
		} else if (vector instanceof TimeSecVector || vector instanceof TimeMilliVector ||
			vector instanceof TimeMicroVector || vector instanceof TimeNanoVector) {
			return new RowTimeWriter(vector);
		} else if (vector instanceof TimeStampVector && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
			return new RowTimestampWriter(vector);
		} else if (vector instanceof ListVector) {
			ListVector listVector = (ListVector) vector;
			LogicalType elementType = ((ArrayType) fieldType).getElementType();
			return new RowArrayWriter(listVector, createRowArrowFieldWriter(listVector.getDataVector(), elementType));
		} else if (vector instanceof StructVector) {
			RowType rowType = (RowType) fieldType;
			ArrowFieldWriter<Row>[] fieldsWriters = new ArrowFieldWriter[rowType.getFieldCount()];
			for (int i = 0; i < fieldsWriters.length; i++) {
				fieldsWriters[i] = createRowArrowFieldWriter(
					((StructVector) vector).getVectorById(i),
					rowType.getTypeAt(i));
			}
			return new RowRowWriter((StructVector) vector, fieldsWriters);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fieldType));
		}
	}

	/**
	 * Creates an {@link ArrowWriter} for blink planner for the specified {@link VectorSchemaRoot}.
	 */
	public static ArrowWriter<RowData> createRowDataArrowWriter(VectorSchemaRoot root, RowType rowType) {
		ArrowFieldWriter<RowData>[] fieldWriters = new ArrowFieldWriter[root.getFieldVectors().size()];
		List<FieldVector> vectors = root.getFieldVectors();
		for (int i = 0; i < vectors.size(); i++) {
			FieldVector vector = vectors.get(i);
			vector.allocateNew();
			fieldWriters[i] = createArrowFieldWriterForRow(vector, rowType.getTypeAt(i));
		}

		return new ArrowWriter<>(root, fieldWriters);
	}

	private static ArrowFieldWriter<RowData> createArrowFieldWriterForRow(ValueVector vector, LogicalType fieldType) {
		if (vector instanceof TinyIntVector) {
			return TinyIntWriter.forRow((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return SmallIntWriter.forRow((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return IntWriter.forRow((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return BigIntWriter.forRow((BigIntVector) vector);
		} else if (vector instanceof BitVector) {
			return BooleanWriter.forRow((BitVector) vector);
		} else if (vector instanceof Float4Vector) {
			return FloatWriter.forRow((Float4Vector) vector);
		} else if (vector instanceof Float8Vector) {
			return DoubleWriter.forRow((Float8Vector) vector);
		} else if (vector instanceof VarCharVector) {
			return VarCharWriter.forRow((VarCharVector) vector);
		} else if (vector instanceof VarBinaryVector) {
			return VarBinaryWriter.forRow((VarBinaryVector) vector);
		} else if (vector instanceof DecimalVector) {
			DecimalVector decimalVector = (DecimalVector) vector;
			return DecimalWriter.forRow(decimalVector, getPrecision(decimalVector), decimalVector.getScale());
		} else if (vector instanceof DateDayVector) {
			return DateWriter.forRow((DateDayVector) vector);
		} else if (vector instanceof TimeSecVector || vector instanceof TimeMilliVector ||
			vector instanceof TimeMicroVector || vector instanceof TimeNanoVector) {
			return TimeWriter.forRow(vector);
		} else if (vector instanceof TimeStampVector && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
			int precision;
			if (fieldType instanceof LocalZonedTimestampType) {
				precision = ((LocalZonedTimestampType) fieldType).getPrecision();
			} else {
				precision = ((TimestampType) fieldType).getPrecision();
			}
			return TimestampWriter.forRow(vector, precision);
		} else if (vector instanceof ListVector) {
			ListVector listVector = (ListVector) vector;
			LogicalType elementType = ((ArrayType) fieldType).getElementType();
			return ArrayWriter.forRow(listVector, createArrowFieldWriterForArray(listVector.getDataVector(), elementType));
		} else if (vector instanceof StructVector) {
			RowType rowType = (RowType) fieldType;
			ArrowFieldWriter<RowData>[] fieldsWriters = new ArrowFieldWriter[rowType.getFieldCount()];
			for (int i = 0; i < fieldsWriters.length; i++) {
				fieldsWriters[i] = createArrowFieldWriterForRow(
					((StructVector) vector).getVectorById(i),
					rowType.getTypeAt(i));
			}
			return RowWriter.forRow((StructVector) vector, fieldsWriters);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fieldType));
		}
	}

	private static ArrowFieldWriter<ArrayData> createArrowFieldWriterForArray(ValueVector vector, LogicalType fieldType) {
		if (vector instanceof TinyIntVector) {
			return TinyIntWriter.forArray((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return SmallIntWriter.forArray((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return IntWriter.forArray((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return BigIntWriter.forArray((BigIntVector) vector);
		} else if (vector instanceof BitVector) {
			return BooleanWriter.forArray((BitVector) vector);
		} else if (vector instanceof Float4Vector) {
			return FloatWriter.forArray((Float4Vector) vector);
		} else if (vector instanceof Float8Vector) {
			return DoubleWriter.forArray((Float8Vector) vector);
		} else if (vector instanceof VarCharVector) {
			return VarCharWriter.forArray((VarCharVector) vector);
		} else if (vector instanceof VarBinaryVector) {
			return VarBinaryWriter.forArray((VarBinaryVector) vector);
		} else if (vector instanceof DecimalVector) {
			DecimalVector decimalVector = (DecimalVector) vector;
			return DecimalWriter.forArray(decimalVector, getPrecision(decimalVector), decimalVector.getScale());
		} else if (vector instanceof DateDayVector) {
			return DateWriter.forArray((DateDayVector) vector);
		} else if (vector instanceof TimeSecVector || vector instanceof TimeMilliVector ||
			vector instanceof TimeMicroVector || vector instanceof TimeNanoVector) {
			return TimeWriter.forArray(vector);
		} else if (vector instanceof TimeStampVector && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
			int precision;
			if (fieldType instanceof LocalZonedTimestampType) {
				precision = ((LocalZonedTimestampType) fieldType).getPrecision();
			} else {
				precision = ((TimestampType) fieldType).getPrecision();
			}
			return TimestampWriter.forArray(vector, precision);
		} else if (vector instanceof ListVector) {
			ListVector listVector = (ListVector) vector;
			LogicalType elementType = ((ArrayType) fieldType).getElementType();
			return ArrayWriter.forArray(listVector, createArrowFieldWriterForArray(listVector.getDataVector(), elementType));
		} else if (vector instanceof StructVector) {
			RowType rowType = (RowType) fieldType;
			ArrowFieldWriter<RowData>[] fieldsWriters = new ArrowFieldWriter[rowType.getFieldCount()];
			for (int i = 0; i < fieldsWriters.length; i++) {
				fieldsWriters[i] = createArrowFieldWriterForRow(
					((StructVector) vector).getVectorById(i),
					rowType.getTypeAt(i));
			}
			return RowWriter.forArray((StructVector) vector, fieldsWriters);
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

	public static ArrowFieldReader createRowArrowFieldReader(ValueVector vector, LogicalType fieldType) {
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
		} else if (vector instanceof TimeStampVector && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
			return new TimestampFieldReader(vector);
		} else if (vector instanceof ListVector) {
			ListVector listVector = (ListVector) vector;
			LogicalType elementType = ((ArrayType) fieldType).getElementType();
			return new ArrayFieldReader(listVector,
				createRowArrowFieldReader(listVector.getDataVector(), elementType),
				elementType);
		} else if (vector instanceof StructVector) {
			StructVector structVector = (StructVector) vector;
			ArrowFieldReader[] fieldReaders = new ArrowFieldReader[structVector.size()];
			for (int i = 0; i < fieldReaders.length; i++) {
				fieldReaders[i] = createRowArrowFieldReader(structVector.getVectorById(i), ((RowType) fieldType).getTypeAt(i));
			}
			return new RowFieldReader(structVector, fieldReaders);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fieldType));
		}
	}

	/**
	 * Creates an {@link ArrowReader} for blink planner for the specified {@link VectorSchemaRoot}.
	 */
	public static RowDataArrowReader createRowDataArrowReader(VectorSchemaRoot root, RowType rowType) {
		List<ColumnVector> columnVectors = new ArrayList<>();
		List<FieldVector> fieldVectors = root.getFieldVectors();
		for (int i = 0; i < fieldVectors.size(); i++) {
			columnVectors.add(createColumnVector(fieldVectors.get(i), rowType.getTypeAt(i)));
		}

		return new RowDataArrowReader(columnVectors.toArray(new ColumnVector[0]));
	}

	public static ColumnVector createColumnVector(ValueVector vector, LogicalType fieldType) {
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
		} else if (vector instanceof TimeStampVector && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
			return new ArrowTimestampColumnVector(vector);
		} else if (vector instanceof ListVector) {
			ListVector listVector = (ListVector) vector;
			return new ArrowArrayColumnVector(listVector,
				createColumnVector(listVector.getDataVector(), ((ArrayType) fieldType).getElementType()));
		} else if (vector instanceof StructVector) {
			StructVector structVector = (StructVector) vector;
			ColumnVector[] fieldColumns = new ColumnVector[structVector.size()];
			for (int i = 0; i < fieldColumns.length; ++i) {
				fieldColumns[i] = createColumnVector(structVector.getVectorById(i), ((RowType) fieldType).getTypeAt(i));
			}
			return new ArrowRowColumnVector(structVector, fieldColumns);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Unsupported type %s.", fieldType));
		}
	}

	public static AbstractArrowTableSource createArrowTableSource(DataType dataType, String fileName) throws IOException {
		try (FileInputStream fis = new FileInputStream(fileName)) {
			if (RowData.class.isAssignableFrom(dataType.getConversionClass())) {
				return new ArrowTableSource(dataType, readArrowBatches(fis.getChannel()));
			} else {
				return new RowArrowTableSource(dataType, readArrowBatches(fis.getChannel()));
			}
		}
	}

	public static byte[][] readArrowBatches(ReadableByteChannel channel) throws IOException {
		List<byte[]> results = new ArrayList<>();
		byte[] batch;
		while ((batch = readNextBatch(channel)) != null) {
			results.add(batch);
		}
		return results.toArray(new byte[0][]);
	}

	private static byte[] readNextBatch(ReadableByteChannel channel) throws IOException {
		MessageMetadataResult metadata = MessageSerializer.readMessage(new ReadChannel(channel));
		if (metadata == null) {
			return null;
		}

		long bodyLength = metadata.getMessageBodyLength();

		// Only care about RecordBatch messages and skip the other kind of messages
		if (metadata.getMessage().headerType() == MessageHeader.RecordBatch) {
			// Buffer backed output large enough to hold 8-byte length + complete serialized message
			ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos((int) (8 + metadata.getMessageLength() + bodyLength));

			// Write message metadata to ByteBuffer output stream
			MessageSerializer.writeMessageBuffer(
				new WriteChannel(Channels.newChannel(baos)),
				metadata.getMessageLength(),
				metadata.getMessageBuffer());

			baos.close();
			ByteBuffer result = ByteBuffer.wrap(baos.getBuf());
			result.position(baos.getPosition());
			result.limit(result.capacity());
			readFully(channel, result);
			return result.array();
		} else {
			if (bodyLength > 0) {
				// Skip message body if not a RecordBatch
				Channels.newInputStream(channel).skip(bodyLength);
			}

			// Proceed to next message
			return readNextBatch(channel);
		}
	}

	/**
	 * Fills a buffer with data read from the channel.
	 */
	private static void readFully(ReadableByteChannel channel, ByteBuffer dst) throws IOException {
		int expected = dst.remaining();
		while (dst.hasRemaining()) {
			if (channel.read(dst) < 0) {
				throw new EOFException(String.format("Not enough bytes in channel (expected %d).",
					expected));
			}
		}
	}

	/**
	 * Convert Flink table to Pandas DataFrame.
	 */
	public static CustomIterator<byte[]> collectAsPandasDataFrame(Table table, int maxArrowBatchSize) throws Exception {
		checkArrowUsable();
		BufferAllocator allocator = getRootAllocator().newChildAllocator("collectAsPandasDataFrame", 0, Long.MAX_VALUE);
		RowType rowType = (RowType) table.getSchema().toRowDataType().getLogicalType();
		VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(root, null, baos);
		arrowStreamWriter.start();

		ArrowWriter arrowWriter;
		Iterator<Row> results = table.execute().collect();
		Iterator convertedResults;
		if (isBlinkPlanner(table)) {
			arrowWriter = createRowDataArrowWriter(root, rowType);
			convertedResults = new Iterator<RowData>() {
				@Override
				public boolean hasNext() {
					return results.hasNext();
				}

				@Override
				public RowData next() {
					// The SelectTableSink of blink planner will convert the table schema and we
					// need to keep the table schema used here be consistent with the converted table schema
					TableSchema convertedTableSchema =
						SelectTableSinkSchemaConverter.changeDefaultConversionClass(table.getSchema());
					DataFormatConverters.DataFormatConverter converter =
						DataFormatConverters.getConverterForDataType(convertedTableSchema.toRowDataType());
					return (RowData) converter.toInternal(results.next());
				}
			};
		} else {
			arrowWriter = createRowArrowWriter(root, rowType);
			convertedResults = results;
		}

		return new CustomIterator<byte[]>() {
			@Override
			public boolean hasNext() {
				return convertedResults.hasNext();
			}

			@Override
			public byte[] next() {
				try {
					int i = 0;
					while (convertedResults.hasNext() && i < maxArrowBatchSize) {
						i++;
						arrowWriter.write(convertedResults.next());
					}
					arrowWriter.finish();
					arrowStreamWriter.writeBatch();
					return baos.toByteArray();
				} catch (Throwable t) {
					String msg = "Failed to serialize the data of the table";
					LOG.error(msg, t);
					throw new RuntimeException(msg, t);
				} finally {
					arrowWriter.reset();
					baos.reset();

					if (!hasNext()) {
						root.close();
						allocator.close();
					}
				}
			}
		};
	}

	private static boolean isBlinkPlanner(Table table) {
		TableEnvironment tableEnv = ((TableImpl) table).getTableEnvironment();
		if (tableEnv instanceof TableEnvImpl) {
			return false;
		} else if (tableEnv instanceof TableEnvironmentImpl) {
			Planner planner = ((TableEnvironmentImpl) tableEnv).getPlanner();
			return planner instanceof PlannerBase;
		} else {
			throw new RuntimeException(String.format(
				"Could not determine the planner type for table environment class %s", tableEnv.getClass()));
		}
	}

	/**
	 * A custom iterator to bypass the Py4J Java collection as the next method of
	 * py4j.java_collections.JavaIterator will eat all the exceptions thrown in Java
	 * which makes it difficult to debug in case of errors.
	 */
	private interface CustomIterator<T> {
		boolean hasNext();

		T next();
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
		public ArrowType visit(LocalZonedTimestampType localZonedTimestampType) {
			if (localZonedTimestampType.getPrecision() == 0) {
				return new ArrowType.Timestamp(TimeUnit.SECOND, null);
			} else if (localZonedTimestampType.getPrecision() >= 1 && localZonedTimestampType.getPrecision() <= 3) {
				return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
			} else if (localZonedTimestampType.getPrecision() >= 4 && localZonedTimestampType.getPrecision() <= 6) {
				return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
			} else {
				return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
			}
		}

		@Override
		public ArrowType visit(TimestampType timestampType) {
			if (timestampType.getPrecision() == 0) {
				return new ArrowType.Timestamp(TimeUnit.SECOND, null);
			} else if (timestampType.getPrecision() >= 1 && timestampType.getPrecision() <= 3) {
				return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
			} else if (timestampType.getPrecision() >= 4 && timestampType.getPrecision() <= 6) {
				return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
			} else {
				return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
			}
		}

		@Override
		public ArrowType visit(ArrayType arrayType) {
			return ArrowType.List.INSTANCE;
		}

		@Override
		public ArrowType visit(RowType rowType) {
			return ArrowType.Struct.INSTANCE;
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
