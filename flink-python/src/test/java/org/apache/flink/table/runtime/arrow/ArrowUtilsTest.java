/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.arrow;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.vector.ColumnVector;
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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ArrowUtils}.
 */
public class ArrowUtilsTest {

	private static List<Tuple7<String, LogicalType, ArrowType, Class<?>, Class<?>, Class<?>, Class<?>>> testFields;
	private static RowType rowType;
	private static BufferAllocator allocator;

	@BeforeClass
	public static void init() {
		testFields = new ArrayList<>();
		testFields.add(Tuple7.of(
			"f1", new TinyIntType(), new ArrowType.Int(8, true), RowTinyIntWriter.class,
			TinyIntWriter.TinyIntWriterForRow.class, TinyIntFieldReader.class, ArrowTinyIntColumnVector.class));

		testFields.add(Tuple7.of("f2", new SmallIntType(), new ArrowType.Int(8 * 2, true),
			RowSmallIntWriter.class, SmallIntWriter.SmallIntWriterForRow.class, SmallIntFieldReader.class, ArrowSmallIntColumnVector.class));

		testFields.add(Tuple7.of("f3", new IntType(), new ArrowType.Int(8 * 4, true),
			RowIntWriter.class, IntWriter.IntWriterForRow.class, IntFieldReader.class, ArrowIntColumnVector.class));

		testFields.add(Tuple7.of("f4", new BigIntType(), new ArrowType.Int(8 * 8, true),
			RowBigIntWriter.class, BigIntWriter.BigIntWriterForRow.class, BigIntFieldReader.class, ArrowBigIntColumnVector.class));

		testFields.add(Tuple7.of("f5", new BooleanType(), new ArrowType.Bool(),
			RowBooleanWriter.class, BooleanWriter.BooleanWriterForRow.class, BooleanFieldReader.class, ArrowBooleanColumnVector.class));

		testFields.add(Tuple7.of("f6", new FloatType(), new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
			RowFloatWriter.class, FloatWriter.FloatWriterForRow.class, FloatFieldReader.class, ArrowFloatColumnVector.class));

		testFields.add(Tuple7.of("f7", new DoubleType(), new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
			RowDoubleWriter.class, DoubleWriter.DoubleWriterForRow.class, DoubleFieldReader.class, ArrowDoubleColumnVector.class));

		testFields.add(Tuple7.of("f8", new VarCharType(), ArrowType.Utf8.INSTANCE,
			RowVarCharWriter.class, VarCharWriter.VarCharWriterForRow.class, VarCharFieldReader.class, ArrowVarCharColumnVector.class));

		testFields.add(Tuple7.of("f9", new VarBinaryType(), ArrowType.Binary.INSTANCE,
			RowVarBinaryWriter.class, VarBinaryWriter.VarBinaryWriterForRow.class, VarBinaryFieldReader.class, ArrowVarBinaryColumnVector.class));

		testFields.add(Tuple7.of("f10", new DecimalType(10, 3), new ArrowType.Decimal(10, 3),
			RowDecimalWriter.class, DecimalWriter.DecimalWriterForRow.class, DecimalFieldReader.class, ArrowDecimalColumnVector.class));

		testFields.add(Tuple7.of("f11", new DateType(), new ArrowType.Date(DateUnit.DAY),
			RowDateWriter.class, DateWriter.DateWriterForRow.class, DateFieldReader.class, ArrowDateColumnVector.class));

		testFields.add(Tuple7.of("f13", new TimeType(0), new ArrowType.Time(TimeUnit.SECOND, 32),
			RowTimeWriter.class, TimeWriter.TimeWriterForRow.class, TimeFieldReader.class, ArrowTimeColumnVector.class));

		testFields.add(Tuple7.of("f14", new TimeType(2), new ArrowType.Time(TimeUnit.MILLISECOND, 32),
			RowTimeWriter.class, TimeWriter.TimeWriterForRow.class, TimeFieldReader.class, ArrowTimeColumnVector.class));

		testFields.add(Tuple7.of("f15", new TimeType(4), new ArrowType.Time(TimeUnit.MICROSECOND, 64),
			RowTimeWriter.class, TimeWriter.TimeWriterForRow.class, TimeFieldReader.class, ArrowTimeColumnVector.class));

		testFields.add(Tuple7.of("f16", new TimeType(8), new ArrowType.Time(TimeUnit.NANOSECOND, 64),
			RowTimeWriter.class, TimeWriter.TimeWriterForRow.class, TimeFieldReader.class, ArrowTimeColumnVector.class));

		testFields.add(Tuple7.of("f17", new LocalZonedTimestampType(0), new ArrowType.Timestamp(TimeUnit.SECOND, null),
			RowTimestampWriter.class, TimestampWriter.TimestampWriterForRow.class, TimestampFieldReader.class, ArrowTimestampColumnVector.class));

		testFields.add(Tuple7.of("f18", new LocalZonedTimestampType(2), new ArrowType.Timestamp(TimeUnit.MILLISECOND, null),
			RowTimestampWriter.class, TimestampWriter.TimestampWriterForRow.class, TimestampFieldReader.class, ArrowTimestampColumnVector.class));

		testFields.add(Tuple7.of("f19", new LocalZonedTimestampType(4), new ArrowType.Timestamp(TimeUnit.MICROSECOND, null),
			RowTimestampWriter.class, TimestampWriter.TimestampWriterForRow.class, TimestampFieldReader.class, ArrowTimestampColumnVector.class));

		testFields.add(Tuple7.of("f20", new LocalZonedTimestampType(8), new ArrowType.Timestamp(TimeUnit.NANOSECOND, null),
			RowTimestampWriter.class, TimestampWriter.TimestampWriterForRow.class, TimestampFieldReader.class, ArrowTimestampColumnVector.class));

		testFields.add(Tuple7.of("f21", new TimestampType(0), new ArrowType.Timestamp(TimeUnit.SECOND, null),
			RowTimestampWriter.class, TimestampWriter.TimestampWriterForRow.class, TimestampFieldReader.class, ArrowTimestampColumnVector.class));

		testFields.add(Tuple7.of("f22", new TimestampType(2), new ArrowType.Timestamp(TimeUnit.MILLISECOND, null),
			RowTimestampWriter.class, TimestampWriter.TimestampWriterForRow.class, TimestampFieldReader.class, ArrowTimestampColumnVector.class));

		testFields.add(Tuple7.of("f23", new TimestampType(4), new ArrowType.Timestamp(TimeUnit.MICROSECOND, null),
			RowTimestampWriter.class, TimestampWriter.TimestampWriterForRow.class, TimestampFieldReader.class, ArrowTimestampColumnVector.class));

		testFields.add(Tuple7.of("f24", new TimestampType(8), new ArrowType.Timestamp(TimeUnit.NANOSECOND, null),
			RowTimestampWriter.class, TimestampWriter.TimestampWriterForRow.class, TimestampFieldReader.class, ArrowTimestampColumnVector.class));

		testFields.add(Tuple7.of("f25", new ArrayType(new VarCharType()), ArrowType.List.INSTANCE,
			RowArrayWriter.class, ArrayWriter.ArrayWriterForRow.class, ArrayFieldReader.class, ArrowArrayColumnVector.class));

		RowType rowFieldType = new RowType(Arrays.asList(
			new RowType.RowField("a", new IntType()),
			new RowType.RowField("b", new VarCharType()),
			new RowType.RowField("c", new ArrayType(new VarCharType())),
			new RowType.RowField("d", new TimestampType(2)),
			new RowType.RowField("e", new RowType((Arrays.asList(
				new RowType.RowField("e1", new IntType()),
				new RowType.RowField("e2", new VarCharType())))))));
		testFields.add(Tuple7.of("f26", rowFieldType, ArrowType.Struct.INSTANCE,
			RowRowWriter.class, RowWriter.RowWriterForRow.class, RowFieldReader.class, ArrowRowColumnVector.class));

		List<RowType.RowField> rowFields = new ArrayList<>();
		for (Tuple7<String, LogicalType, ArrowType, Class<?>, Class<?>, Class<?>, Class<?>> field : testFields) {
			rowFields.add(new RowType.RowField(field.f0, field.f1));
		}
		rowType = new RowType(rowFields);

		allocator = ArrowUtils.getRootAllocator().newChildAllocator("stdout", 0, Long.MAX_VALUE);
	}

	@Test
	public void testConvertBetweenLogicalTypeAndArrowType() {
		Schema schema = ArrowUtils.toArrowSchema(rowType);

		assertEquals(testFields.size(), schema.getFields().size());
		List<Field> fields = schema.getFields();
		for (int i = 0; i < schema.getFields().size(); i++) {
			// verify convert from RowType to ArrowType
			assertEquals(testFields.get(i).f0, fields.get(i).getName());
			assertEquals(testFields.get(i).f2, fields.get(i).getType());
		}
	}

	@Test
	public void testCreateRowArrowReader() {
		VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
		RowArrowReader reader = ArrowUtils.createRowArrowReader(root, rowType);
		ArrowFieldReader[] fieldReaders = reader.getFieldReaders();
		for (int i = 0; i < fieldReaders.length; i++) {
			assertEquals(testFields.get(i).f5, fieldReaders[i].getClass());
		}
	}

	@Test
	public void testCreateRowDataArrowReader() {
		VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
		RowDataArrowReader reader = ArrowUtils.createRowDataArrowReader(root, rowType);
		ColumnVector[] columnVectors = reader.getColumnVectors();
		for (int i = 0; i < columnVectors.length; i++) {
			assertEquals(testFields.get(i).f6, columnVectors[i].getClass());
		}
	}

	@Test
	public void testCreateRowArrowWriter() {
		VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
		ArrowWriter<Row> writer = ArrowUtils.createRowArrowWriter(root, rowType);
		ArrowFieldWriter<Row>[] fieldWriters = writer.getFieldWriters();
		for (int i = 0; i < fieldWriters.length; i++) {
			assertEquals(testFields.get(i).f3, fieldWriters[i].getClass());
		}
	}

	@Test
	public void testCreateRowDataArrowWriter() {
		VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
		ArrowWriter<RowData> writer = ArrowUtils.createRowDataArrowWriter(root, rowType);
		ArrowFieldWriter<RowData>[] fieldWriters = writer.getFieldWriters();
		for (int i = 0; i < fieldWriters.length; i++) {
			assertEquals(testFields.get(i).f4, fieldWriters[i].getClass());
		}
	}

	@Test
	public void testReadArrowBatches() throws IOException {
		VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
		ArrowWriter<RowData> arrowWriter = ArrowUtils.createRowDataArrowWriter(root, rowType);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(root, null, baos);
		arrowStreamWriter.start();

		List<RowData> testData = Arrays.asList(
			new GenericRowData(rowType.getFieldCount()),
			new GenericRowData(rowType.getFieldCount()),
			new GenericRowData(rowType.getFieldCount()),
			new GenericRowData(rowType.getFieldCount()),
			new GenericRowData(rowType.getFieldCount()));
		int batches = 3;
		List<List<RowData>> subLists = Lists.partition(testData, testData.size() / batches + 1);
		for (List<RowData> subList : subLists) {
			for (RowData value : subList) {
				arrowWriter.write(value);
			}
			arrowWriter.finish();
			arrowStreamWriter.writeBatch();
			arrowWriter.reset();
		}

		assertEquals(batches,
			ArrowUtils.readArrowBatches(Channels.newChannel(new ByteArrayInputStream(baos.toByteArray()))).length);
	}
}
