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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
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
			"f1", new TinyIntType(), new ArrowType.Int(8, true), TinyIntWriter.class,
			BaseRowTinyIntWriter.class, TinyIntFieldReader.class, ArrowTinyIntColumnVector.class));
		testFields.add(Tuple7.of("f2", new SmallIntType(), new ArrowType.Int(8 * 2, true),
			SmallIntWriter.class, BaseRowSmallIntWriter.class, SmallIntFieldReader.class, ArrowSmallIntColumnVector.class));
		testFields.add(Tuple7.of("f3", new IntType(), new ArrowType.Int(8 * 4, true),
			IntWriter.class, BaseRowIntWriter.class, IntFieldReader.class, ArrowIntColumnVector.class));
		testFields.add(Tuple7.of("f4", new BigIntType(), new ArrowType.Int(8 * 8, true),
			BigIntWriter.class, BaseRowBigIntWriter.class, BigIntFieldReader.class, ArrowBigIntColumnVector.class));
		testFields.add(Tuple7.of("f5", new BooleanType(), new ArrowType.Bool(),
			BooleanWriter.class, BaseRowBooleanWriter.class, BooleanFieldReader.class, ArrowBooleanColumnVector.class));
		testFields.add(Tuple7.of("f6", new FloatType(), new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
			FloatWriter.class, BaseRowFloatWriter.class, FloatFieldReader.class, ArrowFloatColumnVector.class));
		testFields.add(Tuple7.of("f7", new DoubleType(), new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
			DoubleWriter.class, BaseRowDoubleWriter.class, DoubleFieldReader.class, ArrowDoubleColumnVector.class));
		testFields.add(Tuple7.of("f8", new VarCharType(), ArrowType.Utf8.INSTANCE,
			VarCharWriter.class, BaseRowVarCharWriter.class, VarCharFieldReader.class, ArrowVarCharColumnVector.class));
		testFields.add(Tuple7.of("f9", new VarBinaryType(), ArrowType.Binary.INSTANCE,
			VarBinaryWriter.class, BaseRowVarBinaryWriter.class, VarBinaryFieldReader.class, ArrowVarBinaryColumnVector.class));
		testFields.add(Tuple7.of("f10", new DecimalType(10, 3), new ArrowType.Decimal(10, 3),
			DecimalWriter.class, BaseRowDecimalWriter.class, DecimalFieldReader.class, ArrowDecimalColumnVector.class));
		testFields.add(Tuple7.of("f11", new DateType(), new ArrowType.Date(DateUnit.DAY),
			DateWriter.class, BaseRowDateWriter.class, DateFieldReader.class, ArrowDateColumnVector.class));
		testFields.add(Tuple7.of("f13", new TimeType(0), new ArrowType.Time(TimeUnit.SECOND, 32),
			TimeWriter.class, BaseRowTimeWriter.class, TimeFieldReader.class, ArrowTimeColumnVector.class));
		testFields.add(Tuple7.of("f14", new TimeType(2), new ArrowType.Time(TimeUnit.MILLISECOND, 32),
			TimeWriter.class, BaseRowTimeWriter.class, TimeFieldReader.class, ArrowTimeColumnVector.class));
		testFields.add(Tuple7.of("f15", new TimeType(4), new ArrowType.Time(TimeUnit.MICROSECOND, 64),
			TimeWriter.class, BaseRowTimeWriter.class, TimeFieldReader.class, ArrowTimeColumnVector.class));
		testFields.add(Tuple7.of("f16", new TimeType(8), new ArrowType.Time(TimeUnit.NANOSECOND, 64),
			TimeWriter.class, BaseRowTimeWriter.class, TimeFieldReader.class, ArrowTimeColumnVector.class));

		List<RowType.RowField> rowFields = new ArrayList<>();
		for (Tuple7<String, LogicalType, ArrowType, Class<?>, Class<?>, Class<?>, Class<?>> field : testFields) {
			rowFields.add(new RowType.RowField(field.f0, field.f1));
		}
		rowType = new RowType(rowFields);

		allocator = ArrowUtils.ROOT_ALLOCATOR.newChildAllocator("stdout", 0, Long.MAX_VALUE);
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
	public void testCreateBaseRowArrowReader() {
		VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
		BaseRowArrowReader reader = ArrowUtils.createBaseRowArrowReader(root, rowType);
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
	public void testCreateBaseRowArrowWriter() {
		VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
		ArrowWriter<BaseRow> writer = ArrowUtils.createBaseRowArrowWriter(root, rowType);
		ArrowFieldWriter<BaseRow>[] fieldWriters = writer.getFieldWriters();
		for (int i = 0; i < fieldWriters.length; i++) {
			assertEquals(testFields.get(i).f4, fieldWriters[i].getClass());
		}
	}
}
