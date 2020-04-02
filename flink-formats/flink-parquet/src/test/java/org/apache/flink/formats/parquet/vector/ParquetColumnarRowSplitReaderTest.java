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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import static org.apache.flink.formats.parquet.utils.ParquetWriterUtil.createTempParquetFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link ParquetColumnarRowSplitReader}.
 */
@RunWith(Parameterized.class)
public class ParquetColumnarRowSplitReaderTest {

	private static final int FIELD_NUMBER = 15;
	private static final LocalDateTime BASE_TIME = LocalDateTime.now();

	private static final MessageType PARQUET_SCHEMA = new MessageType(
			"TOP",
			Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
					.named("f0"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL)
					.named("f1"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
					.named("f2"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
					.named("f3"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
					.named("f4"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
					.named("f5"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
					.named("f6"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL)
					.named("f7"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, Type.Repetition.OPTIONAL)
					.named("f8"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
					.precision(5)
					.as(OriginalType.DECIMAL)
					.named("f9"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
					.precision(15)
					.as(OriginalType.DECIMAL)
					.named("f10"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
					.precision(20)
					.as(OriginalType.DECIMAL)
					.named("f11"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Type.Repetition.OPTIONAL)
					.length(16)
					.precision(5)
					.as(OriginalType.DECIMAL)
					.named("f12"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Type.Repetition.OPTIONAL)
					.length(16)
					.precision(15)
					.as(OriginalType.DECIMAL)
					.named("f13"),
			Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Type.Repetition.OPTIONAL)
					.length(16)
					.precision(20)
					.as(OriginalType.DECIMAL)
					.named("f14")
			);

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private final int rowGroupSize;

	@Parameterized.Parameters(name = "rowGroupSize-{0}")
	public static Collection<Integer> parameters() {
		return Arrays.asList(10, 1000);
	}

	public ParquetColumnarRowSplitReaderTest(int rowGroupSize) {
		this.rowGroupSize = rowGroupSize;
	}

	@Test
	public void testNormalTypesReadWithSplits() throws IOException {
		// prepare parquet file
		int number = 10000;
		List<Row> records = new ArrayList<>(number);
		List<Integer> values = new ArrayList<>(number);
		Random random = new Random();
		for (int i = 0; i < number; i++) {
			Integer v = random.nextInt(number / 2);
			if (v % 10 == 0) {
				values.add(null);
				records.add(new Row(FIELD_NUMBER));
			} else {
				values.add(v);
				records.add(newRow(v));
			}
		}

		testNormalTypes(number, records, values);
	}

	private void testNormalTypes(int number, List<Row> records,
			List<Integer> values) throws IOException {
		Path testPath = createTempParquetFile(
				TEMPORARY_FOLDER.newFolder(), PARQUET_SCHEMA, records, rowGroupSize);

		// test reading and splitting
		long fileLen = testPath.getFileSystem().getFileStatus(testPath).getLen();
		int len1 = readSplitAndCheck(0, testPath, 0, fileLen / 3, values);
		int len2 = readSplitAndCheck(len1, testPath, fileLen / 3, fileLen * 2 / 3, values);
		int len3 = readSplitAndCheck(len1 + len2, testPath, fileLen * 2 / 3, Long.MAX_VALUE, values);
		assertEquals(number, len1 + len2 + len3);
	}

	private int readSplitAndCheck(
			int start,
			Path testPath,
			long splitStart,
			long splitLength,
			List<Integer> values) throws IOException {
		LogicalType[] fieldTypes = new LogicalType[]{
				new VarCharType(VarCharType.MAX_LENGTH),
				new BooleanType(),
				new TinyIntType(),
				new SmallIntType(),
				new IntType(),
				new BigIntType(),
				new FloatType(),
				new DoubleType(),
				new TimestampType(9),
				new DecimalType(5, 0),
				new DecimalType(15, 0),
				new DecimalType(20, 0),
				new DecimalType(5, 0),
				new DecimalType(15, 0),
				new DecimalType(20, 0)};

		ParquetColumnarRowSplitReader reader = new ParquetColumnarRowSplitReader(
				false,
				new Configuration(),
				fieldTypes,
				new String[] {
						"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7",
						"f8", "f9", "f10", "f11", "f12", "f13", "f14"},
				VectorizedColumnBatch::new,
				500,
				new org.apache.hadoop.fs.Path(testPath.getPath()),
				splitStart,
				splitLength);

		int i = start;
		while (!reader.reachedEnd()) {
			ColumnarRow row = reader.nextRecord();
			Integer v = values.get(i);
			if (v == null) {
				assertTrue(row.isNullAt(0));
				assertTrue(row.isNullAt(1));
				assertTrue(row.isNullAt(2));
				assertTrue(row.isNullAt(3));
				assertTrue(row.isNullAt(4));
				assertTrue(row.isNullAt(5));
				assertTrue(row.isNullAt(6));
				assertTrue(row.isNullAt(7));
				assertTrue(row.isNullAt(8));
				assertTrue(row.isNullAt(9));
				assertTrue(row.isNullAt(10));
				assertTrue(row.isNullAt(11));
				assertTrue(row.isNullAt(12));
				assertTrue(row.isNullAt(13));
				assertTrue(row.isNullAt(14));
			} else {
				assertEquals("" + v, row.getString(0).toString());
				assertEquals(v % 2 == 0, row.getBoolean(1));
				assertEquals(v.byteValue(), row.getByte(2));
				assertEquals(v.shortValue(), row.getShort(3));
				assertEquals(v.intValue(), row.getInt(4));
				assertEquals(v.longValue(), row.getLong(5));
				assertEquals(v.floatValue(), row.getFloat(6), 0);
				assertEquals(v.doubleValue(), row.getDouble(7), 0);
				assertEquals(
						toDateTime(v),
						row.getTimestamp(8, 9).toLocalDateTime());
				assertEquals(BigDecimal.valueOf(v), row.getDecimal(9, 5, 0).toBigDecimal());
				assertEquals(BigDecimal.valueOf(v), row.getDecimal(10, 15, 0).toBigDecimal());
				assertEquals(BigDecimal.valueOf(v), row.getDecimal(11, 20, 0).toBigDecimal());
				assertEquals(BigDecimal.valueOf(v), row.getDecimal(12, 5, 0).toBigDecimal());
				assertEquals(BigDecimal.valueOf(v), row.getDecimal(13, 15, 0).toBigDecimal());
				assertEquals(BigDecimal.valueOf(v), row.getDecimal(14, 20, 0).toBigDecimal());
			}
			i++;
		}
		reader.close();
		return i - start;
	}

	private Row newRow(Integer v) {
		return Row.of(
				"" + v,
				v % 2 == 0,
				v,
				v,
				v,
				v.longValue(),
				v.floatValue(),
				v.doubleValue(),
				toDateTime(v),
				BigDecimal.valueOf(v),
				BigDecimal.valueOf(v),
				BigDecimal.valueOf(v),
				BigDecimal.valueOf(v),
				BigDecimal.valueOf(v),
				BigDecimal.valueOf(v));
	}

	private LocalDateTime toDateTime(Integer v) {
		v = (v > 0 ? v : -v) % 10000;
		return BASE_TIME.plusNanos(v).plusSeconds(v);
	}

	@Test
	public void testDictionary() throws IOException {
		// prepare parquet file
		int number = 10000;
		List<Row> records = new ArrayList<>(number);
		List<Integer> values = new ArrayList<>(number);
		Random random = new Random();
		int[] intValues = new int[10];
		// test large values in dictionary
		for (int i = 0; i < intValues.length; i++) {
			intValues[i] = random.nextInt();
		}
		for (int i = 0; i < number; i++) {
			Integer v = intValues[random.nextInt(10)];
			if (v == 0) {
				values.add(null);
				records.add(new Row(FIELD_NUMBER));
			} else {
				values.add(v);
				records.add(newRow(v));
			}
		}

		testNormalTypes(number, records, values);
	}

	@Test
	public void testPartialDictionary() throws IOException {
		// prepare parquet file
		int number = 10000;
		List<Row> records = new ArrayList<>(number);
		List<Integer> values = new ArrayList<>(number);
		Random random = new Random();
		int[] intValues = new int[10];
		// test large values in dictionary
		for (int i = 0; i < intValues.length; i++) {
			intValues[i] = random.nextInt();
		}
		for (int i = 0; i < number; i++) {
			Integer v = i < 5000 ? intValues[random.nextInt(10)] : i;
			if (v == 0) {
				values.add(null);
				records.add(new Row(FIELD_NUMBER));
			} else {
				values.add(v);
				records.add(newRow(v));
			}
		}

		testNormalTypes(number, records, values);
	}

	@Test
	public void testContinuousRepetition() throws IOException {
		// prepare parquet file
		int number = 10000;
		List<Row> records = new ArrayList<>(number);
		List<Integer> values = new ArrayList<>(number);
		Random random = new Random();
		for (int i = 0; i < 100; i++) {
			Integer v = random.nextInt(10);
			for (int j = 0; j < 100; j++) {
				if (v == 0) {
					values.add(null);
					records.add(new Row(FIELD_NUMBER));
				} else {
					values.add(v);
					records.add(newRow(v));
				}
			}
		}

		testNormalTypes(number, records, values);
	}

	@Test
	public void testLargeValue() throws IOException {
		// prepare parquet file
		int number = 10000;
		List<Row> records = new ArrayList<>(number);
		List<Integer> values = new ArrayList<>(number);
		Random random = new Random();
		for (int i = 0; i < number; i++) {
			Integer v = random.nextInt();
			if (v % 10 == 0) {
				values.add(null);
				records.add(new Row(FIELD_NUMBER));
			} else {
				values.add(v);
				records.add(newRow(v));
			}
		}

		testNormalTypes(number, records, values);
	}

	@Test
	public void testProject() throws IOException {
		// prepare parquet file
		int number = 1000;
		List<Row> records = new ArrayList<>(number);
		for (int i = 0; i < number; i++) {
			Integer v = i;
			records.add(newRow(v));
		}

		Path testPath = createTempParquetFile(
				TEMPORARY_FOLDER.newFolder(), PARQUET_SCHEMA, records, rowGroupSize);

		// test reader
		LogicalType[] fieldTypes = new LogicalType[]{
				new DoubleType(),
				new TinyIntType(),
				new IntType()};
		ParquetColumnarRowSplitReader reader = new ParquetColumnarRowSplitReader(
				false,
				new Configuration(),
				fieldTypes,
				new String[] {"f7", "f2", "f4"},
				VectorizedColumnBatch::new,
				500,
				new org.apache.hadoop.fs.Path(testPath.getPath()),
				0,
				Long.MAX_VALUE);
		int i = 0;
		while (!reader.reachedEnd()) {
			ColumnarRow row = reader.nextRecord();
			assertEquals(i, row.getDouble(0), 0);
			assertEquals((byte) i, row.getByte(1));
			assertEquals(i, row.getInt(2));
			i++;
		}
		reader.close();
	}

	@Test
	public void testPartitionValues() throws IOException {
		// prepare parquet file
		int number = 1000;
		List<Row> records = new ArrayList<>(number);
		for (int i = 0; i < number; i++) {
			Integer v = i;
			records.add(newRow(v));
		}

		Path testPath = createTempParquetFile(
				TEMPORARY_FOLDER.newFolder(), PARQUET_SCHEMA, records, rowGroupSize);

		// test reader
		Map<String, Object> partSpec = new HashMap<>();
		partSpec.put("f15", true);
		partSpec.put("f16", Date.valueOf("2020-11-23"));
		partSpec.put("f17", LocalDateTime.of(1999, 1, 1, 1, 1));
		partSpec.put("f18", 6.6);
		partSpec.put("f19", (byte) 9);
		partSpec.put("f20", (short) 10);
		partSpec.put("f21", 11);
		partSpec.put("f22", 12L);
		partSpec.put("f23", 13f);
		partSpec.put("f24", new BigDecimal(24));
		partSpec.put("f25", new BigDecimal(25));
		partSpec.put("f26", new BigDecimal(26));
		partSpec.put("f27", "f27");

		innerTestPartitionValues(testPath, partSpec, false);

		for (String k : new ArrayList<>(partSpec.keySet())) {
			partSpec.put(k, null);
		}

		innerTestPartitionValues(testPath, partSpec, true);
	}

	private void innerTestPartitionValues(
			Path testPath,
			Map<String, Object> partSpec,
			boolean nullPartValue) throws IOException {
		LogicalType[] fieldTypes = new LogicalType[]{
				new VarCharType(VarCharType.MAX_LENGTH),
				new BooleanType(),
				new TinyIntType(),
				new SmallIntType(),
				new IntType(),
				new BigIntType(),
				new FloatType(),
				new DoubleType(),
				new TimestampType(9),
				new DecimalType(5, 0),
				new DecimalType(15, 0),
				new DecimalType(20, 0),
				new DecimalType(5, 0),
				new DecimalType(15, 0),
				new DecimalType(20, 0),
				new BooleanType(),
				new DateType(),
				new TimestampType(9),
				new DoubleType(),
				new TinyIntType(),
				new SmallIntType(),
				new IntType(),
				new BigIntType(),
				new FloatType(),
				new DecimalType(5, 0),
				new DecimalType(15, 0),
				new DecimalType(20, 0),
				new VarCharType(VarCharType.MAX_LENGTH)};
		ParquetColumnarRowSplitReader reader = ParquetSplitReaderUtil.genPartColumnarRowReader(
				false,
				new Configuration(),
				IntStream.range(0, 28).mapToObj(i -> "f" + i).toArray(String[]::new),
				Arrays.stream(fieldTypes)
						.map(TypeConversions::fromLogicalToDataType)
						.toArray(DataType[]::new),
				partSpec,
				new int[]{7, 2, 4, 15, 19, 20, 21, 22, 23, 18, 16, 17, 24, 25, 26, 27},
				rowGroupSize,
				new Path(testPath.getPath()),
				0,
				Long.MAX_VALUE);
		int i = 0;
		while (!reader.reachedEnd()) {
			ColumnarRow row = reader.nextRecord();

			// common values
			assertEquals(i, row.getDouble(0), 0);
			assertEquals((byte) i, row.getByte(1));
			assertEquals(i, row.getInt(2));

			// partition values
			if (nullPartValue) {
				for (int j = 3; j < 16; j++) {
					assertTrue(row.isNullAt(j));
				}
			} else {
				assertTrue(row.getBoolean(3));
				assertEquals(9, row.getByte(4));
				assertEquals(10, row.getShort(5));
				assertEquals(11, row.getInt(6));
				assertEquals(12, row.getLong(7));
				assertEquals(13, row.getFloat(8), 0);
				assertEquals(6.6, row.getDouble(9), 0);
				assertEquals(
						SqlDateTimeUtils.dateToInternal(Date.valueOf("2020-11-23")),
						row.getInt(10));
				assertEquals(
						LocalDateTime.of(1999, 1, 1, 1, 1),
						row.getTimestamp(11, 9).toLocalDateTime());
				assertEquals(
						Decimal.fromBigDecimal(new BigDecimal(24), 5, 0),
						row.getDecimal(12, 5, 0));
				assertEquals(
						Decimal.fromBigDecimal(new BigDecimal(25), 15, 0),
						row.getDecimal(13, 15, 0));
				assertEquals(
						Decimal.fromBigDecimal(new BigDecimal(26), 20, 0),
						row.getDecimal(14, 20, 0));
				assertEquals("f27", row.getString(15).toString());
			}

			i++;
		}
		reader.close();
	}
}
