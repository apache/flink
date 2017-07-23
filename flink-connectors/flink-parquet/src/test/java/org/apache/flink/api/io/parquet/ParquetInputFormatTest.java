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

package org.apache.flink.api.io.parquet;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.types.Row;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link ParquetInputFormat} and {@link RowParquetInputFormat}.
 */
public class ParquetInputFormatTest {

	@Test
	public void testIllegalConstructorArguments() {
		org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(getClass().getResource(".").getFile());

		try {
			// empty field name
			new RowParquetInputFormat(path, new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO}, new String[0]);
			assertFalse(true);
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}

		try {
			// empty field type
			new RowParquetInputFormat(path, new TypeInformation[0], new String[]{"f1", "f2"});
			assertFalse(true);
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}

		try {
			// the length of field types and field names is not equal
			new RowParquetInputFormat(
					path,
					new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO},
					new String[]{"f1", "f2"});
			assertFalse(true);
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
	}

	@Test
	public void testSplitOneFileWithOneRowGroup() throws IOException {
		final File tmpFile = File.createTempFile("flink-parquet-test-data", ".parquet");
		tmpFile.deleteOnExit();
		Configuration conf = new Configuration();
		genParquetFileWithOneField(conf, tmpFile.toString(), 100);

		ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(conf, new Path(tmpFile.toURI()), NO_FILTER);
		List<BlockMetaData> blocks = parquetMetadata.getBlocks();
		assertEquals(1, blocks.size());

		RowParquetInputFormat inputFormat = new RowParquetInputFormat(
				new org.apache.flink.core.fs.Path(tmpFile.toURI()),
				new TypeInformation[]{BasicTypeInfo.DOUBLE_TYPE_INFO},
				new String[]{"f1"});
		FileInputSplit[] splits = inputFormat.createInputSplits(2);
		assertEquals(2, splits.length);

		// read first block
		inputFormat.open(splits[0]);
		checkResult(inputFormat, blocks.get(0).getRowCount());

		// read second block
		inputFormat.open(splits[1]);
		checkResult(inputFormat, 0);
	}

	@Test
	public void testSplitOneFileWithMultiRowGroup() throws IOException {
		final File tmpFile = File.createTempFile("flink-parquet-test-data", ".parquet");
		tmpFile.deleteOnExit();
		Configuration conf = new Configuration();
		genParquetFileWithOneField(conf, tmpFile.toString(), 200);

		ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(conf, new Path(tmpFile.toURI()), NO_FILTER);
		List<BlockMetaData> blocks = parquetMetadata.getBlocks();
		assertEquals(2, blocks.size());

		RowParquetInputFormat inputFormat = new RowParquetInputFormat(
				new org.apache.flink.core.fs.Path(tmpFile.toURI()),
				new TypeInformation[]{BasicTypeInfo.DOUBLE_TYPE_INFO},
				new String[]{"f1"});
		FileInputSplit[] splits = inputFormat.createInputSplits(2);
		assertEquals(2, splits.length);

		// read first block
		inputFormat.open(splits[0]);
		checkResult(inputFormat, blocks.get(0).getRowCount());

		// read second block
		inputFormat.open(splits[1]);
		checkResult(inputFormat, blocks.get(1).getRowCount());
	}

	private void checkResult(RowParquetInputFormat inputFormat, long expectedRowCount) throws IOException {
		long totalCount = 0;
		while (!inputFormat.reachedEnd()) {
			totalCount++;
			Row row = inputFormat.nextRecord(null);
			assertEquals(1, row.getArity());
			assertTrue(row.getField(0) instanceof Double);
			double value = (double) row.getField(0);
			assertTrue(0 <= value && value < 1);
		}
		assertEquals(expectedRowCount, totalCount);
		inputFormat.close();
	}

	@Test
	public void testSplitWithMultiFile() throws IOException {
		final File path = new File(getClass().getResource(".").getFile() + "flink-parquet-test");
		FileUtils.deleteDirectory(path);
		if (path.exists()) {
			throw new IOException(" can not delete path " + path);
		}
		if (!path.mkdirs()) {
			throw new IOException("can not create path " + path);
		}

		File[] files = new File[3];
		for (int i = 0; i < files.length; ++i) {
			final File tmpFile = File.createTempFile("flink-parquet-test-data-" + i + "-", ".parquet", path);
			tmpFile.deleteOnExit();
			files[i] = tmpFile;
		}

		Configuration conf = new Configuration();

		for (int i = 0; i < files.length; ++i) {
			genParquetFileWithOneField(conf, files[i].toString(), 200 * (i + 1));
		}

		RowParquetInputFormat inputFormat = new RowParquetInputFormat(
				new org.apache.flink.core.fs.Path(path.toURI()),
				new TypeInformation[]{BasicTypeInfo.DOUBLE_TYPE_INFO},
				new String[]{"f1"});
		FileInputSplit[] splits = inputFormat.createInputSplits(2);
		assertEquals(3, splits.length);

		for (FileInputSplit split : splits) {
			inputFormat.open(split);
			int totalCount = 0;
			while (!inputFormat.reachedEnd()) {
				totalCount++;
				inputFormat.nextRecord(null);
			}
			ParquetMetadata metadata = ParquetFileReader.readFooter(conf, new Path(split.getPath().toUri()), NO_FILTER);
			int rowCount = 0;
			for (BlockMetaData block : metadata.getBlocks()) {
				rowCount += block.getRowCount();
			}
			assertEquals(rowCount, totalCount);
		}
	}

	@Test
	public void testSupportedTypeInfos() throws IOException {
		final File tmpFile = File.createTempFile("flink-parquet-test-data", ".parquet");
		tmpFile.deleteOnExit();
		Configuration conf = new Configuration();
		MessageType parquetSchema = Types.buildMessage().addFields(
				Types.required(BOOLEAN).named("boolean"),
				Types.required(FLOAT).named("float"),
				Types.required(DOUBLE).named("double"),
				Types.required(INT32).as(null).named("int_original_null"),
				Types.required(INT32).as(OriginalType.INT_8).named("int32_original_int8"),
				Types.required(INT32).as(OriginalType.INT_16).named("int32_original_int16"),
				Types.required(INT32).as(OriginalType.INT_32).named("int32_original_int32"),
				Types.required(INT32).as(OriginalType.DATE).named("int32_original_date"),
				Types.required(INT32).as(OriginalType.DECIMAL).precision(9).scale(2).named("int32_original_decimal"),
				Types.required(INT64).as(null).named("int64_original_null"),
				Types.required(INT64).as(OriginalType.INT_64).named("int64_original_int64"),
				Types.required(INT64).as(OriginalType.DECIMAL).precision(9).scale(2).named("int64_original_decimal"),
				Types.required(BINARY).as(null).named("binary_original_null"),
				Types.required(BINARY).as(OriginalType.UTF8).named("binary_original_uft8"),
				Types.required(BINARY).as(OriginalType.ENUM).named("binary_original_enum"),
				Types.required(BINARY).as(OriginalType.JSON).named("binary_original_json"),
				Types.required(BINARY).as(OriginalType.BSON).named("binary_original_bson"),
				Types.required(BINARY).as(OriginalType.DECIMAL).precision(9).scale(2).named("binary_original_decimal"))
				.named("flink-parquet");

		ParquetWriter writer = new ParquetWriter(parquetSchema, tmpFile.toString(), conf);
		writer.write(writer.newGroup()
				.append("boolean", true)
				.append("float", 1.23f)
				.append("double", 1.23456d)
				.append("int_original_null", 123)
				.append("int32_original_int8", Byte.MAX_VALUE)
				.append("int32_original_int16", Short.MAX_VALUE)
				.append("int32_original_int32", Integer.MAX_VALUE)
				.append("int32_original_date", 1234567)
				.append("int32_original_decimal", 123456789)
				.append("int64_original_null", Long.MIN_VALUE)
				.append("int64_original_int64", 1234567890L)
				.append("int64_original_decimal", Long.MAX_VALUE)
				.append("binary_original_null", Binary.EMPTY)
				.append("binary_original_uft8", Binary.fromString("test-utf8"))
				.append("binary_original_enum", Binary.fromString(OriginalType.ENUM.toString()))
				.append("binary_original_json", Binary.fromString("{}"))
				.append("binary_original_bson", Binary.fromString("bson"))
				.append("binary_original_decimal", Binary.fromConstantByteArray(
						new BigDecimal(123456789.123456789).unscaledValue().toByteArray())));
		writer.close();

		TypeInformation<?>[] fieldTypes = {
				BasicTypeInfo.BOOLEAN_TYPE_INFO,
				BasicTypeInfo.FLOAT_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.BYTE_TYPE_INFO,
				BasicTypeInfo.SHORT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.DATE_TYPE_INFO,
				BasicTypeInfo.BIG_DEC_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.BIG_DEC_TYPE_INFO,
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
				BasicTypeInfo.BIG_DEC_TYPE_INFO
		};
		String[] fieldNames = {
				"boolean",
				"float",
				"double",
				"int_original_null",
				"int32_original_int8",
				"int32_original_int16",
				"int32_original_int32",
				"int32_original_date",
				"int32_original_decimal",
				"int64_original_null",
				"int64_original_int64",
				"int64_original_decimal",
				"binary_original_null",
				"binary_original_uft8",
				"binary_original_enum",
				"binary_original_json",
				"binary_original_bson",
				"binary_original_decimal"};

		RowParquetInputFormat inputFormat = new RowParquetInputFormat(
				new org.apache.flink.core.fs.Path(tmpFile.toURI()), fieldTypes, fieldNames);
		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		inputFormat.open(splits[0]);

		assertFalse(inputFormat.reachedEnd());
		Row row = inputFormat.nextRecord(null);
		assertEquals(18, row.getArity());
		assertEquals(true, row.getField(0));
		assertEquals(1.23f, row.getField(1));
		assertEquals(1.23456d, row.getField(2));
		assertEquals(123, row.getField(3));
		assertEquals(Byte.MAX_VALUE, row.getField(4));
		assertEquals(Short.MAX_VALUE, row.getField(5));
		assertEquals(Integer.MAX_VALUE, row.getField(6));
		assertEquals(new Date(1234567), row.getField(7));
		assertEquals(new BigDecimal(123456789), row.getField(8));
		assertEquals(Long.MIN_VALUE, row.getField(9));
		assertEquals(1234567890L, row.getField(10));
		assertEquals(new BigDecimal(Long.MAX_VALUE), row.getField(11));
		assertArrayEquals(Binary.EMPTY.getBytes(), (byte[]) row.getField(12));
		assertEquals("test-utf8", row.getField(13));
		assertEquals(OriginalType.ENUM, OriginalType.valueOf(row.getField(14).toString()));
		assertEquals("{}", row.getField(15));
		assertArrayEquals("bson".getBytes(), (byte[]) row.getField(16));
		assertEquals(new BigDecimal(123456789.123456789).unscaledValue(),
				((BigDecimal) row.getField(17)).unscaledValue());

		assertTrue(inputFormat.reachedEnd());
	}

	@Test
	public void testReadNullableColumn() throws IOException {
		final File tmpFile = File.createTempFile("flink-parquet-test-data", ".parquet");
		tmpFile.deleteOnExit();
		Configuration conf = new Configuration();
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.required(INT32).named("f1"))
				.addFields(Types.optional(BINARY).as(OriginalType.UTF8).named("f2"))
				.named("flink-parquet");
		ParquetWriter writer = new ParquetWriter(parquetSchema, tmpFile.toString(), conf);
		writer.write(writer.newGroup().append("f1", 1).append("f2", "str1"));
		writer.write(writer.newGroup().append("f1", 2)); // the value of f2 is null
		writer.close();

		RowParquetInputFormat inputFormat = new RowParquetInputFormat(
				new org.apache.flink.core.fs.Path(tmpFile.toURI()),
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
				new String[]{"f1", "f2"});
		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		inputFormat.open(splits[0]);

		assertFalse(inputFormat.reachedEnd());
		Row row = inputFormat.nextRecord(null);
		assertEquals(2, row.getArity());
		assertEquals(1, row.getField(0));
		assertEquals("str1", row.getField(1));

		assertFalse(inputFormat.reachedEnd());
		row = inputFormat.nextRecord(null);
		assertEquals(2, row.getArity());
		assertEquals(2, row.getField(0));
		assertNull(row.getField(1));

		assertTrue(inputFormat.reachedEnd());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFieldNameNotFound() throws IOException {
		String file = genParquetFileWithMultiFields();
		RowParquetInputFormat inputFormat = new RowParquetInputFormat(
				new org.apache.flink.core.fs.Path(file),
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO},
				new String[]{"f1", "f4"});
		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		inputFormat.open(splits[0]);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSchemaNotMatch() throws IOException {
		String file = genParquetFileWithMultiFields();
		RowParquetInputFormat inputFormat = new RowParquetInputFormat(
				new org.apache.flink.core.fs.Path(file),
				new TypeInformation[]{BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
				new String[]{"f1", "f2"});
		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		inputFormat.open(splits[0]);
	}

	@Test
	public void testFilterPredicate() throws Exception {
		String file = genParquetFileWithMultiFields();

		RowParquetInputFormat inputFormat = new RowParquetInputFormat(
				new org.apache.flink.core.fs.Path(file),
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO},
				new String[]{"f1", "f3"});

		FilterPredicate filter = FilterApi.and(
				FilterApi.gtEq(FilterApi.intColumn("f1"), 100),
				FilterApi.lt(doubleColumn("f3"), 0.5d));
		inputFormat.setFilterPredicate(filter);
		assertEquals(filter, inputFormat.getFilterPredicate());

		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		inputFormat.open(splits[0]);

		while (!inputFormat.reachedEnd()) {
			Row row = inputFormat.nextRecord(null);
			assertEquals(2, row.getArity());
			assertTrue((int) row.getField(0) >= 100);
			assertTrue(0 < (double) row.getField(1) && (double) row.getField(1) < 0.5);
		}
	}

	private void genParquetFileWithOneField(Configuration conf, String filePath, int recordCount) throws IOException {
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.required(DOUBLE).named("f1"))
				.named("flink-parquet");
		ParquetWriter writer = new ParquetWriter(
				parquetSchema,
				filePath,
				ParquetFileWriter.Mode.OVERWRITE,
				CompressionCodecName.UNCOMPRESSED,
				1024, // row group size
				512, // page size
				128, // dictionary page size
				0, // max padding size
				true,
				false,
				ParquetProperties.WriterVersion.PARQUET_2_0,
				conf);
		Random random = new Random();
		for (int i = 0; i < recordCount; ++i) {
			writer.write(writer.newGroup().append("f1", random.nextDouble()));
		}
		writer.close();
	}

	private String genParquetFileWithMultiFields() throws IOException {
		final File tmpFile = File.createTempFile("flink-parquet-test-data", ".parquet");
		tmpFile.deleteOnExit();
		Configuration conf = new Configuration();
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.required(INT32).named("f1"))
				.addFields(Types.required(BINARY).as(OriginalType.UTF8).named("f2"))
				.addFields(Types.required(DOUBLE).named("f3"))
				.named("flink-parquet");
		ParquetWriter writer = new ParquetWriter(parquetSchema, tmpFile.toString(), conf);
		Random random = new Random();
		for (int i = 0; i < 200; ++i) {
			writer.write(writer.newGroup().append("f1", i).append("f2", "str" + i).append("f3", random.nextDouble()));
		}
		writer.close();
		return tmpFile.toString();
	}

}
