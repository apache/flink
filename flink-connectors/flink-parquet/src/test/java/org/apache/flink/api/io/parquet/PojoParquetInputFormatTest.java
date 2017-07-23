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

import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static junit.framework.Assert.assertNull;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * Tests for {@link PojoParquetInputFormat}.
 */
public class PojoParquetInputFormatTest {

	@Test(expected = IllegalArgumentException.class)
	public void testFieldNameNotFound() throws IOException {
		PojoTypeInfo<PojoItem> typeInfo = (PojoTypeInfo<PojoItem>) TypeExtractor.createTypeInfo(PojoItem.class);
		new PojoParquetInputFormat<>(new Path("/tmp"), typeInfo, new String[]{"field1", "field4"});
	}

	@Test(expected = NullPointerException.class)
	public void testNullFieldName() throws IOException {
		PojoTypeInfo<PojoItem> typeInfo = (PojoTypeInfo<PojoItem>) TypeExtractor.createTypeInfo(PojoItem.class);
		new PojoParquetInputFormat<>(new Path("/tmp"), typeInfo, new String[]{"field1", null});
	}

	@Test
	public void testPojoType() throws IOException {
		final File tmpFile = genParquetFile();

		PojoTypeInfo<PojoItem> typeInfo = (PojoTypeInfo<PojoItem>) TypeExtractor.createTypeInfo(PojoItem.class);
		PojoParquetInputFormat<PojoItem> inputFormat = new PojoParquetInputFormat<>(
				new Path(tmpFile.toURI()), typeInfo);
		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		inputFormat.open(splits[0]);

		assertFalse(inputFormat.reachedEnd());
		PojoItem item = inputFormat.nextRecord(null);
		assertTrue(1 == item.field1);
		assertEquals("str1", item.field2);
		assertTrue(10L == item.field3);

		assertFalse(inputFormat.reachedEnd());
		item = inputFormat.nextRecord(null);
		assertTrue(2 == item.field1);
		assertNull(item.field2);
		assertTrue(20L == item.field3);

		assertFalse(inputFormat.reachedEnd());
		item = inputFormat.nextRecord(null);
		assertTrue(3 == item.field1);
		assertEquals("str3", item.field2);
		assertTrue(30L == item.field3);

		assertTrue(inputFormat.reachedEnd());
	}

	@Test
	public void testPojoTypeWithPartialFields() throws Exception {
		final File tmpFile = genParquetFile();

		PojoTypeInfo<PojoItem> typeInfo = (PojoTypeInfo<PojoItem>) TypeExtractor.createTypeInfo(PojoItem.class);
		PojoParquetInputFormat<PojoItem> inputFormat = new PojoParquetInputFormat<>(
				new Path(tmpFile.toURI()), typeInfo, new String[]{"field1", "field3"});
		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		inputFormat.open(splits[0]);

		assertFalse(inputFormat.reachedEnd());
		PojoItem item = inputFormat.nextRecord(null);
		assertTrue(1 == item.field1);
		assertEquals("", item.field2); // the value StringSerializer#createInstance is ""
		assertTrue(10L == item.field3);

		assertFalse(inputFormat.reachedEnd());
		item = inputFormat.nextRecord(null);
		assertTrue(2 == item.field1);
		assertEquals("", item.field2); // the value StringSerializer#createInstance is ""
		assertTrue(20L == item.field3);

		assertFalse(inputFormat.reachedEnd());
		item = inputFormat.nextRecord(null);
		assertTrue(3 == item.field1);
		assertEquals("", item.field2); // the value StringSerializer#createInstance is ""
		assertTrue(30L == item.field3);

		assertTrue(inputFormat.reachedEnd());
	}

	@Test
	public void testPojoTypeWithPrivateField() throws IOException {
		final File tmpFile = genParquetFile();

		PojoTypeInfo<PrivatePojoItem> typeInfo =
				(PojoTypeInfo<PrivatePojoItem>) TypeExtractor.createTypeInfo(PrivatePojoItem.class);
		PojoParquetInputFormat<PrivatePojoItem> inputFormat = new PojoParquetInputFormat<>(
				new org.apache.flink.core.fs.Path(tmpFile.toURI()), typeInfo);
		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		inputFormat.open(splits[0]);

		assertFalse(inputFormat.reachedEnd());
		PrivatePojoItem item = inputFormat.nextRecord(null);
		assertTrue(1 == item.getField1());
		assertEquals("str1", item.getField2());
		assertTrue(10L == item.getField3());

		assertFalse(inputFormat.reachedEnd());
		item = inputFormat.nextRecord(null);
		assertTrue(2 == item.getField1());
		assertNull(item.getField2());
		assertTrue(20L == item.getField3());

		assertFalse(inputFormat.reachedEnd());
		item = inputFormat.nextRecord(null);
		assertTrue(3 == item.getField1());
		assertEquals("str3", item.getField2());
		assertTrue(30L == item.getField3());

		assertTrue(inputFormat.reachedEnd());
	}

	@Test
	public void testPojoSubclassType() throws Exception {
		final File tmpFile = genParquetFile();

		PojoTypeInfo<SubPOJO> typeInfo = (PojoTypeInfo<SubPOJO>) TypeExtractor.createTypeInfo(SubPOJO.class);
		PojoParquetInputFormat<SubPOJO> inputFormat = new PojoParquetInputFormat<>(
				new Path(tmpFile.toURI()), typeInfo);
		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		inputFormat.open(splits[0]);

		assertFalse(inputFormat.reachedEnd());
		SubPOJO pojo = inputFormat.nextRecord(null);
		assertTrue(1 == pojo.field1);
		assertEquals("str1", pojo.field2);
		assertTrue(10L == pojo.field3);

		assertFalse(inputFormat.reachedEnd());
		pojo = inputFormat.nextRecord(null);
		assertTrue(2 == pojo.field1);
		assertNull(pojo.field2);
		assertTrue(20L == pojo.field3);

		assertFalse(inputFormat.reachedEnd());
		pojo = inputFormat.nextRecord(null);
		assertTrue(3 == pojo.field1);
		assertEquals("str3", pojo.field2);
		assertTrue(30L == pojo.field3);

		assertTrue(inputFormat.reachedEnd());
	}

	private File genParquetFile() throws IOException {
		final File tmpFile = File.createTempFile("flink-parquet-test-data", ".parquet");
		tmpFile.deleteOnExit();
		Configuration conf = new Configuration();
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.required(INT32).named("field1"))
				.addFields(Types.optional(BINARY).as(OriginalType.UTF8).named("field2"))
				.addFields(Types.required(INT64).named("field3"))
				.named("flink-parquet");
		ParquetWriter writer = new ParquetWriter(parquetSchema, tmpFile.toString(), conf);
		writer.write(writer.newGroup().append("field1", 1).append("field2", "str1").append("field3", 10L));
		writer.write(writer.newGroup().append("field1", 2).append("field3", 20L));
		writer.write(writer.newGroup().append("field1", 3).append("field2", "str3").append("field3", 30L));
		writer.close();
		return tmpFile;
	}

	/** */
	public static class PojoItem {
		public int field1;
		public String field2;
		public Long field3;
	}

	/** */
	public static class PrivatePojoItem {
		private int field1;
		private String field2;
		private Long field3;

		public int getField1() {
			return field1;
		}

		public void setField1(int field1) {
			this.field1 = field1;
		}

		public String getField2() {
			return field2;
		}

		public void setField2(String field2) {
			this.field2 = field2;
		}

		public Long getField3() {
			return field3;
		}

		public void setField3(Long field3) {
			this.field3 = field3;
		}
	}

	/** */
	public static class POJO {
		public int field1;
		public String field2;

		public POJO() {
			this(0, "");
		}

		public POJO(int field1, String field2) {
			this.field1 = field1;
			this.field2 = field2;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof POJO) {
				POJO other = (POJO) obj;
				return field1 == other.field1 && field2.equals(other.field2);
			} else {
				return false;
			}
		}
	}

	/** */
	public static class SubPOJO extends POJO {
		public long field3;

		public SubPOJO() {
			this(0, "", 0L);
		}

		public SubPOJO(int field1, String field2, long field3) {
			super(field1, field2);
			this.field3 = field3;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof SubPOJO) {
				SubPOJO other = (SubPOJO) obj;

				return super.equals(other) && field3 == other.field3;
			} else {
				return false;
			}
		}
	}
}
