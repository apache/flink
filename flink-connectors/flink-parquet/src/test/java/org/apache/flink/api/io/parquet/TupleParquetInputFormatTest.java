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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * Tests for {@link TupleParquetInputFormat}.
 */
public class TupleParquetInputFormatTest {

	@Test
	public void testTupleType() throws IOException {
		final File tmpFile = File.createTempFile("flink-parquet-test-data", ".parquet");
		tmpFile.deleteOnExit();
		Configuration conf = new Configuration();
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.required(INT32).named("t1"))
				.addFields(Types.optional(BINARY).as(OriginalType.UTF8).named("t2"))
				.addFields(Types.required(INT64).named("t3"))
				.named("flink-parquet");
		ParquetWriter writer = new ParquetWriter(parquetSchema, tmpFile.toString(), conf);
		writer.write(writer.newGroup().append("t1", 1).append("t2", "str1").append("t3", 10L));
		writer.write(writer.newGroup().append("t1", 2).append("t3", 20L)); // the value of t2 is null
		writer.write(writer.newGroup().append("t1", 3).append("t2", "str3").append("t3", 30L));
		writer.close();

		final TupleTypeInfo<Tuple3<Integer, String, Long>> typeInfo =
				TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, String.class, Long.class);
		TupleParquetInputFormat<Tuple3<Integer, String, Long>> inputFormat = new TupleParquetInputFormat<>(
				new org.apache.flink.core.fs.Path(tmpFile.toURI()),
				typeInfo,
				new String[]{"t1", "t2", "t3"});
		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		inputFormat.open(splits[0]);

		assertFalse(inputFormat.reachedEnd());
		Tuple3<Integer, String, Long> tuple3 = inputFormat.nextRecord(null);
		assertEquals(new Tuple3<>(1, "str1", 10L), tuple3);

		assertFalse(inputFormat.reachedEnd());
		tuple3 = inputFormat.nextRecord(null);
		assertEquals(new Tuple3<>(2, (String) null, 20L), tuple3);

		assertFalse(inputFormat.reachedEnd());
		tuple3 = inputFormat.nextRecord(null);
		assertEquals(new Tuple3<>(3, "str3", 30L), tuple3);

		assertTrue(inputFormat.reachedEnd());
	}
}
