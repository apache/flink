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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.FileUtils;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.data.StringData.fromString;

/**
 * Test for {@link CsvRowDataEncoder}.
 */
public class CsvRowDataEncoderTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	public static RowType rowType = RowType.of(
		new LogicalType[]{new IntType(), new VarCharType()},
		new String[]{"f0", "f1"});

	@Test
	public void testEncoder() throws Exception {
		CsvRowDataSerializationSchema serializationSchema = new CsvRowDataSerializationSchema.Builder(rowType)
			.setLineDelimiter("\r")
			.build();
		Encoder<RowData> encoder = new CsvRowDataEncoder(serializationSchema);
		test(encoder,
			Arrays.asList(rowData(123, "abcde")),
			"123,abcde\r");
	}

	@Test
	public void testEncoderMultiline() throws Exception {
		CsvRowDataSerializationSchema serializationSchema = new CsvRowDataSerializationSchema.Builder(rowType)
			.setFieldDelimiter(';')
			.build();
		Encoder<RowData> encoder = new CsvRowDataEncoder(serializationSchema);
		test(encoder,
			Arrays.asList(
				rowData(123, "abc"),
				rowData(456, "def"),
				rowData(789, "xyz")),
			"123;abc\n456;def\n789;xyz\n");
	}

	private static void test(Encoder<RowData> encoder, List<RowData> rowDataList, String expected) throws Exception {
		File file = TEMP_FOLDER.newFile();
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		for (RowData rowData : rowDataList) {
			encoder.encode(rowData, fileOutputStream);
		}
		fileOutputStream.flush();
		fileOutputStream.close();

		String readString = FileUtils.readFileUtf8(file);
		Assert.assertEquals(expected, readString);
	}

	private static RowData rowData(int integer, String str) {
		return GenericRowData.of(integer, fromString(str));
	}
}
