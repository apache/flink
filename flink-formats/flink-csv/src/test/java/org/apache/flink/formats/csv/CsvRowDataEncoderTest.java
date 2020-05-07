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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
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

	public static TableSchema tableSchema = TableSchema.builder()
		.field("f0", DataTypes.INT())
		.field("f1", DataTypes.STRING())
		.build();

	@Test
	public void testEncoder() throws Exception {
		Encoder<RowData> encoder = CsvRowDataEncoder.builder(tableSchema)
			.setLineDelimiter("\r").build();
		test(encoder,
			Arrays.asList(rowData(123, "abcde")),
			"123,abcde\r");
	}

	@Test
	public void testEncoderMultiline() throws Exception {
		Encoder<RowData> encoder = CsvRowDataEncoder.builder(tableSchema)
			.setFieldDelimiter(';').build();
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
