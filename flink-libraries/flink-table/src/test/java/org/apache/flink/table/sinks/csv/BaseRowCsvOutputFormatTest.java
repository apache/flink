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

package org.apache.flink.table.sinks.csv;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Tests for {@link BaseRowCsvOutputFormat}.
 */
public class BaseRowCsvOutputFormatTest {

	private String path = null;

	@Before
	public void createFile() throws Exception {
		path = File.createTempFile("base_row_csv_output_test_file", ".csv").getAbsolutePath();
	}

	@Test
	public void testQuoteCharacter() throws Exception {
		final BaseRowCsvOutputFormat format = new BaseRowCsvOutputFormat(new Path(path), getType());
		format.setQuoteCharacter('"');
		BinaryRow[] rows = getData(",", "\n", '"');

		String expected1a = "\"\"\"Quoted\"\" \"\"String\"\"\",\"New";
		String expected1b = "Line\",\"[65, 66, 67]\",123,true";
		String expected2 = "\"Comma, String\",Normal String,[65],-123,false";

		try {
			format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
			format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
			format.open(0, 1);
			for (BinaryRow row : rows) {
				format.writeRecord(row);
			}
		} finally {
			format.close();
		}

		java.nio.file.Path p = Paths.get(path);
		Assert.assertTrue(Files.exists(p));
		List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
		Assert.assertEquals(3, lines.size());
		Assert.assertEquals(expected1a, lines.get(0));
		Assert.assertEquals(expected1b, lines.get(1));
		Assert.assertEquals(expected2, lines.get(2));
	}

	@Test
	public void testOutputFieldNameWithNoRecord() throws Exception {
		final BaseRowCsvOutputFormat format = new BaseRowCsvOutputFormat(new Path(path), getType());
		format.setOutputFieldName(true);
		format.setFieldNames(new String[]{"f1", "f2", "f3", "f4", "f5"});

		try {
			format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
			format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
			format.open(0, 1);
		} finally {
			format.close();
		}

		java.nio.file.Path p = Paths.get(path);
		Assert.assertTrue(Files.exists(p));
		List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
		Assert.assertEquals(0, lines.size());
	}

	@Test
	public void testOutputFieldName() throws Exception {
		final BaseRowCsvOutputFormat format = new BaseRowCsvOutputFormat(new Path(path), getType());
		format.setQuoteCharacter('"');
		format.setOutputFieldName(true);
		format.setFieldNames(new String[]{
			"Normal Name",
			"\"Quoted\" \"Name\"",
			"New\nLine\nName",
			"Comma, Name",
			"!@#$%^&*()[]{}\"',.<>?"
		});
		BinaryRow[] rows = getData(",", "\n", '"');

		String expected0a = "Normal Name,\"\"\"Quoted\"\" \"\"Name\"\"\",\"New";
		String expected0b = "Line";
		String expected0c = "Name\",\"Comma, Name\",\"!@#$%^&*()[]{}\"\"',.<>?\"";
		String expected1a = "\"\"\"Quoted\"\" \"\"String\"\"\",\"New";
		String expected1b = "Line\",\"[65, 66, 67]\",123,true";
		String expected2 = "\"Comma, String\",Normal String,[65],-123,false";

		try {
			format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
			format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
			format.open(0, 1);
			for (BinaryRow row : rows) {
				format.writeRecord(row);
			}
		} finally {
			format.close();
		}

		java.nio.file.Path p = Paths.get(path);
		Assert.assertTrue(Files.exists(p));
		List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
		Assert.assertEquals(6, lines.size());
		Assert.assertEquals(expected0a, lines.get(0));
		Assert.assertEquals(expected0b, lines.get(1));
		Assert.assertEquals(expected0c, lines.get(2));
		Assert.assertEquals(expected1a, lines.get(3));
		Assert.assertEquals(expected1b, lines.get(4));
		Assert.assertEquals(expected2, lines.get(5));
	}

	@Test
	public void testNullQuoteCharacter() throws Exception {
		final BaseRowCsvOutputFormat format = new BaseRowCsvOutputFormat(new Path(path), getType());
		BinaryRow[] rows = getData(",", "\n", '"');

		String expected1a = "\"Quoted\" \"String\",New";
		String expected1b = "Line,[65, 66, 67],123,true";
		String expected2 = "Comma, String,Normal String,[65],-123,false";

		try {
			format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
			format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
			format.open(0, 1);
			for (BinaryRow row : rows) {
				format.writeRecord(row);
			}
		} finally {
			format.close();
		}

		java.nio.file.Path p = Paths.get(path);
		Assert.assertTrue(Files.exists(p));
		List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
		Assert.assertEquals(3, lines.size());
		Assert.assertEquals(expected1a, lines.get(0));
		Assert.assertEquals(expected1b, lines.get(1));
		Assert.assertEquals(expected2, lines.get(2));
	}

	@Test
	public void testSelfDefinedCsv() throws Exception {
		final BaseRowCsvOutputFormat format = new BaseRowCsvOutputFormat(new Path(path), getType());
		format.setFieldDelimiter("...");
		format.setRecordDelimiter("$$");
		format.setQuoteCharacter('~');
		BinaryRow[] rows = getData("...", "$$", '~');

		String expected = "~~~Quoted~~ ~~String~~~...~New$$Line~...[65, 66, 67]...123...true$$" +
			"~Comma... String~...Normal String...[65]...-123...false$$";

		try {
			format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
			format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
			format.open(0, 1);
			for (BinaryRow row : rows) {
				format.writeRecord(row);
			}
		} finally {
			format.close();
		}

		java.nio.file.Path p = Paths.get(path);
		Assert.assertTrue(Files.exists(p));
		List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
		Assert.assertEquals(1, lines.size());
		Assert.assertEquals(expected, lines.get(0));
	}

	private InternalType[] getType() {
		return new InternalType[]{
			DataTypes.STRING,
			DataTypes.STRING,
			DataTypes.BYTE_ARRAY,
			DataTypes.INT,
			DataTypes.BOOLEAN
		};
	}

	private BinaryRow[] getData(String c, String n, char q) {
		BinaryRow row1 = new BinaryRow(5);
		BinaryRowWriter writer1 = new BinaryRowWriter(row1);
		writer1.writeString(0, q + "Quoted" + q + " " + q + "String" + q);
		writer1.writeString(1, "New" + n + "Line");
		writer1.writeByteArray(2, new byte[]{(byte) 65, (byte) 66, (byte) 67});
		writer1.writeInt(3, 123);
		writer1.writeBoolean(4, true);
		writer1.complete();

		BinaryRow row2 = new BinaryRow(5);
		BinaryRowWriter writer2 = new BinaryRowWriter(row2);
		writer2.writeString(0, "Comma" + c + " String");
		writer2.writeString(1, "Normal String");
		writer2.writeByteArray(2, new byte[]{(byte) 65});
		writer2.writeInt(3, -123);
		writer2.writeBoolean(4, false);
		writer2.complete();

		return new BinaryRow[]{row1, row2};
	}
}
