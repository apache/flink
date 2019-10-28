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

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Tests for {@link RowCsvOutputFormat}.
 */
public class RowCsvOutputFormatTest {

	private String path = null;

	@Before
	public void createFile() throws Exception {
		path = File.createTempFile("csv_output_test_file", ".csv").getAbsolutePath();
	}

	@Test
	public void test() throws Exception {
		RowCsvOutputFormat.Builder builder = new RowCsvOutputFormat.Builder(
				new RowTypeInfo(Types.STRING, Types.STRING, Types.INT),
				new Path(path))
				.setEscapeCharacter('*')
				.setFieldDelimiter(';')
				.setLineDelimiter("\r\n")
				.setNullLiteral("null")
				.setQuoteCharacter('#');
		RowCsvOutputFormat csvOutputFormat = builder.build();
		try {
			csvOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
			csvOutputFormat.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
			csvOutputFormat.open(0, 1);
			csvOutputFormat.writeRecord(Row.of("One", null, 8));
			csvOutputFormat.writeRecord(Row.of("123'4*", "123", 8));
			csvOutputFormat.writeRecord(Row.of("a;b'c", "123", 8));
			csvOutputFormat.writeRecord(Row.of("Two", null, 8));
		} finally {
			csvOutputFormat.close();
		}

		java.nio.file.Path p = Paths.get(path);
		Assert.assertTrue(Files.exists(p));
		Assert.assertEquals(
				Arrays.asList(
						"One;null;8",
						"#123'4**#;#123#;8",
						"#a;b'c#;#123#;8",
						"Two;null;8"),
				Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8));

		csvOutputFormat = builder.build();
		try {
			csvOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
			csvOutputFormat.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
			csvOutputFormat.open(0, 1);
			csvOutputFormat.writeRecord(Row.of("newV1", "newV1", 8));
			csvOutputFormat.writeRecord(Row.of("newV2", "newV2", 8));
		} finally {
			csvOutputFormat.close();
		}

		Assert.assertEquals(
				Arrays.asList("#newV1#;#newV1#;8", "#newV2#;#newV2#;8"),
				Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8));
	}

	@After
	public void cleanUp() throws IOException {
		Files.deleteIfExists(Paths.get(path));
	}
}
