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

package org.apache.flink.api.java.io;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CsvOutputFormatTest {

	private static final Path PATH = new Path("csv_output_test_file.csv");

	@Test
	public void testNullAllowOnDefault() throws Exception {
		CsvOutputFormat<Tuple3<String, String, Integer>> csvOutputFormat = new CsvOutputFormat<Tuple3<String, String, Integer>>(PATH);
		csvOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		csvOutputFormat.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
		csvOutputFormat.open(0, 1);
		csvOutputFormat.writeRecord(new Tuple3<String, String, Integer>("One", null, 8));
		csvOutputFormat.close();
		final FileSystem fs = PATH.getFileSystem();
		Assert.assertTrue(fs.exists(PATH));
		FSDataInputStream inputStream = fs.open(PATH);
		String csvContent = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
		Assert.assertEquals("One,,8\n", csvContent);
		fs.delete(PATH, true);
	}

	@Test(expected = RuntimeException.class)
	public void testNullNotAllow() throws Exception {
		CsvOutputFormat<Tuple3<String, String, Integer>> csvOutputFormat = new CsvOutputFormat<Tuple3<String, String, Integer>>(PATH);
		csvOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		csvOutputFormat.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
		csvOutputFormat.setAllowNullValues(false);
		csvOutputFormat.open(0, 1);
		csvOutputFormat.writeRecord(new Tuple3<String, String, Integer>("One", null, 8));
		csvOutputFormat.close();
	}

}
