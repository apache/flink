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
package org.apache.flink.api.java;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.core.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class ExecutionEnvironmentReadFileTest {

	private DelimitedInputFormat<String> format = null;

	@Before
	public void setUp() {
		format = new DelimitedInputFormat<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String readRecord(String reuse, byte[] bytes, int offset, int numBytes) throws IOException {
				return new String(bytes, offset, numBytes);
			}
		};
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReadFilesNullInputFormat() throws IOException {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final String tempFile = "/my/imaginary_file";

		env.readFile(null, tempFile);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReadFilesNullFile() throws IOException {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.readFile(format, (String) null);
	}

	@Test
	public void testReadFilesOneFile() throws IOException {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final String tempFile = "/my/imaginary_file";

		DataSet<String> data = env.readFile(format, tempFile);

		assertNotNull("DataSet should not be null", data);

		Path[] filePaths = format.getFilePaths();

		assertNotNull("Should not be null.", filePaths);
		assertEquals("The number of file paths should be correct.", 1, filePaths.length);
		assertEquals("File paths should be correct.", tempFile, filePaths[0].toUri().toString());
	}
	
	@Test
	public void testReadFiles() throws IOException {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final String tempFile = "/my/imaginary_file";
		final String tempFile2 = "/my/imaginary_file2";

		DataSet<String> data = env.readFile(format, tempFile, tempFile2);

		assertNotNull("DataSet should not be null.", data);

		Path[] filePaths = format.getFilePaths();

		assertNotNull("Should not be null.", filePaths);
		assertEquals("The number of file paths should be correct.", 2, filePaths.length);
		assertEquals("File paths should be correct.", tempFile, filePaths[0].toUri().toString());
		assertEquals("File paths should be correct.", tempFile2, filePaths[1].toUri().toString());
	}
}
