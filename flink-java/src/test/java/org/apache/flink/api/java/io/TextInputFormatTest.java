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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link TextInputFormat}.
 */
public class TextInputFormatTest {
	@Test
	public void testSimpleRead() {
		final String first = "First line";
		final String second = "Second line";

		try {
			// create input file
			File tempFile = File.createTempFile("TextInputFormatTest", "tmp");
			tempFile.deleteOnExit();
			tempFile.setWritable(true);

			PrintStream ps = new  PrintStream(tempFile);
			ps.println(first);
			ps.println(second);
			ps.close();

			TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));

			Configuration parameters = new Configuration();
			inputFormat.configure(parameters);

			FileInputSplit[] splits = inputFormat.createInputSplits(1);
			assertTrue("expected at least one input split", splits.length >= 1);

			inputFormat.open(splits[0]);

			String result = "";

			assertFalse(inputFormat.reachedEnd());
			result = inputFormat.nextRecord("");
			assertNotNull("Expecting first record here", result);
			assertEquals(first, result);

			assertFalse(inputFormat.reachedEnd());
			result = inputFormat.nextRecord(result);
			assertNotNull("Expecting second record here", result);
			assertEquals(second, result);

			assertTrue(inputFormat.reachedEnd() || null == inputFormat.nextRecord(result));
		}
		catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

	@Test
	public void testNestedFileRead() {
		String[] dirs = new String[] {"tmp/first/", "tmp/second/"};
		List<String> expectedFiles = new ArrayList<>();

		try {
			for (String dir: dirs) {
				// create input file
				File tmpDir = new File(dir);
				if (!tmpDir.exists()) {
					tmpDir.mkdirs();
				}

				File tempFile = File.createTempFile("TextInputFormatTest", ".tmp", tmpDir);
				tempFile.deleteOnExit();

				expectedFiles.add(new Path(tempFile.getAbsolutePath()).makeQualified(FileSystem.getLocalFileSystem()).toString());
			}
			File parentDir = new File("tmp");

			TextInputFormat inputFormat = new TextInputFormat(new Path(parentDir.toURI()));
			inputFormat.setNestedFileEnumeration(true);
			inputFormat.setNumLineSamples(10);

			// this is to check if the setter overrides the configuration (as expected)
			Configuration config = new Configuration();
			config.setBoolean("recursive.file.enumeration", false);
			config.setString("delimited-format.numSamples", "20");
			inputFormat.configure(config);

			assertTrue(inputFormat.getNestedFileEnumeration());
			assertTrue(inputFormat.getNumLineSamples() == 10);

			FileInputSplit[] splits = inputFormat.createInputSplits(expectedFiles.size());

			List<String> paths = new ArrayList<>();
			for (FileInputSplit split: splits) {
				paths.add(split.getPath().toString());
			}

			Collections.sort(expectedFiles);
			Collections.sort(paths);
			for (int i = 0; i < expectedFiles.size(); i++) {
				assertTrue(expectedFiles.get(i).equals(paths.get(i)));
			}

		} catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

	/**
	 * This tests cases when line ends with \r\n and \n is used as delimiter, the last \r should be removed.
	 */
	@Test
	public void testRemovingTrailingCR() {

		testRemovingTrailingCR("\n", "\n");
		testRemovingTrailingCR("\r\n", "\n");

		testRemovingTrailingCR("|", "|");
		testRemovingTrailingCR("|", "\n");
	}

	private void testRemovingTrailingCR(String lineBreaker, String delimiter) {
		File tempFile = null;

		String first = "First line";
		String second = "Second line";
		String content = first + lineBreaker + second + lineBreaker;

		try {
			// create input file
			tempFile = File.createTempFile("TextInputFormatTest", "tmp");
			tempFile.deleteOnExit();
			tempFile.setWritable(true);

			OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
			wrt.write(content);
			wrt.close();

			TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
			inputFormat.setFilePath(tempFile.toURI().toString());

			Configuration parameters = new Configuration();
			inputFormat.configure(parameters);

			inputFormat.setDelimiter(delimiter);

			FileInputSplit[] splits = inputFormat.createInputSplits(1);

			inputFormat.open(splits[0]);

			String result = "";
			if ((delimiter.equals("\n") && (lineBreaker.equals("\n") || lineBreaker.equals("\r\n")))
					|| (lineBreaker.equals(delimiter))){

				result = inputFormat.nextRecord("");
				assertNotNull("Expecting first record here", result);
				assertEquals(first, result);

				result = inputFormat.nextRecord(result);
				assertNotNull("Expecting second record here", result);
				assertEquals(second, result);

				result = inputFormat.nextRecord(result);
				assertNull("The input file is over", result);

			} else {
				result = inputFormat.nextRecord("");
				assertNotNull("Expecting first record here", result);
				assertEquals(content, result);
			}

		}
		catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

}
