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
import org.apache.flink.core.fs.Path;

import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link PrimitiveInputFormat}.
 */
public class PrimitiveInputFormatTest {

	private static final Path PATH = new Path("an/ignored/file/");

	@Test
	public void testStringInput() {
		try {
			final String fileContent = "abc||def||||";
			final FileInputSplit split = createInputSplit(fileContent);

			final PrimitiveInputFormat<String> format = new PrimitiveInputFormat<String>(PATH, "||", String.class);

			final Configuration parameters = new Configuration();
			format.configure(parameters);
			format.open(split);

			String result = null;

			result = format.nextRecord(result);
			assertEquals("abc", result);

			result = format.nextRecord(result);
			assertEquals("def", result);

			result = format.nextRecord(result);
			assertEquals("", result);

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			ex.printStackTrace();
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testIntegerInput() throws IOException {
		try {
			final String fileContent = "111|222|";
			final FileInputSplit split = createInputSplit(fileContent);

			final PrimitiveInputFormat<Integer> format = new PrimitiveInputFormat<Integer>(PATH, "|", Integer.class);

			format.configure(new Configuration());
			format.open(split);

			Integer result = null;
			result = format.nextRecord(result);
			assertEquals(Integer.valueOf(111), result);

			result = format.nextRecord(result);
			assertEquals(Integer.valueOf(222), result);

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testDoubleInputLinewise() throws IOException {
		try {
			final String fileContent = "1.21\n2.23\n";
			final FileInputSplit split = createInputSplit(fileContent);

			final PrimitiveInputFormat<Double> format = new PrimitiveInputFormat<Double>(PATH, Double.class);

			format.configure(new Configuration());
			format.open(split);

			Double result = null;
			result = format.nextRecord(result);
			assertEquals(Double.valueOf(1.21), result);

			result = format.nextRecord(result);
			assertEquals(Double.valueOf(2.23), result);

			result = format.nextRecord(result);
			assertNull(result);
			assertTrue(format.reachedEnd());
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test
	public void testRemovingTrailingCR() {
		try {
			String first = "First line";
			String second = "Second line";
			String fileContent = first + "\r\n" + second + "\r\n";
			final FileInputSplit split = createInputSplit(fileContent);

			final PrimitiveInputFormat<String> format = new PrimitiveInputFormat<String>(PATH, String.class);

			format.configure(new Configuration());
			format.open(split);

			String result = null;

			result = format.nextRecord(result);
			assertEquals(first, result);

			result = format.nextRecord(result);
			assertEquals(second, result);
		}
		catch (Exception ex) {
			fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
		}
	}

	@Test(expected = IOException.class)
	public void testFailingInput() throws IOException {

		final String fileContent = "111|222|asdf|17";
		final FileInputSplit split = createInputSplit(fileContent);

		final PrimitiveInputFormat<Integer> format = new PrimitiveInputFormat<Integer>(PATH, "|", Integer.class);

		format.configure(new Configuration());
		format.open(split);

		Integer result = null;
		result = format.nextRecord(result);
		assertEquals(Integer.valueOf(111), result);

		result = format.nextRecord(result);
		assertEquals(Integer.valueOf(222), result);

		result = format.nextRecord(result);
	}

	private FileInputSplit createInputSplit(String content) throws IOException {
		File tempFile = File.createTempFile("test_contents", "tmp");
		tempFile.deleteOnExit();

		try (FileWriter wrt = new FileWriter(tempFile)) {
			wrt.write(content);
		}

		return new FileInputSplit(0, new Path(tempFile.toURI().toString()), 0, tempFile.length(), new String[] {"localhost"});
	}

}
