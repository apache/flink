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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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

			PrintStream ps = new PrintStream(tempFile);
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
		} catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

	@Test
	public void testNestedFileRead() {
		String[] dirs = new String[]{"tmp/first/", "tmp/second/"};
		List<String> expectedFiles = new ArrayList<>();

		try {
			for (String dir : dirs) {
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
			for (FileInputSplit split : splits) {
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
				|| (lineBreaker.equals(delimiter))) {

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

		} catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

	/**
	 * Test different file encodings,for example:UTF-8, UTF-8 with bom, UTF-16LE, UTF-16BE, UTF-32LE, UTF-32BE.
	 */
	@Test
	public void testFileCharset() {
		String data = "Hello|ハロー|при\\вет|Bon^*|\\|<>|jour|Сайн. байна уу|안녕*하세요.";
		// Default separator
		testAllFileCharset(data);
		// Specified separator
		testAllFileCharset(data, "^*|\\|<>|");
	}

	private void testAllFileCharset(String data) {
		testAllFileCharset(data, "");
	}

	private void testAllFileCharset(String data, String delimiter) {
		try {
			// test UTF-8, no bom, UTF-8
			testFileCharset(data, "UTF-8", false, "UTF-8", delimiter);
			// test UTF-8, have bom, UTF-8
			testFileCharset(data, "UTF-8", true, "UTF-8", delimiter);
			// test UTF-16BE, no, UTF-16
			testFileCharset(data, "UTF-16BE", false, "UTF-16", delimiter);
			// test UTF-16BE, yes, UTF-16
			testFileCharset(data, "UTF-16BE", true, "UTF-16", delimiter);
			// test UTF-16LE, no, UTF-16LE
			testFileCharset(data, "UTF-16LE", false, "UTF-16LE", delimiter);
			// test UTF-16LE, yes, UTF-16
			testFileCharset(data, "UTF-16LE", true, "UTF-16", delimiter);
			// test UTF-16BE, no, UTF-16BE
			testFileCharset(data, "UTF-16BE", false, "UTF-16BE", delimiter);
			// test UTF-16BE, yes, UTF-16LE
			testFileCharset(data, "UTF-16BE", true, "UTF-16LE", delimiter);
			// test UTF-16LE, yes, UTF-16BE
			testFileCharset(data, "UTF-16LE", true, "UTF-16BE", delimiter);
			// test UTF-32BE, no, UTF-32
			testFileCharset(data, "UTF-32BE", false, "UTF-32", delimiter);
			// test UTF-32BE, yes, UTF-32
			testFileCharset(data, "UTF-32BE", true, "UTF-32", delimiter);
			// test UTF-32LE, yes, UTF-32
			testFileCharset(data, "UTF-32LE", true, "UTF-32", delimiter);
			// test UTF-32LE, no, UTF-32LE
			testFileCharset(data, "UTF-32LE", false, "UTF-32LE", delimiter);
			// test UTF-32BE, no, UTF-32BE
			testFileCharset(data, "UTF-32BE", false, "UTF-32BE", delimiter);
			// test UTF-32BE, yes, UTF-32LE
			testFileCharset(data, "UTF-32BE", true, "UTF-32LE", delimiter);
			// test UTF-32LE, yes, UTF-32BE
			testFileCharset(data, "UTF-32LE", true, "UTF-32BE", delimiter);
			//------------------Don't set the charset------------------------
			// test UTF-8, have bom, Don't set the charset
			testFileCharset(data, "UTF-8", true, delimiter);
			// test UTF-8, no bom, Don't set the charset
			testFileCharset(data, "UTF-8", false, delimiter);
			// test UTF-16BE, no bom, Don't set the charset
			testFileCharset(data, "UTF-16BE", false, delimiter);
			// test UTF-16BE, have bom, Don't set the charset
			testFileCharset(data, "UTF-16BE", true, delimiter);
			// test UTF-16LE, have bom, Don't set the charset
			testFileCharset(data, "UTF-16LE", true, delimiter);
			// test UTF-32BE, no bom, Don't set the charset
			testFileCharset(data, "UTF-32BE", false, delimiter);
			// test UTF-32BE, have bom, Don't set the charset
			testFileCharset(data, "UTF-32BE", true, delimiter);
			// test UTF-32LE, have bom, Don't set the charset
			testFileCharset(data, "UTF-32LE", true, delimiter);
		} catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

	/**
	 * To create UTF EncodedFile.
	 *
	 * @param data
	 * @param fileCharset
	 * @param hasBom
	 * @return
	 */
	private File createUTFEncodedFile(String data, String fileCharset, boolean hasBom) throws Exception {
		BufferedWriter bw = null;
		OutputStreamWriter osw = null;
		FileOutputStream fos = null;

		byte[] bom = new byte[]{};
		if (hasBom) {
			switch (fileCharset) {
				case "UTF-8":
					bom = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
					break;
				case "UTF-16":
					bom = new byte[]{(byte) 0xFE, (byte) 0xFF};
					break;
				case "UTF-16LE":
					bom = new byte[]{(byte) 0xFF, (byte) 0xFE};
					break;
				case "UTF-16BE":
					bom = new byte[]{(byte) 0xFE, (byte) 0xFF};
					break;
				case "UTF-32":
					bom = new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0xFE, (byte) 0xFF};
					break;
				case "UTF-32LE":
					bom = new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0x00, (byte) 0x00};
					break;
				case "UTF-32BE":
					bom = new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0xFE, (byte) 0xFF};
					break;
				default:
					throw new Exception("can not find the utf code");
			}
		}

		// create input file
		File tempFile = File.createTempFile("TextInputFormatTest", "tmp");
		tempFile.deleteOnExit();
		tempFile.setWritable(true);
		fos = new FileOutputStream(tempFile, true);

		if (tempFile.length() < 1) {
			if (hasBom) {
				fos.write(bom);
			}
		}

		osw = new OutputStreamWriter(fos, fileCharset);
		bw = new BufferedWriter(osw);
		bw.write(data);
		bw.newLine();

		bw.close();
		fos.close();

		return tempFile;
	}

	private void testFileCharset(String data, String fileCharset, Boolean hasBom, String delimiter) {
		testFileCharset(data, fileCharset, hasBom, null, delimiter);
	}

	private void testFileCharset(String data, String fileCharset, Boolean hasBom, String specifiedCharset, String delimiter) {
		try {
			int offset = 0;
			String carriageReturn = java.security.AccessController.doPrivileged(
				new sun.security.action.GetPropertyAction("line.separator"));
			String delimiterString = delimiter.isEmpty() ? carriageReturn : delimiter;
			byte[] delimiterBytes = delimiterString.getBytes(fileCharset);
			String[] utfArray = {"UTF-8", "UTF-16", "UTF-16LE", "UTF-16BE"};
			if (hasBom) {
				if (Arrays.asList(utfArray).contains(fileCharset)) {
					offset = 1;
				}
			}

			File tempFile = createUTFEncodedFile(data, fileCharset, hasBom);

			TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
			if (specifiedCharset != null) {
				inputFormat.setCharsetName(specifiedCharset);
			}
			if (delimiterBytes.length > 0) {
				inputFormat.setDelimiter(delimiterBytes);
			}

			Configuration parameters = new Configuration();
			inputFormat.configure(parameters);

			FileInputSplit[] splits = inputFormat.createInputSplits(1);
			assertTrue("expected at least one input split", splits.length >= 1);
			inputFormat.open(splits[0]);

			String result = "";
			int i = 0;
			data = data + carriageReturn;
			String delimiterStr = new String(delimiterBytes, 0, delimiterBytes.length, fileCharset);
			String[] strArr = data.split(delimiterStr
				.replace("\\", "\\\\")
				.replace("^", "\\^")
				.replace("|", "\\|")
				.replace("[", "\\[")
				.replace("*", "\\*")
				.replace(".", "\\.")
			);

			while (!inputFormat.reachedEnd()) {
				if (i < strArr.length) {
					result = inputFormat.nextRecord("");
					if (i == 0) {
						result = result.substring(offset);
					}
					if (Charset.forName(fileCharset) != inputFormat.getCharset()) {
						assertNotEquals(strArr[i], result);
					} else {
						assertEquals(strArr[i], result);
					}
					i++;
				} else {
					inputFormat.nextRecord("");
				}
			}
			assertTrue(inputFormat.reachedEnd() || null == inputFormat.nextRecord(result));
		} catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

	@Test
	public void testFileCharsetReadByMultiSplits() {
		String carriageReturn = java.security.AccessController.doPrivileged(
			new sun.security.action.GetPropertyAction("line.separator"));
		final String data = "First line" + carriageReturn + "Second line";
		try {
			File tempFile = createUTFEncodedFile(data, "UTF-16", false);

			TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
			inputFormat.setCharsetName("UTF-32");

			Configuration parameters = new Configuration();
			inputFormat.configure(parameters);

			FileInputSplit[] splits = inputFormat.createInputSplits(3);
			assertTrue("expected at least one input split", splits.length >= 1);
			String result = "";
			for (FileInputSplit split : splits) {
				inputFormat.open(split);
				result = inputFormat.nextRecord("");
			}
			assertTrue(inputFormat.reachedEnd() || null == inputFormat.nextRecord(result));
		} catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}
}
