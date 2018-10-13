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
	 * Test different file encodings,for example: UTF-8, UTF-8 with bom, UTF-16LE, UTF-16BE, UTF-32LE, UTF-32BE.
	 */
	@Test
	public void testFileCharset() {
		String first = "First line";

		// Test special different languages
		for (final String data : new String[]{"Hello", "ハロー", "привет", "Bonjour", "Сайн байна уу", "안녕하세요."}) {
			testAllFileCharsetNoDelimiter(data);
		}

		// Test special symbol
		for (final String delimiterStr : new String[]{"\\", "^", "|", "[", ".", "*"}) {
			first = "Fir" + delimiterStr + "st li" + delimiterStr + "ne";
			testAllFileCharsetWithDelimiter(first, delimiterStr);
		}
	}

	private void testAllFileCharsetNoDelimiter(String first) {
		testAllFileCharsetWithDelimiter(first, "");
	}

	private void testAllFileCharsetWithDelimiter(String first, String delimiter) {
		try {
			final byte[] noBom = new byte[]{};
			final byte[] utf8Bom = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
			final byte[] utf16LEBom = new byte[]{(byte) 0xFF, (byte) 0xFE};
			final byte[] utf16BEBom = new byte[]{(byte) 0xFE, (byte) 0xFF};
			final byte[] utf32LEBom = new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0x00, (byte) 0x00};
			final byte[] utf32BEBom = new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0xFE, (byte) 0xFF};

			// test UTF-8 have bom
			testFileCharset(first, "UTF-8", "UTF-32", 1, utf8Bom, delimiter.getBytes("UTF-8"));
			// test UTF-8 without bom
			testFileCharset(first, "UTF-8", "UTF-8", 0, noBom, delimiter.getBytes("UTF-8"));
			// test UTF-16LE without bom
			testFileCharset(first, "UTF-16LE", "UTF-16LE", 0, noBom, delimiter.getBytes("UTF-16LE"));
			// test UTF-16BE without bom
			testFileCharset(first, "UTF-16BE", "UTF-16BE", 0, noBom, delimiter.getBytes("UTF-16BE"));
			// test UTF-16LE have bom
			testFileCharset(first, "UTF-16LE", "UTF-16LE", 1, utf16LEBom, delimiter.getBytes("UTF-16LE"));
			// test UTF-16BE have bom
			testFileCharset(first, "UTF-16BE", "UTF-16", 1, utf16BEBom, delimiter.getBytes("UTF-16BE"));
			// test UTF-32LE without bom
			testFileCharset(first, "UTF-32LE", "UTF-32LE", 0, noBom, delimiter.getBytes("UTF-32LE"));
			// test UTF-32BE without bom
			testFileCharset(first, "UTF-32BE", "UTF-32BE", 0, noBom, delimiter.getBytes("UTF-32BE"));
			// test UTF-32LE have bom
			testFileCharset(first, "UTF-32LE", "UTF-32LE", 0, utf32LEBom, delimiter.getBytes("UTF-32LE"));
			// test UTF-32BE have bom
			testFileCharset(first, "UTF-32BE", "UTF-32", 0, utf32BEBom, delimiter.getBytes("UTF-32BE"));
		} catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

	/**
	 * Test different file encodings.
	 *
	 * @param data
	 * @param fileCharset   File itself encoding
	 * @param targetCharset User specified code
	 * @param offset        Return result offset
	 * @param bom           Bom content
	 * @param delimiter
	 */
	private void testFileCharset(String data, String fileCharset, String targetCharset, int offset, byte[] bom, byte[] delimiter) {
		BufferedWriter bw = null;
		OutputStreamWriter osw = null;
		FileOutputStream fos = null;
		try {
			// create input file
			File tempFile = File.createTempFile("TextInputFormatTest", "tmp");
			tempFile.deleteOnExit();
			tempFile.setWritable(true);
//			System.out.println(tempFile.toString());
			fos = new FileOutputStream(tempFile, true);

			// write UTF8 BOM mark if file is empty
			if (tempFile.length() < 1) {
				if (bom.length > 0) {
					fos.write(bom);
				}
			}

			osw = new OutputStreamWriter(fos, fileCharset);
			bw = new BufferedWriter(osw);
			bw.write(data);
			bw.newLine();

			bw.close();
			fos.close();

			TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
			inputFormat.setCharsetName(targetCharset);
//			inputFormat.setCharset(targetCharset);

			if (delimiter.length > 0) {
				inputFormat.setDelimiter(delimiter);
			}

			Configuration parameters = new Configuration();
			inputFormat.configure(parameters);

			FileInputSplit[] splits = inputFormat.createInputSplits(1);
			assertTrue("expected at least one input split", splits.length >= 1);
			inputFormat.open(splits[0]);

			String result = "";

			if (delimiter.length <= 0) {
//				System.out.println("bomCharsetName:" + inputFormat.getCharset());

				assertFalse(inputFormat.reachedEnd());
				result = inputFormat.nextRecord("");
//				System.out.println(result);
				assertNotNull("Expecting first record here", result);
				assertEquals(data, result.substring(offset));

//				assertTrue(inputFormat.reachedEnd() || null == inputFormat.nextRecord(result));
			} else {
//				System.out.println("bomCharsetName:" + inputFormat.getCharset());
				int i = 0;
				data = data + java.security.AccessController.doPrivileged(
					new sun.security.action.GetPropertyAction("line.separator"));
				String delimiterStr = new String(delimiter, 0, delimiter.length, fileCharset);
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
//						System.out.println(result);
						assertEquals(strArr[i], result);
						i++;
					} else {
						inputFormat.nextRecord("");
					}
				}
				assertTrue(inputFormat.reachedEnd() || null == inputFormat.nextRecord(result));
			}


		} catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}

	@Test
	public void testFileCharsetReadByMultiSplits() {
		final String first = "First line";
		final String second = "Second line";

		try {
			// create input file
			File tempFile = File.createTempFile("TextInputFormatTest", "tmp");
			tempFile.deleteOnExit();
			tempFile.setWritable(true);

			PrintStream ps = new PrintStream(tempFile, "UTF-16");
			ps.println(first);
			ps.println(second);
			ps.close();

			TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
			inputFormat.setCharsetName("UTF-32");
//			inputFormat.setCharset("UTF-32");

			Configuration parameters = new Configuration();
			inputFormat.configure(parameters);

//			inputFormat.setDelimiter("\r");
//			inputFormat.setDelimiter("i");

			FileInputSplit[] splits = inputFormat.createInputSplits(3);
			assertTrue("expected at least one input split", splits.length >= 1);
			for (FileInputSplit split : splits) {
				inputFormat.open(split);
				System.out.println("bomCharsetName:" + inputFormat.getCharset());
				String result = inputFormat.nextRecord("");
				System.out.println(result);
			}
		} catch (Throwable t) {
			System.err.println("test failed with exception: " + t.getMessage());
			t.printStackTrace(System.err);
			fail("Test erroneous");
		}
	}
}
