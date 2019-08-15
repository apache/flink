/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.util.IOUtils;
import org.apache.flink.util.NetUtils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StringWriter}.
 */
public class StringWriterTest {

	private static final String CHARSET_NAME = StandardCharsets.UTF_8.name();

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static MiniDFSCluster hdfsCluster;
	private static org.apache.hadoop.fs.FileSystem dfs;

	private static String outputDir;

	@BeforeClass
	public static void createHDFS() throws IOException {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

		File dataDir = TEMPORARY_FOLDER.newFolder();

		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();

		dfs = hdfsCluster.getFileSystem();

		outputDir = "hdfs://"
			+ NetUtils.hostAndPortToUrlString(hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort());
	}

	@AfterClass
	public static void destroyHDFS() {
		if (hdfsCluster != null) {
			hdfsCluster.shutdown();
			hdfsCluster = null;
		}
	}

	@Test
	public void testDuplicate() {
		StringWriter<String> writer = new StringWriter(StandardCharsets.UTF_16.name());
		writer.setSyncOnFlush(true);
		StringWriter<String> other = writer.duplicate();

		assertTrue(StreamWriterBaseComparator.equals(writer, other));

		writer.setSyncOnFlush(false);
		assertFalse(StreamWriterBaseComparator.equals(writer, other));
		assertFalse(StreamWriterBaseComparator.equals(writer, new StringWriter<>()));
	}

	@Test
	public void testMultiRowdelimiters() throws IOException {
		testRowDelimiter("\n");
		testRowDelimiter("\r\n");
		testRowDelimiter("*");
		testRowDelimiter("##");
	}

	private void testRowDelimiter(String rowDelimiter) throws IOException {
		String[] inputData = new String[] {"A", "B", "C", "D", "E"};

		Path outputPath = new Path(TEMPORARY_FOLDER.newFile().getAbsolutePath());

		StringWriter<String> writer = new StringWriter(CHARSET_NAME, rowDelimiter);
		try {
			writer.open(dfs, outputPath);
			for (String input: inputData) {
				writer.write(input);
			}
		}
		finally {
			writer.close();
		}

		try (FSDataInputStream inStream = dfs.open(outputPath)) {
			String expectedOutput = String.join(rowDelimiter, inputData);
			byte[] buffer = new byte[expectedOutput.getBytes(CHARSET_NAME).length];
			readFully(inStream, buffer);

			String outputData = new String(buffer, CHARSET_NAME);
			assertEquals(expectedOutput, outputData);
		}
	}

	private void readFully(InputStream in, byte[] buffer) throws IOException {
		IOUtils.readFully(in, buffer, 0, buffer.length);
	}
}
