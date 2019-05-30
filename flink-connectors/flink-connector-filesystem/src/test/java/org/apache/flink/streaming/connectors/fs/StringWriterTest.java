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

import org.apache.flink.util.NetUtils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Tests for {@link StringWriter}.
 */
public class StringWriterTest {

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	private static MiniDFSCluster hdfsCluster;
	private static org.apache.hadoop.fs.FileSystem dfs;

	private static String outputDir;

	@Before
	public void createHDFS() throws IOException {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

		File dataDir = tempFolder.newFolder();

		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();

		dfs = hdfsCluster.getFileSystem();

		outputDir = "hdfs://"
			+ NetUtils.hostAndPortToUrlString(hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort());
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
		String rowDelimiter1 = "\n";
		String testDat1 = "A" + rowDelimiter1 + "B" + rowDelimiter1 + "C" + rowDelimiter1 + "D" + rowDelimiter1 + "E";
		Path testFile1 = new Path(outputDir + "/test01");
		testRowdelimiter(rowDelimiter1, testDat1, StandardCharsets.UTF_8.name(), testFile1);

		String rowDelimiter2 = "\r\n";
		String testDat2 = "A" + rowDelimiter2 + "B" + rowDelimiter2 + "C" + rowDelimiter2 + "D" + rowDelimiter2 + "E";
		Path testFile2 = new Path(outputDir + "/test02");
		testRowdelimiter(rowDelimiter2, testDat2, StandardCharsets.UTF_8.name(), testFile2);

		String rowDelimiter3 = "*";
		String testDat3 = "A" + rowDelimiter3 + "B" + rowDelimiter3 + "C" + rowDelimiter3 + "D" + rowDelimiter3 + "E";
		Path testFile3 = new Path(outputDir + "/test03");
		testRowdelimiter(rowDelimiter3, testDat3, StandardCharsets.UTF_8.name(), testFile3);

		String rowDelimiter4 = "##";
		String testDat4 = "A" + rowDelimiter4 + "B" + rowDelimiter4 + "C" + rowDelimiter4 + "D" + rowDelimiter4 + "E";
		Path testFile4 = new Path(outputDir + "/test04");
		testRowdelimiter(rowDelimiter4, testDat4, StandardCharsets.UTF_8.name(), testFile4);

	}

	private void testRowdelimiter(String rowDelimiter, String inputData, String charset, Path outputFile) throws IOException {
		StringWriter<String> writer = new StringWriter(charset, rowDelimiter);
		writer.open(dfs, outputFile);
		StringTokenizer lineTokenizer = new StringTokenizer(inputData, rowDelimiter);
		while (lineTokenizer.hasMoreTokens()){
			writer.write(lineTokenizer.nextToken());
		}
		writer.close();
		FSDataInputStream inStream = dfs.open(outputFile);
		byte[] buffer = new byte[inputData.getBytes(charset).length];
		readFully(inStream, buffer);
		inStream.close();
		String outputData = new String(buffer, charset);
		Assert.assertEquals(inputData, outputData);

	}

	private void readFully(InputStream in, byte[] buffer) throws IOException {
		int pos = 0;
		int remaining = buffer.length;

		while (remaining > 0) {
			int read = in.read(buffer, pos, remaining);
			if (read == -1) {
				throw new EOFException();
			}

			pos += read;
			remaining -= read;
		}
	}
}
