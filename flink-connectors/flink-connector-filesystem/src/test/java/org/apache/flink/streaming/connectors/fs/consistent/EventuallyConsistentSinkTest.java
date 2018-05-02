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

package org.apache.flink.streaming.connectors.fs.consistent;

import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Test for the {@code EventuallyConsistentBucketingSink }.
 */
public class EventuallyConsistentSinkTest {
	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	private static MiniDFSCluster hdfsCluster;

	private OneInputStreamOperatorTestHarness<String, Object> createTestSink(File bufferDir, File dataDir, int totalParallelism, int taskIdx) throws Exception {
		EventuallyConsistentBucketingSink<String> sink = new EventuallyConsistentBucketingSink<String>(dataDir.getAbsolutePath())
			.setWriter(new StringWriter<String>())
			.setBufferDirectory(bufferDir.getAbsolutePath());

		return createTestSink(sink, totalParallelism, taskIdx);
	}

	private <T> OneInputStreamOperatorTestHarness<T, Object> createTestSink(
		EventuallyConsistentBucketingSink<T> sink, int totalParallelism, int taskIdx) throws Exception {
		return new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 10, totalParallelism, taskIdx);
	}

	@BeforeClass
	public static void createHDFS() throws IOException {
		Configuration conf = new Configuration();

		File dataDir = tempFolder.newFolder();

		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();

	}

	@AfterClass
	public static void destroyHDFS() {
		hdfsCluster.shutdown();
	}

	@Test
	public void testEventualConsistencySink() throws Exception {
		final File bufferDir = tempFolder.newFolder();
		final File outDir = tempFolder.newFolder();

		OneInputStreamOperatorTestHarness<String, Object> testHarness = createTestSink(bufferDir, outDir, 1, 0);
		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>("test1", 1L));
		testHarness.processElement(new StreamRecord<>("test2", 1L));

		testHarness.snapshot(0, 2);
		testHarness.notifyOfCompletedCheckpoint(0);

		testHarness.processElement(new StreamRecord<>("test3", 3L));
		testHarness.processElement(new StreamRecord<>("test4", 3L));

		testHarness.snapshot(1, 4);
		testHarness.notifyOfCompletedCheckpoint(1);

		testHarness.close();

		checkFs(outDir, 2, 2);
	}

	@Test
	public void testEventualConsistencySinkWithTimedOutCheckpoints() throws Exception {
		final File bufferDir = tempFolder.newFolder();
		final File outDir = tempFolder.newFolder();

		OneInputStreamOperatorTestHarness<String, Object> testHarness = createTestSink(bufferDir, outDir, 1, 0);
		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>("test1", 1L));
		testHarness.processElement(new StreamRecord<>("test2", 1L));

		testHarness.snapshot(0, 2);
		//Do not notify checkpoint complete

		testHarness.processElement(new StreamRecord<>("test3", 3L));
		testHarness.processElement(new StreamRecord<>("test4", 3L));

		testHarness.snapshot(1, 4);
		testHarness.notifyOfCompletedCheckpoint(1);

		testHarness.close();

		checkFs(outDir, 2, 2);
	}

	@Test
	public void testEventualConsistencySinkWithConcurrentCheckpoints() throws Exception {
		final File bufferDir = tempFolder.newFolder();
		final File outDir = tempFolder.newFolder();

		OneInputStreamOperatorTestHarness<String, Object> testHarness = createTestSink(bufferDir, outDir, 1, 0);
		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>("test1", 1L));
		testHarness.processElement(new StreamRecord<>("test2", 1L));

		testHarness.snapshot(0, 2);
		testHarness.processElement(new StreamRecord<>("test3", 3L));

		testHarness.snapshot(1, 4);
		testHarness.processElement(new StreamRecord<>("test4", 3L));

		testHarness.notifyOfCompletedCheckpoint(0);
		testHarness.notifyOfCompletedCheckpoint(1);

		testHarness.close();

		checkFs(outDir, 2, 2);
	}

	private void checkFs(File outDir, int done, int numFiles) throws IOException {
		int countDone = 0;
		int countPart = 0;

		for (File file : FileUtils.listFiles(outDir, null, true)) {
			if (file.getAbsolutePath().endsWith("crc")) {
				continue;
			}

			String path = file.getPath();
			if (path.endsWith("_DONE")) {
				countDone++;
			} else if (path.contains("part")) {
				countPart++;
			}
		}

		Assert.assertEquals(done, countDone);
		Assert.assertEquals(numFiles, countPart);
	}
}
