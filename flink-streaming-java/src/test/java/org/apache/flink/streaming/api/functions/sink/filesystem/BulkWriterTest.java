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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Tests for the {@link StreamingFileSink} with {@link BulkWriter}.
 */
public class BulkWriterTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void testCustomBulkWriter() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		// we set the max bucket size to small so that we can know when it rolls
		try (
				OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness =
						TestUtils.createTestSinkWithBulkEncoder(
								outDir,
								1,
								0,
								10L,
								new TestUtils.TupleToStringBucketer(),
								new TestBulkWriterFactory(),
								new DefaultBucketFactoryImpl<>())
		) {
			testPartFiles(testHarness, outDir, ".part-0-0.inprogress", ".part-0-1.inprogress");
		}
	}

	@Test
	public void testCustomBulkWriterWithPartConfig() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();

		// we set the max bucket size to small so that we can know when it rolls
		try (
			OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness =
					TestUtils.createTestSinkWithBulkEncoder(
							outDir,
							1,
							0,
							10L,
							new TestUtils.TupleToStringBucketer(),
							new TestBulkWriterFactory(),
							new DefaultBucketFactoryImpl<>(),
							"prefix",
							".ext")
		) {
			testPartFiles(testHarness, outDir, ".prefix-0-0.ext.inprogress", ".prefix-0-1.ext.inprogress");
		}
	}

	private void testPartFiles(
			OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness,
			File outDir,
			String partFileName1,
			String partFileName2) throws Exception {

		testHarness.setup();
		testHarness.open();

		// this creates a new bucket "test1" and part-0-0
		testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 1), 1L));
		TestUtils.checkLocalFs(outDir, 1, 0);

		// we take a checkpoint so we roll.
		testHarness.snapshot(1L, 1L);

		// these will close part-0-0 and open part-0-1
		testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 2), 2L));
		testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 3), 3L));

		// we take a checkpoint so we roll again.
		testHarness.snapshot(2L, 2L);

		TestUtils.checkLocalFs(outDir, 2, 0);

		Map<File, String> contents = TestUtils.getFileContentByPath(outDir);
		int fileCounter = 0;
		for (Map.Entry<File, String> fileContents : contents.entrySet()) {
			if (fileContents.getKey().getName().contains(partFileName1)) {
				fileCounter++;
				Assert.assertEquals("test1@1\n", fileContents.getValue());
			} else if (fileContents.getKey().getName().contains(partFileName2)) {
				fileCounter++;
				Assert.assertEquals("test1@2\ntest1@3\n", fileContents.getValue());
			}
		}
		Assert.assertEquals(2L, fileCounter);

		// we acknowledge the latest checkpoint, so everything should be published.
		testHarness.notifyOfCompletedCheckpoint(2L);

		TestUtils.checkLocalFs(outDir, 0, 2);
	}

	/**
	 * A {@link BulkWriter} used for the tests.
	 */
	private static class TestBulkWriter implements BulkWriter<Tuple2<String, Integer>> {

		private static final Charset CHARSET = StandardCharsets.UTF_8;

		private final FSDataOutputStream stream;

		TestBulkWriter(final FSDataOutputStream stream) {
			this.stream = Preconditions.checkNotNull(stream);
		}

		@Override
		public void addElement(Tuple2<String, Integer> element) throws IOException {
			stream.write((element.f0 + '@' + element.f1 + '\n').getBytes(CHARSET));
		}

		@Override
		public void flush() throws IOException {
			stream.flush();
		}

		@Override
		public void finish() throws IOException {
			flush();
		}
	}

	/**
	 * A {@link BulkWriter.Factory} used for the tests.
	 */
	private static class TestBulkWriterFactory implements BulkWriter.Factory<Tuple2<String, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public BulkWriter<Tuple2<String, Integer>> create(FSDataOutputStream out) {
			return new TestBulkWriter(out);
		}
	}
}
