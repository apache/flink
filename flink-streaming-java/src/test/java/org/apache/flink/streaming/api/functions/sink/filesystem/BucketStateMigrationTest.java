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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.util.FileUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Test for restoring bucket state from the old version.
 */
public class BucketStateMigrationTest {

	private static final String version = "1.10";
	private static final Path prefixPath = new Path("src/test/resources/streaming-file-sink-state-migration/version-" + version);
	private static final Path inProgressOutputPath = new Path(prefixPath, "/in-progress/output/");
	private static final Path inProgressSnapshotPath = new Path(prefixPath, "/in-progress/snapshot");
	private static final Path inProgressAndPendingOutputPath = new Path(prefixPath, "/in-progress-and-pending/output/");
	private static final Path inProgressAndPendingSnapshotPath = new Path(prefixPath, "/in-progress-and-pending/snapshot");
	private static final Path pendingOutputPath = new Path(prefixPath, "/pending/output/");
	private static final Path pendingSnapshotPath = new Path(prefixPath, "/pending/snapshot");
	private static final Path templatePath = new Path(prefixPath + "-template");

	@BeforeClass
	public static void prepareDirectory() throws IOException {
		final FileSystem fs = FileSystem.get(templatePath.toUri());
		if (fs.exists(prefixPath)) {
			fs.delete(prefixPath, true);
		}
		if (fs.exists(templatePath)) {
			FileUtils.copy(templatePath, prefixPath, false);
		}
	}

	@AfterClass
	public static void destroyDirectory() throws IOException {
		final FileSystem fs = FileSystem.get(templatePath.toUri());
		if (fs.exists(prefixPath)) {
			fs.delete(prefixPath, true);
		}
	}

	@Test
	public void testRestoreInProgressFile() throws Exception {

		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testSink = createSink(inProgressOutputPath)) {

			final java.nio.file.Path[] beforeRestoreItems =
				FileUtils.listDirectory(java.nio.file.Paths.get(inProgressOutputPath.toString() + "/bucket-1"));
			Assert.assertEquals(1, beforeRestoreItems.length);

			if (!(beforeRestoreItems[0].toString().contains(".part-0-0.inprogress"))) {
				Assert.fail("Find the unexpected file " + beforeRestoreItems[0].toString());
			}

			testSink.setup();
			testSink.initializeState(inProgressSnapshotPath.toString());
			testSink.open();

			final java.nio.file.Path[] afterRestoreItems =
				FileUtils.listDirectory(java.nio.file.Paths.get(inProgressOutputPath.toString() + "/bucket-1"));

			Assert.assertEquals(1, afterRestoreItems.length);

			if (!(afterRestoreItems[0].toString().contains(".part-0-0.inprogress"))) {
				Assert.fail("Find the unexpected file " + afterRestoreItems[0].toString());
			}

			final AbstractUdfStreamOperator abstractUdfStreamOperator = (AbstractUdfStreamOperator) testSink.getOperator();
			final StreamingFileSink streamingFileSink = (StreamingFileSink) abstractUdfStreamOperator.getUserFunction();

			Assert.assertEquals(1, streamingFileSink.getBuckets().getActiveBuckets().size());

			final Map<String, Bucket<Tuple2<String, Integer>, String>>
				buckets = (Map<String, Bucket<Tuple2<String, Integer>, String>>) streamingFileSink.getBuckets().getActiveBuckets();
			final Bucket<Tuple2<String, Integer>, String> bucket = buckets.get("bucket-1");

			Assert.assertEquals(14, bucket.getInProgressPart().getSize());
			Assert.assertEquals(0, bucket.getPendingFileRecoverablesForCurrentCheckpoint().size());
		}
	}

	@Test
	public void testRestoreInProgressAndPendingFile() throws Exception {

		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testSink = createSink(inProgressAndPendingOutputPath)) {
			final java.nio.file.Path[] beforeRestoreItems =
				FileUtils.listDirectory(java.nio.file.Paths.get(inProgressAndPendingOutputPath.toString() + "/bucket-1"));
			Assert.assertEquals(2, beforeRestoreItems.length);
			for (java.nio.file.Path path : beforeRestoreItems) {
				if (!(path.toString().contains(".part-0-0.inprogress") || path.toString().contains(".part-0-1.inprogress"))) {
					Assert.fail("Find the unexpected file " + path.toString());
				}
			}
			testSink.setup();
			testSink.initializeState(inProgressAndPendingSnapshotPath.toString());
			testSink.open();

			final java.nio.file.Path[] afterRestoreItems =
				FileUtils.listDirectory(java.nio.file.Paths.get(inProgressAndPendingOutputPath.toString() + "/bucket-1"));

			Assert.assertEquals(2, afterRestoreItems.length);

			for (java.nio.file.Path path : afterRestoreItems) {
				if (!(path.toString().endsWith("part-0-0") || path.toString().contains(".part-0-1.inprogress"))) {
					Assert.fail("Find the unexpected file " + path.toString());
				}
			}

			final AbstractUdfStreamOperator abstractUdfStreamOperator = (AbstractUdfStreamOperator) testSink.getOperator();
			final StreamingFileSink streamingFileSink = (StreamingFileSink) abstractUdfStreamOperator.getUserFunction();

			Assert.assertEquals(1, streamingFileSink.getBuckets().getActiveBuckets().size());

			final Map<String, Bucket<Tuple2<String, Integer>, String>>
				buckets = (Map<String, Bucket<Tuple2<String, Integer>, String>>) streamingFileSink.getBuckets().getActiveBuckets();
			final Bucket<Tuple2<String, Integer>, String> bucket = buckets.get("bucket-1");

			Assert.assertEquals(15, bucket.getInProgressPart().getSize());
			Assert.assertEquals(0, bucket.getPendingFileRecoverablesForCurrentCheckpoint().size());

		}
	}

	@Test
	public void testRestorePendingFile() throws Exception {

		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testSink = createSink(pendingOutputPath)) {
			final java.nio.file.Path[] beforeRestoreItems =
				FileUtils.listDirectory(java.nio.file.Paths.get(pendingOutputPath.toString() + "/bucket-1"));
			Assert.assertEquals(2, beforeRestoreItems.length);
			for (java.nio.file.Path path : beforeRestoreItems) {
				if (!(path.toString().contains(".part-0-0.inprogress") || path.toString().contains(".part-0-1.inprogress"))) {
					Assert.fail("Find the unexpected file " + path.toString());
				}
			}
			testSink.setup();
			testSink.initializeState(pendingSnapshotPath.toString());
			testSink.open();

			final java.nio.file.Path[] afterRestoreItems =
				FileUtils.listDirectory(java.nio.file.Paths.get(pendingOutputPath.toString() + "/bucket-1"));

			Assert.assertEquals(2, afterRestoreItems.length);

			for (java.nio.file.Path path : afterRestoreItems) {
				if (!(path.toString().endsWith("part-0-0") || path.toString().endsWith("part-0-1"))) {
					Assert.fail("Find the unexpected file " + path.toString());
				}
			}

			final AbstractUdfStreamOperator abstractUdfStreamOperator = (AbstractUdfStreamOperator) testSink.getOperator();
			final StreamingFileSink streamingFileSink = (StreamingFileSink) abstractUdfStreamOperator.getUserFunction();

			final Map<String, Bucket<Tuple2<String, Integer>, String>>
				buckets = (Map<String, Bucket<Tuple2<String, Integer>, String>>) streamingFileSink.getBuckets().getActiveBuckets();
			final Bucket<Tuple2<String, Integer>, String> bucket = buckets.get("bucket-1");

			Assert.assertNull(bucket);
		}
	}

	// ------------------------------------------------------------------------
	//  Utils for preparing old version snapshot of bucket state.
	// ------------------------------------------------------------------------

	@Test
	@Ignore
	public void prepareSnapshots() throws Exception {

		final FileSystem fs = FileSystem.get(prefixPath.toUri());

		if (fs.exists(prefixPath)) {
			fs.delete(prefixPath, true);
		}

		prepareForInProgressSnapshot(fs);

		prepareForInProgressAndPendingFileSnapshot(fs);

		prepareForPendingFileSnapshot(fs);

		if (fs.exists(templatePath)) {
			fs.delete(templatePath, true);
		}
		fs.rename(prefixPath, templatePath);
	}

	private void prepareForInProgressSnapshot(final FileSystem fileSystem) throws Exception {

		fileSystem.mkdirs(inProgressOutputPath);

		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testSink = createSink(inProgressOutputPath)) {
			testSink.open();
			testSink.setup();

			// only has in-progress file
			testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 1234)));

			OperatorSubtaskState snapshot = testSink.snapshot(0L, 0L);
			OperatorSnapshotUtil.writeStateHandle(snapshot, inProgressSnapshotPath.toString());
		}
	}

	private void prepareForInProgressAndPendingFileSnapshot(final FileSystem fileSystem) throws Exception {

		fileSystem.mkdirs(inProgressAndPendingOutputPath);

		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testSink = createSink(inProgressAndPendingOutputPath)) {
			testSink.open();
			testSink.setup();

			testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 1234)));
			testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 23456)));
			testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 34567)));
			final OperatorSubtaskState snapshot = testSink.snapshot(0L, 0L);
			OperatorSnapshotUtil.writeStateHandle(snapshot, inProgressAndPendingSnapshotPath.toString());
		}
	}

	private void prepareForPendingFileSnapshot(final FileSystem fileSystem) throws Exception {

		fileSystem.mkdirs(pendingOutputPath);

		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testSink = createSink(pendingOutputPath)) {
			testSink.open();
			testSink.setup();

			testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 1234)));
			testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 23456)));
			testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 34567)));
			testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 0)));
			final OperatorSubtaskState snapshot = testSink.snapshot(0L, 0L);
			OperatorSnapshotUtil.writeStateHandle(snapshot, pendingSnapshotPath.toString());
		}

	}

	private static OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createSink(final Path outputPath) throws Exception {
		return TestUtils.createBucketStateMigrationTestSink(
			new File(outputPath.toString()),
			15,
			2,
			0,
			new TestUtils.Tuple2Encoder(),
			new TestUtils.TupleToStringBucketer());
	}
}
