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
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.util.FileUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.streaming.api.functions.sink.filesystem.TestUtils.MAX_PARALLELISM;

/**
 * Test for restoring bucket state from the old version.
 */
public class BucketStateMigrationTest {

	private static final String version = "1";
	private static final String classPathPrefix = "src/test/resources/";
	private static final String prefix = "streaming-file-sink-state-migration/version-" + version ;
	private static final String inProgressOutput = prefix + "/in-progress/output/";
	private static final String inProgressSnapshot = prefix + "/in-progress/snapshot";
	private static final String inProgressAndPendingOutput = prefix + "/in-progress-and-pending/output/";
	private static final String inProgressAndPendingSnapshot = prefix + "/in-progress-and-pending/snapshot";
	private static final String pendingOutput = prefix + "/pending/output/";
	private static final String pendingSnapShot = prefix + "/pending/snapshot";


	private static final Path prefixPath = new Path(classPathPrefix, prefix);
	private static final Path inProgressOutputPath = new Path(classPathPrefix, inProgressOutput);
	private static final Path inProgressSnapshotPath = new Path(classPathPrefix, inProgressSnapshot);
	private static final Path inProgressAndPendingOutputPath = new Path(classPathPrefix, inProgressAndPendingOutput);
	private static final Path inProgressAndPendingSnapshotPath = new Path(classPathPrefix, inProgressAndPendingSnapshot);
	private static final Path pendingOutputPath = new Path(classPathPrefix, pendingOutput);
	private static final Path pendingSnapshotPath = new Path(classPathPrefix, pendingSnapShot);

	private static final Path templatePath = new Path(prefixPath + "-template");

	@BeforeClass
	public static void prepareDierctory() throws IOException {
		final FileSystem fs = FileSystem.get(templatePath.toUri());
		if (fs.exists(prefixPath)) {
			fs.delete(prefixPath, true);
		}
		FileUtils.copy(templatePath, prefixPath, false);
	}

	@AfterClass
	public static void destroyDirectory() throws IOException {
		final FileSystem fs = FileSystem.get(templatePath.toUri());
		if (fs.exists(prefixPath)) {
			fs.delete(prefixPath, true);
		}
	}

	@Test
	@Ignore
	public void testPrepare() throws Exception {

		final FileSystem fs = FileSystem.get(prefixPath.toUri());

		if (fs.exists(prefixPath)) {
			fs.delete(prefixPath, true);
		}
		fs.mkdirs(inProgressOutputPath);
		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object>
			testSink = createSink(inProgressOutputPath);

		testSink.open();
		testSink.setup();

		// only has in-progress file
		testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 1234)));

		OperatorSubtaskState snapshot = testSink.snapshot(0L, 0L);
		OperatorSnapshotUtil.writeStateHandle(snapshot, inProgressSnapshotPath.toString());
		testSink.close();

		// in-progress file + pending file
		testSink = createSink(inProgressAndPendingOutputPath);
		testSink.open();
		testSink.setup();

		testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 1234)));
		testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 23456)));
		testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 34567)));
		snapshot = testSink.snapshot(0L, 0L);
		OperatorSnapshotUtil.writeStateHandle(snapshot, inProgressAndPendingSnapshotPath.toString());

		testSink.close();

		// only pending files

		testSink = createSink(pendingOutputPath);
		testSink.open();
		testSink.setup();

		testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 1234)));
		testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 23456)));
		testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 34567)));
		testSink.processElement(new StreamRecord<>(Tuple2.of("bucket-1", 0)));
		snapshot = testSink.snapshot(0L, 0L);
		OperatorSnapshotUtil.writeStateHandle(snapshot, pendingSnapshotPath.toString());

		testSink.close();

		if (fs.exists(templatePath)) {
			fs.delete(templatePath, true);
		}
		fs.rename(prefixPath, templatePath);
	}

	@Test
	public void testeRestoreInprogressFile() throws Exception {
		final OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object>
			testSink = createSink(inProgressOutputPath);

		try {
			testSink.setup();
			testSink.initializeState(inProgressSnapshotPath.toString());
			testSink.open();

			final AbstractUdfStreamOperator abstractUdfStreamOperator = (AbstractUdfStreamOperator) testSink.getOperator();
			final StreamingFileSink streamingFileSink = (StreamingFileSink) abstractUdfStreamOperator.getUserFunction();

			Assert.assertEquals(1, streamingFileSink.getBuckets().getActiveBuckets().size());

			final Map<String, Bucket<Tuple2<String, Integer>, String>>
				buckets = (Map<String, Bucket<Tuple2<String, Integer>, String>>) streamingFileSink.getBuckets().getActiveBuckets();
			final Bucket<Tuple2<String, Integer>, String> bucket = buckets.get("bucket-1");

			Assert.assertEquals(14, bucket.getInProgressPart().getSize());
			Assert.assertEquals(0, bucket.getPendingFileRecoverablesForCurrentCheckpoint().size());
		} finally {
			testSink.close();
		}
	}

	@Test
	public void testeRestoreInprogressAndPendingFile() throws Exception {

		final OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object>
			testSink = createSink(inProgressAndPendingOutputPath);

		final java.nio.file.Path[] beforeRestoreItems =
			FileUtils.listDirectory(java.nio.file.Paths.get(inProgressAndPendingOutputPath.toString() + "/bucket-1"));

		Assert.assertEquals(2, beforeRestoreItems.length);

		for (java.nio.file.Path path : beforeRestoreItems) {
			if (!(path.toString().contains(".part-0-0.inprogress") || path.toString().contains(".part-0-1.inprogress"))) {
				Assert.fail("Find the unexpected file " + path.toString());
			}
		}

		try {
			testSink.setup();
			testSink.initializeState(inProgressAndPendingSnapshotPath.toString());
			testSink.open();

			final java.nio.file.Path[] afterRestoreItems =
				FileUtils.listDirectory(java.nio.file.Paths.get(inProgressAndPendingOutputPath.toString() + "/bucket-1"));

			Assert.assertEquals(2, beforeRestoreItems.length);

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

		} finally {
			testSink.close();
		}
	}

	@Test
	public void testRestorePendingFileWithoutInProgressFile() throws Exception {
		final OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object>
			testSink = createSink(pendingOutputPath);

		final java.nio.file.Path[] beforeRestoreItems =
			FileUtils.listDirectory(java.nio.file.Paths.get(inProgressAndPendingOutputPath.toString() + "/bucket-1"));

		Assert.assertEquals(2, beforeRestoreItems.length);

		for (java.nio.file.Path path : beforeRestoreItems) {
			if (!(path.toString().contains(".part-0-0.inprogress") || path.toString().contains(".part-0-1.inprogress"))) {
				Assert.fail("Find the unexpected file " + path.toString());
			}
		}

		try {
			testSink.setup();
			testSink.initializeState(pendingSnapshotPath.toString());
			testSink.open();

			final java.nio.file.Path[] afterRestoreItems =
				FileUtils.listDirectory(java.nio.file.Paths.get(pendingOutputPath.toString() + "/bucket-1"));

			Assert.assertEquals(2, beforeRestoreItems.length);

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

			Assert.assertEquals(null, bucket);
		} finally {
			testSink.close();
		}
	}

	private OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> createSink(final Path outputPath) throws Exception {
		StreamingFileSink<Tuple2<String, Integer>> sink = StreamingFileSink
			.forRowFormat(outputPath, new TestUtils.Tuple2Encoder())
			.withBucketAssigner(new TestUtils.TupleToStringBucketer())
			.withRollingPolicy(DefaultRollingPolicy.builder().withMaxPartSize(15).build())
			.withBucketCheckInterval(10L)
			.withBucketFactory(new DefaultBucketFactoryImpl<>())
			.build();

		return new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), MAX_PARALLELISM, 2, 0);
	}
}
