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

package org.apache.flink.connector.file.sink.writer;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link FileWriter}.
 */
public class FileWriterTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void testPreCommit() throws Exception {
		File outDir = TEMP_FOLDER.newFolder();
		Path path = new Path(outDir.toURI());

		FileWriter<String> fileWriter = createWriter(
				path,
				OnCheckpointRollingPolicy.build(),
				new OutputFileConfig("part-", ""));

		fileWriter.write("test1", new ContextImpl());
		fileWriter.write("test1", new ContextImpl());
		fileWriter.write("test2", new ContextImpl());
		fileWriter.write("test2", new ContextImpl());
		fileWriter.write("test3", new ContextImpl());

		List<FileSinkCommittable> committables = fileWriter.prepareCommit(false);
		assertEquals(3, committables.size());
	}

	@Test
	public void testSnapshotAndRestore() throws Exception {
		File outDir = TEMP_FOLDER.newFolder();
		Path path = new Path(outDir.toURI());

		FileWriter<String> fileWriter = createWriter(
				path,
				DefaultRollingPolicy.builder().build(),
				new OutputFileConfig("part-", ""));

		fileWriter.write("test1", new ContextImpl());
		fileWriter.write("test2", new ContextImpl());
		fileWriter.write("test3", new ContextImpl());
		assertEquals(3, fileWriter.getActiveBuckets().size());

		fileWriter.prepareCommit(false);
		List<FileWriterBucketState> states = fileWriter.snapshotState();
		assertEquals(3, states.size());

		fileWriter = restoreWriter(
				states,
				path,
				OnCheckpointRollingPolicy.build(),
				new OutputFileConfig("part-", ""));
		assertEquals(
				fileWriter.getActiveBuckets().keySet(),
				new HashSet<>(Arrays.asList("test1", "test2", "test3")));
		for (FileWriterBucket<String> bucket : fileWriter.getActiveBuckets().values()) {
			assertNotNull("The in-progress file should be recovered", bucket.getInProgressPart());
		}
	}

	@Test
	public void testMergingForRescaling() throws Exception {
		File outDir = TEMP_FOLDER.newFolder();
		Path path = new Path(outDir.toURI());

		FileWriter<String> firstFileWriter = createWriter(
				path,
				DefaultRollingPolicy.builder().build(),
				new OutputFileConfig("part-", ""));

		firstFileWriter.write("test1", new ContextImpl());
		firstFileWriter.write("test2", new ContextImpl());
		firstFileWriter.write("test3", new ContextImpl());

		firstFileWriter.prepareCommit(false);
		List<FileWriterBucketState> firstState = firstFileWriter.snapshotState();

		FileWriter<String> secondFileWriter = createWriter(
				path,
				DefaultRollingPolicy.builder().build(),
				new OutputFileConfig("part-", ""));

		secondFileWriter.write("test1", new ContextImpl());
		secondFileWriter.write("test2", new ContextImpl());

		secondFileWriter.prepareCommit(false);
		List<FileWriterBucketState> secondState = secondFileWriter.snapshotState();

		List<FileWriterBucketState> mergedState = new ArrayList<>();
		mergedState.addAll(firstState);
		mergedState.addAll(secondState);

		FileWriter<String> restoredWriter = restoreWriter(
				mergedState,
				path,
				DefaultRollingPolicy.builder().build(),
				new OutputFileConfig("part-", ""));
		assertEquals(3, restoredWriter.getActiveBuckets().size());

		// Merged buckets
		for (String bucketId : Arrays.asList("test1", "test2")) {
			FileWriterBucket<String> bucket = restoredWriter.getActiveBuckets().get(bucketId);
			assertNotNull("The in-progress file should be recovered", bucket.getInProgressPart());
			assertEquals(1, bucket.getPendingFiles().size());
		}

		// Not merged buckets
		for (String bucketId : Collections.singletonList("test3")) {
			FileWriterBucket<String> bucket = restoredWriter.getActiveBuckets().get(bucketId);
			assertNotNull("The in-progress file should be recovered", bucket.getInProgressPart());
			assertEquals(0, bucket.getPendingFiles().size());
		}
	}

	@Test
	public void testBucketIsRemovedWhenNotActive() throws Exception {
		File outDir = TEMP_FOLDER.newFolder();
		Path path = new Path(outDir.toURI());

		FileWriter<String> fileWriter = createWriter(
				path,
				OnCheckpointRollingPolicy.build(),
				new OutputFileConfig("part-", ""));

		fileWriter.write("test", new ContextImpl());
		fileWriter.prepareCommit(false);
		fileWriter.snapshotState();

		// No more records and another call to prepareCommit will makes it inactive
		fileWriter.prepareCommit(false);

		assertTrue(fileWriter.getActiveBuckets().isEmpty());
	}

	// ------------------------------- Mock Classes --------------------------------

	private static class ContextImpl implements SinkWriter.Context {
		private final long watermark;
		private final long timestamp;

		public ContextImpl() {
			this(0, 0);
		}

		private ContextImpl(long watermark, long timestamp) {
			this.watermark = watermark;
			this.timestamp = timestamp;
		}

		@Override
		public long currentWatermark() {
			return watermark;
		}

		@Override
		public Long timestamp() {
			return timestamp;
		}
	}

	// ------------------------------- Utility Methods --------------------------------

	private static FileWriter<String> createWriter(
			Path basePath,
			RollingPolicy<String, String> rollingPolicy,
			OutputFileConfig outputFileConfig) throws IOException {
		return new FileWriter<>(
				basePath,
				new FileSinkTestUtils.StringIdentityBucketAssigner(),
				new DefaultFileWriterBucketFactory<>(),
				new RowWiseBucketWriter<>(FileSystem
						.get(basePath.toUri())
						.createRecoverableWriter(), new SimpleStringEncoder<>()),
				rollingPolicy,
				outputFileConfig);
	}

	private static FileWriter<String> restoreWriter(
			List<FileWriterBucketState> states,
			Path basePath,
			RollingPolicy<String, String> rollingPolicy,
			OutputFileConfig outputFileConfig) throws IOException {
		FileWriter<String> writer = createWriter(basePath, rollingPolicy, outputFileConfig);
		writer.initializeState(states);
		return writer;
	}
}
