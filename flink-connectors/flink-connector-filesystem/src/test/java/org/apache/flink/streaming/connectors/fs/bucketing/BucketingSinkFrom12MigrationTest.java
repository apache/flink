/*
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
package org.apache.flink.streaming.connectors.fs.bucketing;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for checking whether {@link BucketingSink} can restore from snapshots that were done
 * using the Flink 1.2 {@link BucketingSink}.
 *
 * <p>For regenerating the binary snapshot file you have to run the {@code write*()} method on
 * the Flink 1.2 branch.
 */

public class BucketingSinkFrom12MigrationTest {

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	private static final String PART_PREFIX = "part";
	private static final String PENDING_SUFFIX = ".pending";
	private static final String IN_PROGRESS_SUFFIX = ".in-progress";
	private static final String VALID_LENGTH_SUFFIX = ".valid";

	/**
	 * Manually run this to write binary snapshot data. Remove @Ignore to run.
	 */
	@Ignore
	@Test
	public void writeSnapshot() throws Exception {

		final File outDir = tempFolder.newFolder();

		BucketingSink<String> sink = new BucketingSink<String>(outDir.getAbsolutePath())
			.setWriter(new StringWriter<String>())
			.setBatchSize(5)
			.setPartPrefix(PART_PREFIX)
			.setInProgressPrefix("")
			.setPendingPrefix("")
			.setValidLengthPrefix("")
			.setInProgressSuffix(IN_PROGRESS_SUFFIX)
			.setPendingSuffix(PENDING_SUFFIX)
			.setValidLengthSuffix(VALID_LENGTH_SUFFIX);

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>("test1", 0L));
		testHarness.processElement(new StreamRecord<>("test2", 0L));

		checkFs(outDir, 1, 1, 0, 0);

		testHarness.processElement(new StreamRecord<>("test3", 0L));
		testHarness.processElement(new StreamRecord<>("test4", 0L));
		testHarness.processElement(new StreamRecord<>("test5", 0L));

		checkFs(outDir, 1, 4, 0, 0);

		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);

		OperatorSnapshotUtil.writeStateHandle(snapshot, "src/test/resources/bucketing-sink-migration-test-flink1.2-snapshot");
		testHarness.close();
	}

	@Test
	public void testRestore() throws Exception {
		final File outDir = tempFolder.newFolder();

		ValidatingBucketingSink<String> sink = (ValidatingBucketingSink<String>) new ValidatingBucketingSink<String>(outDir.getAbsolutePath())
			.setWriter(new StringWriter<String>())
			.setBatchSize(5)
			.setPartPrefix(PART_PREFIX)
			.setInProgressPrefix("")
			.setPendingPrefix("")
			.setValidLengthPrefix("")
			.setInProgressSuffix(IN_PROGRESS_SUFFIX)
			.setPendingSuffix(PENDING_SUFFIX)
			.setValidLengthSuffix(VALID_LENGTH_SUFFIX);

		OneInputStreamOperatorTestHarness<String, Object> testHarness = new OneInputStreamOperatorTestHarness<>(
			new StreamSink<>(sink), 10, 1, 0);
		testHarness.setup();
		testHarness.initializeState(
				OperatorSnapshotUtil.readStateHandle(
						OperatorSnapshotUtil.getResourceFilename("bucketing-sink-migration-test-flink1.2-snapshot")));
		testHarness.open();

		assertTrue(sink.initializeCalled);

		testHarness.processElement(new StreamRecord<>("test1", 0L));
		testHarness.processElement(new StreamRecord<>("test2", 0L));

		checkFs(outDir, 1, 1, 0, 0);

		testHarness.close();
	}

	private void checkFs(File outDir, int inprogress, int pending, int completed, int valid) throws IOException {
		int inProg = 0;
		int pend = 0;
		int compl = 0;
		int val = 0;

		for (File file: FileUtils.listFiles(outDir, null, true)) {
			if (file.getAbsolutePath().endsWith("crc")) {
				continue;
			}
			String path = file.getPath();
			if (path.endsWith(IN_PROGRESS_SUFFIX)) {
				inProg++;
			} else if (path.endsWith(PENDING_SUFFIX)) {
				pend++;
			} else if (path.endsWith(VALID_LENGTH_SUFFIX)) {
				val++;
			} else if (path.contains(PART_PREFIX)) {
				compl++;
			}
		}

		Assert.assertEquals(inprogress, inProg);
		Assert.assertEquals(pending, pend);
		Assert.assertEquals(completed, compl);
		Assert.assertEquals(valid, val);
	}

	static class ValidatingBucketingSink<T> extends BucketingSink<T> {

		private static final long serialVersionUID = -4263974081712009141L;

		public boolean initializeCalled = false;

		ValidatingBucketingSink(String basePath) {
			super(basePath);
		}

		/**
		 * The actual paths in this depend on the binary checkpoint so it you update this the paths
		 * here have to be updated as well.
		 */
		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			OperatorStateStore stateStore = context.getOperatorStateStore();

			ListState<State<T>> restoredBucketStates = stateStore.getSerializableListState("bucket-states");

			if (context.isRestored()) {

				for (State<T> states : restoredBucketStates.get()) {
					for (String bucketPath : states.bucketStates.keySet()) {
						BucketState state = states.getBucketState(new Path(bucketPath));
						String current = state.currentFile;
						long validLength = state.currentFileValidLength;

						Assert.assertEquals("/var/folders/v_/ry2wp5fx0y7c1rvr41xy9_700000gn/T/junit9160378385359106772/junit479663758539998903/1970-01-01--01/part-0-4", current);
						Assert.assertEquals(6, validLength);

						List<String> pendingFiles = state.pendingFiles;
						assertTrue(pendingFiles.isEmpty());

						final Map<Long, List<String>> pendingFilesPerCheckpoint = state.pendingFilesPerCheckpoint;
						Assert.assertEquals(1, pendingFilesPerCheckpoint.size());

						for (Map.Entry<Long, List<String>> entry: pendingFilesPerCheckpoint.entrySet()) {
							long checkpoint = entry.getKey();
							List<String> files = entry.getValue();

							Assert.assertEquals(0L, checkpoint);
							Assert.assertEquals(4, files.size());

							for (int i = 0; i < 4; i++) {
								Assert.assertEquals(
										"/var/folders/v_/ry2wp5fx0y7c1rvr41xy9_700000gn/T/junit9160378385359106772/junit479663758539998903/1970-01-01--01/part-0-" + i,
										files.get(i));
							}
						}
					}
				}
			}

			initializeCalled = true;
			super.initializeState(context);
		}
	}
}
