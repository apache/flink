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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.fs.bucketing.BucketingSinkTestUtils.IN_PROGRESS_SUFFIX;
import static org.apache.flink.streaming.connectors.fs.bucketing.BucketingSinkTestUtils.PART_PREFIX;
import static org.apache.flink.streaming.connectors.fs.bucketing.BucketingSinkTestUtils.PENDING_SUFFIX;
import static org.apache.flink.streaming.connectors.fs.bucketing.BucketingSinkTestUtils.VALID_LENGTH_SUFFIX;
import static org.apache.flink.streaming.connectors.fs.bucketing.BucketingSinkTestUtils.checkLocalFs;
import static org.junit.Assert.assertTrue;

/**
 * Tests for checking whether {@link BucketingSink} can restore from snapshots that were done
 * using previous Flink versions' {@link BucketingSink}.
 *
 * <p>For regenerating the binary snapshot file you have to run the {@code write*()} method on
 * the corresponding Flink release-* branch.
 */
@RunWith(Parameterized.class)
public class BucketingSinkMigrationTest {

	/**
	 * TODO change this to the corresponding savepoint version to be written (e.g. {@link MigrationVersion#v1_3} for 1.3)
	 * TODO and remove all @Ignore annotations on write*Snapshot() methods to generate savepoints
	 * TODO Note: You should generate the savepoint based on the release branch instead of the master.
	 */
	private final MigrationVersion flinkGenerateSavepointVersion = null;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@BeforeClass
	public static void verifyOS() {
		Assume.assumeTrue("HDFS cluster cannot be started on Windows without extensions.", !OperatingSystem.isWindows());
	}

	/**
	 * The bucket file prefix is the absolute path to the part files, which is stored within the savepoint.
	 */
	@Parameterized.Parameters(name = "Migration Savepoint / Bucket Files Prefix: {0}")
	public static Collection<Tuple2<MigrationVersion, String>> parameters () {
		return Arrays.asList(
			Tuple2.of(MigrationVersion.v1_3, "/var/folders/tv/b_1d8fvx23dgk1_xs8db_95h0000gn/T/junit4273542175898623023/junit3801102997056424640/1970-01-01--01/part-0-"),
			Tuple2.of(MigrationVersion.v1_4, "/var/folders/tv/b_1d8fvx23dgk1_xs8db_95h0000gn/T/junit3198043255809479705/junit8947526563966405708/1970-01-01--01/part-0-"),
			Tuple2.of(MigrationVersion.v1_5, "/tmp/junit4927100426019463155/junit2465610012100182280/1970-01-01--00/part-0-"),
			Tuple2.of(MigrationVersion.v1_6, "/tmp/junit3459711376354834545/junit5114611885650086135/1970-01-01--00/part-0-"),
			Tuple2.of(MigrationVersion.v1_7, "/var/folders/r2/tdhx810x7yxb7q9_brnp49x40000gp/T/junit4288325607215628863/junit8132783417241536320/1970-01-01--08/part-0-"),
			Tuple2.of(MigrationVersion.v1_8, "/var/folders/rc/84k970r94nz456tb9cdlt30s1j0k94/T/junit7271027454784776053/junit5108755539355247469/1970-01-01--08/part-0-"),
			Tuple2.of(MigrationVersion.v1_9, "/var/folders/rc/84k970r94nz456tb9cdlt30s1j0k94/T/junit587754116249874744/junit764636113243634374/1970-01-01--08/part-0-"),
			Tuple2.of(MigrationVersion.v1_10, "/var/folders/v3/vg2lqnvx2ng24b5rr5qg2nv80000gp/T/junit9081786177439346/junit9063714383293729937/1970-01-01--08/part-0-"),
			Tuple2.of(MigrationVersion.v1_11, "/var/folders/v3/vg2lqnvx2ng24b5rr5qg2nv80000gp/T/junit9111597157791840754/junit2799810910414739323/1970-01-01--08/part-0-"));
	}

	private final MigrationVersion testMigrateVersion;
	private final String expectedBucketFilesPrefix;

	public BucketingSinkMigrationTest(Tuple2<MigrationVersion, String> migrateVersionAndExpectedBucketFilesPrefix) {
		this.testMigrateVersion = migrateVersionAndExpectedBucketFilesPrefix.f0;
		this.expectedBucketFilesPrefix = migrateVersionAndExpectedBucketFilesPrefix.f1;
	}

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

		checkLocalFs(outDir, 1, 1, 0, 0);

		testHarness.processElement(new StreamRecord<>("test3", 0L));
		testHarness.processElement(new StreamRecord<>("test4", 0L));
		testHarness.processElement(new StreamRecord<>("test5", 0L));

		checkLocalFs(outDir, 1, 4, 0, 0);

		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);

		OperatorSnapshotUtil.writeStateHandle(snapshot, "src/test/resources/bucketing-sink-migration-test-flink" + flinkGenerateSavepointVersion + "-snapshot");
		testHarness.close();
	}

	@Test
	public void testRestore() throws Exception {
		final File outDir = tempFolder.newFolder();

		ValidatingBucketingSink<String> sink = (ValidatingBucketingSink<String>)
				new ValidatingBucketingSink<String>(outDir.getAbsolutePath(), expectedBucketFilesPrefix)
			.setWriter(new StringWriter<String>())
			.setBatchSize(5)
			.setPartPrefix(PART_PREFIX)
			.setInProgressPrefix("")
			.setPendingPrefix("")
			.setValidLengthPrefix("")
			.setInProgressSuffix(IN_PROGRESS_SUFFIX)
			.setPendingSuffix(PENDING_SUFFIX)
			.setValidLengthSuffix(VALID_LENGTH_SUFFIX)
			.setUseTruncate(false); // don't use truncate because files do not exist

		OneInputStreamOperatorTestHarness<String, Object> testHarness = new OneInputStreamOperatorTestHarness<>(
			new StreamSink<>(sink), 10, 1, 0);
		testHarness.setup();

		testHarness.initializeState(
			OperatorSnapshotUtil.getResourceFilename(
				"bucketing-sink-migration-test-flink" + testMigrateVersion + "-snapshot"));

		testHarness.open();

		assertTrue(sink.initializeCalled);

		testHarness.processElement(new StreamRecord<>("test1", 0L));
		testHarness.processElement(new StreamRecord<>("test2", 0L));

		checkLocalFs(outDir, 1, 1, 0, 0);

		testHarness.close();
	}

	static class ValidatingBucketingSink<T> extends BucketingSink<T> {

		private static final long serialVersionUID = -4263974081712009141L;

		public boolean initializeCalled = false;

		private final String expectedBucketFilesPrefix;

		ValidatingBucketingSink(String basePath, String expectedBucketFilesPrefix) {
			super(basePath);
			this.expectedBucketFilesPrefix = expectedBucketFilesPrefix;
		}

		/**
		 * The actual paths in this depend on the binary checkpoint so it you update this the paths
		 * here have to be updated as well.
		 */
		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			OperatorStateStore stateStore = context.getOperatorStateStore();

			// We are using JavaSerializer from the flink-runtime module here. This is very naughty and
			// we shouldn't be doing it because ideally nothing in the API modules/connector depends
			// directly on flink-runtime. We are doing it here because we need to maintain backwards
			// compatibility with old state and because we will have to rework/remove this code soon.
			ListState<State<T>> restoredBucketStates = stateStore.getListState(new ListStateDescriptor<>("bucket-states", new JavaSerializer<>()));

			if (context.isRestored()) {

				for (State<T> states : restoredBucketStates.get()) {
					for (String bucketPath : states.bucketStates.keySet()) {
						BucketState state = states.getBucketState(new Path(bucketPath));
						String current = state.currentFile;
						long validLength = state.currentFileValidLength;

						Assert.assertEquals(expectedBucketFilesPrefix + "4", current);
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
										expectedBucketFilesPrefix + i,
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
