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

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.FileUtils;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link BucketStateSerializer} that verify we can still read snapshots written using
 * an older version of the serializer. We keep snapshots for all previous versions in version
 * control (including the current version). The tests verify that the current version of the
 * serializer can still read data from all previous versions.
 */
@RunWith(Parameterized.class)
public class BucketStateSerializerTest {

	private static final int CURRENT_VERSION = 2;

	@Parameterized.Parameters(name = "Previous Version = {0}")
	public static Collection<Integer> previousVersions() {
		return Arrays.asList(2);
	}

	@Parameterized.Parameter
	public Integer previousVersion;

	private static final String IN_PROGRESS_CONTENT = "writing";
	private static final String PENDING_CONTENT = "wrote";

	private static final String BUCKET_ID = "test-bucket";

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	private static java.nio.file.Path getResourcePath(
			String scenarioName,
			int version) throws IOException {
		java.nio.file.Path basePath = Paths.get("src/test/resources/")
			.resolve("bucket-state-migration-test")
			.resolve(scenarioName + "-v" + version);
		return basePath;
	}

	private static java.nio.file.Path getSnapshotPath(
			String scenarioName,
			int version) throws IOException {
		java.nio.file.Path basePath = getResourcePath(scenarioName, version);
		return basePath.resolve("snapshot");
	}

	private static java.nio.file.Path getOutputPath(String scenarioName, int version) throws IOException {
		java.nio.file.Path basePath = getResourcePath(scenarioName, version);
		return basePath.resolve("bucket");
	}

	@Test
	public void prepareDeserializationEmpty() throws IOException {

		final String scenarioName = "empty";
		final java.nio.file.Path scenarioPath = getResourcePath(scenarioName, CURRENT_VERSION);

		FileUtils.deleteDirectory(scenarioPath.toFile());
		Files.createDirectories(scenarioPath);

		final java.nio.file.Path outputPath = getOutputPath(scenarioName, CURRENT_VERSION);
		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());

		final Bucket<String, String> bucket =
			createNewBucket(testBucketPath);

		final BucketState<String> bucketState = bucket.onReceptionOfCheckpoint(0);

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(
			bucketStateSerializer(),
			bucketState);
		Files.write(getSnapshotPath(scenarioName, CURRENT_VERSION), bytes);
	}

	@Test
	public void testSerializationEmpty() throws IOException {

		final String scenarioName = "empty";
		final java.nio.file.Path outputPath = getOutputPath(scenarioName, previousVersion);
		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());
		final BucketState<String> recoveredState = readBucketState(scenarioName, previousVersion);

		final Bucket<String, String> bucket = restoreBucket(0, recoveredState);

		Assert.assertEquals(testBucketPath, bucket.getBucketPath());
		Assert.assertNull(bucket.getInProgressPart());
		Assert.assertTrue(bucket.getPendingFileRecoverablesPerCheckpoint().isEmpty());
	}

	@Test
	public void prepareDeserializationOnlyInProgress() throws IOException {

		final String scenarioName = "only-in-progress";
		final java.nio.file.Path scenarioPath = getResourcePath(scenarioName, CURRENT_VERSION);
		FileUtils.deleteDirectory(scenarioPath.toFile());
		Files.createDirectories(scenarioPath);

		final java.nio.file.Path outputPath = getOutputPath(scenarioName, CURRENT_VERSION);
		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());

		final Bucket<String, String> bucket =
			createNewBucket(testBucketPath);

		bucket.write(IN_PROGRESS_CONTENT, System.currentTimeMillis());

		final BucketState<String> bucketState = bucket.onReceptionOfCheckpoint(0);

		final byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(
			bucketStateSerializer(), bucketState);

		Files.write(getSnapshotPath(scenarioName, CURRENT_VERSION), bytes);
	}

	@Test
	public void testSerializationOnlyInProgress() throws IOException {

		final String scenarioName = "only-in-progress";
		final java.nio.file.Path outputPath = getOutputPath(scenarioName, previousVersion);

		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());

		final BucketState<String> recoveredState = readBucketState(scenarioName, previousVersion);

		final Bucket<String, String> bucket = restoreBucket(0, recoveredState);

		Assert.assertEquals(testBucketPath, bucket.getBucketPath());

		//check restore the correct in progress file writer
		Assert.assertEquals(8, bucket.getInProgressPart().getSize());

		long numFiles = Files.list(Paths.get(testBucketPath.toString()))
			.map(file -> {
				assertThat(
					file.getFileName().toString(),
					startsWith(".part-0-0.inprogress"));
				return 1;
			})
			.count();

		assertThat(numFiles, is(1L));
	}

	@Test
	public void prepareDeserializationFull() throws IOException {

		final String scenarioName = "full";
		final java.nio.file.Path scenarioPath = getResourcePath(scenarioName, CURRENT_VERSION);
		FileUtils.deleteDirectory(scenarioPath.toFile());
		Files.createDirectories(scenarioPath);

		final int noOfPendingCheckpoints = 5;

		final java.nio.file.Path outputPath = getOutputPath(scenarioName, CURRENT_VERSION);

		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());

		final Bucket<String, String> bucket = createNewBucket(testBucketPath);

		// pending for checkpoints
		for (int i = 0; i < noOfPendingCheckpoints; i++) {
			// write 10 bytes to the in progress file
			bucket.write(PENDING_CONTENT, System.currentTimeMillis());
			bucket.write(PENDING_CONTENT, System.currentTimeMillis());
			// every checkpoint would produce a pending file
			bucket.onReceptionOfCheckpoint(i);
		}

		// create a in progress file
		bucket.write(IN_PROGRESS_CONTENT, System.currentTimeMillis());

		// 5 pending files and 1 in progress file
		final BucketState<String> bucketState = bucket.onReceptionOfCheckpoint(noOfPendingCheckpoints);

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(
			bucketStateSerializer(), bucketState);

		Files.write(getSnapshotPath(scenarioName, CURRENT_VERSION), bytes);

		// copy the scenario file to a template directory.
		// it is because that the test `testSerializationFull` would change the in progress file to pending files.
		moveToTemplateDirectory(scenarioPath);
	}

	@Test
	public void testSerializationFull() throws IOException {

		final String scenarioName = "full";

		try {
			final java.nio.file.Path outputPath = getOutputPath(scenarioName, previousVersion);

			final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());

			// restore the state
			final BucketState<String> recoveredState = readBucketStateFromTemplate(scenarioName, previousVersion);

			final int noOfPendingCheckpoints = 5;

			// there are 5 checkpoint does not complete.
			final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>>
				pendingFileRecoverables = recoveredState.getPendingFileRecoverablesPerCheckpoint();
			Assert.assertEquals(5L, pendingFileRecoverables.size());

			final Set<String> beforeRestorePaths = Files.list(outputPath.resolve(BUCKET_ID))
				.map(file -> file.getFileName().toString())
				.collect(Collectors.toSet());

			// before retsoring all file has "inprogress"
			for (int i = 0; i < noOfPendingCheckpoints; i++) {
				final String part = ".part-0-" + i + ".inprogress";
				assertThat(beforeRestorePaths, hasItem(startsWith(part)));
			}

			// recover and commit
			final Bucket bucket = restoreBucket(noOfPendingCheckpoints + 1, recoveredState);
			Assert.assertEquals(testBucketPath, bucket.getBucketPath());
			Assert.assertEquals(0, bucket.getPendingFileRecoverablesForCurrentCheckpoint().size());

			final Set<String> afterRestorePaths = Files.list(outputPath.resolve(BUCKET_ID))
				.map(file -> file.getFileName().toString())
				.collect(Collectors.toSet());

			// after restoring all pending files are comitted.
			// there is no "inporgress" in file name for the committed files.
			for (int i = 0; i < noOfPendingCheckpoints; i++) {
				final String part = "part-0-" + i;
				assertThat(afterRestorePaths, hasItem(part));
				afterRestorePaths.remove(part);
			}

			// only the in-progress must be left
			assertThat(afterRestorePaths, iterableWithSize(1));

			// verify that the in-progress file is still there
			assertThat(afterRestorePaths, hasItem(startsWith(".part-0-" + noOfPendingCheckpoints + ".inprogress")));
		} finally {
			FileUtils.deleteDirectory(getResourcePath(scenarioName, previousVersion).toFile());
		}
	}

	/**
	 * Test the case where we don't have an in-progress file.
	 */
	@Test
	public void prepareDeserializationNullInProgress() throws IOException {

		final String scenarioName = "null-in-progress";
		final java.nio.file.Path scenarioPath = getResourcePath(scenarioName, CURRENT_VERSION);
		FileUtils.deleteDirectory(scenarioPath.toFile());
		Files.createDirectories(scenarioPath);

		final java.nio.file.Path outputPath = getOutputPath(scenarioName, CURRENT_VERSION);

		final int noOfPendingCheckpoints = 5;

		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());

		final Bucket<String, String> bucket =
			createNewBucket(testBucketPath);
		BucketState<String> bucketState = null ;

		for (int i = 0; i < noOfPendingCheckpoints; i++) {
			bucket.write(PENDING_CONTENT, System.currentTimeMillis());
			bucket.write(PENDING_CONTENT, System.currentTimeMillis());
			bucketState = bucket.onReceptionOfCheckpoint(i);
		}

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(
			bucketStateSerializer(), bucketState);

		Files.write(getSnapshotPath(scenarioName, CURRENT_VERSION), bytes);

		// copy the scenario file to a template directory.
		// it is because that the test `testSerializationNullInProgress` would change the in progress file to pending files.
		moveToTemplateDirectory(scenarioPath);
	}

	@Test
	public void testSerializationNullInProgress() throws IOException {

		final String scenarioName = "null-in-progress";

		try {

			final java.nio.file.Path outputPath = getOutputPath(scenarioName, previousVersion);

			final int noOfPendingCheckpoints = 5;

			final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());

			// restore the state
			final BucketState<String> recoveredState = readBucketStateFromTemplate(scenarioName, previousVersion);

			final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>>
				pendingFileRecoverables = recoveredState.getPendingFileRecoverablesPerCheckpoint();
			Assert.assertEquals(5L, pendingFileRecoverables.size());

			final Set<String> beforeRestorePaths = Files.list(outputPath.resolve(BUCKET_ID))
				.map(file -> file.getFileName().toString())
				.collect(Collectors.toSet());

			// before retsoring all file has "inprogress"
			for (int i = 0; i < noOfPendingCheckpoints; i++) {
				final String part = ".part-0-" + i + ".inprogress";
				assertThat(beforeRestorePaths, hasItem(startsWith(part)));
			}

			final Bucket<String, String> bucket = restoreBucket(noOfPendingCheckpoints, recoveredState);

			Assert.assertEquals(testBucketPath, bucket.getBucketPath());
			Assert.assertEquals(0, bucket.getPendingFileRecoverablesForCurrentCheckpoint().size());

			// after restoring all pending files are comitted.
			// there is no "inporgress" in file name for the committed files.
			final Set<String> afterRestorepaths = Files.list(outputPath.resolve(BUCKET_ID))
				.map(file -> file.getFileName().toString())
				.collect(Collectors.toSet());

			for (int i = 0; i < noOfPendingCheckpoints; i++) {
				final String part = "part-0-" + i;
				assertThat(afterRestorepaths, hasItem(part));
				afterRestorepaths.remove(part);
			}

			// only the in-progress must be left
			assertThat(afterRestorepaths, empty());
		} finally {
			FileUtils.deleteDirectory(getResourcePath(scenarioName, previousVersion).toFile());
		}
	}

	private static Bucket<String, String> createNewBucket(final Path bucketPath) {
		return Bucket.getNew(
			0,
			BUCKET_ID,
			bucketPath,
			0,
			createBucketWriter(),
			DefaultRollingPolicy.builder().withMaxPartSize(10).build(),
			OutputFileConfig.builder().build());
	}

	private static Bucket<String, String> restoreBucket(final int initialPartCounter, final BucketState bucketState) throws IOException {
		return Bucket.restore(
			0,
			initialPartCounter,
			createBucketWriter(),
			DefaultRollingPolicy.builder().withMaxPartSize(10).build(),
			bucketState,
			OutputFileConfig.builder().build());
	}

	private static RowWisePartWriter.Factory createBucketWriter() {
		return new RowWisePartWriter.Factory<>(
				new TestUtils.LocalRecoverableWriterForBucketStateMigrationTest(), new SimpleStringEncoder());
	}

	private static SimpleVersionedSerializer<BucketState<String>> bucketStateSerializer() {
		final RowWisePartWriter.Factory factory = createBucketWriter();
		return new BucketStateSerializer(
			factory.getProperties().getInProgressFileRecoverableSerializer(),
			factory.getProperties().getPendingFileRecoverableSerializer(),
			SimpleVersionedStringSerializer.INSTANCE);
	}

	private static BucketState<String> readBucketState(final String scenarioName, final int version) throws IOException {
		byte[] bytes = Files.readAllBytes(getSnapshotPath(scenarioName, version));
		return SimpleVersionedSerialization.readVersionAndDeSerialize(bucketStateSerializer(), bytes);
	}

	private static BucketState<String> readBucketStateFromTemplate(final String scenarioName, final int version) throws IOException {
		final java.nio.file.Path scenarioPath =  getResourcePath(scenarioName, version);

		// clear the scenario files first
		FileUtils.deleteDirectory(scenarioPath.toFile());

		// prepare the scenario files
		FileUtils.copy(new Path(scenarioPath.toString() + "-template"), new Path(scenarioPath.toString()), false);

		return readBucketState(scenarioName, version);
	}

	private static void moveToTemplateDirectory(java.nio.file.Path scenarioPath) throws IOException {
		FileUtils.copy(new Path(scenarioPath.toString()), new Path(scenarioPath.toString() + "-template"), false);
		FileUtils.deleteDirectory(scenarioPath.toFile());
	}
}
