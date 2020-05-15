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
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
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

	private static final int CURRENT_VERSION = 1;

	@Parameterized.Parameters(name = "Previous Version = {0}")
	public static Collection<Integer> previousVersions() {
		return Arrays.asList(1);
	}

	@Parameterized.Parameter
	public Integer previousVersion;

	private static final String IN_PROGRESS_CONTENT = "writing";
	private static final String PENDING_CONTENT = "wrote";

	private static final String BUCKET_ID = "test-bucket";

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	private java.nio.file.Path getResourcePath(
			String scenarioName,
			int version) throws IOException {
		java.nio.file.Path basePath = Paths.get(System.getProperty("user.dir"))
				.resolve("src/test/resources/")
				.resolve("bucket-state-migration-test")
				.resolve(scenarioName + "-v" + version);

		Files.createDirectories(basePath);
		return basePath;
	}

	private java.nio.file.Path getSnapshotPath(
			String scenarioName,
			int version) throws IOException {
		java.nio.file.Path basePath = getResourcePath(scenarioName, version);
		return basePath.resolve("snapshot");
	}

	private java.nio.file.Path getOutputPath(String scenarioName, int version) throws IOException {
		java.nio.file.Path basePath = getResourcePath(scenarioName, version);
		return basePath.resolve("bucket");
	}

	@Test
	@Ignore
	public void prepareDeserializationEmpty() throws IOException {
		final String scenarioName = "empty";
		final java.nio.file.Path outputPath = getOutputPath(scenarioName, CURRENT_VERSION);
		final SimpleVersionedSerializer<BucketState<String>> serializer =
				bucketStateSerializer();

		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toUri());

		final Bucket<String, String> bucket = createNewBucket(testBucketPath);

		final BucketState<String> bucketState = bucket.onReceptionOfCheckpoint(0);

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(
				serializer,
				bucketState);

		Files.write(getSnapshotPath(scenarioName, CURRENT_VERSION), bytes);
	}

	@Test
	public void testDeserializationEmpty() throws IOException {
		final String scenarioName = "empty";
		final java.nio.file.Path outputPath = getOutputPath(scenarioName, previousVersion);
		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());
		final SimpleVersionedSerializer<BucketState<String>> serializer =
				bucketStateSerializer();

		byte[] bytes = Files.readAllBytes(getSnapshotPath(scenarioName, previousVersion));

		final BucketState<String> recoveredState =
				SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);

		final Bucket<String, String> bucket = restoreBucket(0, recoveredState);

		Assert.assertEquals(testBucketPath.getPath(), recoveredState.getBucketPath().getPath());
		Assert.assertNull(bucket.getInProgressPart());
		Assert.assertTrue(bucket.getPendingPartsPerCheckpoint().isEmpty());
	}

	@Test
	@Ignore
	public void prepareDeserializationOnlyInProgress() throws IOException {
		final String scenarioName = "only-in-progress";
		final java.nio.file.Path outputPath = getOutputPath(scenarioName, CURRENT_VERSION);
		final SimpleVersionedSerializer<BucketState<String>> serializer =
				bucketStateSerializer();

		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toUri());

		final Bucket<String, String> bucket = createNewBucket(testBucketPath);

		bucket.write(IN_PROGRESS_CONTENT, System.currentTimeMillis());

		final BucketState<String> bucketState = bucket.onReceptionOfCheckpoint(0);

		final byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(
				serializer,
				bucketState);

		Files.write(getSnapshotPath(scenarioName, CURRENT_VERSION), bytes);
	}

	@Test
	public void testDeserializationOnlyInProgress() throws IOException {
		final String scenarioName = "only-in-progress";
		final java.nio.file.Path outputPath = getOutputPath(scenarioName, previousVersion);
		final SimpleVersionedSerializer<BucketState<String>> serializer =
				bucketStateSerializer();

		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toUri());

		byte[] bytes = Files.readAllBytes(getSnapshotPath(scenarioName, previousVersion));

		final BucketState<String> recoveredState = SimpleVersionedSerialization.readVersionAndDeSerialize(
				serializer,
				bytes);

		// work around some inconsistencies in the Flink Path implementation, if a Path
		// is a directory that contains other files for some reason we have a '/' at the end
		Assert.assertEquals(
				testBucketPath.getPath(),
				recoveredState.getBucketPath().getPath() + "/");

		long numFiles = Files.list(outputPath.resolve(BUCKET_ID))
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
	@Ignore
	public void prepareDeserializationFull() throws IOException {
		prepareDeserializationFull(true, "full");
	}

	@Test
	@Ignore
	public void prepareDeserializationFullNoInProgress() throws IOException {
		prepareDeserializationFull(false, "full-no-in-progress");
	}

	public void prepareDeserializationFull(
			boolean withInProgress,
			String scenarioName) throws IOException {
		final int numPendingCheckpoints = 5;

		final java.nio.file.Path outputPath = getOutputPath(scenarioName, CURRENT_VERSION);
		final SimpleVersionedSerializer<BucketState<String>> serializer =
				bucketStateSerializer();

		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toUri());
		final Bucket<String, String> bucket = createNewBucket(testBucketPath);

		// pending for checkpoints
		for (int i = 0; i < numPendingCheckpoints; i++) {
			// write 10 bytes to the in progress file
			bucket.write(PENDING_CONTENT, System.currentTimeMillis());
			bucket.write(PENDING_CONTENT, System.currentTimeMillis());
			// every checkpoint would produce a pending file
			bucket.onReceptionOfCheckpoint(i);
		}

		if (withInProgress) {
			// create a in progress file
			bucket.write(IN_PROGRESS_CONTENT, System.currentTimeMillis());
		}

		// 5 pending files, and (maybe) 1 in progress file
		final BucketState<String> bucketState = bucket.onReceptionOfCheckpoint(numPendingCheckpoints);

		// moves the files to the final location, but in our snapshot they are still "pending"
		// we move them here, because otherwise the actual test code will move them on recovery,
		// which will mess with the files we keep in version control
		bucket.onSuccessfulCompletionOfCheckpoint(numPendingCheckpoints);

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(
				serializer,
				bucketState);

		Files.write(getSnapshotPath(scenarioName, CURRENT_VERSION), bytes);
	}

	@Test
	public void testDeserializationFull() throws IOException {
		testDeserializationFull(true, "full");
	}

	@Test
	public void testDeserializationFullNoInProgress() throws IOException {
		testDeserializationFull(false, "full-no-in-progress");
	}

	public void testDeserializationFull(
			boolean withInProgress,
			String scenarioName) throws IOException {
		final int numPendingCheckpoints = 5;

		final java.nio.file.Path outputPath = getOutputPath(scenarioName, previousVersion);
		final SimpleVersionedSerializer<BucketState<String>> serializer =
				bucketStateSerializer();

		final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toUri());

		byte[] bytes = Files.readAllBytes(getSnapshotPath(scenarioName, previousVersion));

		final BucketState<String> recoveredState = SimpleVersionedSerialization.readVersionAndDeSerialize(
				serializer,
				bytes);

		// there are 5 checkpoints that didn't "complete"
		assertThat(recoveredState.getCommittableFilesPerCheckpoint().size(), is(5));

		// recover and commit
		final Bucket<String, String> bucket = restoreBucket(
				numPendingCheckpoints + 1,
				recoveredState);
		// work around some inconsistencies in the Flink Path implementation, if a Path
		// is a directory that contains other files for some reason we have a '/' at the end
		Assert.assertEquals(testBucketPath.getPath(), bucket.getBucketPath().getPath() + "/");
		Assert.assertEquals(0, bucket.getPendingPartsForCurrentCheckpoint().size());

		final Set<String> paths = Files.list(outputPath.resolve(BUCKET_ID))
				.map(file -> file.getFileName().toString())
				.collect(Collectors.toSet());

		// The pending files have already been committed before, in preparation.
		for (int i = 0; i < numPendingCheckpoints; i++) {
			final String part = "part-0-" + i;
			assertThat(paths, hasItem(part));
			paths.remove(part);
		}

		if (withInProgress) {
			// only the in-progress must be left
			assertThat(paths, iterableWithSize(1));

			// verify that the in-progress file is still there
			assertThat(
					paths,
					hasItem(startsWith(".part-0-" + numPendingCheckpoints + ".inprogress")));
		} else {
			assertThat(paths, empty());
		}

	}

	private static Bucket<String, String> createNewBucket(final Path bucketPath) throws IOException {
		return Bucket.getNew(
				new LocalFileSystem().createRecoverableWriter(),
				0,
				BUCKET_ID,
				bucketPath,
				0,
				createBucketWriter(),
				DefaultRollingPolicy.builder().withMaxPartSize(10).build(),
				OutputFileConfig.builder().build());
	}

	private static Bucket<String, String> restoreBucket(
			final int initialPartCounter,
			final BucketState<String> bucketState) throws IOException {
		return Bucket.restore(
				new LocalFileSystem().createRecoverableWriter(),
				0,
				initialPartCounter,
				createBucketWriter(),
				DefaultRollingPolicy.builder().withMaxPartSize(10).build(),
				bucketState,
				OutputFileConfig.builder().build());
	}

	private static RowWisePartWriter.Factory<String, String> createBucketWriter() {
		return new RowWisePartWriter.Factory<>(new SimpleStringEncoder<>());
	}

	private SimpleVersionedSerializer<BucketState<String>> bucketStateSerializer() throws IOException {
		return new BucketStateSerializer<>(
				new LocalFileSystem().createRecoverableWriter().getResumeRecoverableSerializer(),
				new LocalFileSystem().createRecoverableWriter().getCommitRecoverableSerializer(),
				SimpleVersionedStringSerializer.INSTANCE);
	}

}
