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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.TestUtils.MockListState;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link Buckets}.
 */
public class BucketsTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void testSnapshotAndRestore() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final RollingPolicy<String, String> onCheckpointRollingPolicy = OnCheckpointRollingPolicy.build();

		final Buckets<String, String> buckets = createBuckets(path, onCheckpointRollingPolicy, 0);

		final ListState<byte[]> bucketStateContainer = new MockListState<>();
		final ListState<Long> partCounterContainer = new MockListState<>();

		buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
		buckets.snapshotState(0L, bucketStateContainer, partCounterContainer);

		assertThat(buckets.getActiveBuckets().get("test1"), hasSinglePartFileToBeCommittedOnCheckpointAck(path, "test1"));

		buckets.onElement("test2", new TestUtils.MockSinkContext(null, 1L, 2L));
		buckets.snapshotState(1L, bucketStateContainer, partCounterContainer);

		assertThat(buckets.getActiveBuckets().get("test1"), hasSinglePartFileToBeCommittedOnCheckpointAck(path, "test1"));
		assertThat(buckets.getActiveBuckets().get("test2"), hasSinglePartFileToBeCommittedOnCheckpointAck(path, "test2"));

		Buckets<String, String> restoredBuckets =
				restoreBuckets(path, onCheckpointRollingPolicy, 0, bucketStateContainer, partCounterContainer);

		final Map<String, Bucket<String, String>> activeBuckets = restoredBuckets.getActiveBuckets();

		// because we commit pending files for previous checkpoints upon recovery
		Assert.assertTrue(activeBuckets.isEmpty());
	}

	private static TypeSafeMatcher<Bucket<String, String>> hasSinglePartFileToBeCommittedOnCheckpointAck(final Path testTmpPath, final String bucketId) {
		return new TypeSafeMatcher<Bucket<String, String>>() {
			@Override
			protected boolean matchesSafely(Bucket<String, String> bucket) {
				return bucket.getBucketId().equals(bucketId) &&
						bucket.getBucketPath().equals(new Path(testTmpPath, bucketId)) &&
						bucket.getInProgressPart() == null &&
						bucket.getPendingPartsForCurrentCheckpoint().isEmpty() &&
						bucket.getPendingPartsPerCheckpoint().size() == 1;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a Bucket with a single pending part file @ ")
						.appendValue(new Path(testTmpPath, bucketId))
						.appendText("'");
			}
		};
	}

	@Test
	public void testMergeAtScaleInAndMaxCounterAtRecovery() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final RollingPolicy<String, String> onCheckpointRP =
				DefaultRollingPolicy
						.builder()
						.withMaxPartSize(7L) // roll with 2 elements
						.build();

		final MockListState<byte[]> bucketStateContainerOne = new MockListState<>();
		final MockListState<byte[]> bucketStateContainerTwo = new MockListState<>();

		final MockListState<Long> partCounterContainerOne = new MockListState<>();
		final MockListState<Long> partCounterContainerTwo = new MockListState<>();

		final Buckets<String, String> bucketsOne = createBuckets(path, onCheckpointRP, 0);
		final Buckets<String, String> bucketsTwo = createBuckets(path, onCheckpointRP, 1);

		bucketsOne.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
		bucketsOne.snapshotState(0L, bucketStateContainerOne, partCounterContainerOne);

		Assert.assertEquals(1L, bucketsOne.getMaxPartCounter());

		// make sure we have one in-progress file here
		Assert.assertNotNull(bucketsOne.getActiveBuckets().get("test1").getInProgressPart());

		// add a couple of in-progress files so that the part counter increases.
		bucketsTwo.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
		bucketsTwo.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));

		bucketsTwo.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));

		bucketsTwo.snapshotState(0L, bucketStateContainerTwo, partCounterContainerTwo);

		Assert.assertEquals(2L, bucketsTwo.getMaxPartCounter());

		// make sure we have one in-progress file here and a pending
		Assert.assertEquals(1L, bucketsTwo.getActiveBuckets().get("test1").getPendingPartsPerCheckpoint().size());
		Assert.assertNotNull(bucketsTwo.getActiveBuckets().get("test1").getInProgressPart());

		final ListState<byte[]> mergedBucketStateContainer = new MockListState<>();
		final ListState<Long> mergedPartCounterContainer = new MockListState<>();

		mergedBucketStateContainer.addAll(bucketStateContainerOne.getBackingList());
		mergedBucketStateContainer.addAll(bucketStateContainerTwo.getBackingList());

		mergedPartCounterContainer.addAll(partCounterContainerOne.getBackingList());
		mergedPartCounterContainer.addAll(partCounterContainerTwo.getBackingList());

		final Buckets<String, String> restoredBuckets =
				restoreBuckets(path, onCheckpointRP, 0, mergedBucketStateContainer, mergedPartCounterContainer);

		// we get the maximum of the previous tasks
		Assert.assertEquals(2L, restoredBuckets.getMaxPartCounter());

		final Map<String, Bucket<String, String>> activeBuckets = restoredBuckets.getActiveBuckets();
		Assert.assertEquals(1L, activeBuckets.size());
		Assert.assertTrue(activeBuckets.keySet().contains("test1"));

		final Bucket<String, String> bucket = activeBuckets.get("test1");
		Assert.assertEquals("test1", bucket.getBucketId());
		Assert.assertEquals(new Path(path, "test1"), bucket.getBucketPath());

		Assert.assertNotNull(bucket.getInProgressPart()); // the restored part file

		// this is due to the Bucket#merge(). The in progress file of one
		// of the previous tasks is put in the list of pending files.
		Assert.assertEquals(1L, bucket.getPendingPartsForCurrentCheckpoint().size());

		// we commit the pending for previous checkpoints
		Assert.assertTrue(bucket.getPendingPartsPerCheckpoint().isEmpty());
	}

	@Test
	public void testOnProcessingTime() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
				new OnProcessingTimePolicy<>(2L);

		final Buckets<String, String> buckets =
				createBuckets(path, rollOnProcessingTimeCountingPolicy, 0);

		// it takes the current processing time of the context for the creation time,
		// and for the last modification time.
		buckets.onElement("test", new TestUtils.MockSinkContext(1L, 2L, 3L));

		// now it should roll
		buckets.onProcessingTime(7L);
		Assert.assertEquals(1L, rollOnProcessingTimeCountingPolicy.getOnProcessingTimeRollCounter());

		final Map<String, Bucket<String, String>> activeBuckets = buckets.getActiveBuckets();
		Assert.assertEquals(1L, activeBuckets.size());
		Assert.assertTrue(activeBuckets.keySet().contains("test"));

		final Bucket<String, String> bucket = activeBuckets.get("test");
		Assert.assertEquals("test", bucket.getBucketId());
		Assert.assertEquals(new Path(path, "test"), bucket.getBucketPath());
		Assert.assertEquals("test", bucket.getBucketId());

		Assert.assertNull(bucket.getInProgressPart());
		Assert.assertEquals(1L, bucket.getPendingPartsForCurrentCheckpoint().size());
		Assert.assertTrue(bucket.getPendingPartsPerCheckpoint().isEmpty());
	}

	@Test
	public void testBucketIsRemovedWhenNotActive() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
			new OnProcessingTimePolicy<>(2L);

		final Buckets<String, String> buckets =
				createBuckets(path, rollOnProcessingTimeCountingPolicy, 0);

		// it takes the current processing time of the context for the creation time, and for the last modification time.
		buckets.onElement("test", new TestUtils.MockSinkContext(1L, 2L, 3L));

		// now it should roll
		buckets.onProcessingTime(7L);
		Assert.assertEquals(1L, rollOnProcessingTimeCountingPolicy.getOnProcessingTimeRollCounter());

		buckets.snapshotState(0L, new MockListState<>(), new MockListState<>());
		buckets.commitUpToCheckpoint(0L);

		Assert.assertTrue(buckets.getActiveBuckets().isEmpty());
	}

	@Test
	public void testPartCounterAfterBucketResurrection() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
				new OnProcessingTimePolicy<>(2L);

		final Buckets<String, String> buckets =
				createBuckets(path, rollOnProcessingTimeCountingPolicy, 0);

		// it takes the current processing time of the context for the creation time, and for the last modification time.
		buckets.onElement("test", new TestUtils.MockSinkContext(1L, 2L, 3L));
		Assert.assertEquals(1L, buckets.getActiveBuckets().get("test").getPartCounter());

		// now it should roll
		buckets.onProcessingTime(7L);
		Assert.assertEquals(1L, rollOnProcessingTimeCountingPolicy.getOnProcessingTimeRollCounter());
		Assert.assertEquals(1L, buckets.getActiveBuckets().get("test").getPartCounter());

		buckets.snapshotState(0L, new MockListState<>(), new MockListState<>());
		buckets.commitUpToCheckpoint(0L);

		Assert.assertTrue(buckets.getActiveBuckets().isEmpty());

		buckets.onElement("test", new TestUtils.MockSinkContext(2L, 3L, 4L));
		Assert.assertEquals(2L, buckets.getActiveBuckets().get("test").getPartCounter());
	}

	private static class OnProcessingTimePolicy<IN, BucketID> implements RollingPolicy<IN, BucketID> {

		private static final long serialVersionUID = 1L;

		private int onProcessingTimeRollCounter = 0;

		private final long rolloverInterval;

		OnProcessingTimePolicy(final long rolloverInterval) {
			this.rolloverInterval = rolloverInterval;
		}

		public int getOnProcessingTimeRollCounter() {
			return onProcessingTimeRollCounter;
		}

		@Override
		public boolean shouldRollOnCheckpoint(PartFileInfo<BucketID> partFileState) {
			return false;
		}

		@Override
		public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState, IN element) {
			return false;
		}

		@Override
		public boolean shouldRollOnProcessingTime(PartFileInfo<BucketID> partFileState, long currentTime) {
			boolean result = currentTime - partFileState.getCreationTime() >= rolloverInterval;
			if (result) {
				onProcessingTimeRollCounter++;
			}
			return result;
		}
	}

	@Test
	public void testContextPassingNormalExecution() throws Exception {
		testCorrectTimestampPassingInContext(1L, 2L, 3L);
	}

	@Test
	public void testContextPassingNullTimestamp() throws Exception {
		testCorrectTimestampPassingInContext(null, 2L, 3L);
	}

	private void testCorrectTimestampPassingInContext(Long timestamp, long watermark, long processingTime) throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final Buckets<String, String> buckets = new Buckets<>(
				path,
				new VerifyingBucketAssigner(timestamp, watermark, processingTime),
				new DefaultBucketFactoryImpl<>(),
				new RowWisePartWriter.Factory<>(new SimpleStringEncoder<>()),
				DefaultRollingPolicy.builder().build(),
				2,
				OutputFileConfig.builder().build()
		);

		buckets.onElement(
				"test",
				new TestUtils.MockSinkContext(
						timestamp,
						watermark,
						processingTime)
		);
	}

	private static class VerifyingBucketAssigner implements BucketAssigner<String, String> {

		private static final long serialVersionUID = 7729086510972377578L;

		private final Long expectedTimestamp;
		private final long expectedWatermark;
		private final long expectedProcessingTime;

		VerifyingBucketAssigner(
				final Long expectedTimestamp,
				final long expectedWatermark,
				final long expectedProcessingTime
		) {
			this.expectedTimestamp = expectedTimestamp;
			this.expectedWatermark = expectedWatermark;
			this.expectedProcessingTime = expectedProcessingTime;
		}

		@Override
		public String getBucketId(String element, BucketAssigner.Context context) {
			final Long elementTimestamp = context.timestamp();
			final long watermark = context.currentWatermark();
			final long processingTime = context.currentProcessingTime();

			Assert.assertEquals(expectedTimestamp, elementTimestamp);
			Assert.assertEquals(expectedProcessingTime, processingTime);
			Assert.assertEquals(expectedWatermark, watermark);

			return element;
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

	// ------------------------------- Utility Methods --------------------------------

	private static Buckets<String, String> createBuckets(
			final Path basePath,
			final RollingPolicy<String, String> rollingPolicy,
			final int subtaskIdx) throws IOException {
		return createBuckets(
				basePath,
				rollingPolicy,
				subtaskIdx,
				OutputFileConfig.builder().build());
	}

	private static Buckets<String, String> createBuckets(
			final Path basePath,
			final RollingPolicy<String, String> rollingPolicy,
			final int subtaskIdx,
			final OutputFileConfig outputFileConfig) throws IOException {
		return new Buckets<>(
				basePath,
				new TestUtils.StringIdentityBucketAssigner(),
				new DefaultBucketFactoryImpl<>(),
				new RowWisePartWriter.Factory<>(new SimpleStringEncoder<>()),
				rollingPolicy,
				subtaskIdx,
				outputFileConfig
		);
	}

	private static Buckets<String, String> restoreBuckets(
			final Path basePath,
			final RollingPolicy<String, String> rollingPolicy,
			final int subtaskIdx,
			final ListState<byte[]> bucketState,
			final ListState<Long> partCounterState) throws Exception {
		return restoreBuckets(
				basePath,
				rollingPolicy,
				subtaskIdx,
				bucketState,
				partCounterState,
				OutputFileConfig.builder().build());
	}

	private static Buckets<String, String> restoreBuckets(
			final Path basePath,
			final RollingPolicy<String, String> rollingPolicy,
			final int subtaskIdx,
			final ListState<byte[]> bucketState,
			final ListState<Long> partCounterState,
			final OutputFileConfig outputFileConfig) throws Exception {
		final Buckets<String, String> restoredBuckets = createBuckets(basePath, rollingPolicy, subtaskIdx, outputFileConfig);
		restoredBuckets.initializeState(bucketState, partCounterState);
		return restoredBuckets;
	}
}
