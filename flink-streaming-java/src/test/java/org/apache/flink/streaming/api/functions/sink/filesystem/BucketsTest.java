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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.TestUtils.MockListState;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Tests for {@link Buckets}. */
class BucketsTest {

    @TempDir private static java.nio.file.Path tempFolder;

    @Test
    void testSnapshotAndRestore() throws Exception {
        final File outDir = TempDirUtils.newFolder(tempFolder);
        final Path path = new Path(outDir.toURI());

        final RollingPolicy<String, String> onCheckpointRollingPolicy =
                OnCheckpointRollingPolicy.build();

        final Buckets<String, String> buckets = createBuckets(path, onCheckpointRollingPolicy, 0);

        final ListState<byte[]> bucketStateContainer = new MockListState<>();
        final ListState<Long> partCounterContainer = new MockListState<>();

        buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
        buckets.snapshotState(0L, bucketStateContainer, partCounterContainer);

        assertThat(buckets.getActiveBuckets().get("test1"))
                .is(matching(hasSinglePartFileToBeCommittedOnCheckpointAck(path, "test1")));

        buckets.onElement("test2", new TestUtils.MockSinkContext(null, 1L, 2L));
        buckets.snapshotState(1L, bucketStateContainer, partCounterContainer);

        assertThat(buckets.getActiveBuckets().get("test1"))
                .is(matching(hasSinglePartFileToBeCommittedOnCheckpointAck(path, "test1")));
        assertThat(buckets.getActiveBuckets().get("test2"))
                .is(matching(hasSinglePartFileToBeCommittedOnCheckpointAck(path, "test2")));

        Buckets<String, String> restoredBuckets =
                restoreBuckets(
                        path,
                        onCheckpointRollingPolicy,
                        0,
                        bucketStateContainer,
                        partCounterContainer);

        final Map<String, Bucket<String, String>> activeBuckets =
                restoredBuckets.getActiveBuckets();

        // because we commit pending files for previous checkpoints upon recovery
        assertThat(activeBuckets).isEmpty();
    }

    private static TypeSafeMatcher<Bucket<String, String>>
            hasSinglePartFileToBeCommittedOnCheckpointAck(
                    final Path testTmpPath, final String bucketId) {
        return new TypeSafeMatcher<Bucket<String, String>>() {
            @Override
            protected boolean matchesSafely(Bucket<String, String> bucket) {
                return bucket.getBucketId().equals(bucketId)
                        && bucket.getBucketPath().equals(new Path(testTmpPath, bucketId))
                        && bucket.getInProgressPart() == null
                        && bucket.getPendingFileRecoverablesForCurrentCheckpoint().isEmpty()
                        && bucket.getPendingFileRecoverablesPerCheckpoint().size() == 1;
            }

            @Override
            public void describeTo(Description description) {
                description
                        .appendText("a Bucket with a single pending part file @ ")
                        .appendValue(new Path(testTmpPath, bucketId))
                        .appendText("'");
            }
        };
    }

    @Test
    void testMergeAtScaleInAndMaxCounterAtRecovery() throws Exception {
        final File outDir = TempDirUtils.newFolder(tempFolder);
        final Path path = new Path(outDir.toURI());

        final RollingPolicy<String, String> onCheckpointRP =
                DefaultRollingPolicy.builder()
                        .withMaxPartSize(new MemorySize(7L)) // roll with 2 elements
                        .build();

        final MockListState<byte[]> bucketStateContainerOne = new MockListState<>();
        final MockListState<byte[]> bucketStateContainerTwo = new MockListState<>();

        final MockListState<Long> partCounterContainerOne = new MockListState<>();
        final MockListState<Long> partCounterContainerTwo = new MockListState<>();

        final Buckets<String, String> bucketsOne = createBuckets(path, onCheckpointRP, 0);
        final Buckets<String, String> bucketsTwo = createBuckets(path, onCheckpointRP, 1);

        bucketsOne.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
        bucketsOne.snapshotState(0L, bucketStateContainerOne, partCounterContainerOne);

        assertThat(bucketsOne.getMaxPartCounter()).isOne();

        // make sure we have one in-progress file here
        assertThat(bucketsOne.getActiveBuckets().get("test1").getInProgressPart()).isNotNull();

        // add a couple of in-progress files so that the part counter increases.
        bucketsTwo.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
        bucketsTwo.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));

        bucketsTwo.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));

        bucketsTwo.snapshotState(0L, bucketStateContainerTwo, partCounterContainerTwo);

        assertThat(bucketsTwo.getMaxPartCounter()).isEqualTo(2L);

        // make sure we have one in-progress file here and a pending
        assertThat(
                        bucketsTwo
                                .getActiveBuckets()
                                .get("test1")
                                .getPendingFileRecoverablesPerCheckpoint())
                .hasSize(1);
        assertThat(bucketsTwo.getActiveBuckets().get("test1").getInProgressPart()).isNotNull();

        final ListState<byte[]> mergedBucketStateContainer = new MockListState<>();
        final ListState<Long> mergedPartCounterContainer = new MockListState<>();

        mergedBucketStateContainer.addAll(bucketStateContainerOne.getBackingList());
        mergedBucketStateContainer.addAll(bucketStateContainerTwo.getBackingList());

        mergedPartCounterContainer.addAll(partCounterContainerOne.getBackingList());
        mergedPartCounterContainer.addAll(partCounterContainerTwo.getBackingList());

        final Buckets<String, String> restoredBuckets =
                restoreBuckets(
                        path,
                        onCheckpointRP,
                        0,
                        mergedBucketStateContainer,
                        mergedPartCounterContainer);

        // we get the maximum of the previous tasks
        assertThat(restoredBuckets.getMaxPartCounter()).isEqualTo(2L);

        final Map<String, Bucket<String, String>> activeBuckets =
                restoredBuckets.getActiveBuckets();
        assertThat(activeBuckets).hasSize(1).containsKey("test1");

        final Bucket<String, String> bucket = activeBuckets.get("test1");
        assertThat(bucket.getBucketId()).isEqualTo("test1");
        assertThat(bucket.getBucketPath()).isEqualTo(new Path(path, "test1"));

        assertThat(bucket.getInProgressPart()).isNotNull(); // the restored part file

        // this is due to the Bucket#merge(). The in progress file of one
        // of the previous tasks is put in the list of pending files.
        assertThat(bucket.getPendingFileRecoverablesForCurrentCheckpoint()).hasSize(1);

        // we commit the pending for previous checkpoints
        assertThat(bucket.getPendingFileRecoverablesPerCheckpoint()).isEmpty();
    }

    @Test
    void testOnProcessingTime() throws Exception {
        final File outDir = TempDirUtils.newFolder(tempFolder);
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
        assertThat(rollOnProcessingTimeCountingPolicy.getOnProcessingTimeRollCounter()).isOne();

        final Map<String, Bucket<String, String>> activeBuckets = buckets.getActiveBuckets();
        assertThat(activeBuckets).hasSize(1).containsKey("test");

        final Bucket<String, String> bucket = activeBuckets.get("test");
        assertThat(bucket.getBucketId()).isEqualTo("test");
        assertThat(bucket.getBucketPath()).isEqualTo(new Path(path, "test"));
        assertThat(bucket.getBucketId()).isEqualTo("test");

        assertThat(bucket.getInProgressPart()).isNull();
        assertThat(bucket.getPendingFileRecoverablesForCurrentCheckpoint()).hasSize(1);
        assertThat(bucket.getPendingFileRecoverablesPerCheckpoint()).isEmpty();
    }

    @Test
    void testBucketIsRemovedWhenNotActive() throws Exception {
        final File outDir = TempDirUtils.newFolder(tempFolder);
        final Path path = new Path(outDir.toURI());

        final OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
                new OnProcessingTimePolicy<>(2L);

        final Buckets<String, String> buckets =
                createBuckets(path, rollOnProcessingTimeCountingPolicy, 0);

        // it takes the current processing time of the context for the creation time, and for the
        // last modification time.
        buckets.onElement("test", new TestUtils.MockSinkContext(1L, 2L, 3L));

        // now it should roll
        buckets.onProcessingTime(7L);
        assertThat(rollOnProcessingTimeCountingPolicy.getOnProcessingTimeRollCounter()).isOne();

        buckets.snapshotState(0L, new MockListState<>(), new MockListState<>());
        buckets.commitUpToCheckpoint(0L);

        assertThat(buckets.getActiveBuckets()).isEmpty();
    }

    @Test
    void testPartCounterAfterBucketResurrection() throws Exception {
        final File outDir = TempDirUtils.newFolder(tempFolder);
        final Path path = new Path(outDir.toURI());

        final OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
                new OnProcessingTimePolicy<>(2L);

        final Buckets<String, String> buckets =
                createBuckets(path, rollOnProcessingTimeCountingPolicy, 0);

        // it takes the current processing time of the context for the creation time, and for the
        // last modification time.
        buckets.onElement("test", new TestUtils.MockSinkContext(1L, 2L, 3L));
        assertThat(buckets.getActiveBuckets().get("test").getPartCounter()).isOne();

        // now it should roll
        buckets.onProcessingTime(7L);
        assertThat(rollOnProcessingTimeCountingPolicy.getOnProcessingTimeRollCounter()).isOne();
        assertThat(buckets.getActiveBuckets().get("test").getPartCounter()).isOne();

        buckets.snapshotState(0L, new MockListState<>(), new MockListState<>());
        buckets.commitUpToCheckpoint(0L);

        assertThat(buckets.getActiveBuckets()).isEmpty();

        buckets.onElement("test", new TestUtils.MockSinkContext(2L, 3L, 4L));
        assertThat(buckets.getActiveBuckets().get("test").getPartCounter()).isEqualTo(2L);
    }

    private static class OnProcessingTimePolicy<IN, BucketID>
            implements RollingPolicy<IN, BucketID> {

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
        public boolean shouldRollOnProcessingTime(
                PartFileInfo<BucketID> partFileState, long currentTime) {
            boolean result = currentTime - partFileState.getCreationTime() >= rolloverInterval;
            if (result) {
                onProcessingTimeRollCounter++;
            }
            return result;
        }
    }

    @Test
    void testContextPassingNormalExecution() throws Exception {
        testCorrectTimestampPassingInContext(1L, 2L, 3L);
    }

    @Test
    void testContextPassingNullTimestamp() throws Exception {
        testCorrectTimestampPassingInContext(null, 2L, 3L);
    }

    private void testCorrectTimestampPassingInContext(
            Long timestamp, long watermark, long processingTime) throws Exception {
        final File outDir = TempDirUtils.newFolder(tempFolder);
        final Path path = new Path(outDir.toURI());

        final Buckets<String, String> buckets =
                new Buckets<>(
                        path,
                        new VerifyingBucketAssigner(timestamp, watermark, processingTime),
                        new DefaultBucketFactoryImpl<>(),
                        new RowWiseBucketWriter<>(
                                FileSystem.get(path.toUri()).createRecoverableWriter(),
                                new SimpleStringEncoder<>()),
                        DefaultRollingPolicy.builder().build(),
                        2,
                        OutputFileConfig.builder().build());

        buckets.onElement(
                "test", new TestUtils.MockSinkContext(timestamp, watermark, processingTime));
    }

    private static class VerifyingBucketAssigner implements BucketAssigner<String, String> {

        private static final long serialVersionUID = 7729086510972377578L;

        private final Long expectedTimestamp;
        private final long expectedWatermark;
        private final long expectedProcessingTime;

        VerifyingBucketAssigner(
                final Long expectedTimestamp,
                final long expectedWatermark,
                final long expectedProcessingTime) {
            this.expectedTimestamp = expectedTimestamp;
            this.expectedWatermark = expectedWatermark;
            this.expectedProcessingTime = expectedProcessingTime;
        }

        @Override
        public String getBucketId(String element, BucketAssigner.Context context) {
            final Long elementTimestamp = context.timestamp();
            final long watermark = context.currentWatermark();
            final long processingTime = context.currentProcessingTime();

            assertThat(elementTimestamp).isEqualTo(expectedTimestamp);
            assertThat(processingTime).isEqualTo(expectedProcessingTime);
            assertThat(watermark).isEqualTo(expectedWatermark);

            return element;
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    @Test
    void testBucketLifeCycleListenerOnCreatingAndInactive() throws Exception {
        File outDir = TempDirUtils.newFolder(tempFolder);
        Path path = new Path(outDir.toURI());
        OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
                new OnProcessingTimePolicy<>(2L);
        RecordBucketLifeCycleListener bucketLifeCycleListener = new RecordBucketLifeCycleListener();
        Buckets<String, String> buckets =
                createBuckets(
                        path,
                        rollOnProcessingTimeCountingPolicy,
                        bucketLifeCycleListener,
                        null,
                        0,
                        OutputFileConfig.builder().build());
        ListState<byte[]> bucketStateContainer = new MockListState<>();
        ListState<Long> partCounterContainer = new MockListState<>();

        buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
        buckets.onElement("test2", new TestUtils.MockSinkContext(null, 1L, 3L));

        // Will close the part file writer of the bucket "test1".
        buckets.onProcessingTime(4);
        buckets.snapshotState(0, bucketStateContainer, partCounterContainer);
        buckets.commitUpToCheckpoint(0);

        // Will close the part file writer of the bucket "test2".
        buckets.onProcessingTime(6);
        buckets.snapshotState(1, bucketStateContainer, partCounterContainer);
        buckets.commitUpToCheckpoint(1);

        List<Tuple2<RecordBucketLifeCycleListener.EventType, String>> expectedEvents =
                Arrays.asList(
                        new Tuple2<>(RecordBucketLifeCycleListener.EventType.CREATED, "test1"),
                        new Tuple2<>(RecordBucketLifeCycleListener.EventType.CREATED, "test2"),
                        new Tuple2<>(RecordBucketLifeCycleListener.EventType.INACTIVE, "test1"),
                        new Tuple2<>(RecordBucketLifeCycleListener.EventType.INACTIVE, "test2"));
        assertThat(bucketLifeCycleListener.getEvents()).isEqualTo(expectedEvents);
    }

    @Test
    void testBucketLifeCycleListenerOnRestoring() throws Exception {
        File outDir = TempDirUtils.newFolder(tempFolder);
        Path path = new Path(outDir.toURI());
        OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
                new OnProcessingTimePolicy<>(2L);
        RecordBucketLifeCycleListener bucketLifeCycleListener = new RecordBucketLifeCycleListener();
        Buckets<String, String> buckets =
                createBuckets(
                        path,
                        rollOnProcessingTimeCountingPolicy,
                        bucketLifeCycleListener,
                        null,
                        0,
                        OutputFileConfig.builder().build());
        ListState<byte[]> bucketStateContainer = new MockListState<>();
        ListState<Long> partCounterContainer = new MockListState<>();

        buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
        buckets.onElement("test2", new TestUtils.MockSinkContext(null, 1L, 3L));

        // Will close the part file writer of the bucket "test1". Now bucket "test1" have only
        // one pending file while bucket "test2" has an on-writing in-progress file.
        buckets.onProcessingTime(4);
        buckets.snapshotState(0, bucketStateContainer, partCounterContainer);

        // On restoring the bucket "test1" will commit its pending file and become inactive.
        buckets =
                restoreBuckets(
                        path,
                        rollOnProcessingTimeCountingPolicy,
                        bucketLifeCycleListener,
                        null,
                        0,
                        bucketStateContainer,
                        partCounterContainer,
                        OutputFileConfig.builder().build());

        assertThat(buckets.getActiveBuckets().keySet()).containsOnly("test2");
        List<Tuple2<RecordBucketLifeCycleListener.EventType, String>> expectedEvents =
                Arrays.asList(
                        new Tuple2<>(RecordBucketLifeCycleListener.EventType.CREATED, "test1"),
                        new Tuple2<>(RecordBucketLifeCycleListener.EventType.CREATED, "test2"),
                        new Tuple2<>(RecordBucketLifeCycleListener.EventType.INACTIVE, "test1"));
        assertThat(bucketLifeCycleListener.getEvents()).isEqualTo(expectedEvents);
    }

    private static class RecordBucketLifeCycleListener
            implements BucketLifeCycleListener<String, String> {
        public enum EventType {
            CREATED,
            INACTIVE
        }

        private List<Tuple2<EventType, String>> events = new ArrayList<>();

        @Override
        public void bucketCreated(Bucket<String, String> bucket) {
            events.add(new Tuple2<>(EventType.CREATED, bucket.getBucketId()));
        }

        @Override
        public void bucketInactive(Bucket<String, String> bucket) {
            events.add(new Tuple2<>(EventType.INACTIVE, bucket.getBucketId()));
        }

        public List<Tuple2<EventType, String>> getEvents() {
            return events;
        }
    }

    @Test
    void testFileLifeCycleListener() throws Exception {
        File outDir = TempDirUtils.newFolder(tempFolder);
        Path path = new Path(outDir.toURI());

        OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
                new OnProcessingTimePolicy<>(2L);

        TestFileLifeCycleListener fileLifeCycleListener = new TestFileLifeCycleListener();
        Buckets<String, String> buckets =
                createBuckets(
                        path,
                        rollOnProcessingTimeCountingPolicy,
                        null,
                        fileLifeCycleListener,
                        0,
                        OutputFileConfig.builder().build());

        buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
        buckets.onElement("test2", new TestUtils.MockSinkContext(null, 1L, 3L));

        // Will close the part file writer of the bucket "test1". Now bucket "test1" have only
        // one pending file while bucket "test2" has an on-writing in-progress file.
        buckets.onProcessingTime(4);

        buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 5L));
        buckets.onElement("test2", new TestUtils.MockSinkContext(null, 1L, 6L));

        assertThat(fileLifeCycleListener.files).hasSize(2);
        assertThat(fileLifeCycleListener.files.get("test1"))
                .containsExactly("part-0-0", "part-0-1");
        assertThat(fileLifeCycleListener.files.get("test2")).containsExactly("part-0-1");
    }

    private static class TestFileLifeCycleListener implements FileLifeCycleListener<String> {

        private final Map<String, List<String>> files = new HashMap<>();

        @Override
        public void onPartFileOpened(String bucket, Path newPath) {
            files.computeIfAbsent(bucket, k -> new ArrayList<>()).add(newPath.getName());
        }
    }

    // ------------------------------- Utility Methods --------------------------------

    private static Buckets<String, String> createBuckets(
            final Path basePath,
            final RollingPolicy<String, String> rollingPolicy,
            final int subtaskIdx)
            throws IOException {
        return createBuckets(
                basePath,
                rollingPolicy,
                null,
                null,
                subtaskIdx,
                OutputFileConfig.builder().build());
    }

    private static Buckets<String, String> createBuckets(
            final Path basePath,
            final RollingPolicy<String, String> rollingPolicy,
            final BucketLifeCycleListener<String, String> bucketLifeCycleListener,
            final FileLifeCycleListener<String> fileLifeCycleListener,
            final int subtaskIdx,
            final OutputFileConfig outputFileConfig)
            throws IOException {
        Buckets<String, String> buckets =
                new Buckets<>(
                        basePath,
                        new TestUtils.StringIdentityBucketAssigner(),
                        new DefaultBucketFactoryImpl<>(),
                        new RowWiseBucketWriter<>(
                                FileSystem.get(basePath.toUri()).createRecoverableWriter(),
                                new SimpleStringEncoder<>()),
                        rollingPolicy,
                        subtaskIdx,
                        outputFileConfig);

        if (bucketLifeCycleListener != null) {
            buckets.setBucketLifeCycleListener(bucketLifeCycleListener);
        }

        if (fileLifeCycleListener != null) {
            buckets.setFileLifeCycleListener(fileLifeCycleListener);
        }

        return buckets;
    }

    private static Buckets<String, String> restoreBuckets(
            final Path basePath,
            final RollingPolicy<String, String> rollingPolicy,
            final int subtaskIdx,
            final ListState<byte[]> bucketState,
            final ListState<Long> partCounterState)
            throws Exception {
        return restoreBuckets(
                basePath,
                rollingPolicy,
                null,
                null,
                subtaskIdx,
                bucketState,
                partCounterState,
                OutputFileConfig.builder().build());
    }

    private static Buckets<String, String> restoreBuckets(
            final Path basePath,
            final RollingPolicy<String, String> rollingPolicy,
            final BucketLifeCycleListener<String, String> bucketLifeCycleListener,
            final FileLifeCycleListener<String> fileLifeCycleListener,
            final int subtaskIdx,
            final ListState<byte[]> bucketState,
            final ListState<Long> partCounterState,
            final OutputFileConfig outputFileConfig)
            throws Exception {
        final Buckets<String, String> restoredBuckets =
                createBuckets(
                        basePath,
                        rollingPolicy,
                        bucketLifeCycleListener,
                        fileLifeCycleListener,
                        subtaskIdx,
                        outputFileConfig);
        restoredBuckets.initializeState(bucketState, partCounterState);
        return restoredBuckets;
    }
}
