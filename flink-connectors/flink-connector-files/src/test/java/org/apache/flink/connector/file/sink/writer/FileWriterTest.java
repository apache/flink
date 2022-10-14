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

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ScheduledFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileWriter}. */
class FileWriterTest {

    private MetricListener metricListener;

    @BeforeEach
    void setUp() {
        metricListener = new MetricListener();
    }

    @Test
    void testPreCommit(@TempDir java.nio.file.Path tempDir) throws Exception {
        Path path = new Path(tempDir.toUri());

        FileWriter<String> fileWriter =
                createWriter(
                        path, OnCheckpointRollingPolicy.build(), new OutputFileConfig("part-", ""));

        fileWriter.write("test1", new ContextImpl());
        fileWriter.write("test1", new ContextImpl());
        fileWriter.write("test2", new ContextImpl());
        fileWriter.write("test2", new ContextImpl());
        fileWriter.write("test3", new ContextImpl());

        Collection<FileSinkCommittable> committables = fileWriter.prepareCommit();

        assertThat(committables.size()).isEqualTo(3);
    }

    @Test
    void testSnapshotAndRestore(@TempDir java.nio.file.Path tempDir) throws Exception {
        Path path = new Path(tempDir.toUri());

        FileWriter<String> fileWriter =
                createWriter(
                        path,
                        DefaultRollingPolicy.builder().build(),
                        new OutputFileConfig("part-", ""));

        fileWriter.write("test1", new ContextImpl());
        fileWriter.write("test2", new ContextImpl());
        fileWriter.write("test3", new ContextImpl());
        assertThat(fileWriter.getActiveBuckets().size()).isEqualTo(3);

        fileWriter.prepareCommit();
        List<FileWriterBucketState> states = fileWriter.snapshotState(1L);

        assertThat(states.size()).isEqualTo(3);

        fileWriter =
                restoreWriter(
                        states,
                        path,
                        OnCheckpointRollingPolicy.build(),
                        new OutputFileConfig("part-", ""));

        assertThat(fileWriter.getActiveBuckets().keySet())
                .isEqualTo(new HashSet<>(Arrays.asList("test1", "test2", "test3")));

        for (FileWriterBucket<String> bucket : fileWriter.getActiveBuckets().values()) {
            assertThat(bucket.getInProgressPart())
                    .as("The in-progress file should be recovered")
                    .isNotNull();
        }
    }

    @Test
    void testMergingForRescaling(@TempDir java.nio.file.Path tempDir) throws Exception {
        Path path = new Path(tempDir.toUri());

        FileWriter<String> firstFileWriter =
                createWriter(
                        path,
                        DefaultRollingPolicy.builder().build(),
                        new OutputFileConfig("part-", ""));

        firstFileWriter.write("test1", new ContextImpl());
        firstFileWriter.write("test2", new ContextImpl());
        firstFileWriter.write("test3", new ContextImpl());

        firstFileWriter.prepareCommit();
        List<FileWriterBucketState> firstState = firstFileWriter.snapshotState(1L);

        FileWriter<String> secondFileWriter =
                createWriter(
                        path,
                        DefaultRollingPolicy.builder().build(),
                        new OutputFileConfig("part-", ""));

        secondFileWriter.write("test1", new ContextImpl());
        secondFileWriter.write("test2", new ContextImpl());

        secondFileWriter.prepareCommit();
        List<FileWriterBucketState> secondState = secondFileWriter.snapshotState(1L);

        List<FileWriterBucketState> mergedState = new ArrayList<>();
        mergedState.addAll(firstState);
        mergedState.addAll(secondState);

        FileWriter<String> restoredWriter =
                restoreWriter(
                        mergedState,
                        path,
                        DefaultRollingPolicy.builder().build(),
                        new OutputFileConfig("part-", ""));

        assertThat(restoredWriter.getActiveBuckets().size()).isEqualTo(3);

        // Merged buckets
        for (String bucketId : Arrays.asList("test1", "test2")) {
            FileWriterBucket<String> bucket = restoredWriter.getActiveBuckets().get(bucketId);

            assertThat(bucket.getInProgressPart())
                    .as("The in-progress file should be recovered")
                    .isNotNull();
            assertThat(bucket.getPendingFiles().size()).isEqualTo(1);
        }

        // Not merged buckets
        for (String bucketId : Collections.singletonList("test3")) {
            FileWriterBucket<String> bucket = restoredWriter.getActiveBuckets().get(bucketId);

            assertThat(bucket.getInProgressPart())
                    .as("The in-progress file should be recovered")
                    .isNotNull();
            assertThat(bucket.getPendingFiles().size()).isEqualTo(0);
        }
    }

    @Test
    void testBucketIsRemovedWhenNotActive(@TempDir java.nio.file.Path tempDir) throws Exception {
        Path path = new Path(tempDir.toUri());

        FileWriter<String> fileWriter =
                createWriter(
                        path, OnCheckpointRollingPolicy.build(), new OutputFileConfig("part-", ""));

        fileWriter.write("test", new ContextImpl());
        fileWriter.prepareCommit();
        fileWriter.snapshotState(1L);

        // No more records and another call to prepareCommit will makes it inactive
        fileWriter.prepareCommit();

        assertThat(fileWriter.getActiveBuckets().isEmpty()).isTrue();
    }

    @Test
    void testOnProcessingTime(@TempDir java.nio.file.Path tempDir) throws Exception {
        Path path = new Path(tempDir.toUri());

        // Create the processing timer service starts from 10.
        ManuallyTriggeredProcessingTimeService processingTimeService =
                new ManuallyTriggeredProcessingTimeService();
        processingTimeService.advanceTo(10);

        FileWriter<String> fileWriter =
                createWriter(
                        path,
                        new FileSinkTestUtils.StringIdentityBucketAssigner(),
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMillis(10))
                                .build(),
                        new OutputFileConfig("part-", ""),
                        processingTimeService,
                        5);
        fileWriter.initializeState(Collections.emptyList());

        // Test timer registered timer@15 on startup
        fileWriter.write("test1", new ContextImpl());
        processingTimeService.advanceTo(15);
        fileWriter.write("test2", new ContextImpl());
        processingTimeService.advanceTo(20);

        FileWriterBucket<String> test1Bucket = fileWriter.getActiveBuckets().get("test1");

        assertThat(test1Bucket.getInProgressPart())
                .as("The in-progress part of test1 should be rolled")
                .isNull();
        assertThat(test1Bucket.getPendingFiles().size()).isEqualTo(1);

        FileWriterBucket<String> test2Bucket = fileWriter.getActiveBuckets().get("test2");

        assertThat(test2Bucket.getInProgressPart())
                .as("The in-progress part of test2 should not be rolled")
                .isNotNull();
        assertThat(test2Bucket.getPendingFiles().size()).isEqualTo(0);

        // Close, pre-commit & clear all the pending records.
        processingTimeService.advanceTo(30);
        fileWriter.prepareCommit();

        // Test timer re-registration.
        fileWriter.write("test1", new ContextImpl());
        processingTimeService.advanceTo(35);
        fileWriter.write("test2", new ContextImpl());
        processingTimeService.advanceTo(40);

        test1Bucket = fileWriter.getActiveBuckets().get("test1");

        assertThat(test1Bucket.getInProgressPart())
                .as("The in-progress part of test1 should be rolled")
                .isNull();
        assertThat(test1Bucket.getPendingFiles().size()).isEqualTo(1);

        test2Bucket = fileWriter.getActiveBuckets().get("test2");

        assertThat(test2Bucket.getInProgressPart())
                .as("The in-progress part of test2 should not be rolled")
                .isNotNull();
        assertThat(test2Bucket.getPendingFiles().size()).isEqualTo(0);
    }

    @Test
    void testContextPassingNormalExecution(@TempDir java.nio.file.Path tempDir) throws Exception {
        testCorrectTimestampPassingInContext(1L, 2L, 3L, tempDir);
    }

    @Test
    void testContextPassingNullTimestamp(@TempDir java.nio.file.Path tempDir) throws Exception {
        testCorrectTimestampPassingInContext(null, 4L, 5L, tempDir);
    }

    @Test
    void testNumberRecordsOutCounter(@TempDir java.nio.file.Path tempDir)
            throws IOException, InterruptedException {
        Path path = new Path(tempDir.toUri());

        final OperatorIOMetricGroup operatorIOMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup().getIOMetricGroup();
        final SinkWriterMetricGroup sinkWriterMetricGroup =
                InternalSinkWriterMetricGroup.mock(
                        metricListener.getMetricGroup(), operatorIOMetricGroup);

        Counter recordsCounter = sinkWriterMetricGroup.getIOMetricGroup().getNumRecordsOutCounter();
        SinkWriter.Context context = new ContextImpl();
        FileWriter<String> fileWriter =
                createWriter(
                        path,
                        DefaultRollingPolicy.builder().build(),
                        new OutputFileConfig("part-", ""),
                        sinkWriterMetricGroup);

        assertThat(recordsCounter.getCount()).isEqualTo(0);

        fileWriter.write("1", context);

        assertThat(recordsCounter.getCount()).isEqualTo(1);

        fileWriter.write("2", context);
        fileWriter.write("3", context);

        assertThat(recordsCounter.getCount()).isEqualTo(3);
    }

    private void testCorrectTimestampPassingInContext(
            Long timestamp, long watermark, long processingTime, java.nio.file.Path tempDir)
            throws Exception {
        Path path = new Path(tempDir.toUri());

        // Create the processing timer service starts from 10.
        ManuallyTriggeredProcessingTimeService processingTimeService =
                new ManuallyTriggeredProcessingTimeService();
        processingTimeService.advanceTo(processingTime);

        FileWriter<String> fileWriter =
                createWriter(
                        path,
                        new VerifyingBucketAssigner(timestamp, watermark, processingTime),
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMillis(10))
                                .build(),
                        new OutputFileConfig("part-", ""),
                        processingTimeService,
                        5);
        fileWriter.initializeState(Collections.emptyList());
        fileWriter.write("test", new ContextImpl(watermark, timestamp));
    }

    // ------------------------------- Mock Classes --------------------------------

    private static class ContextImpl implements SinkWriter.Context {
        private final long watermark;
        private final Long timestamp;

        public ContextImpl() {
            this(0, 0L);
        }

        private ContextImpl(long watermark, Long timestamp) {
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

    private static class ManuallyTriggeredProcessingTimeService implements ProcessingTimeService {

        private long now;

        private final Queue<Tuple2<Long, ProcessingTimeCallback>> timers =
                new PriorityQueue<>(Comparator.comparingLong(o -> o.f0));

        @Override
        public long getCurrentProcessingTime() {
            return now;
        }

        @Override
        public ScheduledFuture<?> registerTimer(
                long time, ProcessingTimeCallback processingTimeCallback) {
            if (time <= now) {
                try {
                    processingTimeCallback.onProcessingTime(now);
                } catch (Exception e) {
                    ExceptionUtils.rethrow(e);
                }
            } else {
                timers.add(new Tuple2<>(time, processingTimeCallback));
            }
            return null;
        }

        public void advanceTo(long time) throws Exception {
            if (time > now) {
                now = time;

                Tuple2<Long, ProcessingTimeCallback> timer;
                while ((timer = timers.peek()) != null && timer.f0 <= now) {
                    timer.f1.onProcessingTime(now);
                    timers.poll();
                }
            }
        }
    }

    private static class VerifyingBucketAssigner implements BucketAssigner<String, String> {

        private static final long serialVersionUID = 7729086510972377578L;

        private final Long expectedTimestamp;
        private final long expectedWatermark;
        private final long expectedProcessingTime;

        VerifyingBucketAssigner(
                Long expectedTimestamp, long expectedWatermark, long expectedProcessingTime) {
            this.expectedTimestamp = expectedTimestamp;
            this.expectedWatermark = expectedWatermark;
            this.expectedProcessingTime = expectedProcessingTime;
        }

        @Override
        public String getBucketId(String element, BucketAssigner.Context context) {
            Long elementTimestamp = context.timestamp();
            long watermark = context.currentWatermark();
            long processingTime = context.currentProcessingTime();

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

    // ------------------------------- Utility Methods --------------------------------

    private FileWriter<String> createWriter(
            Path basePath,
            RollingPolicy<String, String> rollingPolicy,
            OutputFileConfig outputFileConfig,
            SinkWriterMetricGroup sinkWriterMetricGroup)
            throws IOException {
        return new FileWriter<>(
                basePath,
                sinkWriterMetricGroup,
                new FileSinkTestUtils.StringIdentityBucketAssigner(),
                new DefaultFileWriterBucketFactory<>(),
                new RowWiseBucketWriter<>(
                        FileSystem.get(basePath.toUri()).createRecoverableWriter(),
                        new SimpleStringEncoder<>()),
                rollingPolicy,
                outputFileConfig,
                new ManuallyTriggeredProcessingTimeService(),
                10);
    }

    private FileWriter<String> createWriter(
            Path basePath,
            RollingPolicy<String, String> rollingPolicy,
            OutputFileConfig outputFileConfig)
            throws IOException {
        return createWriter(
                basePath,
                rollingPolicy,
                outputFileConfig,
                InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup()));
    }

    private FileWriter<String> createWriter(
            Path basePath,
            BucketAssigner<String, String> bucketAssigner,
            RollingPolicy<String, String> rollingPolicy,
            OutputFileConfig outputFileConfig,
            ProcessingTimeService processingTimeService,
            long bucketCheckInterval)
            throws IOException {
        return new FileWriter<>(
                basePath,
                InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup()),
                bucketAssigner,
                new DefaultFileWriterBucketFactory<>(),
                new RowWiseBucketWriter<>(
                        FileSystem.get(basePath.toUri()).createRecoverableWriter(),
                        new SimpleStringEncoder<>()),
                rollingPolicy,
                outputFileConfig,
                processingTimeService,
                bucketCheckInterval);
    }

    private FileWriter<String> restoreWriter(
            List<FileWriterBucketState> states,
            Path basePath,
            RollingPolicy<String, String> rollingPolicy,
            OutputFileConfig outputFileConfig)
            throws IOException {
        FileWriter<String> writer = createWriter(basePath, rollingPolicy, outputFileConfig);
        writer.initializeState(states);
        return writer;
    }
}
