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
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link FileWriter}. */
public class FileWriterTest {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Test
    public void testPreCommit() throws Exception {
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());

        FileWriter<String> fileWriter =
                createWriter(
                        path, OnCheckpointRollingPolicy.build(), new OutputFileConfig("part-", ""));

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

        FileWriter<String> fileWriter =
                createWriter(
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

        fileWriter =
                restoreWriter(
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

        FileWriter<String> firstFileWriter =
                createWriter(
                        path,
                        DefaultRollingPolicy.builder().build(),
                        new OutputFileConfig("part-", ""));

        firstFileWriter.write("test1", new ContextImpl());
        firstFileWriter.write("test2", new ContextImpl());
        firstFileWriter.write("test3", new ContextImpl());

        firstFileWriter.prepareCommit(false);
        List<FileWriterBucketState> firstState = firstFileWriter.snapshotState();

        FileWriter<String> secondFileWriter =
                createWriter(
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

        FileWriter<String> restoredWriter =
                restoreWriter(
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

        FileWriter<String> fileWriter =
                createWriter(
                        path, OnCheckpointRollingPolicy.build(), new OutputFileConfig("part-", ""));

        fileWriter.write("test", new ContextImpl());
        fileWriter.prepareCommit(false);
        fileWriter.snapshotState();

        // No more records and another call to prepareCommit will makes it inactive
        fileWriter.prepareCommit(false);

        assertTrue(fileWriter.getActiveBuckets().isEmpty());
    }

    @Test
    public void testOnProcessingTime() throws IOException {
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());

        // Create the processing timer service starts from 10.
        ManuallyTriggeredProcessingTimeService processingTimeService =
                new ManuallyTriggeredProcessingTimeService();
        processingTimeService.advanceTo(10);

        FileWriter<String> fileWriter =
                createWriter(
                        path,
                        new FileSinkTestUtils.StringIdentityBucketAssigner(),
                        DefaultRollingPolicy.builder().withRolloverInterval(10).build(),
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
        assertNull(
                "The in-progress part of test1 should be rolled", test1Bucket.getInProgressPart());
        assertEquals(1, test1Bucket.getPendingFiles().size());

        FileWriterBucket<String> test2Bucket = fileWriter.getActiveBuckets().get("test2");
        assertNotNull(
                "The in-progress part of test2 should not be rolled",
                test2Bucket.getInProgressPart());
        assertEquals(0, test2Bucket.getPendingFiles().size());

        // Close, pre-commit & clear all the pending records.
        processingTimeService.advanceTo(30);
        fileWriter.prepareCommit(false);

        // Test timer re-registration.
        fileWriter.write("test1", new ContextImpl());
        processingTimeService.advanceTo(35);
        fileWriter.write("test2", new ContextImpl());
        processingTimeService.advanceTo(40);

        test1Bucket = fileWriter.getActiveBuckets().get("test1");
        assertNull(
                "The in-progress part of test1 should be rolled", test1Bucket.getInProgressPart());
        assertEquals(1, test1Bucket.getPendingFiles().size());

        test2Bucket = fileWriter.getActiveBuckets().get("test2");
        assertNotNull(
                "The in-progress part of test2 should not be rolled",
                test2Bucket.getInProgressPart());
        assertEquals(0, test2Bucket.getPendingFiles().size());
    }

    @Test
    public void testContextPassingNormalExecution() throws Exception {
        testCorrectTimestampPassingInContext(1L, 2L, 3L);
    }

    @Test
    public void testContextPassingNullTimestamp() throws Exception {
        testCorrectTimestampPassingInContext(null, 4L, 5L);
    }

    private void testCorrectTimestampPassingInContext(
            Long timestamp, long watermark, long processingTime) throws Exception {
        final File outDir = TEMP_FOLDER.newFolder();
        final Path path = new Path(outDir.toURI());

        // Create the processing timer service starts from 10.
        ManuallyTriggeredProcessingTimeService processingTimeService =
                new ManuallyTriggeredProcessingTimeService();
        processingTimeService.advanceTo(processingTime);

        FileWriter<String> fileWriter =
                createWriter(
                        path,
                        new VerifyingBucketAssigner(timestamp, watermark, processingTime),
                        DefaultRollingPolicy.builder().withRolloverInterval(10).build(),
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

    private static class ManuallyTriggeredProcessingTimeService
            implements Sink.ProcessingTimeService {

        private long now;

        private final Queue<Tuple2<Long, ProcessingTimeCallback>> timers =
                new PriorityQueue<>(Comparator.comparingLong(o -> o.f0));

        @Override
        public long getCurrentProcessingTime() {
            return now;
        }

        @Override
        public void registerProcessingTimer(
                long time, ProcessingTimeCallback processingTimeCallback) {
            if (time <= now) {
                try {
                    processingTimeCallback.onProcessingTime(now);
                } catch (IOException e) {
                    ExceptionUtils.rethrow(e);
                }
            } else {
                timers.add(new Tuple2<>(time, processingTimeCallback));
            }
        }

        public void advanceTo(long time) throws IOException {
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

    private static FileWriter<String> createWriter(
            Path basePath,
            RollingPolicy<String, String> rollingPolicy,
            OutputFileConfig outputFileConfig)
            throws IOException {
        return new FileWriter<>(
                basePath,
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

    private static FileWriter<String> createWriter(
            Path basePath,
            BucketAssigner<String, String> bucketAssigner,
            RollingPolicy<String, String> rollingPolicy,
            OutputFileConfig outputFileConfig,
            Sink.ProcessingTimeService processingTimeService,
            long bucketCheckInterval)
            throws IOException {
        return new FileWriter<>(
                basePath,
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

    private static FileWriter<String> restoreWriter(
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
