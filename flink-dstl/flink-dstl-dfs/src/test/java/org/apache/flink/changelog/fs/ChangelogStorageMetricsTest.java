package org.apache.flink.changelog.fs;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.StateChangeUploadScheduler.UploadTask;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.testutils.EmptyStreamStateHandle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.flink.changelog.fs.ChangelogStorageMetricGroup.CHANGELOG_STORAGE_UPLOAD_QUEUE_SIZE;
import static org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups.createUnregisteredTaskManagerJobMetricGroup;
import static org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup;
import static org.apache.flink.runtime.state.KeyGroupRange.EMPTY_KEY_GROUP_RANGE;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link ChangelogStorageMetricGroup} test. */
public class ChangelogStorageMetricsTest {

    @TempDir java.nio.file.Path tempFolder;

    @Test
    void testUploadsCounter() throws Exception {
        ChangelogStorageMetricGroup metrics =
                new ChangelogStorageMetricGroup(createUnregisteredTaskManagerJobMetricGroup());

        try (FsStateChangelogStorage storage =
                new FsStateChangelogStorage(
                        JobID.generate(),
                        Path.fromLocalFile(tempFolder.toFile()),
                        false,
                        100,
                        metrics,
                        TaskChangelogRegistry.NO_OP,
                        TestLocalRecoveryConfig.disabled())) {
            FsStateChangelogWriter writer = createWriter(storage);
            int numUploads = 5;
            for (int i = 0; i < numUploads; i++) {
                SequenceNumber from = writer.nextSequenceNumber();
                writer.append(0, new byte[] {0, 1, 2, 3});
                writer.persist(from, 1L).get();
            }
            assertThat(metrics.getUploadsCounter().getCount()).isEqualTo(numUploads);
            assertThat(metrics.getUploadLatenciesNanos().getStatistics().getMin()).isGreaterThan(0);
        }
    }

    @Test
    void testUploadSizes() throws Exception {
        ChangelogStorageMetricGroup metrics =
                new ChangelogStorageMetricGroup(createUnregisteredTaskManagerJobMetricGroup());

        try (FsStateChangelogStorage storage =
                new FsStateChangelogStorage(
                        JobID.generate(),
                        Path.fromLocalFile(tempFolder.toFile()),
                        false,
                        100,
                        metrics,
                        TaskChangelogRegistry.NO_OP,
                        TestLocalRecoveryConfig.disabled())) {
            FsStateChangelogWriter writer = createWriter(storage);

            // upload single byte to infer header size
            SequenceNumber from = writer.nextSequenceNumber();
            writer.append(0, new byte[] {0});
            writer.persist(from, 1L).get();
            long headerSize = metrics.getUploadSizes().getStatistics().getMin() - 1;

            byte[] upload = new byte[33];
            for (int i = 0; i < 5; i++) {
                from = writer.nextSequenceNumber();
                writer.append(0, upload);
                writer.persist(from, 1L).get();
            }
            long expected = upload.length + headerSize;
            assertThat(metrics.getUploadSizes().getStatistics().getMax()).isEqualTo(expected);
        }
    }

    @Test
    void testUploadFailuresCounter() throws Exception {
        // using file instead of folder will cause a failure
        File file = Files.createTempFile(tempFolder, UUID.randomUUID().toString(), "").toFile();
        ChangelogStorageMetricGroup metrics =
                new ChangelogStorageMetricGroup(createUnregisteredTaskManagerJobMetricGroup());
        try (FsStateChangelogStorage storage =
                new FsStateChangelogStorage(
                        JobID.generate(),
                        Path.fromLocalFile(file),
                        false,
                        100,
                        metrics,
                        TaskChangelogRegistry.NO_OP,
                        TestLocalRecoveryConfig.disabled())) {
            FsStateChangelogWriter writer = createWriter(storage);

            int numUploads = 5;
            for (int i = 0; i < numUploads; i++) {
                SequenceNumber from = writer.nextSequenceNumber();
                writer.append(0, new byte[] {0, 1, 2, 3});
                try {
                    writer.persist(from, 1L).get();
                } catch (IOException e) {
                    // ignore
                }
            }
            assertThat(metrics.getUploadFailuresCounter().getCount()).isEqualTo(numUploads);
        }
    }

    @Test
    void testUploadBatchSizes() throws Exception {
        int numWriters = 5, numUploads = 5;

        ChangelogStorageMetricGroup metrics =
                new ChangelogStorageMetricGroup(createUnregisteredTaskManagerJobMetricGroup());
        Path basePath = Path.fromLocalFile(tempFolder.toFile());
        StateChangeFsUploader uploader =
                new StateChangeFsUploader(
                        JobID.generate(),
                        basePath,
                        basePath.getFileSystem(),
                        false,
                        100,
                        metrics,
                        TaskChangelogRegistry.NO_OP);
        ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();
        BatchingStateChangeUploadScheduler batcher =
                new BatchingStateChangeUploadScheduler(
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        RetryPolicy.NONE,
                        uploader,
                        scheduler,
                        new RetryingExecutor(
                                1,
                                metrics.getAttemptsPerUpload(),
                                metrics.getTotalAttemptsPerUpload()),
                        metrics);

        FsStateChangelogStorage storage =
                new FsStateChangelogStorage(
                        batcher,
                        Integer.MAX_VALUE,
                        TaskChangelogRegistry.NO_OP,
                        TestLocalRecoveryConfig.disabled());
        FsStateChangelogWriter[] writers = new FsStateChangelogWriter[numWriters];
        for (int i = 0; i < numWriters; i++) {
            writers[i] =
                    storage.createWriter(
                            Integer.toString(i), EMPTY_KEY_GROUP_RANGE, new SyncMailboxExecutor());
        }

        try {
            for (int upload = 0; upload < numUploads; upload++) {
                for (int writer = 0; writer < numWriters; writer++) {
                    // with all thresholds on MAX and manually triggered executor, this shouldn't
                    // cause actual uploads
                    SequenceNumber from = writers[writer].nextSequenceNumber();
                    writers[writer].append(0, new byte[] {0, 1, 2, 3});
                    writers[writer].persist(from, 1L);
                }
                // now the uploads should be grouped and executed at once
                scheduler.triggerScheduledTasks();
            }
            assertThat(metrics.getUploadBatchSizes().getStatistics().getMin())
                    .isEqualTo(numWriters);
            assertThat(metrics.getUploadBatchSizes().getStatistics().getMax())
                    .isEqualTo(numWriters);
        } finally {
            storage.close();
        }
    }

    @Test
    void testAttemptsPerUpload() throws Exception {
        int numUploads = 7, maxAttempts = 3;

        ChangelogStorageMetricGroup metrics =
                new ChangelogStorageMetricGroup(createUnregisteredTaskManagerJobMetricGroup());

        BatchingStateChangeUploadScheduler batcher =
                new BatchingStateChangeUploadScheduler(
                        Long.MAX_VALUE,
                        1,
                        Long.MAX_VALUE,
                        RetryPolicy.fixed(maxAttempts, Long.MAX_VALUE, 0),
                        new MaxAttemptUploader(maxAttempts),
                        newSingleThreadScheduledExecutor(),
                        new RetryingExecutor(
                                1,
                                metrics.getAttemptsPerUpload(),
                                metrics.getTotalAttemptsPerUpload()),
                        metrics);

        FsStateChangelogStorage storage =
                new FsStateChangelogStorage(
                        batcher,
                        Integer.MAX_VALUE,
                        TaskChangelogRegistry.NO_OP,
                        TestLocalRecoveryConfig.disabled());
        FsStateChangelogWriter writer = createWriter(storage);

        try {
            for (int upload = 0; upload < numUploads; upload++) {
                SequenceNumber from = writer.nextSequenceNumber();
                writer.append(0, new byte[] {0, 1, 2, 3});
                writer.persist(from, 1L).get();
            }
        } finally {
            storage.close();
        }
        HistogramStatistics histogram = metrics.getAttemptsPerUpload().getStatistics();
        assertThat(histogram.getMin()).isEqualTo(maxAttempts);
        assertThat(histogram.getMax()).isEqualTo(maxAttempts);
    }

    @Test
    void testTotalAttemptsPerUpload() throws Exception {
        int numUploads = 10, maxAttempts = 3;
        long timeout = 50;
        int numUploadThreads = 4; // must bigger or equal than maxAttempts

        ChangelogStorageMetricGroup metrics =
                new ChangelogStorageMetricGroup(createUnregisteredTaskManagerJobMetricGroup());

        BatchingStateChangeUploadScheduler batcher =
                new BatchingStateChangeUploadScheduler(
                        Long.MAX_VALUE,
                        1,
                        Long.MAX_VALUE,
                        RetryPolicy.fixed(maxAttempts, timeout, 0),
                        new WaitingMaxAttemptUploader(maxAttempts),
                        newSingleThreadScheduledExecutor(),
                        new RetryingExecutor(
                                numUploadThreads,
                                metrics.getAttemptsPerUpload(),
                                metrics.getTotalAttemptsPerUpload()),
                        metrics);

        FsStateChangelogStorage storage =
                new FsStateChangelogStorage(
                        batcher,
                        Integer.MAX_VALUE,
                        TaskChangelogRegistry.NO_OP,
                        TestLocalRecoveryConfig.disabled());
        FsStateChangelogWriter writer = createWriter(storage);

        try {
            for (int upload = 0; upload < numUploads; upload++) {
                SequenceNumber from = writer.nextSequenceNumber();
                writer.append(0, new byte[] {0, 1, 2, 3});
                writer.persist(from, 1L).get();
            }
        } finally {
            storage.close();
        }
        HistogramStatistics histogram = metrics.getTotalAttemptsPerUpload().getStatistics();
        assertThat(histogram.getMin()).isEqualTo(maxAttempts);
        assertThat(histogram.getMax()).isEqualTo(maxAttempts);
    }

    @Test
    void testQueueSize() throws Exception {
        JobID jobID = JobID.generate();
        AtomicReference<Gauge<Integer>> queueSizeGauge = new AtomicReference<>();
        ChangelogStorageMetricGroup metrics =
                new ChangelogStorageMetricGroup(
                        new TaskManagerJobMetricGroup(
                                TestingMetricRegistry.builder()
                                        .setRegisterConsumer(
                                                (metric, name, unused) -> {
                                                    if (name.equals(
                                                            CHANGELOG_STORAGE_UPLOAD_QUEUE_SIZE)) {
                                                        queueSizeGauge.set((Gauge<Integer>) metric);
                                                    }
                                                })
                                        .build(),
                                createUnregisteredTaskManagerMetricGroup(),
                                jobID,
                                "test"));

        Path path = Path.fromLocalFile(tempFolder.toFile());
        StateChangeFsUploader delegate =
                new StateChangeFsUploader(
                        jobID,
                        path,
                        path.getFileSystem(),
                        false,
                        100,
                        metrics,
                        TaskChangelogRegistry.NO_OP);
        ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();
        BatchingStateChangeUploadScheduler batcher =
                new BatchingStateChangeUploadScheduler(
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        RetryPolicy.NONE,
                        delegate,
                        scheduler,
                        new RetryingExecutor(
                                1,
                                metrics.getAttemptsPerUpload(),
                                metrics.getTotalAttemptsPerUpload()),
                        metrics);
        try (FsStateChangelogStorage storage =
                new FsStateChangelogStorage(
                        batcher,
                        Long.MAX_VALUE,
                        TaskChangelogRegistry.NO_OP,
                        TestLocalRecoveryConfig.disabled())) {
            FsStateChangelogWriter writer = createWriter(storage);
            int numUploads = 11;
            for (int i = 0; i < numUploads; i++) {
                SequenceNumber from = writer.nextSequenceNumber();
                writer.append(0, new byte[] {0});
                writer.persist(from, 1L);
            }
            assertThat((int) queueSizeGauge.get().getValue()).isEqualTo(numUploads);
            scheduler.triggerScheduledTasks();
            assertThat((int) queueSizeGauge.get().getValue()).isEqualTo(0);
        }
    }

    private static class MaxAttemptUploader implements StateChangeUploader {
        private final Map<UploadTask, Integer> attemptsPerTask;
        private final int maxAttempts;

        public MaxAttemptUploader(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            this.attemptsPerTask = new HashMap<>();
        }

        @Override
        public UploadTasksResult upload(Collection<UploadTask> tasks) throws IOException {
            Map<UploadTask, Map<StateChangeSet, Tuple2<Long, Long>>> map = new HashMap<>();
            for (UploadTask uploadTask : tasks) {
                int currentAttempt = 1 + attemptsPerTask.getOrDefault(uploadTask, 0);
                if (currentAttempt == maxAttempts) {
                    attemptsPerTask.remove(uploadTask);
                    map.put(
                            uploadTask,
                            uploadTask.changeSets.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    Function.identity(),
                                                    ign -> Tuple2.of(0L, 0L))));
                } else {
                    attemptsPerTask.put(uploadTask, currentAttempt);
                    throw new IOException();
                }
            }
            return new UploadTasksResult(map, new EmptyStreamStateHandle());
        }

        @Override
        public void close() {
            attemptsPerTask.clear();
        }
    }

    private static class WaitingMaxAttemptUploader implements StateChangeUploader {
        private final ConcurrentHashMap<UploadTask, CountDownLatch> remainingAttemptsPerTask;
        private final Map<UploadTask, Integer> attemptsPerTask;
        private final int maxAttempts;

        public WaitingMaxAttemptUploader(int maxAttempts) {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts < 0");
            }
            this.maxAttempts = maxAttempts;
            this.remainingAttemptsPerTask = new ConcurrentHashMap<>();
            this.attemptsPerTask = new ConcurrentHashMap<>();
        }

        @Override
        public UploadTasksResult upload(Collection<UploadTask> tasks) throws IOException {
            int currentAttempt = 0;
            for (UploadTask uploadTask : tasks) {
                remainingAttemptsPerTask
                        .computeIfAbsent(uploadTask, ign -> new CountDownLatch(maxAttempts))
                        .countDown();
                currentAttempt = 1 + attemptsPerTask.getOrDefault(uploadTask, 0);
                attemptsPerTask.put(uploadTask, currentAttempt);
            }
            if (currentAttempt > 1 && currentAttempt < maxAttempts) {
                throw new IOException();
            }

            for (UploadTask uploadTask : tasks) {
                try {
                    remainingAttemptsPerTask.get(uploadTask).await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }

            Map<UploadTask, Map<StateChangeSet, Tuple2<Long, Long>>> map = new HashMap<>();
            for (UploadTask uploadTask : tasks) {
                map.put(
                        uploadTask,
                        uploadTask.changeSets.stream()
                                .collect(
                                        Collectors.toMap(
                                                Function.identity(), ign -> Tuple2.of(0L, 0L))));
            }
            return new UploadTasksResult(map, new EmptyStreamStateHandle());
        }

        @Override
        public void close() throws Exception {
            // nothing to close
        }
    }

    private FsStateChangelogWriter createWriter(FsStateChangelogStorage storage) {
        return storage.createWriter("writer", EMPTY_KEY_GROUP_RANGE, new SyncMailboxExecutor());
    }
}
