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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.FsStateChangelogStorage;
import org.apache.flink.changelog.fs.StateChangeSet;
import org.apache.flink.changelog.fs.StateChangeUploadScheduler;
import org.apache.flink.changelog.fs.StateChangeUploadScheduler.UploadTask;
import org.apache.flink.changelog.fs.StateChangeUploader;
import org.apache.flink.changelog.fs.TaskChangelogRegistry;
import org.apache.flink.changelog.fs.UploadResult;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.TestingStreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorageAccess;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.TriConsumerWithException;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingLong;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.flink.changelog.fs.StateChangeUploadScheduler.directScheduler;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.state.SnapshotResult.empty;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.concurrent.Executors.directExecutor;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that any unused state created by {@link ChangelogStateBackend} is discarded. This is
 * achieved by testing integration between {@link ChangelogKeyedStateBackend} and {@link
 * StateChangelogWriter} created by {@link FsStateChangelogStorage}.
 */
public class ChangelogStateDiscardTest {
    private static final Random RANDOM = new Random();

    @Test
    public void testPreEmptiveUploadDiscardedOnMaterialization() throws Exception {
        singleBackendTest(
                (backend, writer, uploader) -> {
                    changeAndLogRandomState(backend, uploader.results::size);
                    checkpoint(backend, 1L);
                    backend.notifyCheckpointSubsumed(1L);
                    assertRetained(uploader.results); // may still be used for future checkpoints
                    materialize(backend, writer);
                    assertDiscarded(uploader.results);
                });
    }

    @Test
    public void testPreEmptiveUploadDiscardedOnSubsumption() throws Exception {
        singleBackendTest(
                (backend, writer, uploader) -> {
                    changeAndLogRandomState(backend, uploader.results::size);
                    materialize(backend, writer);
                    checkpoint(backend, 1L);
                    assertRetained(uploader.results); // may be used in non-subsumed checkpoints
                    backend.notifyCheckpointSubsumed(1L);
                    assertDiscarded(uploader.results);
                });
    }

    @Test
    public void testPreEmptiveUploadNotDiscardedWithoutNotification() throws Exception {
        singleBackendTest(
                (backend, writer, uploader) -> {
                    changeAndLogRandomState(backend, uploader.results::size);
                    checkpoint(backend, 1L);
                    materialize(backend, writer);
                    assertRetained(uploader.results);
                });
    }

    @Test
    public void testPreEmptiveUploadDiscardedOnMaterializationIfCompletedLater() throws Exception {
        final TaskChangelogRegistry registry =
                TaskChangelogRegistry.defaultChangelogRegistry(directExecutor());
        final TestingUploadScheduler scheduler = new TestingUploadScheduler(registry);
        singleBackendTest(
                new FsStateChangelogStorage(
                        scheduler, 0L, registry, TestLocalRecoveryConfig.disabled()),
                (backend, writer) -> {
                    changeAndLogRandomState(backend, scheduler.uploads::size);
                    truncate(writer, backend);

                    checkState(scheduler.uploads.stream().noneMatch(UploadTask::isFinished));
                    List<UploadResult> results =
                            scheduler.completeUploads(ChangelogStateDiscardTest::uploadResult);

                    assertDiscarded(
                            results.stream()
                                    .map(h -> (TestingStreamStateHandle) h.getStreamStateHandle())
                                    .collect(toList()));
                });
    }

    @Test
    public void testPreEmptiveUploadDiscardedOnClose() throws Exception {
        final List<TestingStreamStateHandle> afterCheckpoint = new ArrayList<>();
        final List<TestingStreamStateHandle> beforeCheckpoint = new ArrayList<>();
        singleBackendTest(
                (backend, writer, uploader) -> {
                    changeAndLogRandomState(backend, uploader.results::size);
                    uploader.drainResultsTo(beforeCheckpoint);
                    checkpoint(backend, 1L);
                    changeAndLogRandomState(backend, uploader.results::size);
                    uploader.drainResultsTo(afterCheckpoint);
                });
        assertRetained(beforeCheckpoint);
        assertDiscarded(afterCheckpoint);
    }

    /**
     * Test that an upload is discarded only when it's not used by all backends for which it was
     * initiated.
     *
     * <p>Scenario:
     *
     * <ol>
     *   <li>Two backends start pre-emptive uploads, both go into the same file (StreamStateHandle)
     *   <li>First backend materializes (starts and finishes)
     *   <li>State should not be discarded
     *   <li>Second backend materializes (starts and finishes)
     *   <li>State should be discarded
     * </ol>
     */
    @Test
    public void testPreEmptiveUploadForMultipleBackends() throws Exception {
        // using the same range (rescaling not involved)
        final KeyGroupRange kgRange = KeyGroupRange.of(0, 10);
        final JobID jobId = new JobID();
        final ExecutionConfig cfg = new ExecutionConfig();

        final TaskChangelogRegistry registry =
                TaskChangelogRegistry.defaultChangelogRegistry(directExecutor());
        final TestingUploadScheduler scheduler = new TestingUploadScheduler(registry);
        final StateChangelogStorage<?> storage =
                new FsStateChangelogStorage(
                        scheduler, 0, registry, TestLocalRecoveryConfig.disabled());
        final StateChangelogWriter<?>
                w1 = storage.createWriter("test-operator-1", kgRange, new SyncMailboxExecutor()),
                w2 = storage.createWriter("test-operator-2", kgRange, new SyncMailboxExecutor());

        try (ChangelogKeyedStateBackend<String> b1 = backend(jobId, kgRange, cfg, w1);
                ChangelogKeyedStateBackend<String> b2 = backend(jobId, kgRange, cfg, w2)) {

            changeAndLogRandomState(b1, scheduler.uploads::size);
            changeAndLogRandomState(b2, scheduler.uploads::size);

            // emulate sharing the same file
            final TestingStreamStateHandle handle = new TestingStreamStateHandle();
            scheduler.completeUploads(task -> uploadResult(task, () -> handle));

            truncate(w1, b1);
            assertRetained(singletonList(handle));

            truncate(w1, b2);
            assertDiscarded(singletonList(handle));
        }
    }

    private void singleBackendTest(
            TriConsumerWithException<
                            ChangelogKeyedStateBackend<String>,
                            StateChangelogWriter<?>,
                            TestingUploader,
                            Exception>
                    testCase)
            throws Exception {
        TaskChangelogRegistry registry =
                TaskChangelogRegistry.defaultChangelogRegistry(directExecutor());
        TestingUploader uploader = new TestingUploader(registry);
        long preEmptivePersistThresholdInBytes = 0L; // flush ASAP
        singleBackendTest(
                new FsStateChangelogStorage(
                        directScheduler(uploader),
                        preEmptivePersistThresholdInBytes,
                        registry,
                        TestLocalRecoveryConfig.disabled()),
                (backend, writer) -> testCase.accept(backend, writer, uploader));
    }

    /** Provided storage will be closed. */
    private void singleBackendTest(
            StateChangelogStorage<?> storage,
            BiConsumerWithException<
                            ChangelogKeyedStateBackend<String>, StateChangelogWriter<?>, Exception>
                    testCase)
            throws Exception {
        final JobID jobId = new JobID();
        final KeyGroupRange kgRange = KeyGroupRange.of(0, 10);
        final ExecutionConfig cfg = new ExecutionConfig();
        StateChangelogWriter<?> writer =
                storage.createWriter("test-operator", kgRange, new SyncMailboxExecutor());
        try {
            try (ChangelogKeyedStateBackend<String> backend =
                    backend(jobId, kgRange, cfg, writer)) {
                testCase.accept(backend, writer);
            }
        } finally {
            storage.close();
        }
    }

    private static ChangelogKeyedStateBackend<String> backend(
            JobID jobId,
            KeyGroupRange kgRange,
            ExecutionConfig executionConfig,
            StateChangelogWriter<?> writer)
            throws IOException {
        AbstractKeyedStateBackend<String> nestedBackend =
                new HeapKeyedStateBackendBuilder<>(
                                new KvStateRegistry().createTaskRegistry(jobId, new JobVertexID()),
                                StringSerializer.INSTANCE,
                                StringSerializer.class.getClassLoader(),
                                kgRange.getNumberOfKeyGroups(),
                                kgRange,
                                executionConfig,
                                TtlTimeProvider.DEFAULT,
                                LatencyTrackingStateConfig.disabled(),
                                emptyList(),
                                UncompressedStreamCompressionDecorator.INSTANCE,
                                new LocalRecoveryConfig(null),
                                new HeapPriorityQueueSetFactory(
                                        kgRange, kgRange.getNumberOfKeyGroups(), 128),
                                true,
                                new CloseableRegistry())
                        .build();
        return new ChangelogKeyedStateBackend<>(
                nestedBackend,
                "test-subtask",
                executionConfig,
                TtlTimeProvider.DEFAULT,
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup(),
                writer,
                emptyList(),
                new MemoryBackendCheckpointStorageAccess(
                        jobId, null, null, 1 /* don't expect any materialization */));
    }

    private static String randomString() {
        byte[] bytes = new byte[10];
        RANDOM.nextBytes(bytes);
        return new String(bytes);
    }

    private static void changeAndLogRandomState(
            ChangelogKeyedStateBackend<String> backend, Supplier<Integer> changelogLength)
            throws Exception {
        for (int numExistingResults = changelogLength.get();
                changelogLength.get() == numExistingResults; ) {
            changeAndRandomState(backend);
        }
    }

    private static void changeAndRandomState(ChangelogKeyedStateBackend<String> backend)
            throws Exception {
        backend.setCurrentKey(randomString());
        backend.getPartitionedState(
                        "ns",
                        StringSerializer.INSTANCE,
                        new ValueStateDescriptor<>(randomString(), String.class))
                .update(randomString());
    }

    private static List<UploadResult> uploadResult(UploadTask upload) {
        return uploadResult(upload, TestingStreamStateHandle::new);
    }

    private static List<UploadResult> uploadResult(
            UploadTask upload, Supplier<StreamStateHandle> handleSupplier) {
        return upload.getChangeSets().stream()
                .map(
                        changes ->
                                new UploadResult(
                                        handleSupplier.get(),
                                        0L, // offset
                                        changes.getSequenceNumber(),
                                        changes.getSize()))
                .collect(toList());
    }

    private static void assertRetained(List<TestingStreamStateHandle> toRetain) {
        assertTrue(
                "Some state handles were discarded: \n" + toRetain,
                toRetain.stream().noneMatch(TestingStreamStateHandle::isDisposed));
    }

    private static void assertDiscarded(List<TestingStreamStateHandle> toDiscard) {
        assertTrue(
                "Not all state handles were discarded: \n" + toDiscard,
                toDiscard.stream().allMatch(TestingStreamStateHandle::isDisposed));
    }

    private static void checkpoint(ChangelogKeyedStateBackend<String> backend, long checkpointId)
            throws Exception {
        backend.snapshot(
                checkpointId,
                1L,
                new MemCheckpointStreamFactory(1000),
                CheckpointOptions.unaligned(
                        CHECKPOINT, CheckpointStorageLocationReference.getDefault()));
    }

    /**
     * An uploader that uses a {@link TestingStreamStateHandle} as a result. The usage of that
     * handle is tracked with the {@link #registry}.
     */
    private static class TestingUploader implements StateChangeUploader {
        private final List<TestingStreamStateHandle> results = new ArrayList<>();
        private final TaskChangelogRegistry registry;

        public TestingUploader(TaskChangelogRegistry registry) {
            this.registry = registry;
        }

        @Override
        public UploadTasksResult upload(Collection<UploadTask> tasks) throws IOException {
            TestingStreamStateHandle handle = new TestingStreamStateHandle();
            results.add(handle);
            // todo: avoid making StateChangeSet and its internals public?
            // todo: make the contract more explicit or extract common code
            Map<UploadTask, Map<StateChangeSet, Tuple2<Long, Long>>> taskOffsets =
                    tasks.stream().collect(toMap(identity(), this::mapOffsets));

            long refCount = tasks.stream().flatMap(t -> t.getChangeSets().stream()).count();
            registry.startTracking(handle, refCount);

            return new UploadTasksResult(taskOffsets, handle);
        }

        private Map<StateChangeSet, Tuple2<Long, Long>> mapOffsets(UploadTask task) {
            return task.getChangeSets().stream()
                    .collect(Collectors.toMap(identity(), ign -> Tuple2.of(0L, 0L)));
        }

        @Override
        public void close() throws Exception {}

        private void drainResultsTo(List<TestingStreamStateHandle> toDiscard) {
            toDiscard.addAll(results);
            results.clear();
        }
    }

    /**
     * An upload scheduler that collects the upload tasks and allows them to be {@link
     * #completeUploads(Function) completed arbitrarily}. State handles used for completion are
     * tracked by {@link #registry}.
     */
    private static class TestingUploadScheduler implements StateChangeUploadScheduler {
        private final List<UploadTask> uploads = new ArrayList<>();
        private final TaskChangelogRegistry registry;

        private TestingUploadScheduler(TaskChangelogRegistry registry) {
            this.registry = registry;
        }

        @Override
        public void upload(UploadTask uploadTask) throws IOException {
            uploads.add(uploadTask);
        }

        /**
         * Complete the accumulated tasks using the provided results and register resulting state
         * with the {@link #registry}.
         *
         * @return upload results
         */
        public List<UploadResult> completeUploads(
                Function<UploadTask, List<UploadResult>> resultsProvider) {

            List<UploadResult> allResults = new ArrayList<>();
            List<Tuple2<UploadTask, List<UploadResult>>> taskResults = new ArrayList<>();
            uploads.forEach(
                    task -> {
                        List<UploadResult> results = resultsProvider.apply(task);
                        taskResults.add(Tuple2.of(task, results));
                        allResults.addAll(results);
                    });

            Map<StreamStateHandle, Long> stateHandleAndRefCounts =
                    allResults.stream()
                            .collect(
                                    groupingBy(
                                            UploadResult::getStreamStateHandle,
                                            summingLong(x -> 1L)));
            stateHandleAndRefCounts.forEach(
                    (handle, refCount) -> registry.startTracking(handle, refCount));

            taskResults.forEach(
                    taskResult -> {
                        UploadTask task = taskResult.f0;
                        List<UploadResult> results = taskResult.f1;
                        task.complete(results);
                        checkState(task.isFinished());
                    });

            uploads.clear();
            return allResults;
        }

        @Override
        public void close() {}
    }

    private static void materialize(
            ChangelogKeyedStateBackend<String> backend, StateChangelogWriter<?> writer) {
        backend.handleMaterializationResult(empty(), 0L, writer.nextSequenceNumber());
    }

    private static void truncate(
            StateChangelogWriter<?> writer, ChangelogKeyedStateBackend<String> backend)
            throws Exception {
        materialize(backend, writer);
        checkpoint(backend, 1L);
        backend.notifyCheckpointSubsumed(1L);
    }
}
