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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorageAccess;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises recovery and checkpoint backpressure through the MiniCluster's production factories.
 */
class AdaptiveRecoveryIoIsolationITCase {

    private static final int MAX_PARALLELISM = 4;
    private static final long ADVANCE = -1;
    private static final long CANCEL = -2;
    private static final Duration WAIT = Duration.ofSeconds(20);
    private static final Map<JobID, Control> CONTROLS = new ConcurrentHashMap<>();

    @TempDir private java.nio.file.Path temporaryDirectory;

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    @Timeout(120)
    void restoresStateWhileAllGeneralIoWorkersAreDeleting(int ioWorkers) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(JobManagerOptions.JOB_MANAGER_IO_POOL_SIZE, ioWorkers);
        configuration.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 2);
        configuration.set(CheckpointingOptions.CLEANER_PARALLEL_MODE, true);
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ofMillis(1));
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ofMillis(1));
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING,
                Duration.ofMillis(1));

        final JobVertex vertex = new JobVertex("Checkpointed counters");
        vertex.setInvokableClass(CounterInvokable.class);
        vertex.setParallelism(2);
        vertex.setMaxParallelism(MAX_PARALLELISM);
        final JobGraph job = JobGraphTestUtils.streamingJobGraph(vertex);
        job.setSnapshotSettings(
                new JobCheckpointingSettings(
                        CheckpointCoordinatorConfiguration.builder()
                                .setCheckpointInterval(Long.MAX_VALUE)
                                .setCheckpointTimeout(60_000)
                                .build(),
                        null,
                        TernaryBoolean.FALSE,
                        new SerializedValue<>(new ObservedCheckpointStorage()),
                        null));
        RestartStrategyUtils.configureFixedDelayRestartStrategy(job, 2, 0);

        final Control control = new Control(temporaryDirectory, ioWorkers);
        CONTROLS.put(job.getJobID(), control);
        final MiniCluster cluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(2)
                                .build());
        try {
            cluster.start();
            cluster.submitJob(job).get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
            control.expectProgress(2, -1, 0);
            control.advance(2, -1, 1);
            final long first = checkpoint(cluster, job);
            assertThat(first).isOne();
            control.advance(2, -1, 2);
            final long second = checkpoint(cluster, job);
            control.assertRetained(first, second);
            control.advance(2, -1, 3);
            final long third = checkpoint(cluster, job);

            assertThat(control.deletionsEntered.await(WAIT.toMillis(), TimeUnit.MILLISECONDS))
                    .as("every configured general IO worker is blocked in real checkpoint disposal")
                    .isTrue();
            assertThat(control.blockedWorkers).hasSize(ioWorkers);
            assertThat(control.blockedWorkers.keySet())
                    .allSatisfy(name -> assertThat(name).startsWith("jobmanager-io"));
            assertThat(control.blockedWorkers.values()).contains("shared");
            if (ioWorkers == 2) {
                assertThat(control.blockedWorkers.values()).contains("private");
            }
            control.assertRetained(second, third);

            rescale(cluster, job, vertex, 1);
            control.expectProgress(1, third, 3);
            control.advance(1, third, 4);
            assertThat(control.graphCreationThreads)
                    .hasSize(2)
                    .allSatisfy(name -> assertThat(name).startsWith("jobmanager-recovery"));

            awaitRunning(cluster, job);
            final CompletableFuture<Long> blockedCheckpoint =
                    cluster.triggerCheckpoint(job.getJobID(), CheckpointType.CONFIGURED);
            assertThatThrownBy(() -> blockedCheckpoint.get(300, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(control.initializations).containsOnlyKeys(first, second, third);
            control.advance(1, third, 5);
            assertThat(blockedCheckpoint).isNotDone();
            assertThat(control.deletionGate.getCount()).isOne();
            control.assertRetained(second, third);

            control.deletionGate.countDown();
            final long fourth = blockedCheckpoint.get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(fourth).isGreaterThan(third);
            control.awaitDiscarded(first, second);
            control.assertRetained(third, fourth);
            assertThat(control.initializations.get(fourth)).startsWith("jobmanager-io");

            rescale(cluster, job, vertex, 2);
            control.expectProgress(2, fourth, 5);
            control.advance(2, fourth, 6);
            final long fifth = checkpoint(cluster, job);
            assertThat(fifth).isGreaterThan(fourth);
            control.awaitDiscarded(third);
            control.assertRetained(fourth, fifth);

            cluster.cancelJob(job.getJobID()).get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
            cluster.requestJobResult(job.getJobID()).get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(
                            cluster.getJobStatus(job.getJobID())
                                    .get(WAIT.toMillis(), TimeUnit.MILLISECONDS))
                    .isEqualTo(JobStatus.CANCELED);
            control.awaitDiscarded(first, second, third, fourth, fifth);
            assertThat(control.mailboxes).isEmpty();
        } finally {
            control.deletionGate.countDown();
            try {
                cluster.closeAsync().get(30, TimeUnit.SECONDS);
            } finally {
                CONTROLS.remove(job.getJobID());
            }
        }
    }

    private static long checkpoint(MiniCluster cluster, JobGraph job) throws Exception {
        awaitRunning(cluster, job);
        return cluster.triggerCheckpoint(job.getJobID(), CheckpointType.CONFIGURED)
                .get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static void awaitRunning(MiniCluster cluster, JobGraph job) throws Exception {
        waitUntilCondition(
                () -> {
                    boolean found = false;
                    for (ArchivedExecutionVertex vertex :
                            cluster.getArchivedExecutionGraph(job.getJobID())
                                    .get(WAIT.toMillis(), TimeUnit.MILLISECONDS)
                                    .getAllExecutionVertices()) {
                        found = true;
                        if (vertex.getExecutionState() != ExecutionState.RUNNING) {
                            return false;
                        }
                    }
                    return found;
                },
                10L,
                (int) WAIT.toMillis() / 10);
    }

    private static void rescale(
            MiniCluster cluster, JobGraph job, JobVertex vertex, int parallelism) throws Exception {
        cluster.updateJobResourceRequirements(
                        job.getJobID(),
                        JobResourceRequirements.newBuilder()
                                .setParallelismForJobVertex(
                                        vertex.getID(), parallelism, parallelism)
                                .build())
                .get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static final class Control {
        private final java.nio.file.Path directory;
        private final CountDownLatch deletionGate = new CountDownLatch(1);
        private final CountDownLatch deletionsEntered;
        private final Map<String, String> blockedWorkers = new ConcurrentHashMap<>();
        private final Map<Long, String> initializations = new ConcurrentHashMap<>();
        private final BlockingQueue<String> graphCreationThreads = new LinkedBlockingQueue<>();
        private final Map<Integer, BlockingQueue<Long>> mailboxes = new ConcurrentHashMap<>();
        private final BlockingQueue<Progress> progress = new LinkedBlockingQueue<>();
        private final Map<String, TrackedFile> files = new ConcurrentHashMap<>();

        private Control(java.nio.file.Path directory, int ioWorkers) {
            this.directory = directory;
            deletionsEntered = new CountDownLatch(ioWorkers);
        }

        private void advance(int parallelism, long restoredCheckpoint, long expected)
                throws Exception {
            assertThat(mailboxes).hasSize(parallelism);
            mailboxes.values().forEach(mailbox -> mailbox.add(ADVANCE));
            expectProgress(parallelism, restoredCheckpoint, expected);
        }

        private void expectProgress(int parallelism, long restoredCheckpoint, long expected)
                throws Exception {
            final Map<Integer, Long> counters = new HashMap<>();
            final Set<Integer> subtasks = new HashSet<>();
            for (int i = 0; i < parallelism; i++) {
                final Progress next = progress.poll(WAIT.toMillis(), TimeUnit.MILLISECONDS);
                assertThat(next)
                        .as(
                                "physical task progress after restoring checkpoint %s",
                                restoredCheckpoint)
                        .isNotNull();
                assertThat(next.parallelism).isEqualTo(parallelism);
                assertThat(next.restoredCheckpoint).isEqualTo(restoredCheckpoint);
                assertThat(subtasks.add(next.subtask)).isTrue();
                next.counters.forEach(
                        (key, value) -> {
                            assertThat(counters.put(key, value)).isNull();
                            assertThat(value).isEqualTo(key * 1000L + expected);
                        });
            }
            assertThat(counters).containsOnlyKeys(0, 1, 2, 3);
        }

        private void assertRetained(long... checkpointIds) {
            for (long checkpointId : checkpointIds) {
                assertThat(files.values()).anyMatch(file -> file.checkpointId == checkpointId);
                files.values().stream()
                        .filter(file -> file.checkpointId == checkpointId)
                        .forEach(
                                file -> {
                                    assertThat(file.path).exists();
                                    assertThat(file.discardAttempts.get()).isZero();
                                });
            }
        }

        private void awaitDiscarded(long... checkpointIds) throws Exception {
            for (long checkpointId : checkpointIds) {
                assertThat(files.values()).anyMatch(file -> file.checkpointId == checkpointId);
                waitUntilCondition(
                        () ->
                                files.values().stream()
                                        .filter(file -> file.checkpointId == checkpointId)
                                        .allMatch(file -> file.discardCompletions.get() > 0),
                        10L,
                        (int) WAIT.toMillis() / 10);
                files.values().stream()
                        .filter(file -> file.checkpointId == checkpointId)
                        .forEach(
                                file -> {
                                    assertThat(file.path).doesNotExist();
                                    assertThat(file.discardAttempts.get()).isOne();
                                    assertThat(file.discardCompletions.get()).isOne();
                                });
            }
        }
    }

    private static final class TrackedFile {
        private final java.nio.file.Path path;
        private final long checkpointId;
        private final int subtask;
        private final String kind;
        private final AtomicInteger discardAttempts = new AtomicInteger();
        private final AtomicInteger discardCompletions = new AtomicInteger();

        private TrackedFile(java.nio.file.Path path, long checkpointId, int subtask, String kind) {
            this.path = path;
            this.checkpointId = checkpointId;
            this.subtask = subtask;
            this.kind = kind;
        }
    }

    private static final class Progress {
        private final int subtask;
        private final int parallelism;
        private final long restoredCheckpoint;
        private final Map<Integer, Long> counters;

        private Progress(
                int subtask,
                int parallelism,
                long restoredCheckpoint,
                Map<Integer, Long> counters) {
            this.subtask = subtask;
            this.parallelism = parallelism;
            this.restoredCheckpoint = restoredCheckpoint;
            this.counters = new HashMap<>(counters);
        }
    }

    /**
     * A task with file-backed keyed counters and a mailbox shared by input and checkpoint triggers.
     */
    public static final class CounterInvokable extends AbstractInvokable {
        private final BlockingQueue<Long> mailbox = new LinkedBlockingQueue<>();
        private final Map<Integer, Long> counters = new HashMap<>();

        public CounterInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            final Environment environment = getEnvironment();
            final Control control = CONTROLS.get(environment.getJobID());
            final int subtask = getIndexInSubtaskGroup();
            final int parallelism = environment.getTaskInfo().getNumberOfParallelSubtasks();
            final KeyGroupRange range =
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            MAX_PARALLELISM, parallelism, subtask);
            final OperatorID operator = OperatorID.fromJobVertexID(environment.getJobVertexId());
            final long restored =
                    environment.getTaskStateManager().getRestoreCheckpointId().orElse(-1L);
            if (restored < 0) {
                for (int key : range) {
                    counters.put(key, key * 1000L);
                }
            } else {
                final OperatorSubtaskState state =
                        environment
                                .getTaskStateManager()
                                .getSubtaskJobManagerRestoredState(operator)
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        "Missing restored counter state"));
                for (KeyedStateHandle keyed : state.getManagedKeyedState()) {
                    final IncrementalRemoteKeyedStateHandle incremental =
                            (IncrementalRemoteKeyedStateHandle) keyed;
                    assertThat(incremental.getCheckpointId()).isEqualTo(restored);
                    for (HandleAndLocalPath handle : incremental.getSharedState()) {
                        restore(handle.getHandle(), range, restored);
                    }
                    for (HandleAndLocalPath handle : incremental.getPrivateState()) {
                        try (DataInputStream input =
                                new DataInputStream(handle.getHandle().openInputStream())) {
                            assertThat(input.readLong()).isEqualTo(restored);
                        }
                    }
                }
                assertThat(counters).hasSize(range.getNumberOfKeyGroups());
            }

            control.mailboxes.put(subtask, mailbox);
            try {
                control.progress.add(new Progress(subtask, parallelism, restored, counters));
                long signal;
                while ((signal = mailbox.take()) != CANCEL) {
                    if (signal == ADVANCE) {
                        counters.replaceAll((key, value) -> value + 1);
                        control.progress.add(
                                new Progress(subtask, parallelism, restored, counters));
                    } else {
                        final FileStateHandle shared =
                                writeState(control, signal, subtask, "shared");
                        final FileStateHandle privateState =
                                writeState(control, signal, subtask, "private");
                        final IncrementalRemoteKeyedStateHandle snapshot =
                                new IncrementalRemoteKeyedStateHandle(
                                        UUID.randomUUID(),
                                        range,
                                        signal,
                                        Collections.singletonList(
                                                HandleAndLocalPath.of(shared, "counters")),
                                        Collections.singletonList(
                                                HandleAndLocalPath.of(privateState, "manifest")),
                                        new ByteStreamStateHandle(
                                                "meta-" + signal + "-" + subtask, new byte[0]));
                        final TaskStateSnapshot taskState = new TaskStateSnapshot();
                        taskState.putSubtaskStateByOperatorID(
                                operator,
                                OperatorSubtaskState.builder()
                                        .setManagedKeyedState(snapshot)
                                        .build());
                        environment.acknowledgeCheckpoint(
                                signal, new CheckpointMetrics(), taskState);
                    }
                }
            } finally {
                control.mailboxes.remove(subtask, mailbox);
            }
        }

        private void restore(StreamStateHandle handle, KeyGroupRange range, long checkpointId)
                throws IOException {
            try (DataInputStream input = new DataInputStream(handle.openInputStream())) {
                assertThat(input.readLong()).isEqualTo(checkpointId);
                final int size = input.readInt();
                for (int i = 0; i < size; i++) {
                    final int key = input.readInt();
                    final long value = input.readLong();
                    if (range.contains(key)) {
                        assertThat(counters.put(key, value)).isNull();
                    }
                }
            }
        }

        private FileStateHandle writeState(
                Control control, long checkpointId, int subtask, String kind) throws IOException {
            final java.nio.file.Path path =
                    Files.createTempFile(
                            control.directory, checkpointId + "-" + subtask + "-" + kind, ".state");
            try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(path))) {
                output.writeLong(checkpointId);
                output.writeInt(counters.size());
                for (Map.Entry<Integer, Long> entry : counters.entrySet()) {
                    output.writeInt(entry.getKey());
                    output.writeLong(entry.getValue());
                }
            }
            final String id = path.toUri().toString();
            control.files.put(id, new TrackedFile(path, checkpointId, subtask, kind));
            return new GatedFileStateHandle(
                    getEnvironment().getJobID(), new Path(path.toUri()), Files.size(path));
        }

        @Override
        public void cancel() {
            mailbox.add(CANCEL);
        }

        @Override
        public CompletableFuture<Boolean> triggerCheckpointAsync(
                CheckpointMetaData metadata, CheckpointOptions options) {
            mailbox.add(metadata.getCheckpointId());
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<Void> notifyCheckpointSubsumedAsync(long checkpointId) {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class GatedFileStateHandle extends FileStateHandle {
        private static final long serialVersionUID = 1L;
        private final JobID jobId;

        private GatedFileStateHandle(JobID jobId, Path path, long size) {
            super(path, size);
            this.jobId = jobId;
        }

        @Override
        public void discardState() throws Exception {
            final Control control = CONTROLS.get(jobId);
            final TrackedFile file = control.files.get(getFilePath().toUri().toString());
            file.discardAttempts.incrementAndGet();
            if (file.checkpointId == 1 && file.subtask == 0) {
                control.blockedWorkers.put(Thread.currentThread().getName(), file.kind);
                control.deletionsEntered.countDown();
                if (!control.deletionGate.await(60, TimeUnit.SECONDS)) {
                    throw new IOException("Checkpoint deletion gate timed out");
                }
            }
            super.discardState();
            file.discardCompletions.incrementAndGet();
        }
    }

    private static final class ObservedCheckpointStorage implements CheckpointStorage {
        private static final long serialVersionUID = 1L;

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer)
                throws IOException {
            return new JobManagerCheckpointStorage().resolveCheckpoint(pointer);
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
            final Control control = CONTROLS.get(jobId);
            control.graphCreationThreads.add(Thread.currentThread().getName());
            return new MemoryBackendCheckpointStorageAccess(
                    jobId, null, null, true, JobManagerCheckpointStorage.DEFAULT_MAX_STATE_SIZE) {
                @Override
                public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId)
                        throws IOException {
                    control.initializations.put(checkpointId, Thread.currentThread().getName());
                    return super.initializeLocationForCheckpoint(checkpointId);
                }
            };
        }
    }
}
