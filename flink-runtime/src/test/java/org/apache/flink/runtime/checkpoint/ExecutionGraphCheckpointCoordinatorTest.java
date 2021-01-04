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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the interaction between the {@link ExecutionGraph} and the {@link
 * CheckpointCoordinator}.
 */
public class ExecutionGraphCheckpointCoordinatorTest extends TestLogger {

    /** Tests that the checkpoint coordinator is shut down if the execution graph is failed. */
    @Test
    public void testShutdownCheckpointCoordinatorOnFailure() throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter = new TestingCheckpointIDCounter(counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

        final SchedulerBase scheduler = createSchedulerAndEnableCheckpointing(counter, store);
        final ExecutionGraph graph = scheduler.getExecutionGraph();
        final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

        assertThat(checkpointCoordinator, Matchers.notNullValue());
        assertThat(checkpointCoordinator.isShutdown(), is(false));

        graph.failGlobal(new Exception("Test Exception"));

        assertThat(checkpointCoordinator.isShutdown(), is(true));
        assertThat(counterShutdownFuture.get(), is(JobStatus.FAILED));
        assertThat(storeShutdownFuture.get(), is(JobStatus.FAILED));
    }

    /** Tests that the checkpoint coordinator is shut down if the execution graph is suspended. */
    @Test
    public void testShutdownCheckpointCoordinatorOnSuspend() throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter = new TestingCheckpointIDCounter(counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

        final SchedulerBase scheduler = createSchedulerAndEnableCheckpointing(counter, store);
        final ExecutionGraph graph = scheduler.getExecutionGraph();
        final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

        assertThat(checkpointCoordinator, Matchers.notNullValue());
        assertThat(checkpointCoordinator.isShutdown(), is(false));

        graph.suspend(new Exception("Test Exception"));

        assertThat(checkpointCoordinator.isShutdown(), is(true));
        assertThat(counterShutdownFuture.get(), is(JobStatus.SUSPENDED));
        assertThat(storeShutdownFuture.get(), is(JobStatus.SUSPENDED));
    }

    /** Tests that the checkpoint coordinator is shut down if the execution graph is finished. */
    @Test
    public void testShutdownCheckpointCoordinatorOnFinished() throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter = new TestingCheckpointIDCounter(counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

        final SchedulerBase scheduler = createSchedulerAndEnableCheckpointing(counter, store);
        final ExecutionGraph graph = scheduler.getExecutionGraph();
        final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

        assertThat(checkpointCoordinator, Matchers.notNullValue());
        assertThat(checkpointCoordinator.isShutdown(), is(false));

        scheduler.startScheduling();

        for (ExecutionVertex executionVertex : graph.getAllExecutionVertices()) {
            final Execution currentExecutionAttempt = executionVertex.getCurrentExecutionAttempt();
            scheduler.updateTaskExecutionState(
                    new TaskExecutionState(
                            graph.getJobID(),
                            currentExecutionAttempt.getAttemptId(),
                            ExecutionState.FINISHED));
        }

        assertThat(graph.getTerminationFuture().get(), is(JobStatus.FINISHED));

        assertThat(checkpointCoordinator.isShutdown(), is(true));
        assertThat(counterShutdownFuture.get(), is(JobStatus.FINISHED));
        assertThat(storeShutdownFuture.get(), is(JobStatus.FINISHED));
    }

    private SchedulerBase createSchedulerAndEnableCheckpointing(
            CheckpointIDCounter counter, CompletedCheckpointStore store) throws Exception {
        final Time timeout = Time.days(1L);

        final JobVertex jobVertex = new JobVertex("MockVertex");
        jobVertex.setInvokableClass(AbstractInvokable.class);

        final JobGraph jobGraph = new JobGraph(jobVertex);
        final CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration(
                        100,
                        100,
                        100,
                        1,
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                        true,
                        false,
                        false,
                        0);
        final JobCheckpointingSettings checkpointingSettings =
                new JobCheckpointingSettings(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        chkConfig,
                        null);
        jobGraph.setSnapshotSettings(checkpointingSettings);

        final SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(jobGraph)
                        .setCheckpointRecoveryFactory(
                                new TestingCheckpointRecoveryFactory(store, counter))
                        .setRpcTimeout(timeout)
                        .build();

        scheduler.initialize(ComponentMainThreadExecutorServiceAdapter.forMainThread());

        return scheduler;
    }

    private static class TestingCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

        private final CompletedCheckpointStore store;
        private final CheckpointIDCounter counter;

        private TestingCheckpointRecoveryFactory(
                CompletedCheckpointStore store, CheckpointIDCounter counter) {
            this.store = store;
            this.counter = counter;
        }

        @Override
        public CompletedCheckpointStore createCheckpointStore(
                JobID jobId, int maxNumberOfCheckpointsToRetain, ClassLoader userClassLoader) {
            return store;
        }

        @Override
        public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) {
            return counter;
        }
    }

    private static final class TestingCheckpointIDCounter implements CheckpointIDCounter {

        private final CompletableFuture<JobStatus> shutdownStatus;

        private TestingCheckpointIDCounter(CompletableFuture<JobStatus> shutdownStatus) {
            this.shutdownStatus = shutdownStatus;
        }

        @Override
        public void start() {}

        @Override
        public void shutdown(JobStatus jobStatus) {
            shutdownStatus.complete(jobStatus);
        }

        @Override
        public long getAndIncrement() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public long get() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public void setCount(long newId) {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }

    private static final class TestingCompletedCheckpointStore implements CompletedCheckpointStore {

        private final CompletableFuture<JobStatus> shutdownStatus;

        private TestingCompletedCheckpointStore(CompletableFuture<JobStatus> shutdownStatus) {
            this.shutdownStatus = shutdownStatus;
        }

        @Override
        public void recover() {}

        @Override
        public void addCheckpoint(
                CompletedCheckpoint checkpoint,
                CheckpointsCleaner checkpointsCleaner,
                Runnable postCleanup) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public CompletedCheckpoint getLatestCheckpoint(boolean isPreferCheckpointForRecovery) {
            return null;
        }

        @Override
        public void shutdown(
                JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner, Runnable postCleanup) {
            shutdownStatus.complete(jobStatus);
        }

        @Override
        public List<CompletedCheckpoint> getAllCheckpoints() {
            return Collections.emptyList();
        }

        @Override
        public int getNumberOfRetainedCheckpoints() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public int getMaxNumberOfRetainedCheckpoints() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public boolean requiresExternalizedCheckpoints() {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }
}
