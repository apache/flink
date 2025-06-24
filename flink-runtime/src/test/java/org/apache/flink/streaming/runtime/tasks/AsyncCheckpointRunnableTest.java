/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.testutils.ExceptionallyDoneFuture;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AsyncCheckpointRunnable}. */
class AsyncCheckpointRunnableTest {

    @Test
    void testReportIncompleteStats() {
        long checkpointId = 1000L;
        TestEnvironment env = new TestEnvironment();
        new AsyncCheckpointRunnable(
                        new HashMap<>(),
                        new CheckpointMetaData(checkpointId, 1),
                        new CheckpointMetricsBuilder(),
                        0,
                        "Task Name",
                        r -> {},
                        env,
                        (msg, ex) -> {},
                        false,
                        false,
                        () -> true)
                .close();
        assertThat(((TestTaskStateManager) env.getTaskStateManager()).getReportedCheckpointId())
                .isEqualTo(checkpointId);
    }

    @Test
    void testDeclineWithAsyncCheckpointExceptionWhenRunning() {
        testAsyncCheckpointException(true);
    }

    @Test
    void testDeclineWithAsyncCheckpointExceptionWhenNotRunning() {
        testAsyncCheckpointException(false);
    }

    private void testAsyncCheckpointException(boolean isTaskRunning) {
        final Map<OperatorID, OperatorSnapshotFutures> snapshotsInProgress = new HashMap<>();
        snapshotsInProgress.put(
                new OperatorID(),
                new OperatorSnapshotFutures(
                        ExceptionallyDoneFuture.of(
                                new RuntimeException("Async Checkpoint Exception")),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty())));

        final TestEnvironment environment = new TestEnvironment();
        final AsyncCheckpointRunnable runnable =
                createAsyncRunnable(snapshotsInProgress, environment, false, isTaskRunning);
        runnable.run();

        if (isTaskRunning) {
            assertThat(environment.getCause().getCheckpointFailureReason())
                    .isSameAs(CheckpointFailureReason.CHECKPOINT_ASYNC_EXCEPTION);
        } else {
            assertThat(environment.getCause()).isNull();
        }
    }

    @Test
    void testDeclineAsyncCheckpoint() {
        CheckpointFailureReason originalReason =
                CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM;

        final Map<OperatorID, OperatorSnapshotFutures> snapshotsInProgress = new HashMap<>();
        snapshotsInProgress.put(
                new OperatorID(),
                new OperatorSnapshotFutures(
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        ExceptionallyDoneFuture.of(new CheckpointException(originalReason)),
                        DoneFuture.of(SnapshotResult.empty())));

        final TestEnvironment environment = new TestEnvironment();
        final AsyncCheckpointRunnable runnable =
                createAsyncRunnable(snapshotsInProgress, environment, false, true);
        runnable.run();

        assertThat(environment.getCause().getCheckpointFailureReason()).isSameAs(originalReason);
    }

    @Test
    void testReportFinishedOnRestoreTaskSnapshots() {
        TestEnvironment environment = new TestEnvironment();
        AsyncCheckpointRunnable asyncCheckpointRunnable =
                createAsyncRunnable(new HashMap<>(), environment, true, true);
        asyncCheckpointRunnable.run();
        TestTaskStateManager testTaskStateManager =
                (TestTaskStateManager) environment.getTaskStateManager();

        assertThat(testTaskStateManager.getReportedCheckpointId())
                .isEqualTo(asyncCheckpointRunnable.getCheckpointId());
        assertThat(testTaskStateManager.getLastJobManagerTaskStateSnapshot())
                .isEqualTo(TaskStateSnapshot.FINISHED_ON_RESTORE);
        assertThat(testTaskStateManager.getLastTaskManagerTaskStateSnapshot())
                .isEqualTo(TaskStateSnapshot.FINISHED_ON_RESTORE);
        assertThat(asyncCheckpointRunnable.getFinishedFuture()).isDone();
    }

    private AsyncCheckpointRunnable createAsyncRunnable(
            Map<OperatorID, OperatorSnapshotFutures> snapshotsInProgress,
            TestEnvironment environment,
            boolean isTaskDeployedAsFinished,
            boolean isTaskRunning) {
        return new AsyncCheckpointRunnable(
                snapshotsInProgress,
                new CheckpointMetaData(1, 1L),
                new CheckpointMetricsBuilder()
                        .setBytesProcessedDuringAlignment(0)
                        .setAlignmentDurationNanos(0),
                1L,
                "Task Name",
                r -> {},
                environment,
                (msg, ex) -> {},
                isTaskDeployedAsFinished,
                false,
                () -> isTaskRunning);
    }

    private static class TestEnvironment extends StreamMockEnvironment {

        CheckpointException cause = null;

        TestEnvironment() {
            this(
                    new Configuration(),
                    new Configuration(),
                    new ExecutionConfig(),
                    1L,
                    new MockInputSplitProvider(),
                    1,
                    new TestTaskStateManager());
        }

        TestEnvironment(
                Configuration jobConfig,
                Configuration taskConfig,
                ExecutionConfig executionConfig,
                long memorySize,
                MockInputSplitProvider inputSplitProvider,
                int bufferSize,
                TaskStateManager taskStateManager) {
            super(
                    jobConfig,
                    taskConfig,
                    executionConfig,
                    memorySize,
                    inputSplitProvider,
                    bufferSize,
                    taskStateManager);
        }

        @Override
        public void declineCheckpoint(long checkpointId, CheckpointException checkpointException) {
            this.cause = checkpointException;
        }

        CheckpointException getCause() {
            return cause;
        }
    }
}
