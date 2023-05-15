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
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.testutils.EmptyStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Unit tests for the {@link CompletedCheckpoint}. */
public class CompletedCheckpointTest {

    @Test
    void testCompareCheckpointsWithDifferentOrder() {

        CompletedCheckpoint checkpoint1 =
                new CompletedCheckpoint(
                        new JobID(),
                        0,
                        0,
                        1,
                        new HashMap<>(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        CompletedCheckpoint checkpoint2 =
                new CompletedCheckpoint(
                        new JobID(),
                        1,
                        0,
                        1,
                        new HashMap<>(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        List<CompletedCheckpoint> checkpoints1 = new ArrayList<>();
        checkpoints1.add(checkpoint1);
        checkpoints1.add(checkpoint2);
        checkpoints1.add(checkpoint1);

        List<CompletedCheckpoint> checkpoints2 = new ArrayList<>();
        checkpoints2.add(checkpoint2);
        checkpoints2.add(checkpoint1);
        checkpoints2.add(checkpoint2);

        assertThat(CompletedCheckpoint.checkpointsMatch(checkpoints1, checkpoints2)).isFalse();
    }

    @Test
    void testCompareCheckpointsWithSameOrder() {

        CompletedCheckpoint checkpoint1 =
                new CompletedCheckpoint(
                        new JobID(),
                        0,
                        0,
                        1,
                        new HashMap<>(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        CompletedCheckpoint checkpoint2 =
                new CompletedCheckpoint(
                        new JobID(),
                        1,
                        0,
                        1,
                        new HashMap<>(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        List<CompletedCheckpoint> checkpoints1 = new ArrayList<>();
        checkpoints1.add(checkpoint1);
        checkpoints1.add(checkpoint2);
        checkpoints1.add(checkpoint1);

        List<CompletedCheckpoint> checkpoints2 = new ArrayList<>();
        checkpoints2.add(checkpoint1);
        checkpoints2.add(checkpoint2);
        checkpoints2.add(checkpoint1);

        assertThat(CompletedCheckpoint.checkpointsMatch(checkpoints1, checkpoints2)).isTrue();
    }

    /** Verify that both JobID and checkpoint id are taken into account when comparing. */
    @Test
    void testCompareCheckpointsWithSameJobID() {
        JobID jobID = new JobID();

        CompletedCheckpoint checkpoint1 =
                new CompletedCheckpoint(
                        jobID,
                        0,
                        0,
                        1,
                        new HashMap<>(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        CompletedCheckpoint checkpoint2 =
                new CompletedCheckpoint(
                        jobID,
                        1,
                        0,
                        1,
                        new HashMap<>(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        List<CompletedCheckpoint> checkpoints1 = new ArrayList<>();
        checkpoints1.add(checkpoint1);

        List<CompletedCheckpoint> checkpoints2 = new ArrayList<>();
        checkpoints2.add(checkpoint2);

        assertThat(CompletedCheckpoint.checkpointsMatch(checkpoints1, checkpoints2)).isFalse();
    }

    /** Verify that both JobID and checkpoint id are taken into account when comparing. */
    @Test
    void testCompareCheckpointsWithSameCheckpointId() {
        JobID jobID1 = new JobID();
        JobID jobID2 = new JobID();

        CompletedCheckpoint checkpoint1 =
                new CompletedCheckpoint(
                        jobID1,
                        0,
                        0,
                        1,
                        new HashMap<>(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        CompletedCheckpoint checkpoint2 =
                new CompletedCheckpoint(
                        jobID2,
                        0,
                        0,
                        1,
                        new HashMap<>(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        List<CompletedCheckpoint> checkpoints1 = new ArrayList<>();
        checkpoints1.add(checkpoint1);

        List<CompletedCheckpoint> checkpoints2 = new ArrayList<>();
        checkpoints2.add(checkpoint2);

        assertThat(CompletedCheckpoint.checkpointsMatch(checkpoints1, checkpoints2)).isFalse();
    }

    @Test
    void testRegisterStatesAtRegistry() {
        OperatorState state = mock(OperatorState.class);
        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        operatorStates.put(new OperatorID(), state);

        CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        new JobID(),
                        0,
                        0,
                        1,
                        operatorStates,
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        checkpoint.registerSharedStatesAfterRestored(sharedStateRegistry, RestoreMode.DEFAULT);
        verify(state, times(1)).registerSharedStates(sharedStateRegistry, 0L);
    }

    /** Tests that the garbage collection properties are respected when subsuming checkpoints. */
    @Test
    void testCleanUpOnSubsume() throws Exception {
        OperatorState state = mock(OperatorState.class);
        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        operatorStates.put(new OperatorID(), state);

        EmptyStreamStateHandle metadata = new EmptyStreamStateHandle();
        TestCompletedCheckpointStorageLocation location =
                new TestCompletedCheckpointStorageLocation(metadata, "ptr");

        CheckpointProperties props =
                new CheckpointProperties(
                        false, CheckpointType.CHECKPOINT, true, false, false, false, false, false);

        CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        new JobID(),
                        0,
                        0,
                        1,
                        operatorStates,
                        Collections.emptyList(),
                        props,
                        location,
                        null);

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        checkpoint.registerSharedStatesAfterRestored(sharedStateRegistry, RestoreMode.DEFAULT);
        verify(state, times(1)).registerSharedStates(sharedStateRegistry, 0L);

        // Subsume
        checkpoint.markAsDiscardedOnSubsume().discard();

        verify(state, times(1)).discardState();
        assertThat(location.isDisposed()).isTrue();
        assertThat(metadata.isDisposed()).isTrue();
    }

    /** Tests that the garbage collection properties are respected when shutting down. */
    @Test
    void testCleanUpOnShutdown() throws Exception {
        JobStatus[] terminalStates =
                new JobStatus[] {
                    JobStatus.FINISHED, JobStatus.CANCELED, JobStatus.FAILED, JobStatus.SUSPENDED
                };

        for (JobStatus status : terminalStates) {

            OperatorState state = mock(OperatorState.class);
            Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
            operatorStates.put(new OperatorID(), state);

            EmptyStreamStateHandle retainedHandle = new EmptyStreamStateHandle();
            TestCompletedCheckpointStorageLocation retainedLocation =
                    new TestCompletedCheckpointStorageLocation(retainedHandle, "ptr");

            // Keep
            CheckpointProperties retainProps =
                    new CheckpointProperties(
                            false,
                            CheckpointType.CHECKPOINT,
                            false,
                            false,
                            false,
                            false,
                            false,
                            false);
            CompletedCheckpoint checkpoint =
                    new CompletedCheckpoint(
                            new JobID(),
                            0,
                            0,
                            1,
                            new HashMap<>(operatorStates),
                            Collections.emptyList(),
                            retainProps,
                            retainedLocation,
                            null);

            checkpoint.markAsDiscardedOnShutdown(status).discard();

            verify(state, times(0)).discardState();
            assertThat(retainedLocation.isDisposed()).isFalse();
            assertThat(retainedHandle.isDisposed()).isFalse();

            // Discard
            EmptyStreamStateHandle discardHandle = new EmptyStreamStateHandle();
            TestCompletedCheckpointStorageLocation discardLocation =
                    new TestCompletedCheckpointStorageLocation(discardHandle, "ptr");

            // Keep
            CheckpointProperties discardProps =
                    new CheckpointProperties(
                            false, CheckpointType.CHECKPOINT, true, true, true, true, true, false);

            checkpoint =
                    new CompletedCheckpoint(
                            new JobID(),
                            0,
                            0,
                            1,
                            new HashMap<>(operatorStates),
                            Collections.emptyList(),
                            discardProps,
                            discardLocation,
                            null);

            checkpoint.markAsDiscardedOnShutdown(status).discard();

            verify(state, times(1)).discardState();
            assertThat(discardLocation.isDisposed()).isTrue();
            assertThat(discardHandle.isDisposed()).isTrue();
        }
    }

    /** Tests that the stats callbacks happen if the callback is registered. */
    @Test
    void testCompletedCheckpointStatsCallbacks() throws Exception {
        Map<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
        JobVertexID jobVertexId = new JobVertexID();
        taskStats.put(jobVertexId, new TaskStateStats(jobVertexId, 1));
        CompletedCheckpointStats checkpointStats =
                new CompletedCheckpointStats(
                        1,
                        0,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        1,
                        taskStats,
                        1,
                        1,
                        1,
                        1,
                        true,
                        mock(SubtaskStateStats.class),
                        null);
        CompletedCheckpoint completed =
                new CompletedCheckpoint(
                        new JobID(),
                        0,
                        0,
                        1,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation(),
                        checkpointStats);

        completed.markAsDiscardedOnShutdown(JobStatus.FINISHED).discard();
        assertThat(checkpointStats.isDiscarded()).isTrue();
    }

    @Test
    void testIsJavaSerializable() throws Exception {
        TaskStateStats task1 = new TaskStateStats(new JobVertexID(), 3);
        TaskStateStats task2 = new TaskStateStats(new JobVertexID(), 4);

        HashMap<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
        taskStats.put(task1.getJobVertexId(), task1);
        taskStats.put(task2.getJobVertexId(), task2);

        CompletedCheckpointStats completed =
                new CompletedCheckpointStats(
                        123123123L,
                        10123L,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        1337,
                        taskStats,
                        1337,
                        123129837912L,
                        42L,
                        44L,
                        true,
                        new SubtaskStateStats(
                                123, 213123, 123123, 123123, 0, 0, 0, 0, 0, 0, false, true),
                        null);

        CompletedCheckpointStats copy = CommonTestUtils.createCopySerializable(completed);

        assertThat(copy.getCheckpointId()).isEqualTo(completed.getCheckpointId());
        assertThat(copy.getTriggerTimestamp()).isEqualTo(completed.getTriggerTimestamp());
        assertThat(copy.getProperties()).isEqualTo(completed.getProperties());
        assertThat(copy.getNumberOfSubtasks()).isEqualTo(completed.getNumberOfSubtasks());
        assertThat(copy.getNumberOfAcknowledgedSubtasks())
                .isEqualTo(completed.getNumberOfAcknowledgedSubtasks());
        assertThat(copy.getEndToEndDuration()).isEqualTo(completed.getEndToEndDuration());
        assertThat(copy.getStateSize()).isEqualTo(completed.getStateSize());
        assertThat(copy.getProcessedData()).isEqualTo(completed.getProcessedData());
        assertThat(copy.getPersistedData()).isEqualTo(completed.getPersistedData());
        assertThat(copy.isUnalignedCheckpoint()).isEqualTo(completed.isUnalignedCheckpoint());
        assertThat(copy.getLatestAcknowledgedSubtaskStats().getSubtaskIndex())
                .isEqualTo(completed.getLatestAcknowledgedSubtaskStats().getSubtaskIndex());
        assertThat(copy.getStatus()).isEqualTo(completed.getStatus());
    }
}
