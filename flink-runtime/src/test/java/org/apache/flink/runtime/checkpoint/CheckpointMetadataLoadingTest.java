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
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A test that checks that checkpoint metadata loading works properly, including validation of
 * resumed state and dropped state.
 */
public class CheckpointMetadataLoadingTest {

    private final ClassLoader cl = getClass().getClassLoader();

    /** Tests correct savepoint loading. */
    @Test
    public void testAllStateRestored() throws Exception {
        final JobID jobId = new JobID();
        final OperatorID operatorId = new OperatorID();
        final long checkpointId = Integer.MAX_VALUE + 123123L;
        final int parallelism = 128128;

        final CompletedCheckpointStorageLocation testSavepoint =
                createSavepointWithOperatorSubtaskState(checkpointId, operatorId, parallelism);
        final Map<JobVertexID, ExecutionJobVertex> tasks =
                createTasks(operatorId, parallelism, parallelism);

        final CompletedCheckpoint loaded =
                Checkpoints.loadAndValidateCheckpoint(jobId, tasks, testSavepoint, cl, false);

        assertEquals(jobId, loaded.getJobId());
        assertEquals(checkpointId, loaded.getCheckpointID());
    }

    /** Tests that savepoint loading fails when there is a max-parallelism mismatch. */
    @Test
    public void testMaxParallelismMismatch() throws Exception {
        final OperatorID operatorId = new OperatorID();
        final int parallelism = 128128;

        final CompletedCheckpointStorageLocation testSavepoint =
                createSavepointWithOperatorSubtaskState(242L, operatorId, parallelism);
        final Map<JobVertexID, ExecutionJobVertex> tasks =
                createTasks(operatorId, parallelism, parallelism + 1);

        try {
            Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, false);
            fail("Did not throw expected Exception");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("Max parallelism mismatch"));
        }
    }

    /**
     * Tests that savepoint loading fails when there is non-restored state, but it is not allowed.
     */
    @Test
    public void testNonRestoredStateWhenDisallowed() throws Exception {
        final OperatorID operatorId = new OperatorID();
        final int parallelism = 9;

        final CompletedCheckpointStorageLocation testSavepoint =
                createSavepointWithOperatorSubtaskState(242L, operatorId, parallelism);
        final Map<JobVertexID, ExecutionJobVertex> tasks = Collections.emptyMap();

        try {
            Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, false);
            fail("Did not throw expected Exception");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("allowNonRestoredState"));
        }
    }

    /**
     * Tests that savepoint loading succeeds when there is non-restored state and it is not allowed.
     */
    @Test
    public void testNonRestoredStateWhenAllowed() throws Exception {
        final OperatorID operatorId = new OperatorID();
        final int parallelism = 9;

        final CompletedCheckpointStorageLocation testSavepoint =
                createSavepointWithOperatorSubtaskState(242L, operatorId, parallelism);
        final Map<JobVertexID, ExecutionJobVertex> tasks = Collections.emptyMap();

        final CompletedCheckpoint loaded =
                Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, true);

        assertTrue(loaded.getOperatorStates().isEmpty());
    }

    /**
     * Tests that savepoint loading fails when there is non-restored coordinator state only, and
     * non-restored state is not allowed.
     */
    @Test
    public void testUnmatchedCoordinatorOnlyStateFails() throws Exception {
        final OperatorID operatorID = new OperatorID();
        final int maxParallelism = 1234;

        final OperatorState state =
                new OperatorState(operatorID, maxParallelism / 2, maxParallelism);
        state.setCoordinatorState(new ByteStreamStateHandle("coordinatorState", new byte[0]));

        final CompletedCheckpointStorageLocation testSavepoint =
                createSavepointWithOperatorState(42L, state);
        final Map<JobVertexID, ExecutionJobVertex> tasks = Collections.emptyMap();

        try {
            Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, false);
            fail("Did not throw expected Exception");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("allowNonRestoredState"));
        }
    }

    // ------------------------------------------------------------------------
    //  setup utils
    // ------------------------------------------------------------------------

    private static CompletedCheckpointStorageLocation createSavepointWithOperatorState(
            final long checkpointId, final OperatorState state) throws IOException {

        final CheckpointMetadata savepoint =
                new CheckpointMetadata(
                        checkpointId, Collections.singletonList(state), Collections.emptyList());
        final StreamStateHandle serializedMetadata;

        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            Checkpoints.storeCheckpointMetadata(savepoint, os);
            serializedMetadata = new ByteStreamStateHandle("checkpoint", os.toByteArray());
        }

        return new TestCompletedCheckpointStorageLocation(serializedMetadata, "dummy/pointer");
    }

    private static CompletedCheckpointStorageLocation createSavepointWithOperatorSubtaskState(
            final long checkpointId, final OperatorID operatorId, final int parallelism)
            throws IOException {

        final Random rnd = new Random();

        final OperatorSubtaskState subtaskState =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                new OperatorStreamStateHandle(
                                        Collections.emptyMap(),
                                        new ByteStreamStateHandle("testHandler", new byte[0])))
                        .setInputChannelState(singleton(createNewInputChannelStateHandle(10, rnd)))
                        .setResultSubpartitionState(
                                singleton(createNewResultSubpartitionStateHandle(10, rnd)))
                        .build();

        final OperatorState state = new OperatorState(operatorId, parallelism, parallelism);
        state.putState(0, subtaskState);

        return createSavepointWithOperatorState(checkpointId, state);
    }

    private static Map<JobVertexID, ExecutionJobVertex> createTasks(
            OperatorID operatorId, int parallelism, int maxParallelism) {
        final JobVertexID vertexId =
                new JobVertexID(operatorId.getLowerPart(), operatorId.getUpperPart());

        ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
        when(vertex.getParallelism()).thenReturn(parallelism);
        when(vertex.getMaxParallelism()).thenReturn(maxParallelism);
        when(vertex.getOperatorIDs())
                .thenReturn(Collections.singletonList(OperatorIDPair.generatedIDOnly(operatorId)));

        if (parallelism != maxParallelism) {
            when(vertex.isMaxParallelismConfigured()).thenReturn(true);
        }

        Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
        tasks.put(vertexId, vertex);

        return tasks;
    }
}
