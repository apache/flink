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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for failure of checkpoint coordinator. */
public class CheckpointCoordinatorFailureTest extends TestLogger {

    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

    /**
     * Tests that a failure while storing a completed checkpoint in the completed checkpoint store
     * will properly fail the originating pending checkpoint and clean upt the completed checkpoint.
     */
    @Test
    public void testFailingCompletedCheckpointStoreAdd() throws Exception {
        JobVertexID jobVertexId = new JobVertexID();

        final ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();

        ExecutionGraph testGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexId)
                        .build();

        ExecutionVertex vertex = testGraph.getJobVertex(jobVertexId).getTaskVertices()[0];

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(testGraph)
                        .setCompletedCheckpointStore(
                                new FailingCompletedCheckpointStore(
                                        new Exception(
                                                "The failing completed checkpoint store failed again... :-(")))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        coord.triggerCheckpoint(false);

        manuallyTriggeredScheduledExecutor.triggerAll();

        assertEquals(1, coord.getNumberOfPendingCheckpoints());

        PendingCheckpoint pendingCheckpoint =
                coord.getPendingCheckpoints().values().iterator().next();

        assertFalse(pendingCheckpoint.isDisposed());

        final long checkpointId = coord.getPendingCheckpoints().keySet().iterator().next();

        KeyedStateHandle managedKeyedHandle = mock(KeyedStateHandle.class);
        KeyedStateHandle rawKeyedHandle = mock(KeyedStateHandle.class);
        OperatorStateHandle managedOpHandle = mock(OperatorStreamStateHandle.class);
        OperatorStateHandle rawOpHandle = mock(OperatorStreamStateHandle.class);
        InputChannelStateHandle inputChannelStateHandle =
                new InputChannelStateHandle(
                        new InputChannelInfo(0, 1),
                        mock(StreamStateHandle.class),
                        Collections.singletonList(1L));
        ResultSubpartitionStateHandle resultSubpartitionStateHandle =
                new ResultSubpartitionStateHandle(
                        new ResultSubpartitionInfo(0, 1),
                        mock(StreamStateHandle.class),
                        Collections.singletonList(1L));

        final OperatorSubtaskState operatorSubtaskState =
                spy(
                        OperatorSubtaskState.builder()
                                .setManagedOperatorState(managedOpHandle)
                                .setRawOperatorState(rawOpHandle)
                                .setManagedKeyedState(managedKeyedHandle)
                                .setRawKeyedState(rawKeyedHandle)
                                .setInputChannelState(
                                        StateObjectCollection.singleton(inputChannelStateHandle))
                                .setResultSubpartitionState(
                                        StateObjectCollection.singleton(
                                                resultSubpartitionStateHandle))
                                .build());

        TaskStateSnapshot subtaskState = spy(new TaskStateSnapshot());
        subtaskState.putSubtaskStateByOperatorID(new OperatorID(), operatorSubtaskState);

        when(subtaskState.getSubtaskStateByOperatorID(
                        OperatorID.fromJobVertexID(vertex.getJobvertexId())))
                .thenReturn(operatorSubtaskState);

        AcknowledgeCheckpoint acknowledgeMessage =
                new AcknowledgeCheckpoint(
                        testGraph.getJobID(),
                        vertex.getCurrentExecutionAttempt().getAttemptId(),
                        checkpointId,
                        new CheckpointMetrics(),
                        subtaskState);

        try {
            coord.receiveAcknowledgeMessage(acknowledgeMessage, "Unknown location");
            fail(
                    "Expected a checkpoint exception because the completed checkpoint store could not "
                            + "store the completed checkpoint.");
        } catch (CheckpointException e) {
            // ignore because we expected this exception
        }

        // make sure that the pending checkpoint has been discarded after we could not complete it
        assertTrue(pendingCheckpoint.isDisposed());

        // make sure that the subtask state has been discarded after we could not complete it.
        verify(operatorSubtaskState).discardState();
        verify(operatorSubtaskState.getManagedOperatorState().iterator().next()).discardState();
        verify(operatorSubtaskState.getRawOperatorState().iterator().next()).discardState();
        verify(operatorSubtaskState.getManagedKeyedState().iterator().next()).discardState();
        verify(operatorSubtaskState.getRawKeyedState().iterator().next()).discardState();
        verify(operatorSubtaskState.getInputChannelState().iterator().next().getDelegate())
                .discardState();
        verify(operatorSubtaskState.getResultSubpartitionState().iterator().next().getDelegate())
                .discardState();
    }

    @Test
    public void testCleanupForGenericFailure() throws Exception {
        testStoringFailureHandling(new FlinkRuntimeException("Expected exception"), 1);
    }

    @Test
    public void testCleanupOmissionForPossibleInconsistentStateException() throws Exception {
        testStoringFailureHandling(new PossibleInconsistentStateException(), 0);
    }

    private void testStoringFailureHandling(Exception failure, int expectedCleanupCalls)
            throws Exception {
        final JobVertexID jobVertexID1 = new JobVertexID();

        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .build();

        final ExecutionVertex vertex = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        final ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();

        final StandaloneCheckpointIDCounter checkpointIDCounter =
                new StandaloneCheckpointIDCounter();

        final ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();

        final CompletedCheckpointStore completedCheckpointStore =
                new FailingCompletedCheckpointStore(failure);

        final AtomicInteger cleanupCallCount = new AtomicInteger(0);
        final CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCheckpointIDCounter(checkpointIDCounter)
                        .setCheckpointsCleaner(
                                new CheckpointsCleaner() {

                                    private static final long serialVersionUID =
                                            2029876992397573325L;

                                    @Override
                                    public void cleanCheckpointOnFailedStoring(
                                            CompletedCheckpoint completedCheckpoint,
                                            Executor executor) {
                                        cleanupCallCount.incrementAndGet();
                                        super.cleanCheckpointOnFailedStoring(
                                                completedCheckpoint, executor);
                                    }
                                })
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();
        checkpointCoordinator.triggerSavepoint(tmpFolder.newFolder().getAbsolutePath());
        manuallyTriggeredScheduledExecutor.triggerAll();

        try {
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            graph.getJobID(), attemptId, checkpointIDCounter.getLast()),
                    "unknown location");
            fail("CheckpointException should have been thrown.");
        } catch (CheckpointException e) {
            assertThat(
                    e.getCheckpointFailureReason(),
                    is(CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE));
        }

        assertThat(cleanupCallCount.get(), is(expectedCleanupCalls));
    }

    private static final class FailingCompletedCheckpointStore implements CompletedCheckpointStore {

        private final Exception addCheckpointFailure;

        public FailingCompletedCheckpointStore(Exception addCheckpointFailure) {
            this.addCheckpointFailure = addCheckpointFailure;
        }

        @Override
        public void addCheckpoint(
                CompletedCheckpoint checkpoint,
                CheckpointsCleaner checkpointsCleaner,
                Runnable postCleanup)
                throws Exception {
            throw addCheckpointFailure;
        }

        @Override
        public CompletedCheckpoint getLatestCheckpoint() throws Exception {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner)
                throws Exception {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public int getNumberOfRetainedCheckpoints() {
            return -1;
        }

        @Override
        public int getMaxNumberOfRetainedCheckpoints() {
            return 1;
        }

        @Override
        public boolean requiresExternalizedCheckpoints() {
            return false;
        }
    }
}
