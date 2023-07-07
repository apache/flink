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
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTest.assertStatsMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for failure of checkpoint coordinator. */
class CheckpointCoordinatorFailureTest extends TestLogger {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    /**
     * Tests that a failure while storing a completed checkpoint in the completed checkpoint store
     * will properly fail the originating pending checkpoint and clean upt the completed checkpoint.
     */
    @Test
    void testFailingCompletedCheckpointStoreAdd() throws Exception {
        JobVertexID jobVertexId = new JobVertexID();

        final ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();

        ExecutionGraph testGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexId)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex = testGraph.getJobVertex(jobVertexId).getTaskVertices()[0];

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(
                                new FailingCompletedCheckpointStore(
                                        new Exception(
                                                "The failing completed checkpoint store failed again... :-(")))
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .build(testGraph);

        coord.triggerCheckpoint(false);

        manuallyTriggeredScheduledExecutor.triggerAll();

        assertThat(coord.getNumberOfPendingCheckpoints()).isOne();

        PendingCheckpoint pendingCheckpoint =
                coord.getPendingCheckpoints().values().iterator().next();

        assertThat(pendingCheckpoint.isDisposed()).isFalse();

        final long checkpointId = coord.getPendingCheckpoints().keySet().iterator().next();

        KeyedStateHandle managedKeyedHandle = mock(KeyedStateHandle.class);
        when(managedKeyedHandle.getStateHandleId()).thenReturn(StateHandleID.randomStateHandleId());
        KeyedStateHandle rawKeyedHandle = mock(KeyedStateHandle.class);
        when(rawKeyedHandle.getStateHandleId()).thenReturn(StateHandleID.randomStateHandleId());
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
        assertThat(pendingCheckpoint.isDisposed()).isTrue();

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
    void testCleanupForGenericFailure() throws Exception {
        testStoringFailureHandling(new FlinkRuntimeException("Expected exception"), 1);
    }

    @Test
    void testCleanupOmissionForPossibleInconsistentStateException() throws Exception {
        testStoringFailureHandling(new PossibleInconsistentStateException(), 0);
    }

    private void testStoringFailureHandling(Exception failure, int expectedCleanupCalls)
            throws Exception {
        final JobVertexID jobVertexID1 = new JobVertexID();

        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final ExecutionVertex vertex = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        final ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();

        final StandaloneCheckpointIDCounter checkpointIDCounter =
                new StandaloneCheckpointIDCounter();

        final ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();

        final CompletedCheckpointStore completedCheckpointStore =
                new FailingCompletedCheckpointStore(failure);

        CheckpointStatsTracker statsTracker =
                new CheckpointStatsTracker(Integer.MAX_VALUE, new UnregisteredMetricsGroup());
        final AtomicInteger cleanupCallCount = new AtomicInteger(0);
        final CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
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
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .setCheckpointStatsTracker(statsTracker)
                        .build(graph);
        checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        CheckpointMetrics expectedReportedMetrics =
                new CheckpointMetricsBuilder()
                        .setTotalBytesPersisted(18)
                        .setBytesPersistedOfThisCheckpoint(18)
                        .setBytesProcessedDuringAlignment(19)
                        .setAsyncDurationMillis(20)
                        .setAlignmentDurationNanos(123 * 1_000_000)
                        .setCheckpointStartDelayNanos(567 * 1_000_000)
                        .build();
        try {
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            graph.getJobID(),
                            attemptId,
                            checkpointIDCounter.getLast(),
                            expectedReportedMetrics,
                            new TaskStateSnapshot()),
                    "unknown location");
            fail("CheckpointException should have been thrown.");
        } catch (CheckpointException e) {
            assertThat(e.getCheckpointFailureReason())
                    .isEqualTo(CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE);
        }

        AbstractCheckpointStats actualStats =
                statsTracker
                        .createSnapshot()
                        .getHistory()
                        .getCheckpointById(checkpointIDCounter.getLast());

        assertThat(actualStats.getCheckpointId()).isEqualTo(checkpointIDCounter.getLast());
        assertThat(actualStats.getStatus()).isEqualTo(CheckpointStatsStatus.FAILED);
        assertStatsMetrics(vertex.getJobvertexId(), 0, expectedReportedMetrics, actualStats);

        assertThat(cleanupCallCount.get()).isEqualTo(expectedCleanupCalls);
    }

    private static final class FailingCompletedCheckpointStore
            extends AbstractCompleteCheckpointStore {

        private final Exception addCheckpointFailure;

        public FailingCompletedCheckpointStore(Exception addCheckpointFailure) {
            super(
                    SharedStateRegistry.DEFAULT_FACTORY.create(
                            Executors.directExecutor(), emptyList(), RestoreMode.DEFAULT));
            this.addCheckpointFailure = addCheckpointFailure;
        }

        @Override
        public CompletedCheckpoint addCheckpointAndSubsumeOldestOne(
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
            return Collections.emptyList();
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
