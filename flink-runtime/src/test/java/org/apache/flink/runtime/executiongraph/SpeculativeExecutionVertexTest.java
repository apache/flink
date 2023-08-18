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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.TestingInternalFailuresListener;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link SpeculativeExecutionVertex}. */
class SpeculativeExecutionVertexTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private TestingInternalFailuresListener internalFailuresListener;

    @BeforeEach
    void setUp() {
        internalFailuresListener = new TestingInternalFailuresListener();
    }

    @Test
    void testCreateSpeculativeExecution() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();
        assertThat(ev.getCurrentExecutions()).hasSize(1);

        ev.createNewSpeculativeExecution(System.currentTimeMillis());
        assertThat(ev.getCurrentExecutions()).hasSize(2);
    }

    @Test
    void testResetExecutionVertex() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();
        final Execution e1 = ev.getCurrentExecutionAttempt();
        final Execution e2 = ev.createNewSpeculativeExecution(System.currentTimeMillis());

        e1.transitionState(ExecutionState.RUNNING);
        e1.markFinished();
        e2.cancel();
        ev.resetForNewExecution();

        assertThat(ev.getExecutionHistory().getHistoricalExecution(0).get().getAttemptId())
                .isEqualTo(e1.getAttemptId());
        assertThat(ev.getExecutionHistory().getHistoricalExecution(1).get().getAttemptId())
                .isEqualTo(e2.getAttemptId());
        assertThat(ev.getCurrentExecutions()).hasSize(1);
        assertThat(ev.getCurrentExecutionAttempt().getAttemptNumber()).isEqualTo(2);
    }

    @Test
    void testCancel() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();
        final Execution e1 = ev.getCurrentExecutionAttempt();
        final Execution e2 = ev.createNewSpeculativeExecution(System.currentTimeMillis());

        ev.cancel();
        assertThat(e1.getState()).isSameAs(ExecutionState.CANCELED);
        assertThat(e2.getState()).isSameAs(ExecutionState.CANCELED);
    }

    @Test
    void testSuspend() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();
        final Execution e1 = ev.getCurrentExecutionAttempt();
        final Execution e2 = ev.createNewSpeculativeExecution(System.currentTimeMillis());

        ev.suspend();
        assertThat(e1.getState()).isSameAs(ExecutionState.CANCELED);
        assertThat(e2.getState()).isSameAs(ExecutionState.CANCELED);
    }

    @Test
    void testFail() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();
        final Execution e1 = ev.getCurrentExecutionAttempt();
        final Execution e2 = ev.createNewSpeculativeExecution(System.currentTimeMillis());

        ev.fail(new Exception("Forced test failure."));
        assertThat(internalFailuresListener.getFailedTasks())
                .containsExactly(e1.getAttemptId(), e2.getAttemptId());
    }

    @Test
    void testMarkFailed() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();
        final Execution e1 = ev.getCurrentExecutionAttempt();
        final Execution e2 = ev.createNewSpeculativeExecution(System.currentTimeMillis());

        ev.markFailed(new Exception("Forced test failure."));
        assertThat(internalFailuresListener.getFailedTasks())
                .containsExactly(e1.getAttemptId(), e2.getAttemptId());
    }

    @Test
    void testVertexTerminationAndJobTermination() throws Exception {
        final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(jobVertex);
        final ExecutionGraph eg = createExecutionGraph(jobGraph);
        eg.transitionToRunning();

        final SpeculativeExecutionVertex ev =
                (SpeculativeExecutionVertex)
                        eg.getJobVertex(jobVertex.getID()).getTaskVertices()[0];
        final Execution e1 = ev.getCurrentExecutionAttempt();
        final Execution e2 = ev.createNewSpeculativeExecution(System.currentTimeMillis());
        final CompletableFuture<?> terminationFuture = ev.getTerminationFuture();

        e1.transitionState(ExecutionState.RUNNING);
        e1.markFinished();
        assertThat(terminationFuture).isNotDone();
        assertThat(eg.getState()).isSameAs(JobStatus.RUNNING);

        e2.cancel();
        assertThat(terminationFuture).isDone();
        assertThat(eg.getState()).isSameAs(JobStatus.FINISHED);
    }

    @Test
    void testArchiveFailedExecutions() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();

        final Execution e1 = ev.getCurrentExecutionAttempt();
        e1.transitionState(ExecutionState.RUNNING);

        final Execution e2 = ev.createNewSpeculativeExecution(0);
        e2.transitionState(ExecutionState.FAILED);

        ev.archiveFailedExecution(e2.getAttemptId());
        assertThat(ev.getCurrentExecutions()).hasSize(1);
        assertThat(ev.currentExecution).isSameAs(e1);

        final Execution e3 = ev.createNewSpeculativeExecution(0);
        e3.transitionState(ExecutionState.RUNNING);
        e1.transitionState(ExecutionState.FAILED);

        ev.archiveFailedExecution(e1.getAttemptId());
        assertThat(ev.getCurrentExecutions()).hasSize(1);
        assertThat(ev.currentExecution).isSameAs(e3);
    }

    @Test
    void testArchiveTheOnlyCurrentExecution() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();

        final Execution e1 = ev.getCurrentExecutionAttempt();
        e1.transitionState(ExecutionState.FAILED);

        ev.archiveFailedExecution(e1.getAttemptId());

        assertThat(ev.getCurrentExecutions()).hasSize(1);
        assertThat(ev.currentExecution).isSameAs(e1);
    }

    @Test
    void testArchiveNonFailedExecutionWithArchiveFailedExecutionMethod() {
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();

                    final Execution e1 = ev.getCurrentExecutionAttempt();
                    e1.transitionState(ExecutionState.FAILED);

                    final Execution e2 = ev.createNewSpeculativeExecution(0);
                    e2.transitionState(ExecutionState.RUNNING);

                    ev.archiveFailedExecution(e2.getAttemptId());
                });
    }

    @Test
    void testGetExecutionState() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();

        final Execution e1 = ev.getCurrentExecutionAttempt();
        e1.transitionState(ExecutionState.CANCELED);
        assertThat(ev.getExecutionState()).isSameAs(ExecutionState.CANCELED);

        // the latter added state is more likely to reach FINISH state
        final List<ExecutionState> statesSortedByPriority = new ArrayList<>();
        statesSortedByPriority.add(ExecutionState.FAILED);
        statesSortedByPriority.add(ExecutionState.CANCELING);
        statesSortedByPriority.add(ExecutionState.CREATED);
        statesSortedByPriority.add(ExecutionState.SCHEDULED);
        statesSortedByPriority.add(ExecutionState.DEPLOYING);
        statesSortedByPriority.add(ExecutionState.INITIALIZING);
        statesSortedByPriority.add(ExecutionState.RUNNING);
        statesSortedByPriority.add(ExecutionState.FINISHED);

        for (ExecutionState state : statesSortedByPriority) {
            final Execution execution = ev.createNewSpeculativeExecution(0);
            execution.transitionState(state);
            assertThat(ev.getExecutionState()).isSameAs(state);
        }
    }

    @Test
    void testGetNextInputSplit() throws Exception {
        final TestInputSource source = new TestInputSource();
        final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
        jobVertex.setInputSplitSource(source);

        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex(jobVertex);

        final int numExecutions = 3;
        for (int i = 0; i < numExecutions - 1; ++i) {
            ev.createNewSpeculativeExecution(0);
        }
        final List<Execution> executions = new ArrayList<>(ev.getCurrentExecutions());

        final Map<Integer, List<InputSplit>> splitsOfAttempts = new HashMap<>();
        final Random rand = new Random();
        while (executions.size() > 0) {
            final int index = rand.nextInt(executions.size());
            final Execution execution = executions.get(index);
            final Optional<InputSplit> split = execution.getNextInputSplit();
            if (split.isPresent()) {
                splitsOfAttempts
                        .computeIfAbsent(execution.getAttemptNumber(), k -> new ArrayList<>())
                        .add(split.get());
            } else {
                executions.remove(index);
            }
        }

        assertThat(splitsOfAttempts).hasSize(numExecutions);
        assertThat(splitsOfAttempts.get(0)).containsExactlyInAnyOrder(source.splits);
        assertThat(splitsOfAttempts.get(1)).isEqualTo(splitsOfAttempts.get(0));
        assertThat(splitsOfAttempts.get(2)).isEqualTo(splitsOfAttempts.get(0));
    }

    private SpeculativeExecutionVertex createSpeculativeExecutionVertex() throws Exception {
        return createSpeculativeExecutionVertex(ExecutionGraphTestUtils.createNoOpVertex(1));
    }

    private SpeculativeExecutionVertex createSpeculativeExecutionVertex(final JobVertex jobVertex)
            throws Exception {
        final JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(jobVertex);
        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        return (SpeculativeExecutionVertex)
                executionGraph.getJobVertex(jobVertex.getID()).getTaskVertices()[0];
    }

    private ExecutionGraph createExecutionGraph(final JobGraph jobGraph) throws Exception {
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .setExecutionJobVertexFactory(new SpeculativeExecutionJobVertex.Factory())
                        .build(EXECUTOR_RESOURCE.getExecutor());

        executionGraph.setInternalTaskFailuresListener(internalFailuresListener);
        executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

        return executionGraph;
    }

    private class TestInputSource extends GenericInputFormat<Integer> {
        private GenericInputSplit[] splits;

        public GenericInputSplit[] createInputSplits(int numSplitsHint) {
            final int numSplits = numSplitsHint * 10;
            splits = new GenericInputSplit[numSplits];
            for (int i = 0; i < numSplits; ++i) {
                splits[i] = new GenericInputSplit(i, numSplits);
            }
            return splits;
        }

        @Override
        public boolean reachedEnd() throws IOException {
            return false;
        }

        @Override
        public Integer nextRecord(Integer reuse) throws IOException {
            return null;
        }
    }
}
