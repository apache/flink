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

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.TestingInternalFailuresListener;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link ArchivedExecutionVertex} created from a {@link SpeculativeExecutionVertex}.
 */
class ArchivedExecutionVertexWithSpeculativeExecutionTest {

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
        ev.createNewSpeculativeExecution(System.currentTimeMillis());

        ArchivedExecutionVertex aev = ev.archive();
        ArchivedExecutionGraphTestUtils.compareExecutionVertex(ev, aev);
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

        ArchivedExecutionVertex aev = ev.archive();
        ArchivedExecutionGraphTestUtils.compareExecutionVertex(ev, aev);
    }

    @Test
    void testCancel() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();
        ev.createNewSpeculativeExecution(System.currentTimeMillis());
        ev.cancel();

        ArchivedExecutionVertex aev = ev.archive();
        ArchivedExecutionGraphTestUtils.compareExecutionVertex(ev, aev);
    }

    @Test
    void testSuspend() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();
        ev.createNewSpeculativeExecution(System.currentTimeMillis());
        ev.suspend();

        ArchivedExecutionVertex aev = ev.archive();
        ArchivedExecutionGraphTestUtils.compareExecutionVertex(ev, aev);
    }

    @Test
    void testFail() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();
        ev.createNewSpeculativeExecution(System.currentTimeMillis());
        ev.fail(new Exception("Forced test failure."));

        ArchivedExecutionVertex aev = ev.archive();
        ArchivedExecutionGraphTestUtils.compareExecutionVertex(ev, aev);
    }

    @Test
    void testMarkFailed() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();
        ev.createNewSpeculativeExecution(System.currentTimeMillis());
        ev.markFailed(new Exception("Forced test failure."));

        ArchivedExecutionVertex aev = ev.archive();
        ArchivedExecutionGraphTestUtils.compareExecutionVertex(ev, aev);
    }

    @Test
    void testVertexTerminationAndJobTermination() throws Exception {
        final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(jobVertex);
        final ExecutionGraph eg = createExecutionGraph(jobGraph);
        eg.transitionToRunning();

        ExecutionJobVertex jv = eg.getJobVertex(jobVertex.getID());
        assertThat(jv).isNotNull();
        final SpeculativeExecutionVertex ev = (SpeculativeExecutionVertex) jv.getTaskVertices()[0];
        final Execution e1 = ev.getCurrentExecutionAttempt();
        final Execution e2 = ev.createNewSpeculativeExecution(System.currentTimeMillis());

        e1.transitionState(ExecutionState.RUNNING);
        e1.markFinished();
        e2.cancel();

        ArchivedExecutionVertex aev = ev.archive();
        ArchivedExecutionGraphTestUtils.compareExecutionVertex(ev, aev);
    }

    @Test
    void testArchiveFailedExecutions() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();

        final Execution e1 = ev.getCurrentExecutionAttempt();
        e1.transitionState(ExecutionState.RUNNING);

        final Execution e2 = ev.createNewSpeculativeExecution(0);
        e2.transitionState(ExecutionState.FAILED);
        ev.archiveFailedExecution(e2.getAttemptId());

        final Execution e3 = ev.createNewSpeculativeExecution(0);
        e3.transitionState(ExecutionState.RUNNING);
        e1.transitionState(ExecutionState.FAILED);
        ev.archiveFailedExecution(e1.getAttemptId());

        ArchivedExecutionVertex aev = ev.archive();
        ArchivedExecutionGraphTestUtils.compareExecutionVertex(ev, aev);
    }

    @Test
    void testArchiveTheOnlyCurrentExecution() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();

        final Execution e1 = ev.getCurrentExecutionAttempt();
        e1.transitionState(ExecutionState.FAILED);

        ev.archiveFailedExecution(e1.getAttemptId());

        ArchivedExecutionVertex aev = ev.archive();
        ArchivedExecutionGraphTestUtils.compareExecutionVertex(ev, aev);
    }

    @Test
    void testGetExecutionState() throws Exception {
        final SpeculativeExecutionVertex ev = createSpeculativeExecutionVertex();

        final Execution e1 = ev.getCurrentExecutionAttempt();
        e1.transitionState(ExecutionState.CANCELED);

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

            // Check the AchievedExecutionVertex in each state.
            ArchivedExecutionVertex aev = ev.archive();
            ArchivedExecutionGraphTestUtils.compareExecutionVertex(ev, aev);
        }
    }

    private SpeculativeExecutionVertex createSpeculativeExecutionVertex() throws Exception {
        final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(jobVertex);
        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        ExecutionJobVertex jv = executionGraph.getJobVertex(jobVertex.getID());
        assertThat(jv).isNotNull();
        return (SpeculativeExecutionVertex) jv.getTaskVertices()[0];
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
}
