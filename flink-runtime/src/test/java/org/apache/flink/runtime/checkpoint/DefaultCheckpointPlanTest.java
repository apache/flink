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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphCheckpointPlanCalculatorContext;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.TestingStreamStateHandle;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.createSubtaskStateWithUnionListState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests the behavior of the {@link DefaultCheckpointPlan}. */
public class DefaultCheckpointPlanTest {

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Rule public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testAbortionIfPartiallyFinishedVertexUsedUnionListState() throws Exception {
        JobVertexID jobVertexId = new JobVertexID();
        OperatorID operatorId = new OperatorID();

        ExecutionGraph executionGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(
                                jobVertexId,
                                2,
                                2,
                                Collections.singletonList(
                                        OperatorIDPair.generatedIDOnly(operatorId)),
                                true)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionVertex[] tasks = executionGraph.getJobVertex(jobVertexId).getTaskVertices();
        tasks[0].getCurrentExecutionAttempt().markFinished();

        CheckpointPlan checkpointPlan = createCheckpointPlan(executionGraph);

        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        OperatorState operatorState = new OperatorState(operatorId, 2, 2);
        operatorState.putState(0, createSubtaskStateWithUnionListState(TEMPORARY_FOLDER.newFile()));
        operatorStates.put(operatorId, operatorState);

        expectedException.expect(FlinkRuntimeException.class);
        expectedException.expectMessage(
                String.format(
                        "The vertex %s (id = %s) has "
                                + "used UnionListState, but part of its tasks are FINISHED",
                        executionGraph.getJobVertex(jobVertexId).getName(), jobVertexId));
        checkpointPlan.fulfillFinishedTaskStatus(operatorStates);
    }

    @Test
    public void testAbortionIfPartiallyOperatorsFinishedVertexUsedUnionListState()
            throws Exception {
        JobVertexID jobVertexId = new JobVertexID();
        OperatorID operatorId = new OperatorID();

        ExecutionGraph executionGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(
                                jobVertexId,
                                2,
                                2,
                                Collections.singletonList(
                                        OperatorIDPair.generatedIDOnly(operatorId)),
                                true)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionVertex[] tasks = executionGraph.getJobVertex(jobVertexId).getTaskVertices();

        CheckpointPlan checkpointPlan = createCheckpointPlan(executionGraph);

        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        OperatorState operatorState = new OperatorState(operatorId, 2, 2);
        operatorState.putState(0, createSubtaskStateWithUnionListState(TEMPORARY_FOLDER.newFile()));

        operatorState.putState(1, createSubtaskStateWithUnionListState(TEMPORARY_FOLDER.newFile()));
        checkpointPlan.reportTaskHasFinishedOperators(tasks[1]);
        operatorStates.put(operatorId, operatorState);

        expectedException.expect(FlinkRuntimeException.class);
        expectedException.expectMessage(
                String.format(
                        "The vertex %s (id = %s) has "
                                + "used UnionListState, but part of its tasks has called operators' finish method.",
                        executionGraph.getJobVertex(jobVertexId).getName(), jobVertexId));
        checkpointPlan.fulfillFinishedTaskStatus(operatorStates);
    }

    @Test
    public void testFulfillFinishedStates() throws Exception {
        JobVertexID fullyFinishedVertexId = new JobVertexID();
        JobVertexID finishedOnRestoreVertexId = new JobVertexID();
        JobVertexID partiallyFinishedVertexId = new JobVertexID();
        OperatorID fullyFinishedOperatorId = new OperatorID();
        OperatorID finishedOnRestoreOperatorId = new OperatorID();
        OperatorID partiallyFinishedOperatorId = new OperatorID();

        ExecutionGraph executionGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(
                                fullyFinishedVertexId,
                                2,
                                2,
                                Collections.singletonList(
                                        OperatorIDPair.generatedIDOnly(fullyFinishedOperatorId)),
                                true)
                        .addJobVertex(
                                finishedOnRestoreVertexId,
                                2,
                                2,
                                Collections.singletonList(
                                        OperatorIDPair.generatedIDOnly(
                                                finishedOnRestoreOperatorId)),
                                true)
                        .addJobVertex(
                                partiallyFinishedVertexId,
                                2,
                                2,
                                Collections.singletonList(
                                        OperatorIDPair.generatedIDOnly(
                                                partiallyFinishedOperatorId)),
                                true)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionVertex[] fullyFinishedVertexTasks =
                executionGraph.getJobVertex(fullyFinishedVertexId).getTaskVertices();
        ExecutionVertex[] finishedOnRestoreVertexTasks =
                executionGraph.getJobVertex(finishedOnRestoreVertexId).getTaskVertices();
        ExecutionVertex[] partiallyFinishedVertexTasks =
                executionGraph.getJobVertex(partiallyFinishedVertexId).getTaskVertices();
        Arrays.stream(fullyFinishedVertexTasks)
                .forEach(task -> task.getCurrentExecutionAttempt().markFinished());
        partiallyFinishedVertexTasks[0].getCurrentExecutionAttempt().markFinished();

        CheckpointPlan checkpointPlan = createCheckpointPlan(executionGraph);
        Arrays.stream(finishedOnRestoreVertexTasks)
                .forEach(checkpointPlan::reportTaskFinishedOnRestore);

        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        checkpointPlan.fulfillFinishedTaskStatus(operatorStates);

        assertEquals(3, operatorStates.size());
        assertTrue(operatorStates.get(fullyFinishedOperatorId).isFullyFinished());
        assertTrue(operatorStates.get(finishedOnRestoreOperatorId).isFullyFinished());
        OperatorState operatorState = operatorStates.get(partiallyFinishedOperatorId);
        assertFalse(operatorState.isFullyFinished());
        assertTrue(operatorState.getState(0).isFinished());
    }

    @Test
    public void testFulfillFullyFinishedStatesWithCoordinator() throws Exception {
        JobVertexID finishedJobVertexID = new JobVertexID();
        OperatorID finishedOperatorID = new OperatorID();

        ExecutionGraph executionGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(
                                finishedJobVertexID,
                                1,
                                256,
                                Collections.singletonList(
                                        OperatorIDPair.generatedIDOnly(finishedOperatorID)),
                                true)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        executionGraph
                .getJobVertex(finishedJobVertexID)
                .getTaskVertices()[0]
                .getCurrentExecutionAttempt()
                .markFinished();
        CheckpointPlan checkpointPlan = createCheckpointPlan(executionGraph);

        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        OperatorState operatorState = new OperatorState(finishedOperatorID, 1, 256);
        operatorState.setCoordinatorState(new TestingStreamStateHandle());
        operatorStates.put(finishedOperatorID, operatorState);

        checkpointPlan.fulfillFinishedTaskStatus(operatorStates);
        assertEquals(1, operatorStates.size());
        assertTrue(operatorStates.get(finishedOperatorID).isFullyFinished());
    }

    private CheckpointPlan createCheckpointPlan(ExecutionGraph executionGraph) throws Exception {
        PlanCalculator planCalculator =
                new DefaultCheckpointPlanCalculator(
                        new JobID(),
                        new ExecutionGraphCheckpointPlanCalculatorContext(executionGraph),
                        executionGraph.getVerticesTopologically(),
                        true);
        return (CheckpointPlan) planCalculator.calculateEventPlan().get();
    }
}
