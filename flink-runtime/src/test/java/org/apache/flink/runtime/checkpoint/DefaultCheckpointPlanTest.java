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
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.createSubtaskStateWithUnionListState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the behavior of the {@link DefaultCheckpointPlan}. */
class DefaultCheckpointPlanTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @TempDir private java.nio.file.Path temporaryFolder;

    @Test
    void testAbortionIfPartiallyFinishedVertexUsedUnionListState() throws Exception {
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
                        .build(EXECUTOR_EXTENSION.getExecutor());
        ExecutionVertex[] tasks = executionGraph.getJobVertex(jobVertexId).getTaskVertices();
        tasks[0].getCurrentExecutionAttempt().markFinished();

        CheckpointPlan checkpointPlan = createCheckpointPlan(executionGraph);

        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        OperatorState operatorState = new OperatorState(operatorId, 2, 2);
        operatorState.putState(
                0, createSubtaskStateWithUnionListState(TempDirUtils.newFile(temporaryFolder)));
        operatorStates.put(operatorId, operatorState);

        assertThatThrownBy(() -> checkpointPlan.fulfillFinishedTaskStatus(operatorStates))
                .hasMessage(
                        String.format(
                                "The vertex %s (id = %s) has "
                                        + "used UnionListState, but part of its tasks are FINISHED.",
                                executionGraph.getJobVertex(jobVertexId).getName(), jobVertexId))
                .isInstanceOf(FlinkRuntimeException.class);
    }

    @Test
    void testAbortionIfPartiallyOperatorsFinishedVertexUsedUnionListState() throws Exception {
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
                        .build(EXECUTOR_EXTENSION.getExecutor());
        ExecutionVertex[] tasks = executionGraph.getJobVertex(jobVertexId).getTaskVertices();

        CheckpointPlan checkpointPlan = createCheckpointPlan(executionGraph);

        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        OperatorState operatorState = new OperatorState(operatorId, 2, 2);
        operatorState.putState(
                0, createSubtaskStateWithUnionListState(TempDirUtils.newFile(temporaryFolder)));

        operatorState.putState(
                1, createSubtaskStateWithUnionListState(TempDirUtils.newFile(temporaryFolder)));
        checkpointPlan.reportTaskHasFinishedOperators(tasks[1]);
        operatorStates.put(operatorId, operatorState);

        assertThatThrownBy(() -> checkpointPlan.fulfillFinishedTaskStatus(operatorStates))
                .hasMessage(
                        String.format(
                                "The vertex %s (id = %s) has "
                                        + "used UnionListState, but part of its tasks has called operators' finish method.",
                                executionGraph.getJobVertex(jobVertexId).getName(), jobVertexId))
                .isInstanceOf(FlinkRuntimeException.class);
    }

    @Test
    void testFulfillFinishedStates() throws Exception {
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
                        .build(EXECUTOR_EXTENSION.getExecutor());
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

        assertThat(operatorStates).hasSize(3);
        assertThat(operatorStates.get(fullyFinishedOperatorId).isFullyFinished()).isTrue();
        assertThat(operatorStates.get(finishedOnRestoreOperatorId).isFullyFinished()).isTrue();
        OperatorState operatorState = operatorStates.get(partiallyFinishedOperatorId);
        assertThat(operatorState.isFullyFinished()).isFalse();
        assertThat(operatorState.getState(0).isFinished()).isTrue();
    }

    @Test
    void testFulfillFullyFinishedStatesWithCoordinator() throws Exception {
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
                        .build(EXECUTOR_EXTENSION.getExecutor());
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
        assertThat(operatorStates).hasSize(1);
        assertThat(operatorStates.get(finishedOperatorID).isFullyFinished()).isTrue();
    }

    private CheckpointPlan createCheckpointPlan(ExecutionGraph executionGraph) throws Exception {
        CheckpointPlanCalculator checkpointPlanCalculator =
                new DefaultCheckpointPlanCalculator(
                        new JobID(),
                        new ExecutionGraphCheckpointPlanCalculatorContext(executionGraph),
                        executionGraph.getVerticesTopologically(),
                        true);
        return checkpointPlanCalculator.calculateCheckpointPlan().get();
    }
}
