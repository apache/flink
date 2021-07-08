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

import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphCheckpointPlanCalculatorContext;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.EnumSet.complementOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Declarative tests for {@link DefaultCheckpointPlanCalculator}.
 *
 * <p>This test contains a framework for declaring vertex and edge states to then assert the
 * calculator behavior.
 */
public class DefaultCheckpointPlanCalculatorTest {

    @Test
    public void testComputeAllRunningGraph() throws Exception {
        runSingleTest(
                Arrays.asList(
                        new VertexDeclaration(3, Collections.emptySet()),
                        new VertexDeclaration(4, Collections.emptySet()),
                        new VertexDeclaration(5, Collections.emptySet()),
                        new VertexDeclaration(6, Collections.emptySet())),
                Arrays.asList(
                        new EdgeDeclaration(0, 2, DistributionPattern.ALL_TO_ALL),
                        new EdgeDeclaration(1, 2, DistributionPattern.POINTWISE),
                        new EdgeDeclaration(2, 3, DistributionPattern.ALL_TO_ALL)),
                Arrays.asList(
                        new TaskDeclaration(0, range(0, 3)), new TaskDeclaration(1, range(0, 4))));
    }

    @Test
    public void testAllToAllEdgeWithSomeSourcesFinished() throws Exception {
        runSingleTest(
                Arrays.asList(
                        new VertexDeclaration(3, range(0, 2)),
                        new VertexDeclaration(4, Collections.emptySet())),
                Collections.singletonList(
                        new EdgeDeclaration(0, 1, DistributionPattern.ALL_TO_ALL)),
                Collections.singletonList(new TaskDeclaration(0, range(2, 3))));
    }

    @Test
    public void testOneToOneEdgeWithSomeSourcesFinished() throws Exception {
        runSingleTest(
                Arrays.asList(
                        new VertexDeclaration(4, range(0, 2)),
                        new VertexDeclaration(4, Collections.emptySet())),
                Collections.singletonList(new EdgeDeclaration(0, 1, DistributionPattern.POINTWISE)),
                Arrays.asList(
                        new TaskDeclaration(0, range(2, 4)), new TaskDeclaration(1, range(0, 2))));
    }

    @Test
    public void testOneToOnEdgeWithSomeSourcesAndTargetsFinished() throws Exception {
        runSingleTest(
                Arrays.asList(
                        new VertexDeclaration(4, range(0, 2)), new VertexDeclaration(4, of(0))),
                Collections.singletonList(new EdgeDeclaration(0, 1, DistributionPattern.POINTWISE)),
                Arrays.asList(
                        new TaskDeclaration(0, range(2, 4)), new TaskDeclaration(1, range(1, 2))));
    }

    @Test
    public void testComputeWithMultipleInputs() throws Exception {
        runSingleTest(
                Arrays.asList(
                        new VertexDeclaration(3, range(0, 3)),
                        new VertexDeclaration(5, of(0, 2, 3)),
                        new VertexDeclaration(5, of(2, 4)),
                        new VertexDeclaration(5, of(2))),
                Arrays.asList(
                        new EdgeDeclaration(0, 3, DistributionPattern.ALL_TO_ALL),
                        new EdgeDeclaration(1, 3, DistributionPattern.POINTWISE),
                        new EdgeDeclaration(2, 3, DistributionPattern.POINTWISE)),
                Arrays.asList(
                        new TaskDeclaration(1, of(1, 4)), new TaskDeclaration(2, of(0, 1, 3))));
    }

    @Test
    public void testComputeWithMultipleLevels() throws Exception {
        runSingleTest(
                Arrays.asList(
                        new VertexDeclaration(16, range(0, 4)),
                        new VertexDeclaration(16, range(0, 16)),
                        new VertexDeclaration(16, range(0, 2)),
                        new VertexDeclaration(16, Collections.emptySet()),
                        new VertexDeclaration(16, Collections.emptySet())),
                Arrays.asList(
                        new EdgeDeclaration(0, 2, DistributionPattern.POINTWISE),
                        new EdgeDeclaration(0, 3, DistributionPattern.POINTWISE),
                        new EdgeDeclaration(1, 2, DistributionPattern.ALL_TO_ALL),
                        new EdgeDeclaration(1, 3, DistributionPattern.POINTWISE),
                        new EdgeDeclaration(2, 4, DistributionPattern.POINTWISE),
                        new EdgeDeclaration(3, 4, DistributionPattern.ALL_TO_ALL)),
                Arrays.asList(
                        new TaskDeclaration(0, range(4, 16)),
                        new TaskDeclaration(2, range(2, 4)),
                        new TaskDeclaration(3, range(0, 4))));
    }

    @Test
    public void testPlanCalculationWhenOneTaskNotRunning() throws Exception {
        // when: All combinations of Source/Not Source for one RUNNING and one NOT RUNNING tasks.
        runWithNotRunningTask(true, true);
        runWithNotRunningTask(true, false);
        runWithNotRunningTask(false, false);
        runWithNotRunningTask(false, true);

        // then: The plan failed because one task didn't have RUNNING state.
    }

    private void runWithNotRunningTask(
            boolean isRunningVertexSource, boolean isNotRunningVertexSource) throws Exception {
        for (ExecutionState notRunningState : complementOf(EnumSet.of(ExecutionState.RUNNING))) {
            JobVertexID runningVertex = new JobVertexID();
            JobVertexID notRunningVertex = new JobVertexID();

            ExecutionGraph graph =
                    new CheckpointExecutionGraphBuilder()
                            .addJobVertex(runningVertex, isRunningVertexSource)
                            .addJobVertex(notRunningVertex, isNotRunningVertexSource)
                            .setTransitToRunning(false)
                            .build();

            // The first vertex is always RUNNING.
            transitVertexToState(graph, runningVertex, ExecutionState.RUNNING);
            // The second vertex is everything except RUNNING.
            transitVertexToState(graph, notRunningVertex, notRunningState);

            DefaultCheckpointPlanCalculator checkpointPlanCalculator =
                    createCheckpointPlanCalculator(graph);

            try {
                checkpointPlanCalculator.calculateCheckpointPlan().get();
                fail(
                        "The computation should fail since some tasks to trigger are in "
                                + notRunningState
                                + " state");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                assertThat(cause, instanceOf(CheckpointException.class));
                assertEquals(
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING,
                        ((CheckpointException) cause).getCheckpointFailureReason());
            }
        }
    }

    private void transitVertexToState(
            ExecutionGraph graph, JobVertexID jobVertexID, ExecutionState state) {
        Arrays.stream(graph.getJobVertex(jobVertexID).getTaskVertices())
                .filter(vertex -> vertex.getJobvertexId().equals(jobVertexID))
                .findFirst()
                .get()
                .getCurrentExecutionAttempt()
                .transitionState(state);
    }

    // ------------------------- Utility methods ---------------------------------------

    private void runSingleTest(
            List<VertexDeclaration> vertexDeclarations,
            List<EdgeDeclaration> edgeDeclarations,
            List<TaskDeclaration> expectedToTriggerTaskDeclarations)
            throws Exception {
        runSingleTest(
                vertexDeclarations,
                edgeDeclarations,
                expectedToTriggerTaskDeclarations,
                IntStream.range(0, vertexDeclarations.size())
                        .mapToObj(
                                i ->
                                        new TaskDeclaration(
                                                i,
                                                vertexDeclarations.get(i).finishedSubtaskIndices))
                        .collect(Collectors.toList()));
    }

    private void runSingleTest(
            List<VertexDeclaration> vertexDeclarations,
            List<EdgeDeclaration> edgeDeclarations,
            List<TaskDeclaration> expectedToTriggerTaskDeclarations,
            List<TaskDeclaration> expectedFinishedTaskDeclarations)
            throws Exception {

        ExecutionGraph graph = createExecutionGraph(vertexDeclarations, edgeDeclarations);
        DefaultCheckpointPlanCalculator planCalculator = createCheckpointPlanCalculator(graph);

        List<TaskDeclaration> expectedRunningTaskDeclarations = new ArrayList<>();
        List<ExecutionJobVertex> expectedFullyFinishedJobVertices = new ArrayList<>();

        expectedFinishedTaskDeclarations.forEach(
                finishedDeclaration -> {
                    ExecutionJobVertex jobVertex =
                            chooseJobVertex(graph, finishedDeclaration.vertexIndex);
                    expectedRunningTaskDeclarations.add(
                            new TaskDeclaration(
                                    finishedDeclaration.vertexIndex,
                                    minus(
                                            range(0, jobVertex.getParallelism()),
                                            finishedDeclaration.subtaskIndices)));
                    if (finishedDeclaration.subtaskIndices.size() == jobVertex.getParallelism()) {
                        expectedFullyFinishedJobVertices.add(jobVertex);
                    }
                });

        List<ExecutionVertex> expectedRunningTasks =
                chooseTasks(graph, expectedRunningTaskDeclarations.toArray(new TaskDeclaration[0]));
        List<Execution> expectedFinishedTasks =
                chooseTasks(graph, expectedFinishedTaskDeclarations.toArray(new TaskDeclaration[0]))
                        .stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList());
        List<ExecutionVertex> expectedToTriggerTasks =
                chooseTasks(
                        graph, expectedToTriggerTaskDeclarations.toArray(new TaskDeclaration[0]));

        // Tests computing checkpoint plan(isUnalignedCheckpoint flag doesn't influence on result
        // because all tasks are in RUNNING state here).
        CheckpointPlan checkpointPlan = planCalculator.calculateCheckpointPlan().get();
        checkCheckpointPlan(
                expectedToTriggerTasks,
                expectedRunningTasks,
                expectedFinishedTasks,
                expectedFullyFinishedJobVertices,
                checkpointPlan);
    }

    private ExecutionGraph createExecutionGraph(
            List<VertexDeclaration> vertexDeclarations, List<EdgeDeclaration> edgeDeclarations)
            throws Exception {

        JobVertex[] jobVertices = new JobVertex[vertexDeclarations.size()];
        for (int i = 0; i < vertexDeclarations.size(); ++i) {
            jobVertices[i] =
                    ExecutionGraphTestUtils.createJobVertex(
                            vertexName(i),
                            vertexDeclarations.get(i).parallelism,
                            NoOpInvokable.class);
        }

        for (EdgeDeclaration edgeDeclaration : edgeDeclarations) {
            jobVertices[edgeDeclaration.target].connectNewDataSetAsInput(
                    jobVertices[edgeDeclaration.source],
                    edgeDeclaration.distributionPattern,
                    ResultPartitionType.PIPELINED);
        }

        ExecutionGraph graph = ExecutionGraphTestUtils.createSimpleTestGraph(jobVertices);
        graph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
        graph.transitionToRunning();
        graph.getAllExecutionVertices()
                .forEach(
                        task ->
                                task.getCurrentExecutionAttempt()
                                        .transitionState(ExecutionState.RUNNING));

        for (int i = 0; i < vertexDeclarations.size(); ++i) {
            JobVertexID jobVertexId = jobVertices[i].getID();
            vertexDeclarations
                    .get(i)
                    .finishedSubtaskIndices
                    .forEach(
                            index -> {
                                graph.getJobVertex(jobVertexId)
                                        .getTaskVertices()[index]
                                        .getCurrentExecutionAttempt()
                                        .markFinished();
                            });
        }

        return graph;
    }

    private DefaultCheckpointPlanCalculator createCheckpointPlanCalculator(ExecutionGraph graph) {
        DefaultCheckpointPlanCalculator checkpointPlanCalculator =
                new DefaultCheckpointPlanCalculator(
                        graph.getJobID(),
                        new ExecutionGraphCheckpointPlanCalculatorContext(graph),
                        graph.getVerticesTopologically());
        checkpointPlanCalculator.setAllowCheckpointsAfterTasksFinished(true);
        return checkpointPlanCalculator;
    }

    private void checkCheckpointPlan(
            List<ExecutionVertex> expectedToTrigger,
            List<ExecutionVertex> expectedRunning,
            List<Execution> expectedFinished,
            List<ExecutionJobVertex> expectedFullyFinished,
            CheckpointPlan plan) {

        // Compares tasks to trigger
        List<Execution> expectedTriggeredExecutions =
                expectedToTrigger.stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList());
        assertSameInstancesWithoutOrder(
                "The computed tasks to trigger is different from expected",
                expectedTriggeredExecutions,
                plan.getTasksToTrigger());

        // Compares running tasks
        assertSameInstancesWithoutOrder(
                "The computed running tasks is different from expected",
                expectedRunning,
                plan.getTasksToCommitTo());

        // Compares finished tasks
        assertSameInstancesWithoutOrder(
                "The computed finished tasks is different from expected",
                expectedFinished,
                plan.getFinishedTasks());

        // Compares fully finished job vertices
        assertSameInstancesWithoutOrder(
                "The computed fully finished JobVertex is different from expected",
                expectedFullyFinished,
                plan.getFullyFinishedJobVertex());

        // Compares tasks to ack
        assertSameInstancesWithoutOrder(
                "The computed tasks to ack is different from expected",
                expectedRunning.stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList()),
                plan.getTasksToWaitFor());
    }

    private <T> void assertSameInstancesWithoutOrder(
            String comment, Collection<T> expected, Collection<T> actual) {
        assertThat(
                comment,
                expected,
                containsInAnyOrder(
                        actual.stream()
                                .map(CoreMatchers::sameInstance)
                                .collect(Collectors.toList())));
    }

    private List<ExecutionVertex> chooseTasks(
            ExecutionGraph graph, TaskDeclaration... chosenDeclarations) {
        List<ExecutionVertex> tasks = new ArrayList<>();

        for (TaskDeclaration chosenDeclaration : chosenDeclarations) {
            ExecutionJobVertex jobVertex = chooseJobVertex(graph, chosenDeclaration.vertexIndex);
            chosenDeclaration.subtaskIndices.forEach(
                    index -> tasks.add(jobVertex.getTaskVertices()[index]));
        }

        return tasks;
    }

    private ExecutionJobVertex chooseJobVertex(ExecutionGraph graph, int vertexIndex) {
        String name = vertexName(vertexIndex);
        Optional<ExecutionJobVertex> foundVertex =
                graph.getAllVertices().values().stream()
                        .filter(jobVertex -> jobVertex.getName().equals(name))
                        .findFirst();

        if (!foundVertex.isPresent()) {
            throw new RuntimeException("Vertex not found with index " + vertexIndex);
        }

        return foundVertex.get();
    }

    private String vertexName(int index) {
        return "vertex_" + index;
    }

    private Set<Integer> range(int start, int end) {
        return IntStream.range(start, end).boxed().collect(Collectors.toSet());
    }

    private Set<Integer> of(Integer... index) {
        return new HashSet<>(Arrays.asList(index));
    }

    private Set<Integer> minus(Set<Integer> all, Set<Integer> toMinus) {
        return all.stream().filter(e -> !toMinus.contains(e)).collect(Collectors.toSet());
    }

    // ------------------------- Utility helper classes ---------------------------------------

    private static class VertexDeclaration {
        final int parallelism;
        final Set<Integer> finishedSubtaskIndices;

        public VertexDeclaration(int parallelism, Set<Integer> finishedSubtaskIndices) {
            this.parallelism = parallelism;
            this.finishedSubtaskIndices = finishedSubtaskIndices;
        }
    }

    private static class EdgeDeclaration {
        final int source;
        final int target;
        final DistributionPattern distributionPattern;

        public EdgeDeclaration(int source, int target, DistributionPattern distributionPattern) {
            this.source = source;
            this.target = target;
            this.distributionPattern = distributionPattern;
        }
    }

    private static class TaskDeclaration {
        final int vertexIndex;

        final Set<Integer> subtaskIndices;

        public TaskDeclaration(int vertexIndex, Set<Integer> subtaskIndices) {
            this.vertexIndex = vertexIndex;
            this.subtaskIndices = subtaskIndices;
        }
    }
}
