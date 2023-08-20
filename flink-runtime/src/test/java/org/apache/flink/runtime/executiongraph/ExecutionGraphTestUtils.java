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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** A collection of utility methods for testing the ExecutionGraph and its related classes. */
public class ExecutionGraphTestUtils {

    // ------------------------------------------------------------------------
    //  reaching states
    // ------------------------------------------------------------------------

    /**
     * Waits until the Job has reached a certain state.
     *
     * <p>This method is based on polling and might miss very fast state transitions!
     */
    public static void waitUntilJobStatus(ExecutionGraph eg, JobStatus status, long maxWaitMillis)
            throws TimeoutException {
        checkNotNull(eg);
        checkNotNull(status);
        checkArgument(maxWaitMillis >= 0);

        // this is a poor implementation - we may want to improve it eventually
        final long deadline =
                maxWaitMillis == 0
                        ? Long.MAX_VALUE
                        : System.nanoTime() + (maxWaitMillis * 1_000_000);

        while (eg.getState() != status && System.nanoTime() < deadline) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException ignored) {
            }
        }

        if (System.nanoTime() >= deadline) {
            throw new TimeoutException(
                    String.format(
                            "The job did not reach status %s in time. Current status is %s.",
                            status, eg.getState()));
        }
    }

    /**
     * Waits until the Execution has reached a certain state.
     *
     * <p>This method is based on polling and might miss very fast state transitions!
     */
    public static void waitUntilExecutionState(
            Execution execution, ExecutionState state, long maxWaitMillis) throws TimeoutException {
        checkNotNull(execution);
        checkNotNull(state);
        checkArgument(maxWaitMillis >= 0);

        // this is a poor implementation - we may want to improve it eventually
        final long deadline =
                maxWaitMillis == 0
                        ? Long.MAX_VALUE
                        : System.nanoTime() + (maxWaitMillis * 1_000_000);

        while (execution.getState() != state && System.nanoTime() < deadline) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException ignored) {
            }
        }

        if (System.nanoTime() >= deadline) {
            throw new TimeoutException(
                    String.format(
                            "The execution did not reach state %s in time. Current state is %s.",
                            state, execution.getState()));
        }
    }

    /**
     * Waits until the ExecutionVertex has reached a certain state.
     *
     * <p>This method is based on polling and might miss very fast state transitions!
     */
    public static void waitUntilExecutionVertexState(
            ExecutionVertex executionVertex, ExecutionState state, long maxWaitMillis)
            throws TimeoutException {
        checkNotNull(executionVertex);
        checkNotNull(state);
        checkArgument(maxWaitMillis >= 0);

        // this is a poor implementation - we may want to improve it eventually
        final long deadline =
                maxWaitMillis == 0
                        ? Long.MAX_VALUE
                        : System.nanoTime() + (maxWaitMillis * 1_000_000);

        while (true) {
            Execution execution = executionVertex.getCurrentExecutionAttempt();

            if (execution == null
                    || (execution.getState() != state && System.nanoTime() < deadline)) {
                try {
                    Thread.sleep(2);
                } catch (InterruptedException ignored) {
                }
            } else {
                break;
            }

            if (System.nanoTime() >= deadline) {
                if (execution != null) {
                    throw new TimeoutException(
                            String.format(
                                    "The execution vertex did not reach state %s in time. Current state is %s.",
                                    state, execution.getState()));
                } else {
                    throw new TimeoutException(
                            "Cannot get current execution attempt of " + executionVertex + '.');
                }
            }
        }
    }

    /**
     * Waits until all executions fulfill the given predicate.
     *
     * @param executionGraph for which to check the executions
     * @param executionPredicate predicate which is to be fulfilled
     * @param maxWaitMillis timeout for the wait operation
     * @throws TimeoutException if the executions did not reach the target state in time
     */
    public static void waitForAllExecutionsPredicate(
            ExecutionGraph executionGraph,
            Predicate<AccessExecution> executionPredicate,
            long maxWaitMillis)
            throws TimeoutException {
        final Predicate<AccessExecutionGraph> allExecutionsPredicate =
                allExecutionsPredicate(executionPredicate);
        final Deadline deadline = Deadline.fromNow(Duration.ofMillis(maxWaitMillis));
        boolean predicateResult;

        do {
            predicateResult = allExecutionsPredicate.test(executionGraph);

            if (!predicateResult) {
                try {
                    Thread.sleep(2L);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        } while (!predicateResult && deadline.hasTimeLeft());

        if (!predicateResult) {
            throw new TimeoutException("Not all executions fulfilled the predicate in time.");
        }
    }

    public static Predicate<AccessExecutionGraph> allExecutionsPredicate(
            final Predicate<AccessExecution> executionPredicate) {
        return accessExecutionGraph -> {
            final Iterable<? extends AccessExecutionVertex> allExecutionVertices =
                    accessExecutionGraph.getAllExecutionVertices();

            for (AccessExecutionVertex executionVertex : allExecutionVertices) {
                final AccessExecution currentExecutionAttempt =
                        executionVertex.getCurrentExecutionAttempt();

                if (currentExecutionAttempt == null
                        || !executionPredicate.test(currentExecutionAttempt)) {
                    return false;
                }
            }

            return true;
        };
    }

    public static Predicate<AccessExecution> isInExecutionState(ExecutionState executionState) {
        return (AccessExecution execution) -> execution.getState() == executionState;
    }

    /**
     * Takes all vertices in the given ExecutionGraph and switches their current execution to
     * INITIALIZING.
     */
    public static void switchAllVerticesToInitializing(ExecutionGraph eg) {
        for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
            vertex.getCurrentExecutionAttempt().switchToInitializing();
        }
    }

    /**
     * Takes all vertices in the given ExecutionGraph and switches their current execution to
     * RUNNING.
     */
    public static void switchAllVerticesToRunning(ExecutionGraph eg) {
        for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
            vertex.getCurrentExecutionAttempt().switchToInitializing();
            vertex.getCurrentExecutionAttempt().switchToRunning();
        }
    }

    /**
     * Takes all vertices in the given ExecutionGraph and attempts to move them from CANCELING to
     * CANCELED.
     */
    public static void completeCancellingForAllVertices(ExecutionGraph eg) {
        for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
            vertex.getCurrentExecutionAttempt().completeCancelling();
        }
    }

    public static void finishJobVertex(ExecutionGraph executionGraph, JobVertexID jobVertexId) {
        for (ExecutionVertex vertex :
                Objects.requireNonNull(executionGraph.getJobVertex(jobVertexId))
                        .getTaskVertices()) {
            finishExecutionVertex(executionGraph, vertex);
        }
    }

    public static void finishExecutionVertex(
            ExecutionGraph executionGraph, ExecutionVertex executionVertex) {
        executionGraph.updateState(
                new TaskExecutionStateTransition(
                        new TaskExecutionState(
                                executionVertex.getCurrentExecutionAttempt().getAttemptId(),
                                ExecutionState.FINISHED)));
    }

    /**
     * Takes all vertices in the given ExecutionGraph and switches their current execution to
     * FINISHED.
     */
    public static void finishAllVertices(ExecutionGraph eg) {
        for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
            vertex.getCurrentExecutionAttempt().markFinished();
        }
    }

    /** Checks that all execution are in state DEPLOYING and then switches them to state RUNNING. */
    public static void switchToRunning(ExecutionGraph eg) {
        // check that all execution are in state DEPLOYING
        for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
            final Execution exec = ev.getCurrentExecutionAttempt();
            final ExecutionState executionState = exec.getState();
            assert executionState == ExecutionState.DEPLOYING
                    : "Expected executionState to be DEPLOYING, was: " + executionState;
        }

        // switch executions to RUNNING
        for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
            final Execution exec = ev.getCurrentExecutionAttempt();
            exec.switchToRunning();
        }
    }

    // ------------------------------------------------------------------------
    //  state modifications
    // ------------------------------------------------------------------------

    public static void setVertexState(ExecutionVertex vertex, ExecutionState state) {
        try {
            Execution exec = vertex.getCurrentExecutionAttempt();

            Field f = Execution.class.getDeclaredField("state");
            f.setAccessible(true);
            f.set(exec, state);
        } catch (Exception e) {
            throw new RuntimeException("Modifying the state failed", e);
        }
    }

    public static void setVertexResource(ExecutionVertex vertex, LogicalSlot slot) {
        Execution exec = vertex.getCurrentExecutionAttempt();

        if (!exec.tryAssignResource(slot)) {
            throw new RuntimeException("Could not assign resource.");
        }
    }

    // ------------------------------------------------------------------------
    //  Mocking ExecutionGraph
    // ------------------------------------------------------------------------

    public static DefaultExecutionGraph createExecutionGraph(
            ScheduledExecutorService executor, JobVertex... vertices) throws Exception {

        return createExecutionGraph(executor, Time.seconds(10L), vertices);
    }

    public static DefaultExecutionGraph createExecutionGraph(
            ScheduledExecutorService executor, Time timeout, JobVertex... vertices)
            throws Exception {

        checkNotNull(vertices);
        checkNotNull(timeout);

        DefaultExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(JobGraphTestUtils.streamingJobGraph(vertices))
                        .setRpcTimeout(timeout)
                        .build(executor);
        executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
        return executionGraph;
    }

    public static JobVertex createNoOpVertex(int parallelism) {
        return createNoOpVertex("vertex", parallelism);
    }

    public static JobVertex createNoOpVertex(String name, int parallelism) {
        return createNoOpVertex(name, parallelism, JobVertex.MAX_PARALLELISM_DEFAULT);
    }

    public static JobVertex createNoOpVertex(String name, int parallelism, int maxParallelism) {
        JobVertex vertex = new JobVertex(name);
        vertex.setInvokableClass(NoOpInvokable.class);
        vertex.setParallelism(parallelism);
        vertex.setMaxParallelism(maxParallelism);
        return vertex;
    }

    // ------------------------------------------------------------------------
    //  utility mocking methods
    // ------------------------------------------------------------------------

    public static ExecutionVertexID createRandomExecutionVertexId() {
        return new ExecutionVertexID(new JobVertexID(), new Random().nextInt(Integer.MAX_VALUE));
    }

    public static JobVertex createJobVertex(
            String task1, int numTasks, Class<NoOpInvokable> invokable) {
        JobVertex groupVertex = new JobVertex(task1);
        groupVertex.setInvokableClass(invokable);
        groupVertex.setParallelism(numTasks);
        return groupVertex;
    }

    public static ExecutionJobVertex getExecutionJobVertex(
            JobVertexID id, ScheduledExecutorService executor) throws Exception {

        return getExecutionJobVertex(id, 1, null, executor);
    }

    public static ExecutionJobVertex getExecutionJobVertex(
            JobVertexID id,
            int parallelism,
            @Nullable SlotSharingGroup slotSharingGroup,
            ScheduledExecutorService executor)
            throws Exception {

        JobVertex ajv = new JobVertex("TestVertex", id);
        ajv.setInvokableClass(AbstractInvokable.class);
        ajv.setParallelism(parallelism);
        if (slotSharingGroup != null) {
            ajv.setSlotSharingGroup(slotSharingGroup);
        }

        return getExecutionJobVertex(ajv, executor);
    }

    public static ExecutionJobVertex getExecutionJobVertex(JobVertex jobVertex) throws Exception {
        return getExecutionJobVertex(jobVertex, new DirectScheduledExecutorService());
    }

    public static ExecutionJobVertex getExecutionJobVertex(
            JobVertex jobVertex, ScheduledExecutorService executor) throws Exception {

        JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(jobVertex);

        SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph,
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                executor)
                        .build();

        return scheduler.getExecutionJobVertex(jobVertex.getID());
    }

    public static ExecutionJobVertex getExecutionJobVertex(JobVertexID id) throws Exception {
        return getExecutionJobVertex(id, new DirectScheduledExecutorService());
    }

    public static ExecutionVertex getExecutionVertex() throws Exception {
        return getExecutionJobVertex(new JobVertexID(), new DirectScheduledExecutorService())
                .getTaskVertices()[0];
    }

    public static Execution getExecution() throws Exception {
        final ExecutionJobVertex ejv = getExecutionJobVertex(new JobVertexID());
        return ejv.getTaskVertices()[0].getCurrentExecutionAttempt();
    }

    public static Execution getExecution(
            final JobVertexID jid,
            final int subtaskIndex,
            final int numTasks,
            final SlotSharingGroup slotSharingGroup)
            throws Exception {

        final ExecutionJobVertex ejv =
                getExecutionJobVertex(
                        jid, numTasks, slotSharingGroup, new DirectScheduledExecutorService());

        return ejv.getTaskVertices()[subtaskIndex].getCurrentExecutionAttempt();
    }

    public static ExecutionAttemptID createExecutionAttemptId() {
        return createExecutionAttemptId(new JobVertexID(0, 0));
    }

    public static ExecutionAttemptID createExecutionAttemptId(JobVertexID jobVertexId) {
        return createExecutionAttemptId(jobVertexId, 0, 0);
    }

    public static ExecutionAttemptID createExecutionAttemptId(
            JobVertexID jobVertexId, int subtaskIndex, int attemptNumber) {
        return createExecutionAttemptId(
                new ExecutionVertexID(jobVertexId, subtaskIndex), attemptNumber);
    }

    public static ExecutionAttemptID createExecutionAttemptId(
            ExecutionVertexID executionVertexId, int attemptNumber) {
        return new ExecutionAttemptID(new ExecutionGraphID(), executionVertexId, attemptNumber);
    }

    // ------------------------------------------------------------------------
    //  graph vertex verifications
    // ------------------------------------------------------------------------

    /**
     * Verifies the generated {@link ExecutionJobVertex} for a given {@link JobVertex} in a {@link
     * ExecutionGraph}.
     *
     * @param executionGraph the generated execution graph
     * @param originJobVertex the vertex to verify for
     * @param inputJobVertices upstream vertices of the verified vertex, used to check inputs of
     *     generated vertex
     * @param outputJobVertices downstream vertices of the verified vertex, used to check produced
     *     data sets of generated vertex
     */
    static void verifyGeneratedExecutionJobVertex(
            ExecutionGraph executionGraph,
            JobVertex originJobVertex,
            @Nullable List<JobVertex> inputJobVertices,
            @Nullable List<JobVertex> outputJobVertices) {

        ExecutionJobVertex ejv = executionGraph.getAllVertices().get(originJobVertex.getID());
        assertThat(ejv).isNotNull();

        // verify basic properties
        assertThat(originJobVertex.getParallelism()).isEqualTo(ejv.getParallelism());
        assertThat(executionGraph.getJobID()).isEqualTo(ejv.getJobId());
        assertThat(originJobVertex.getID()).isEqualTo(ejv.getJobVertexId());
        assertThat(originJobVertex).isEqualTo(ejv.getJobVertex());

        // verify produced data sets
        if (outputJobVertices == null) {
            assertThat(ejv.getProducedDataSets()).isEmpty();
        } else {
            assertThat(outputJobVertices).hasSize(ejv.getProducedDataSets().length);
            for (int i = 0; i < outputJobVertices.size(); i++) {
                assertThat(originJobVertex.getProducedDataSets().get(i).getId())
                        .isEqualTo(ejv.getProducedDataSets()[i].getId());
                assertThat(originJobVertex.getParallelism())
                        .isEqualTo(ejv.getProducedDataSets()[0].getPartitions().length);
            }
        }

        // verify task vertices for their basic properties and their inputs
        assertThat(originJobVertex.getParallelism()).isEqualTo(ejv.getTaskVertices().length);

        int subtaskIndex = 0;
        for (ExecutionVertex ev : ejv.getTaskVertices()) {
            assertThat(executionGraph.getJobID()).isEqualTo(ev.getJobId());
            assertThat(originJobVertex.getID()).isEqualTo(ev.getJobvertexId());

            assertThat(originJobVertex.getParallelism())
                    .isEqualTo(ev.getTotalNumberOfParallelSubtasks());
            assertThat(subtaskIndex).isEqualTo(ev.getParallelSubtaskIndex());

            if (inputJobVertices == null) {
                assertThat(ev.getNumberOfInputs()).isZero();
            } else {
                assertThat(inputJobVertices).hasSize(ev.getNumberOfInputs());

                for (int i = 0; i < inputJobVertices.size(); i++) {
                    ConsumedPartitionGroup consumedPartitionGroup = ev.getConsumedPartitionGroup(i);
                    assertThat(inputJobVertices.get(i).getParallelism())
                            .isEqualTo(consumedPartitionGroup.size());

                    int expectedPartitionNum = 0;
                    for (IntermediateResultPartitionID consumedPartitionId :
                            consumedPartitionGroup) {
                        assertThat(consumedPartitionId.getPartitionNumber())
                                .isEqualTo(expectedPartitionNum);

                        expectedPartitionNum++;
                    }
                }
            }

            subtaskIndex++;
        }
    }
}
