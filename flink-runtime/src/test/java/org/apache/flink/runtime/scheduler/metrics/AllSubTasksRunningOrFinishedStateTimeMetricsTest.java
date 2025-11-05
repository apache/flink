/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.metrics;

import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.events.Event;
import org.apache.flink.events.EventBuilder;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.CoordinatorStoreImpl;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.scheduler.metrics.AllSubTasksRunningOrFinishedStateTimeMetrics.STATUS_ATTRIBUTE;
import static org.apache.flink.runtime.scheduler.metrics.AllSubTasksRunningOrFinishedStateTimeMetrics.Status.ALL_RUNNING_OR_FINISHED;
import static org.apache.flink.runtime.scheduler.metrics.AllSubTasksRunningOrFinishedStateTimeMetrics.Status.NOT_ALL_RUNNING_OR_FINISHED;
import static org.apache.flink.runtime.scheduler.metrics.StateTimeMetricTest.enable;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AllSubTasksRunningOrFinishedStateTimeMetrics}. */
class AllSubTasksRunningOrFinishedStateTimeMetricsTest {
    private static final Executor DIRECT_EXECUTOR = Runnable::run;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private static final MetricOptions.JobStatusMetricsSettings settings =
            enable(
                    MetricOptions.JobStatusMetrics.STATE,
                    MetricOptions.JobStatusMetrics.CURRENT_TIME,
                    MetricOptions.JobStatusMetrics.TOTAL_TIME);

    @Test
    void testInitialValues() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final AllSubTasksRunningOrFinishedStateTimeMetrics runningMetrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(
                        JobType.STREAMING, settings, clock);

        assertThat(runningMetrics.getCurrentTime()).isEqualTo(0L);
        assertThat(runningMetrics.getTotalTime()).isEqualTo(0L);
        assertThat(runningMetrics.getBinary()).isEqualTo(0L);
    }

    @Test
    void testScheduledToFinishedWithSingleExecutionNoFailure() {
        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(JobType.STREAMING, settings);

        final Execution exec1 = newExecution();

        metrics.onStateUpdate(exec1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(exec1, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);
    }

    @Test
    void testScheduledToFinishedWithSingleExecutionWithFailure() {
        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(JobType.STREAMING, settings);

        Execution exec1 = newExecution();

        metrics.onStateUpdate(exec1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(exec1, ExecutionState.RUNNING, ExecutionState.CANCELING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.CANCELING, ExecutionState.CANCELED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        Execution exec2 = newAttempt(exec1);

        metrics.onStateUpdate(exec2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(exec2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);
    }

    @Test
    void testScheduledToFinishedWithMultipleExecutionsNoFailure() {
        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(JobType.STREAMING, settings);

        Execution exec1 = newExecution();
        Execution exec2 = newExecution();

        metrics.onStateUpdate(exec1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        // Only signal running once both executions are in running state
        metrics.onStateUpdate(exec2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        // Maintain value 1 as executions finish
        metrics.onStateUpdate(exec1, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(exec2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);
    }

    @Test
    void testScheduledToFinishedWithMultipleExecutionsWithFailure() {
        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(JobType.STREAMING, settings);

        Execution exec1 = newExecution();
        Execution exec2 = newExecution();

        metrics.onStateUpdate(exec1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        // Only signal running once both executions are in running state
        metrics.onStateUpdate(exec2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        // One subtask cancelling move the state to 0
        metrics.onStateUpdate(exec1, ExecutionState.RUNNING, ExecutionState.CANCELING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(exec1, ExecutionState.CANCELING, ExecutionState.CANCELED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        Execution id1V2 = newAttempt(exec1);

        metrics.onStateUpdate(id1V2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1V2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1V2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        // Both subtasks again up and running
        metrics.onStateUpdate(id1V2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(id1V2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(exec2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);
    }

    @Test
    void testGetCurrentTime() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(
                        JobType.STREAMING, settings, clock);

        Execution exec1 = newExecution();

        metrics.onStateUpdate(exec1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(exec1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(exec1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(exec1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(5L);
        clock.advanceTime(Duration.ofMillis(10));
        assertThat(metrics.getCurrentTime()).isEqualTo(15L);
        metrics.onStateUpdate(exec1, ExecutionState.RUNNING, ExecutionState.CANCELING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(exec1, ExecutionState.CANCELING, ExecutionState.CANCELED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);

        Execution exec2 = newAttempt(exec1);
        metrics.onStateUpdate(exec2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(exec2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(exec2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(exec2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(5L);
        metrics.onStateUpdate(exec2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        clock.advanceTime(Duration.ofMillis(10));
        assertThat(metrics.getCurrentTime()).isEqualTo(15L);
    }

    @Test
    void testGetTotalTime() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(
                        JobType.STREAMING, settings, clock);

        Execution exec1 = newExecution();

        metrics.onStateUpdate(exec1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(0L);
        metrics.onStateUpdate(exec1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(0L);
        metrics.onStateUpdate(exec1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(0L);
        metrics.onStateUpdate(exec1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(5L);
        clock.advanceTime(Duration.ofMillis(10));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);
        metrics.onStateUpdate(exec1, ExecutionState.RUNNING, ExecutionState.CANCELING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);
        metrics.onStateUpdate(exec1, ExecutionState.CANCELING, ExecutionState.CANCELED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);

        Execution exec2 = newAttempt(exec1);
        metrics.onStateUpdate(exec2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);
        metrics.onStateUpdate(exec2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);
        metrics.onStateUpdate(exec2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);
        metrics.onStateUpdate(exec2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(20L);
        metrics.onStateUpdate(exec2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        clock.advanceTime(Duration.ofMillis(10));
        assertThat(metrics.getTotalTime()).isEqualTo(30L);
    }

    @Test
    void testStatusEvents() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(
                        JobType.STREAMING, settings, clock);

        final List<Event> events = new ArrayList<>();

        metrics.registerMetrics(
                new UnregisteredMetricsGroup() {
                    @Override
                    public void addEvent(EventBuilder eventBuilder) {
                        events.add(eventBuilder.build(getAllVariables()));
                    }
                });

        Execution exec1 = newExecution();

        metrics.onStateUpdate(exec1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(exec1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(exec1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(exec1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(events).hasSize(1);
        assertThat(events.remove(0).getAttributes())
                .containsEntry(STATUS_ATTRIBUTE, ALL_RUNNING_OR_FINISHED.toString());
        metrics.onStateUpdate(exec1, ExecutionState.RUNNING, ExecutionState.CANCELING);
        assertThat(events).hasSize(1);
        assertThat(events.remove(0).getAttributes())
                .containsEntry(STATUS_ATTRIBUTE, NOT_ALL_RUNNING_OR_FINISHED.toString());
        metrics.onStateUpdate(exec1, ExecutionState.CANCELING, ExecutionState.CANCELED);
        assertThat(events).isEmpty();

        Execution exec2 = newAttempt(exec1);
        metrics.onStateUpdate(exec2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(exec2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(exec2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(exec2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(events).hasSize(1);
        assertThat(events.remove(0).getAttributes())
                .containsEntry(STATUS_ATTRIBUTE, ALL_RUNNING_OR_FINISHED.toString());
        metrics.onStateUpdate(exec2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(events).isEmpty();
    }

    @Test
    void testScaleDown() {
        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(JobType.STREAMING, settings);

        final List<Event> events = new ArrayList<>();

        metrics.registerMetrics(
                new UnregisteredMetricsGroup() {
                    @Override
                    public void addEvent(EventBuilder eventBuilder) {
                        events.add(eventBuilder.build(getAllVariables()));
                    }
                });

        final JobVertex jobVertex = newJobVertex();

        final List<Execution> originalExecutions = newExecutions(2, jobVertex);
        goThroughExecutionStatesToRunningJob(metrics, originalExecutions);
        assertThat(events.remove(0).getAttributes())
                .containsEntry(STATUS_ATTRIBUTE, ALL_RUNNING_OR_FINISHED.toString());

        goThroughExecutionStatesFromRunningJobToCanceled(metrics, originalExecutions);
        assertThat(events.remove(0).getAttributes())
                .containsEntry(STATUS_ATTRIBUTE, NOT_ALL_RUNNING_OR_FINISHED.toString());

        final List<Execution> postScaleDownExecutions = newExecutions(1, jobVertex);
        goThroughExecutionStatesToRunningJob(metrics, postScaleDownExecutions);
        assertThat(events.remove(0).getAttributes())
                .containsEntry(STATUS_ATTRIBUTE, ALL_RUNNING_OR_FINISHED.toString());
    }

    private static void goThroughExecutionStatesToRunningJob(
            ExecutionStatusMetricsRegistrar metrics, Collection<Execution> executions) {
        for (Execution execution : executions) {
            metrics.onStateUpdate(
                    execution.getAttemptId(), ExecutionState.CREATED, ExecutionState.SCHEDULED);
        }
        for (Execution execution : executions) {
            metrics.onStateUpdate(
                    execution.getAttemptId(), ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
            metrics.onStateUpdate(
                    execution.getAttemptId(), ExecutionState.DEPLOYING, ExecutionState.RUNNING);
        }
    }

    private static void goThroughExecutionStatesFromRunningJobToCanceled(
            ExecutionStatusMetricsRegistrar metrics, Collection<Execution> executions) {
        for (Execution execution : executions) {
            metrics.onStateUpdate(
                    execution.getAttemptId(), ExecutionState.RUNNING, ExecutionState.CANCELING);
            metrics.onStateUpdate(
                    execution.getAttemptId(), ExecutionState.CANCELING, ExecutionState.CANCELED);
        }
    }

    private static Execution newExecution() {
        return newExecutions(1, newJobVertex()).get(0);
    }

    private static List<Execution> newExecutions(int parallelism, JobVertex jv) {
        try {
            DefaultExecutionGraph eg =
                    TestingDefaultExecutionGraphBuilder.newBuilder()
                            .buildDynamicGraph(new DirectScheduledExecutorService());
            ExecutionJobVertex ejv =
                    new ExecutionJobVertex(
                            eg,
                            jv,
                            new DefaultVertexParallelismInfo(
                                    parallelism, parallelism, ignored -> Optional.empty()),
                            new CoordinatorStoreImpl(),
                            UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
            return IntStream.range(0, parallelism)
                    .mapToObj(subtaskIndex -> newExecutionVertex(ejv, subtaskIndex))
                    .map(ev -> new Execution(DIRECT_EXECUTOR, ev, 0, 0, TIMEOUT))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static JobVertex newJobVertex() {
        return ExecutionGraphTestUtils.createJobVertex("task1", 1, NoOpInvokable.class);
    }

    private static ExecutionVertex newExecutionVertex(ExecutionJobVertex ejv, int subtaskIndex) {
        return new ExecutionVertex(ejv, subtaskIndex, new IntermediateResult[0], TIMEOUT, 0L, 1, 0);
    }

    private static Execution newAttempt(Execution execution) {
        return new Execution(
                DIRECT_EXECUTOR,
                execution.getVertex(),
                execution.getAttemptNumber() + 1,
                System.currentTimeMillis(),
                TIMEOUT);
    }
}
