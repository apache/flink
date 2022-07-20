/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.slowtaskdetector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SlowTaskDetectorOptions;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ExecutionTimeBasedSlowTaskDetector}. */
class ExecutionTimeBasedSlowTaskDetectorTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testNoFinishedTaskButRatioIsZero() throws Exception {
        final int parallelism = 3;
        final JobVertex jobVertex = createNoOpVertex(parallelism);
        final ExecutionGraph executionGraph = createExecutionGraph(jobVertex);

        final ExecutionTimeBasedSlowTaskDetector slowTaskDetector = createSlowTaskDetector(0, 1, 0);

        final Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks =
                slowTaskDetector.findSlowTasks(executionGraph);

        assertThat(slowTasks).hasSize(parallelism);
    }

    @Test
    void testFinishedTaskNotExceedRatio() throws Exception {
        final int parallelism = 3;
        final JobVertex jobVertex = createNoOpVertex(parallelism);
        final ExecutionGraph executionGraph = createExecutionGraph(jobVertex);

        final ExecutionTimeBasedSlowTaskDetector slowTaskDetector =
                createSlowTaskDetector(0.5, 1, 0);
        final ExecutionVertex ev1 =
                executionGraph.getJobVertex(jobVertex.getID()).getTaskVertices()[0];
        ev1.getCurrentExecutionAttempt().markFinished();

        final Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks =
                slowTaskDetector.findSlowTasks(executionGraph);

        assertThat(slowTasks).isEmpty();
    }

    @Test
    void testFinishedTaskExceedRatio() throws Exception {
        final int parallelism = 3;
        final JobVertex jobVertex = createNoOpVertex(parallelism);
        final ExecutionGraph executionGraph = createExecutionGraph(jobVertex);

        final ExecutionTimeBasedSlowTaskDetector slowTaskDetector =
                createSlowTaskDetector(0.3, 1, 0);

        // ev3 transitions to DEPLOYING later so that its execution time is the shortest
        final ExecutionVertex ev3 =
                executionGraph.getJobVertex(jobVertex.getID()).getTaskVertices()[2];
        ev3.getCurrentExecutionAttempt().markFinished();

        final Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks =
                slowTaskDetector.findSlowTasks(executionGraph);

        assertThat(slowTasks).hasSize(2);
    }

    @Test
    void testLargeLowerBound() throws Exception {
        final int parallelism = 3;
        final JobVertex jobVertex = createNoOpVertex(parallelism);
        final ExecutionGraph executionGraph = createExecutionGraph(jobVertex);

        final ExecutionTimeBasedSlowTaskDetector slowTaskDetector =
                createSlowTaskDetector(0.3, 1, Integer.MAX_VALUE);

        final ExecutionVertex ev3 =
                executionGraph.getJobVertex(jobVertex.getID()).getTaskVertices()[2];
        ev3.getCurrentExecutionAttempt().markFinished();

        final Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks =
                slowTaskDetector.findSlowTasks(executionGraph);

        // no task can exceed the large baseline
        assertThat(slowTasks).isEmpty();
    }

    @Test
    void testLargeMultiplier() throws Exception {
        final int parallelism = 3;
        final JobVertex jobVertex = createNoOpVertex(parallelism);
        final ExecutionGraph executionGraph = createExecutionGraph(jobVertex);

        final ExecutionTimeBasedSlowTaskDetector slowTaskDetector =
                createSlowTaskDetector(0.3, 1_000_000, 0);

        Thread.sleep(10);

        final ExecutionVertex ev3 =
                executionGraph.getJobVertex(jobVertex.getID()).getTaskVertices()[2];
        ev3.getCurrentExecutionAttempt().markFinished();

        final Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks =
                slowTaskDetector.findSlowTasks(executionGraph);

        // no task can exceed the large baseline
        assertThat(slowTasks).isEmpty();
    }

    @Test
    void testMultipleJobVertexFinishedTaskExceedRatio() throws Exception {
        final int parallelism = 3;
        final JobVertex jobVertex1 = createNoOpVertex(parallelism);
        final JobVertex jobVertex2 = createNoOpVertex(parallelism);
        jobVertex2.connectNewDataSetAsInput(
                jobVertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        final ExecutionGraph executionGraph = createExecutionGraph(jobVertex1, jobVertex2);

        final ExecutionTimeBasedSlowTaskDetector slowTaskDetector =
                createSlowTaskDetector(0.3, 1, 0);

        final ExecutionVertex ev13 =
                executionGraph.getJobVertex(jobVertex1.getID()).getTaskVertices()[2];
        ev13.getCurrentExecutionAttempt().markFinished();
        final ExecutionVertex ev23 =
                executionGraph.getJobVertex(jobVertex2.getID()).getTaskVertices()[2];
        ev23.getCurrentExecutionAttempt().markFinished();

        final Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks =
                slowTaskDetector.findSlowTasks(executionGraph);

        assertThat(slowTasks).hasSize(4);
    }

    @Test
    void testFinishedTaskExceedRatioInDynamicGraph() throws Exception {
        final int parallelism = 3;
        final JobVertex jobVertex1 = createNoOpVertex(parallelism);
        // create jobVertex2 and leave its parallelism unset
        final JobVertex jobVertex2 = new JobVertex("vertex2");
        jobVertex2.setInvokableClass(NoOpInvokable.class);
        jobVertex2.connectNewDataSetAsInput(
                jobVertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        final ExecutionGraph executionGraph = createDynamicExecutionGraph(jobVertex1, jobVertex2);

        final ExecutionTimeBasedSlowTaskDetector slowTaskDetector =
                createSlowTaskDetector(0.3, 1, 0);

        final ExecutionVertex ev13 =
                executionGraph.getJobVertex(jobVertex1.getID()).getTaskVertices()[2];
        ev13.getCurrentExecutionAttempt().markFinished();

        final Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks =
                slowTaskDetector.findSlowTasks(executionGraph);

        assertThat(slowTasks).hasSize(2);
    }

    private ExecutionGraph createExecutionGraph(JobVertex... jobVertices) throws Exception {
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertices);

        final SchedulerBase scheduler =
                SchedulerTestingUtils.createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        EXECUTOR_RESOURCE.getExecutor());

        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        scheduler.startScheduling();
        ExecutionGraphTestUtils.switchAllVerticesToRunning(executionGraph);

        return executionGraph;
    }

    private ExecutionGraph createDynamicExecutionGraph(JobVertex... jobVertices) throws Exception {
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertices);

        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph,
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .buildAdaptiveBatchJobScheduler();

        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        scheduler.startScheduling();
        ExecutionGraphTestUtils.switchAllVerticesToRunning(executionGraph);

        return executionGraph;
    }

    private ExecutionTimeBasedSlowTaskDetector createSlowTaskDetector(
            double ratio, double multiplier, long lowerBoundMillis) {

        final Configuration configuration = new Configuration();
        configuration.set(
                SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_LOWER_BOUND,
                Duration.ofMillis(lowerBoundMillis));
        configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_RATIO, ratio);
        configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_MULTIPLIER, multiplier);

        return new ExecutionTimeBasedSlowTaskDetector(configuration);
    }
}
