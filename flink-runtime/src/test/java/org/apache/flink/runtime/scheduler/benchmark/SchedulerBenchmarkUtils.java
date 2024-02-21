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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.benchmark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchScheduler;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulerOperations;
import org.apache.flink.runtime.scheduler.strategy.VertexwiseSchedulingStrategy;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder.createCustomParallelismDecider;
import static org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchSchedulerFactory.loadInputConsumableDeciderFactory;

/** Utilities for scheduler benchmarks. */
public class SchedulerBenchmarkUtils {

    public static List<JobVertex> createDefaultJobVertices(JobConfiguration jobConfiguration) {

        final List<JobVertex> jobVertices = new ArrayList<>();

        final JobVertex source = new JobVertex("source");
        source.setInvokableClass(NoOpInvokable.class);
        source.setParallelism(jobConfiguration.getParallelism());
        jobVertices.add(source);

        final JobVertex sink = new JobVertex("sink");
        sink.setInvokableClass(NoOpInvokable.class);
        sink.setParallelism(jobConfiguration.getParallelism());
        jobVertices.add(sink);

        sink.connectNewDataSetAsInput(
                source,
                jobConfiguration.getDistributionPattern(),
                jobConfiguration.getResultPartitionType());

        return jobVertices;
    }

    public static JobGraph createJobGraph(JobConfiguration jobConfiguration) throws IOException {
        return createJobGraph(Collections.emptyList(), jobConfiguration);
    }

    public static JobGraph createJobGraph(
            List<JobVertex> jobVertices, JobConfiguration jobConfiguration) throws IOException {

        final JobGraph jobGraph =
                JobGraphTestUtils.streamingJobGraph(jobVertices.toArray(new JobVertex[0]));

        jobGraph.setJobType(jobConfiguration.getJobType());

        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setExecutionMode(jobConfiguration.getExecutionMode());
        jobGraph.setExecutionConfig(executionConfig);

        return jobGraph;
    }

    public static DefaultScheduler createAndInitScheduler(
            List<JobVertex> jobVertices,
            JobConfiguration jobConfiguration,
            ScheduledExecutorService scheduledExecutorService)
            throws Exception {

        final JobGraph jobGraph = createJobGraph(jobVertices, jobConfiguration);

        final ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forMainThread();

        DefaultSchedulerBuilder schedulerBuilder =
                new DefaultSchedulerBuilder(jobGraph, mainThreadExecutor, scheduledExecutorService)
                        .setIoExecutor(scheduledExecutorService)
                        .setFutureExecutor(scheduledExecutorService)
                        .setDelayExecutor(
                                new ScheduledExecutorServiceAdapter(scheduledExecutorService));
        if (jobConfiguration.getJobType() == JobType.BATCH) {
            AdaptiveBatchScheduler adaptiveBatchScheduler =
                    createAdaptiveBatchScheduler(schedulerBuilder, jobConfiguration);
            adaptiveBatchScheduler.initializeVerticesIfPossible();
            return adaptiveBatchScheduler;
        } else {
            return schedulerBuilder.build();
        }
    }

    public static AdaptiveBatchScheduler createAdaptiveBatchScheduler(
            DefaultSchedulerBuilder schedulerBuilder, JobConfiguration jobConfiguration)
            throws Exception {
        return schedulerBuilder
                .setVertexParallelismAndInputInfosDecider(
                        createCustomParallelismDecider(jobConfiguration.getParallelism()))
                .setHybridPartitionDataConsumeConstraint(
                        jobConfiguration.getHybridPartitionDataConsumeConstraint())
                .setInputConsumableDeciderFactory(
                        loadInputConsumableDeciderFactory(
                                jobConfiguration.getHybridPartitionDataConsumeConstraint()))
                .buildAdaptiveBatchJobScheduler();
    }

    public static ExecutionGraph createAndInitExecutionGraph(
            List<JobVertex> jobVertices,
            JobConfiguration jobConfiguration,
            ScheduledExecutorService scheduledExecutorService)
            throws Exception {
        DefaultScheduler scheduler =
                createAndInitScheduler(jobVertices, jobConfiguration, scheduledExecutorService);
        return scheduler.getExecutionGraph();
    }

    public static SchedulingStrategy createSchedulingStrategy(
            JobConfiguration jobConfiguration, SchedulingTopology schedulingTopology) {
        TestingSchedulerOperations schedulerOperations = new TestingSchedulerOperations();

        if (jobConfiguration.getJobType() == JobType.BATCH) {
            return new VertexwiseSchedulingStrategy(
                    schedulerOperations,
                    schedulingTopology,
                    loadInputConsumableDeciderFactory(
                            jobConfiguration.getHybridPartitionDataConsumeConstraint()));
        } else {
            return new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);
        }
    }

    public static void deployTasks(
            ExecutionGraph executionGraph,
            JobVertexID jobVertexID,
            TestingLogicalSlotBuilder slotBuilder)
            throws JobException, ExecutionException, InterruptedException {

        for (ExecutionVertex vertex : executionGraph.getJobVertex(jobVertexID).getTaskVertices()) {
            LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
            Execution execution = vertex.getCurrentExecutionAttempt();
            execution.transitionState(ExecutionState.SCHEDULED);
            execution.registerProducedPartitions(slot.getTaskManagerLocation()).get();
            assignResourceAndDeploy(vertex, slot);
        }
    }

    public static void deployAllTasks(
            ExecutionGraph executionGraph, TestingLogicalSlotBuilder slotBuilder)
            throws JobException, ExecutionException, InterruptedException {

        for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
            LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
            Execution execution = vertex.getCurrentExecutionAttempt();
            execution.transitionState(ExecutionState.SCHEDULED);
            execution.registerProducedPartitions(slot.getTaskManagerLocation()).get();
            assignResourceAndDeploy(vertex, slot);
        }
    }

    private static void assignResourceAndDeploy(ExecutionVertex vertex, LogicalSlot slot)
            throws JobException {
        vertex.tryAssignResource(slot);
        vertex.deploy();
    }

    public static void transitionTaskStatus(
            ExecutionGraph executionGraph, JobVertexID jobVertexID, ExecutionState state) {

        for (ExecutionVertex vertex : executionGraph.getJobVertex(jobVertexID).getTaskVertices()) {
            executionGraph.updateState(
                    new TaskExecutionStateTransition(
                            new TaskExecutionState(
                                    vertex.getCurrentExecutionAttempt().getAttemptId(), state)));
        }
    }

    public static void transitionTaskStatus(
            DefaultScheduler scheduler,
            AccessExecutionJobVertex vertex,
            int subtask,
            ExecutionState executionState) {

        final ExecutionAttemptID attemptId =
                vertex.getTaskVertices()[subtask].getCurrentExecutionAttempt().getAttemptId();
        scheduler.updateTaskExecutionState(new TaskExecutionState(attemptId, executionState));
    }
}
