/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testtasks.OnceBlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.InternalMiniClusterExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.RestartStrategyUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class contains integration tests for the adaptive scheduler which start a {@link
 * org.apache.flink.runtime.minicluster.MiniCluster} per test case.
 */
public class AdaptiveSchedulerClusterITCase {

    private static final Logger log = LoggerFactory.getLogger(AdaptiveSchedulerClusterITCase.class);

    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int NUMBER_TASK_MANAGERS = 2;
    private static final int PARALLELISM = NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS;
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();

    private final Configuration configuration = createConfiguration();

    private final InternalMiniClusterExtension internalMiniClusterExtension =
            new InternalMiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configuration)
                            .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                            .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
                            .build());

    @RegisterExtension
    private final EachCallbackWrapper<InternalMiniClusterExtension> eachCallbackWrapper =
            new EachCallbackWrapper<>(internalMiniClusterExtension);

    private static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();

        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ofMillis(1L));
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING,
                Duration.ofMillis(1L));
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ofMillis(1L));
        // required for #testCheckpointStatsPersistedAcrossRescale
        configuration.set(WebOptions.CHECKPOINTS_HISTORY_SIZE, Integer.MAX_VALUE);

        return configuration;
    }

    @BeforeEach
    void setUp() {
        OnceBlockingNoOpInvokable.reset();
    }

    @Test
    void testAutomaticScaleDownInCaseOfLostSlots() throws Exception {
        final MiniCluster miniCluster = internalMiniClusterExtension.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();
        final CompletableFuture<JobResult> resultFuture =
                miniCluster.requestJobResult(jobGraph.getJobID());

        waitUntilParallelismForVertexReached(
                jobGraph.getJobID(),
                JOB_VERTEX_ID,
                NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS);

        miniCluster.terminateTaskManager(0);

        waitUntilParallelismForVertexReached(
                jobGraph.getJobID(),
                JOB_VERTEX_ID,
                NUMBER_SLOTS_PER_TASK_MANAGER * (NUMBER_TASK_MANAGERS - 1));
        OnceBlockingNoOpInvokable.unblock();

        final JobResult jobResult = resultFuture.join();

        assertThat(jobResult.isSuccess()).isTrue();
    }

    @Test
    void testAutomaticScaleUp() throws Exception {
        final MiniCluster miniCluster = internalMiniClusterExtension.getMiniCluster();
        int initialInstanceCount = NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS;
        int targetInstanceCount = initialInstanceCount + NUMBER_SLOTS_PER_TASK_MANAGER;
        final JobGraph jobGraph = createBlockingJobGraph(targetInstanceCount);

        log.info(
                "Submitting job with parallelism of "
                        + targetInstanceCount
                        + ", to a cluster with only one TM.");
        miniCluster.submitJob(jobGraph).join();
        final CompletableFuture<JobResult> jobResultFuture =
                miniCluster.requestJobResult(jobGraph.getJobID());

        waitUntilParallelismForVertexReached(
                jobGraph.getJobID(), JOB_VERTEX_ID, initialInstanceCount);

        log.info("Start additional TaskManager to scale up to the full parallelism.");
        miniCluster.startTaskManager();

        log.info("Waiting until Invokable is running with higher parallelism");
        waitUntilParallelismForVertexReached(
                jobGraph.getJobID(), JOB_VERTEX_ID, targetInstanceCount);
        OnceBlockingNoOpInvokable.unblock();

        assertThat(jobResultFuture.join().isSuccess()).isTrue();
    }

    @Test
    void testCheckpointStatsPersistedAcrossRescale() throws Exception {
        final MiniCluster miniCluster = internalMiniClusterExtension.getMiniCluster();

        JobVertex jobVertex = new JobVertex("jobVertex", JOB_VERTEX_ID);
        jobVertex.setInvokableClass(CheckpointingNoOpInvokable.class);
        jobVertex.setParallelism(PARALLELISM);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex);
        jobGraph.setSnapshotSettings(
                new JobCheckpointingSettings(
                        CheckpointCoordinatorConfiguration.builder()
                                .setCheckpointInterval(100)
                                .setCheckpointTimeout(1000)
                                .build(),
                        null));

        miniCluster.submitJob(jobGraph).join();

        // wait until the desired parallelism is reached
        waitUntilParallelismForVertexReached(
                jobGraph.getJobID(),
                JOB_VERTEX_ID,
                NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS);

        // wait until some checkpoints have been completed
        CommonTestUtils.waitUntilCondition(
                () ->
                        miniCluster
                                .getExecutionGraph(jobGraph.getJobID())
                                .thenApply(
                                        eg ->
                                                eg.getCheckpointStatsSnapshot()
                                                                .getCounts()
                                                                .getNumberOfCompletedCheckpoints()
                                                        > 0)
                                .get());

        miniCluster.terminateTaskManager(0);

        waitUntilParallelismForVertexReached(
                jobGraph.getJobID(),
                JOB_VERTEX_ID,
                NUMBER_SLOTS_PER_TASK_MANAGER * (NUMBER_TASK_MANAGERS - 1));

        // check that the very first checkpoint is still accessible
        final List<AbstractCheckpointStats> checkpointHistory =
                miniCluster
                        .getExecutionGraph(jobGraph.getJobID())
                        .thenApply(
                                eg -> eg.getCheckpointStatsSnapshot().getHistory().getCheckpoints())
                        .get();
        assertThat(checkpointHistory.get(checkpointHistory.size() - 1).getCheckpointId()).isOne();
    }

    /** An invokable that doesn't do anything interesting, but does support checkpointing. */
    public static class CheckpointingNoOpInvokable extends AbstractInvokable {

        private static final long CANCEL_SIGNAL = -2L;
        private final BlockingQueue<Long> checkpointsToConfirm = new ArrayBlockingQueue<>(1);

        public CheckpointingNoOpInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            long signal = checkpointsToConfirm.take();
            while (signal != CANCEL_SIGNAL) {
                getEnvironment().acknowledgeCheckpoint(signal, new CheckpointMetrics());
                signal = checkpointsToConfirm.take();
            }
        }

        @Override
        public void cancel() throws Exception {
            checkpointsToConfirm.add(CANCEL_SIGNAL);
        }

        @Override
        public CompletableFuture<Boolean> triggerCheckpointAsync(
                CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
            checkpointsToConfirm.add(checkpointMetaData.getCheckpointId());
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<Void> notifyCheckpointSubsumedAsync(long checkpointId) {
            return CompletableFuture.completedFuture(null);
        }
    }

    private JobGraph createBlockingJobGraph(int parallelism) throws IOException {
        final JobVertex blockingOperator = new JobVertex("Blocking operator", JOB_VERTEX_ID);

        blockingOperator.setInvokableClass(OnceBlockingNoOpInvokable.class);

        blockingOperator.setParallelism(parallelism);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(blockingOperator);

        RestartStrategyUtils.configureFixedDelayRestartStrategy(jobGraph, 1, 0L);

        return jobGraph;
    }

    private void waitUntilParallelismForVertexReached(
            JobID jobId, JobVertexID jobVertexId, int targetParallelism) throws Exception {

        CommonTestUtils.waitUntilCondition(
                () -> {
                    final ArchivedExecutionGraph archivedExecutionGraph =
                            internalMiniClusterExtension
                                    .getMiniCluster()
                                    .getArchivedExecutionGraph(jobId)
                                    .get();

                    final AccessExecutionJobVertex executionJobVertex =
                            archivedExecutionGraph.getAllVertices().get(jobVertexId);

                    if (executionJobVertex == null) {
                        // parallelism was not yet determined
                        return false;
                    }

                    return executionJobVertex.getParallelism() == targetParallelism;
                });
    }
}
