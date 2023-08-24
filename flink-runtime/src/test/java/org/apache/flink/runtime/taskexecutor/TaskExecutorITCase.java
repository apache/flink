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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.testutils.CustomExtension;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.TestingAbstractInvokables;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.InternalMiniClusterExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the {@link TaskExecutor}. */
class TaskExecutorITCase {

    private static final int NUM_TMS = 2;
    private static final int SLOTS_PER_TM = 2;
    private static final int PARALLELISM = NUM_TMS * SLOTS_PER_TM;

    private static final InternalMiniClusterExtension MINI_CLUSTER_EXTENSION =
            new InternalMiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(NUM_TMS)
                            .setNumberSlotsPerTaskManager(SLOTS_PER_TM)
                            .build());

    @RegisterExtension
    private final EachCallbackWrapper<?> miniClusterExtensionWrapper =
            new EachCallbackWrapper<>(
                    new CustomExtension() {
                        @Override
                        public void before(ExtensionContext context) throws Exception {
                            MINI_CLUSTER_EXTENSION.beforeAll(context);
                        }

                        @Override
                        public void after(ExtensionContext context) throws Exception {
                            MINI_CLUSTER_EXTENSION.afterAll(context);
                        }
                    });

    /**
     * Tests that a job can be re-executed after the job has failed due to a TaskExecutor
     * termination.
     */
    @Test
    void testJobReExecutionAfterTaskExecutorTermination() throws Exception {
        final JobGraph jobGraph = createJobGraph(PARALLELISM);

        final MiniCluster miniCluster = MINI_CLUSTER_EXTENSION.getMiniCluster();

        final CompletableFuture<JobResult> jobResultFuture =
                submitJobAndWaitUntilRunning(jobGraph, miniCluster);

        // kill one TaskExecutor which should fail the job execution
        miniCluster.terminateTaskManager(0);

        final JobResult jobResult = jobResultFuture.get();

        assertThat(jobResult.isSuccess()).isFalse();

        miniCluster.startTaskManager();

        final JobGraph newJobGraph = createJobGraph(PARALLELISM);
        BlockingOperator.unblock();
        miniCluster.submitJob(newJobGraph).get();

        miniCluster.requestJobResult(newJobGraph.getJobID()).get();
    }

    /** Tests that the job can recover from a failing {@link TaskExecutor}. */
    @Test
    void testJobRecoveryWithFailingTaskExecutor() throws Exception {
        final JobGraph jobGraph = createJobGraphWithRestartStrategy(PARALLELISM);

        final MiniCluster miniCluster = MINI_CLUSTER_EXTENSION.getMiniCluster();

        final CompletableFuture<JobResult> jobResultFuture =
                submitJobAndWaitUntilRunning(jobGraph, miniCluster);

        // start an additional TaskExecutor
        miniCluster.startTaskManager();

        miniCluster.terminateTaskManager(0).get(); // this should fail the job

        BlockingOperator.unblock();

        assertThat(jobResultFuture.get().isSuccess()).isTrue();
    }

    private static CompletableFuture<JobResult> submitJobAndWaitUntilRunning(
            JobGraph jobGraph, MiniCluster miniCluster) throws Exception {
        miniCluster.submitJob(jobGraph).get();

        final CompletableFuture<JobResult> jobResultFuture =
                miniCluster.requestJobResult(jobGraph.getJobID());

        assertThat(jobResultFuture).isNotDone();

        CommonTestUtils.waitUntilCondition(
                jobIsRunning(() -> miniCluster.getExecutionGraph(jobGraph.getJobID())), 50L);

        return jobResultFuture;
    }

    private static SupplierWithException<Boolean, Exception> jobIsRunning(
            Supplier<CompletableFuture<? extends AccessExecutionGraph>>
                    executionGraphFutureSupplier) {
        final Predicate<AccessExecution> runningOrFinished =
                ExecutionGraphTestUtils.isInExecutionState(ExecutionState.RUNNING)
                        .or(ExecutionGraphTestUtils.isInExecutionState(ExecutionState.FINISHED));
        final Predicate<AccessExecutionGraph> allExecutionsRunning =
                ExecutionGraphTestUtils.allExecutionsPredicate(runningOrFinished);

        return () -> {
            final AccessExecutionGraph executionGraph = executionGraphFutureSupplier.get().join();
            return allExecutionsRunning.test(executionGraph)
                    && executionGraph.getState() == JobStatus.RUNNING;
        };
    }

    private JobGraph createJobGraphWithRestartStrategy(int parallelism) throws IOException {
        final JobGraph jobGraph = createJobGraph(parallelism);
        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 0L));
        jobGraph.setExecutionConfig(executionConfig);

        return jobGraph;
    }

    private JobGraph createJobGraph(int parallelism) {
        final JobVertex sender = new JobVertex("Sender");
        sender.setParallelism(parallelism);
        sender.setInvokableClass(TestingAbstractInvokables.Sender.class);

        final JobVertex receiver = new JobVertex("Blocking receiver");
        receiver.setParallelism(parallelism);
        receiver.setInvokableClass(BlockingOperator.class);
        BlockingOperator.reset();

        receiver.connectNewDataSetAsInput(
                sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        sender.setSlotSharingGroup(slotSharingGroup);
        receiver.setSlotSharingGroup(slotSharingGroup);

        return JobGraphTestUtils.streamingJobGraph(sender, receiver);
    }

    /** Blocking invokable which is controlled by a static field. */
    public static class BlockingOperator extends TestingAbstractInvokables.Receiver {
        private static CountDownLatch countDownLatch = new CountDownLatch(1);

        public BlockingOperator(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            countDownLatch.await();
            super.invoke();
        }

        public static void unblock() {
            countDownLatch.countDown();
        }

        public static void reset() {
            countDownLatch = new CountDownLatch(1);
        }
    }
}
