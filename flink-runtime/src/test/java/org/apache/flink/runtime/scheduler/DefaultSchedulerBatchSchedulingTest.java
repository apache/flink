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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolBridgeBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProviderImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolUtils;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.function.CheckedSupplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.fail;

/** Tests base for the scheduling of batch jobs. */
class DefaultSchedulerBatchSchedulingTest {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private static ScheduledExecutorService singleThreadScheduledExecutorService;
    private static ComponentMainThreadExecutor mainThreadExecutor;

    @BeforeAll
    private static void setupClass() {
        singleThreadScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        singleThreadScheduledExecutorService);
    }

    @AfterAll
    private static void teardownClass() {
        if (singleThreadScheduledExecutorService != null) {
            singleThreadScheduledExecutorService.shutdownNow();
        }
    }

    /**
     * Tests that a batch job can be executed with fewer slots than its parallelism. See FLINK-13187
     * for more information.
     */
    @Test
    void testSchedulingOfJobWithFewerSlotsThanParallelism() throws Exception {
        final int parallelism = 5;
        final Time batchSlotTimeout = Time.milliseconds(5L);
        final JobGraph jobGraph = createBatchJobGraph(parallelism);

        try (final SlotPool slotPool = createSlotPool(mainThreadExecutor, batchSlotTimeout)) {
            final ArrayBlockingQueue<ExecutionAttemptID> submittedTasksQueue =
                    new ArrayBlockingQueue<>(parallelism);
            TestingTaskExecutorGateway testingTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setSubmitTaskConsumer(
                                    (tdd, ignored) -> {
                                        submittedTasksQueue.offer(tdd.getExecutionAttemptId());
                                        return CompletableFuture.completedFuture(Acknowledge.get());
                                    })
                            .createTestingTaskExecutorGateway();

            final PhysicalSlotProvider slotProvider =
                    new PhysicalSlotProviderImpl(
                            LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
            final GloballyTerminalJobStatusListener jobStatusListener =
                    new GloballyTerminalJobStatusListener();
            final SchedulerNG scheduler =
                    createScheduler(
                            jobGraph,
                            mainThreadExecutor,
                            slotProvider,
                            batchSlotTimeout,
                            jobStatusListener);

            CompletableFuture.runAsync(scheduler::startScheduling, mainThreadExecutor).join();

            // register a single slot at the slot pool
            SlotPoolUtils.offerSlots(
                    slotPool,
                    mainThreadExecutor,
                    Collections.singletonList(ResourceProfile.ANY),
                    new RpcTaskManagerGateway(testingTaskExecutorGateway, JobMasterId.generate()));

            // wait until the batch slot timeout has been reached
            Thread.sleep(batchSlotTimeout.toMilliseconds());

            final CompletableFuture<JobStatus> terminationFuture =
                    jobStatusListener.getTerminationFuture();

            for (int i = 0; i < parallelism; i++) {
                final CompletableFuture<ExecutionAttemptID> submittedTaskFuture =
                        CompletableFuture.supplyAsync(
                                CheckedSupplier.unchecked(submittedTasksQueue::take));

                // wait until one of them is completed
                CompletableFuture.anyOf(submittedTaskFuture, terminationFuture).join();

                if (submittedTaskFuture.isDone()) {
                    finishExecution(submittedTaskFuture.get(), scheduler, mainThreadExecutor);
                } else {
                    fail(
                            String.format(
                                    "Job reached a globally terminal state %s before all executions were finished.",
                                    terminationFuture.get()));
                }
            }

            assertThatFuture(terminationFuture).isCompletedWithValue(JobStatus.FINISHED);
        }
    }

    private void finishExecution(
            ExecutionAttemptID executionAttemptId,
            SchedulerNG scheduler,
            ComponentMainThreadExecutor mainThreadExecutor) {
        CompletableFuture.runAsync(
                        () -> {
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(
                                            executionAttemptId, ExecutionState.INITIALIZING));
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(
                                            executionAttemptId, ExecutionState.RUNNING));
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(
                                            executionAttemptId, ExecutionState.FINISHED));
                        },
                        mainThreadExecutor)
                .join();
    }

    private SlotPool createSlotPool(
            ComponentMainThreadExecutor mainThreadExecutor, Time batchSlotTimeout)
            throws Exception {
        return new DeclarativeSlotPoolBridgeBuilder()
                .setBatchSlotTimeout(batchSlotTimeout)
                .buildAndStart(mainThreadExecutor);
    }

    private JobGraph createBatchJobGraph(int parallelism) {
        final JobVertex jobVertex = new JobVertex("testing task");
        jobVertex.setParallelism(parallelism);
        jobVertex.setInvokableClass(NoOpInvokable.class);
        return JobGraphTestUtils.batchJobGraph(jobVertex);
    }

    private SchedulerNG createScheduler(
            JobGraph jobGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            PhysicalSlotProvider physicalSlotProvider,
            Time slotRequestTimeout,
            JobStatusListener jobStatusListener)
            throws Exception {
        return new DefaultSchedulerBuilder(
                        jobGraph, mainThreadExecutor, EXECUTOR_EXTENSION.getExecutor())
                .setExecutionSlotAllocatorFactory(
                        SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                physicalSlotProvider, slotRequestTimeout))
                .setJobStatusListener(jobStatusListener)
                .build();
    }
}
