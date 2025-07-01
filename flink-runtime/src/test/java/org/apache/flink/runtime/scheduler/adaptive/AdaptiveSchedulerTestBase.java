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
 *
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredComponentMainThreadExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.TestingDeclarativeSlotPoolBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.TestingFreeSlotTracker;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TestingSlot;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.assertj.core.api.Assertions.assertThat;

public class AdaptiveSchedulerTestBase {
    protected static final Duration DEFAULT_TIMEOUT = Duration.ofHours(1);
    protected static final int PARALLELISM = 4;
    protected static final JobVertex JOB_VERTEX = createNoOpVertex("v1", PARALLELISM);

    @RegisterExtension
    protected static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @RegisterExtension
    protected static final TestExecutorExtension<ScheduledExecutorService> TEST_EXECUTOR_RESOURCE =
            new TestExecutorExtension<>(Executors::newSingleThreadScheduledExecutor);

    protected final ManuallyTriggeredComponentMainThreadExecutor mainThreadExecutor =
            new ManuallyTriggeredComponentMainThreadExecutor(Thread.currentThread());

    protected final ComponentMainThreadExecutor singleThreadMainThreadExecutor =
            ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                    TEST_EXECUTOR_RESOURCE.getExecutor());

    protected final ClassLoader classLoader = ClassLoader.getSystemClassLoader();

    protected AdaptiveScheduler scheduler;

    @BeforeEach
    void before() {
        scheduler = null;
    }

    @AfterEach
    void after() {
        closeInExecutorService(scheduler, singleThreadMainThreadExecutor);
    }

    protected static JobGraph createJobGraphWithCheckpointing(final JobVertex... jobVertex) {
        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertices(Arrays.asList(jobVertex))
                        .build();
        SchedulerTestingUtils.enableCheckpointing(
                jobGraph, null, null, Duration.ofHours(1).toMillis(), true);
        return jobGraph;
    }

    protected static void closeInExecutorService(
            @Nullable AdaptiveScheduler scheduler, Executor executor) {
        if (scheduler != null) {
            final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
            executor.execute(
                    () -> {
                        try {
                            scheduler.cancel();

                            FutureUtils.forward(scheduler.closeAsync(), closeFuture);
                        } catch (Throwable t) {
                            closeFuture.completeExceptionally(t);
                        }
                    });

            // we have to wait for the job termination outside the main thread because the
            // cancellation tasks are scheduled on the main thread as well.
            scheduler
                    .getJobTerminationFuture()
                    .whenCompleteAsync(
                            (jobStatus, error) -> {
                                assertThat(scheduler.getState().getClass())
                                        .isEqualTo(Finished.class);

                                if (error != null) {
                                    closeFuture.completeExceptionally(error);
                                } else {
                                    try {
                                        FutureUtils.forward(scheduler.closeAsync(), closeFuture);
                                    } catch (Throwable t) {
                                        closeFuture.completeExceptionally(t);
                                    }
                                }
                            },
                            executor);
            assertThatFuture(closeFuture).eventuallySucceeds();
        }
    }

    /**
     * Creates a testing SlotPool instance that would allow for the scheduler to transition to
     * Executing state.
     */
    protected static DeclarativeSlotPool getSlotPoolWithFreeSlots(int freeSlots) {
        return new TestingDeclarativeSlotPoolBuilder()
                .setContainsFreeSlotFunction(allocationID -> true)
                .setReserveFreeSlotFunction(
                        (allocationId, resourceProfile) ->
                                TestingPhysicalSlot.builder()
                                        .withAllocationID(allocationId)
                                        .build())
                .setGetFreeSlotTrackerSupplier(
                        () ->
                                TestingFreeSlotTracker.newBuilder()
                                        .setGetFreeSlotsInformationSupplier(
                                                () -> TestingSlot.getSlots(freeSlots))
                                        .build())
                .build();
    }

    protected void startTestInstanceInMainThread() {
        runInMainThread(() -> scheduler.startScheduling());
    }

    protected void runInMainThread(final Runnable callback) {
        CompletableFuture.runAsync(callback, singleThreadMainThreadExecutor).join();
    }

    protected <T> T supplyInMainThread(final Supplier<T> supplier) throws Exception {
        return CompletableFuture.supplyAsync(supplier, singleThreadMainThreadExecutor).get();
    }
}
