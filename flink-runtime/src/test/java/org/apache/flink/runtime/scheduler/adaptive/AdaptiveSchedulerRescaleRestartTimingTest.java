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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultAllocatedSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.ResourceCounter;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.streamingJobGraph;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.createSlotOffersForResourceRequirements;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test proving that {@link WaitingForResources}, driven by the real {@link
 * DefaultStateTransitionManager} (not a no-op test double), correctly gates a rescale-triggered
 * restart on genuinely free slots reaching the pre-restart target, and falls back once the
 * rescale resource-stabilization timeout elapses.
 *
 * <p>Kept in its own file, following the precedent set by {@link
 * AdaptiveSchedulerFreeSlotVertexParallelismTest} and other companion test files extracted out of
 * {@link AdaptiveSchedulerTest}.
 */
class AdaptiveSchedulerRescaleRestartTimingTest extends AdaptiveSchedulerTestBase {

    private static final int RETRY_INTERVAL_MILLIS = 20;
    private static final int RETRY_ATTEMPTS = 250;

    @Test
    void testWaitingForResourcesDoesNotTransitionUntilFreeSlotsReachRescaleTarget()
            throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        // long enough that the stabilization timeout cannot fire during this test.
        scheduler =
                prepareScheduler(jobGraph, declarativeSlotPool, Duration.ofSeconds(10)).build();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(2);

        // go straight to the restart-triggered WaitingForResources from the initial Created
        // state, the same way Restarting#goToSubsequentState does - never through the plain
        // submission path (startScheduling()), which would race a second, unrelated
        // WaitingForResources transition using the submission timeout config.
        final VertexParallelism restartTarget = vertexParallelism(2);
        runInMainThread(
                () ->
                        scheduler.goToWaitingForResources(
                                new StateTrackingMockExecutionGraph(), restartTarget));

        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);

        offerSlots(declarativeSlotPool, taskManagerGateway, 1);

        // only 1 of the 2 targeted slots is free: must not have shortcut to
        // CreatingExecutionGraph yet, even though 1 slot is already "sufficient" to run the job
        // at a lower parallelism.
        Thread.sleep(300);
        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);

        offerSlots(declarativeSlotPool, taskManagerGateway, 1);

        CommonTestUtils.waitUntilCondition(
                () -> scheduler.getState() instanceof CreatingExecutionGraph,
                RETRY_INTERVAL_MILLIS,
                RETRY_ATTEMPTS);
    }

    @Test
    void testWaitingForResourcesFallsBackAfterRescaleResourceStabilizationTimeoutElapses()
            throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        scheduler =
                prepareScheduler(jobGraph, declarativeSlotPool, Duration.ofMillis(300)).build();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(1);

        // go straight to the restart-triggered WaitingForResources from the initial Created
        // state, as in the test above - never through the plain submission path.
        final VertexParallelism restartTarget = vertexParallelism(2);
        runInMainThread(
                () ->
                        scheduler.goToWaitingForResources(
                                new StateTrackingMockExecutionGraph(), restartTarget));

        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);

        // only 1 of the 2 targeted slots is ever offered.
        offerSlots(declarativeSlotPool, taskManagerGateway, 1);

        // the target is never reached, but the stabilization timeout must still force the
        // transition once it elapses, rather than waiting forever.
        CommonTestUtils.waitUntilCondition(
                () -> scheduler.getState() instanceof CreatingExecutionGraph,
                RETRY_INTERVAL_MILLIS,
                RETRY_ATTEMPTS);
    }

    private VertexParallelism vertexParallelism(int parallelism) {
        return new VertexParallelism(Collections.singletonMap(JOB_VERTEX.getID(), parallelism));
    }

    private void offerSlots(
            DefaultDeclarativeSlotPool declarativeSlotPool,
            SubmissionBufferingTaskManagerGateway taskManagerGateway,
            int numSlots) {
        runInMainThread(
                () ->
                        declarativeSlotPool.offerSlots(
                                createSlotOffersForResourceRequirements(
                                        ResourceCounter.withResource(
                                                ResourceProfile.UNKNOWN, numSlots)),
                                new LocalTaskManagerLocation(),
                                taskManagerGateway,
                                System.currentTimeMillis()));
    }

    private static JobGraph createJobGraph() {
        return streamingJobGraph(JOB_VERTEX);
    }

    private static DefaultDeclarativeSlotPool createDeclarativeSlotPool(
            JobID jobId, ComponentMainThreadExecutor mainThreadExecutor) {
        return new DefaultDeclarativeSlotPool(
                jobId,
                new DefaultAllocatedSlotPool(),
                ignored -> {},
                DEFAULT_TIMEOUT,
                DEFAULT_TIMEOUT,
                Duration.ZERO,
                mainThreadExecutor);
    }

    private AdaptiveSchedulerBuilder prepareScheduler(
            JobGraph jobGraph,
            DefaultDeclarativeSlotPool declarativeSlotPool,
            Duration rescaleResourceStabilizationTimeout) {
        return new AdaptiveSchedulerBuilder(
                        jobGraph, singleThreadMainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                .setDeclarativeSlotPool(declarativeSlotPool)
                .setJobMasterConfiguration(
                        createConfiguration(rescaleResourceStabilizationTimeout));
    }

    private static Configuration createConfiguration(
            Duration rescaleResourceStabilizationTimeout) {
        return new Configuration()
                .set(
                        JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                        Duration.ofMillis(-1L))
                .set(
                        JobManagerOptions.SCHEDULER_RESCALE_RESOURCE_STABILIZATION_TIMEOUT,
                        rescaleResourceStabilizationTimeout)
                .set(
                        JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                        Duration.ofMillis(1L));
    }
}
