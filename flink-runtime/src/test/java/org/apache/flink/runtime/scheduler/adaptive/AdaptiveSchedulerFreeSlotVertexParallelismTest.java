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
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.streamingJobGraph;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.createSlotOffersForResourceRequirements;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test proving that {@link AdaptiveScheduler#getAvailableVertexParallelism()} (which
 * over-counts slots still reserved by a running task) and {@link
 * AdaptiveScheduler#getFreeSlotVertexParallelism()} (which only counts genuinely free slots)
 * diverge when a slot is reserved-but-not-yet-freed.
 *
 * <p>This is kept in its own file rather than {@link AdaptiveSchedulerTest} to avoid growing that
 * file past the checkstyle {@code FileLength} limit, following the precedent set by extracting
 * {@code LocalRecoveryTest} and other companion test files out of {@code AdaptiveSchedulerTest}.
 */
class AdaptiveSchedulerFreeSlotVertexParallelismTest extends AdaptiveSchedulerTestBase {

    @Test
    void testFreeSlotVertexParallelismExcludesReservedSlots() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        scheduler = prepareSchedulerWithNoTimeouts(jobGraph, declarativeSlotPool).build();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(1);

        startTestInstanceInMainThread();

        // one slot is offered and immediately claimed by the running (parallelism-1) job.
        runInMainThread(
                () ->
                        declarativeSlotPool.offerSlots(
                                createSlotOffersForResourceRequirements(
                                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)),
                                new LocalTaskManagerLocation(),
                                taskManagerGateway,
                                System.currentTimeMillis()));
        taskManagerGateway.waitForSubmissions(1);

        // a second slot joins, but stays genuinely free: nothing has requested it yet.
        runInMainThread(
                () ->
                        declarativeSlotPool.offerSlots(
                                createSlotOffersForResourceRequirements(
                                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)),
                                new LocalTaskManagerLocation(),
                                taskManagerGateway,
                                System.currentTimeMillis()));

        runInMainThread(
                () -> {
                    // over-counted view: 1 reserved (in use by the running task) + 1 free = 2.
                    // This is correct for Executing's use (predicting a restart's target).
                    assertThat(scheduler.getAvailableVertexParallelism())
                            .hasValueSatisfying(
                                    parallelism ->
                                            assertThat(
                                                            parallelism.getParallelism(
                                                                    JOB_VERTEX.getID()))
                                                    .isEqualTo(2));

                    // free-slots-based view: only the genuinely free slot counts = 1.
                    // This is what a restart can *actually* reserve right now.
                    assertThat(scheduler.getFreeSlotVertexParallelism())
                            .hasValueSatisfying(
                                    parallelism ->
                                            assertThat(
                                                            parallelism.getParallelism(
                                                                    JOB_VERTEX.getID()))
                                                    .isEqualTo(1));
                });
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

    private AdaptiveSchedulerBuilder prepareSchedulerWithNoTimeouts(
            JobGraph jobGraph, DefaultDeclarativeSlotPool declarativeSlotPool) {
        return new AdaptiveSchedulerBuilder(
                        jobGraph, singleThreadMainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                .setDeclarativeSlotPool(declarativeSlotPool)
                .setJobMasterConfiguration(createConfigurationWithNoTimeouts());
    }

    private static Configuration createConfigurationWithNoTimeouts() {
        return new Configuration()
                .set(
                        JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                        Duration.ofMillis(-1L))
                .set(
                        JobManagerOptions.SCHEDULER_RESCALE_RESOURCE_STABILIZATION_TIMEOUT,
                        Duration.ZERO)
                .set(
                        JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                        Duration.ofMillis(1L));
    }
}
