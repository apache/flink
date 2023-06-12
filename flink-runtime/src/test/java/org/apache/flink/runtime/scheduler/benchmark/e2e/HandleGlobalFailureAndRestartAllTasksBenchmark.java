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

package org.apache.flink.runtime.scheduler.benchmark.e2e;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.failover.flip1.FixedDelayRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolUtils;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.completeCancellingForAllVertices;
import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createAdaptiveBatchScheduler;

/**
 * The benchmark of handling global failure and restarting tasks in a STREAMING/BATCH job. The
 * related method is {@link DefaultScheduler#handleGlobalFailure}.
 */
public class HandleGlobalFailureAndRestartAllTasksBenchmark extends SchedulerEndToEndBenchmarkBase {
    private static final int SLOTS_PER_TASK_EXECUTOR = 4;
    private DefaultScheduler scheduler;
    private ManuallyTriggeredScheduledExecutor taskRestartExecutor;

    public void setup(JobConfiguration jobConfiguration) throws Exception {
        taskRestartExecutor = new ManuallyTriggeredScheduledExecutor();
        // Use DirectScheduledExecutorService to ensure that we can run
        // DefaultScheduler#restartTasks in the current thread synchronously when tasks restart is
        // triggered.
        scheduledExecutorService = new DirectScheduledExecutorService();

        super.setup(jobConfiguration);

        scheduler =
                createScheduler(
                        jobGraph,
                        physicalSlotProvider,
                        mainThreadExecutor,
                        scheduledExecutorService,
                        taskRestartExecutor,
                        new FixedDelayRestartBackoffTimeStrategy
                                        .FixedDelayRestartBackoffTimeStrategyFactory(1, 1)
                                .create());

        scheduler.startScheduling();
        offerSlots();
    }

    public void handleGlobalFailureAndRestartAllTasks() throws Exception {
        // trigger failover, force reset state to canceled.
        scheduler.handleGlobalFailure(new RuntimeException("For test."));
        completeCancellingForAllVertices(scheduler.getExecutionGraph());

        taskRestartExecutor.triggerScheduledTasks();
    }

    private DefaultScheduler createScheduler(
            JobGraph jobGraph,
            PhysicalSlotProvider physicalSlotProvider,
            ComponentMainThreadExecutor mainThreadExecutor,
            ScheduledExecutorService executorService,
            ScheduledExecutor taskRestartExecutor,
            RestartBackoffTimeStrategy restartBackoffTimeStrategy)
            throws Exception {
        DefaultSchedulerBuilder schedulerBuilder =
                new DefaultSchedulerBuilder(
                                jobGraph,
                                mainThreadExecutor,
                                executorService,
                                executorService,
                                taskRestartExecutor)
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                        physicalSlotProvider))
                        .setRestartBackoffTimeStrategy(restartBackoffTimeStrategy);
        if (jobGraph.getJobType() == JobType.BATCH) {
            return createAdaptiveBatchScheduler(schedulerBuilder, jobConfiguration);
        } else {
            return schedulerBuilder.build();
        }
    }

    private void offerSlots() {
        final int numberSlots =
                StreamSupport.stream(jobGraph.getVertices().spliterator(), false)
                        .mapToInt(JobVertex::getParallelism)
                        .sum();

        for (int i = 0; i < Math.ceil((double) numberSlots / SLOTS_PER_TASK_EXECUTOR); i++) {
            SlotPoolUtils.tryOfferSlots(
                    slotPool,
                    mainThreadExecutor,
                    Collections.nCopies(SLOTS_PER_TASK_EXECUTOR, ResourceProfile.ANY));
        }
    }
}
