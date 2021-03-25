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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProviderImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createDefaultJobVertices;
import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createJobGraph;

/**
 * The base class of benchmarks related to {@link DefaultScheduler}'s creation, scheduling and
 * deploying.
 */
public class SchedulerBenchmarkBase {

    ScheduledExecutorService scheduledExecutorService;
    ComponentMainThreadExecutor mainThreadExecutor;

    JobGraph jobGraph;
    PhysicalSlotProvider physicalSlotProvider;

    public void setup(JobConfiguration jobConfiguration) throws Exception {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        scheduledExecutorService);

        final List<JobVertex> jobVertices = createDefaultJobVertices(jobConfiguration);
        jobGraph = createJobGraph(jobVertices, jobConfiguration);

        physicalSlotProvider =
                createPhysicalSlotProvider(
                        jobConfiguration, jobVertices.size(), mainThreadExecutor);
    }

    public void teardown() throws Exception {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    private static PhysicalSlotProvider createPhysicalSlotProvider(
            JobConfiguration jobConfiguration,
            int numberOfJobVertices,
            ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception {
        final int slotPoolSize = jobConfiguration.getParallelism() * numberOfJobVertices;

        final SlotPoolImpl slotPool = new SlotPoolBuilder(mainThreadExecutor).build();
        final TestingTaskExecutorGateway testingTaskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        offerSlots(
                slotPool,
                new RpcTaskManagerGateway(testingTaskExecutorGateway, JobMasterId.generate()),
                slotPoolSize,
                mainThreadExecutor);

        return new PhysicalSlotProviderImpl(
                LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
    }

    private static void offerSlots(
            SlotPoolImpl slotPool,
            TaskManagerGateway taskManagerGateway,
            int slotPoolSize,
            ComponentMainThreadExecutor mainThreadExecutor) {
        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        CompletableFuture.runAsync(
                        () -> {
                            slotPool.registerTaskManager(taskManagerLocation.getResourceID());
                            final Collection<SlotOffer> slotOffers =
                                    IntStream.range(0, slotPoolSize)
                                            .mapToObj(
                                                    i ->
                                                            new SlotOffer(
                                                                    new AllocationID(),
                                                                    i,
                                                                    ResourceProfile.ANY))
                                            .collect(Collectors.toList());
                            slotPool.offerSlots(
                                    taskManagerLocation, taskManagerGateway, slotOffers);
                        },
                        mainThreadExecutor)
                .join();
    }

    static DefaultScheduler createScheduler(
            JobGraph jobGraph,
            PhysicalSlotProvider physicalSlotProvider,
            ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception {
        return SchedulerTestingUtils.newSchedulerBuilder(jobGraph, mainThreadExecutor)
                .setExecutionSlotAllocatorFactory(
                        SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                physicalSlotProvider))
                .build();
    }
}
