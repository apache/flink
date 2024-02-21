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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.clock.SystemClock;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DeclarativeSlotPoolService}. */
class DeclarativeSlotPoolServiceTest {

    private static final JobID jobId = new JobID();
    private static final JobMasterId jobMasterId = JobMasterId.generate();
    private final ComponentMainThreadExecutor mainThreadExecutor =
            ComponentMainThreadExecutorServiceAdapter.forMainThread();
    private static final String address = "localhost";

    @Test
    void testUnknownTaskManagerRegistration() throws Exception {
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService()) {
            final ResourceID unknownTaskManager = ResourceID.generate();

            assertThat(
                            declarativeSlotPoolService.isTaskManagerRegistered(
                                    unknownTaskManager.getResourceID()))
                    .isFalse();
        }
    }

    @Test
    void testKnownTaskManagerRegistration() throws Exception {
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService()) {
            final ResourceID knownTaskManager = ResourceID.generate();
            declarativeSlotPoolService.registerTaskManager(knownTaskManager);

            assertThat(
                            declarativeSlotPoolService.isTaskManagerRegistered(
                                    knownTaskManager.getResourceID()))
                    .isTrue();
        }
    }

    @Test
    void testReleaseTaskManager() throws Exception {
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService()) {
            final ResourceID knownTaskManager = ResourceID.generate();
            declarativeSlotPoolService.registerTaskManager(knownTaskManager);
            declarativeSlotPoolService.releaseTaskManager(
                    knownTaskManager, new FlinkException("Test cause"));

            assertThat(
                            declarativeSlotPoolService.isTaskManagerRegistered(
                                    knownTaskManager.getResourceID()))
                    .isFalse();
        }
    }

    @Test
    void testSlotOfferingOfUnknownTaskManagerIsIgnored() throws Exception {
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService()) {
            final Collection<SlotOffer> slotOffers =
                    Collections.singletonList(
                            new SlotOffer(new AllocationID(), 0, ResourceProfile.UNKNOWN));

            final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();

            final Collection<SlotOffer> acceptedSlots =
                    declarativeSlotPoolService.offerSlots(
                            taskManagerLocation,
                            new RpcTaskManagerGateway(
                                    new TestingTaskExecutorGatewayBuilder()
                                            .createTestingTaskExecutorGateway(),
                                    jobMasterId),
                            slotOffers);

            assertThat(acceptedSlots).isEmpty();
        }
    }

    @Test
    void testSlotOfferingOfKnownTaskManager() throws Exception {
        final AtomicReference<Collection<? extends SlotOffer>> receivedSlotOffers =
                new AtomicReference<>();
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService(
                        new TestingDeclarativeSlotPoolFactory(
                                new TestingDeclarativeSlotPoolBuilder()
                                        .setOfferSlotsFunction(
                                                (slotOffers,
                                                        taskManagerLocation,
                                                        taskManagerGateway,
                                                        aLong) -> {
                                                    receivedSlotOffers.set(slotOffers);
                                                    return new ArrayList<>(slotOffers);
                                                })))) {
            final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();

            declarativeSlotPoolService.registerTaskManager(taskManagerLocation.getResourceID());

            final Collection<SlotOffer> slotOffers =
                    Collections.singletonList(
                            new SlotOffer(new AllocationID(), 0, ResourceProfile.UNKNOWN));

            declarativeSlotPoolService.offerSlots(
                    taskManagerLocation,
                    new RpcTaskManagerGateway(
                            new TestingTaskExecutorGatewayBuilder()
                                    .createTestingTaskExecutorGateway(),
                            jobMasterId),
                    slotOffers);

            assertThat(receivedSlotOffers.get()).isEqualTo(slotOffers);
        }
    }

    @Test
    void testConnectToResourceManagerDeclaresRequiredResources() throws Exception {
        final Collection<ResourceRequirement> requiredResources =
                Arrays.asList(
                        ResourceRequirement.create(ResourceProfile.UNKNOWN, 2),
                        ResourceRequirement.create(ResourceProfile.ZERO, 4));

        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService(
                        new TestingDeclarativeSlotPoolFactory(
                                new TestingDeclarativeSlotPoolBuilder()
                                        .setGetResourceRequirementsSupplier(
                                                () -> requiredResources)))) {
            final TestingResourceManagerGateway resourceManagerGateway =
                    new TestingResourceManagerGateway();

            final CompletableFuture<ResourceRequirements> declaredResourceRequirements =
                    new CompletableFuture<>();

            resourceManagerGateway.setDeclareRequiredResourcesFunction(
                    (jobMasterId, resourceRequirements) -> {
                        declaredResourceRequirements.complete(resourceRequirements);
                        return CompletableFuture.completedFuture(Acknowledge.get());
                    });

            declarativeSlotPoolService.connectToResourceManager(resourceManagerGateway);

            final ResourceRequirements resourceRequirements = declaredResourceRequirements.join();

            assertThat(resourceRequirements.getResourceRequirements()).isEqualTo(requiredResources);
            assertThat(resourceRequirements.getJobId()).isEqualTo(jobId);
            assertThat(resourceRequirements.getTargetAddress()).isEqualTo(address);
        }
    }

    @Test
    void testCreateAllocatedSlotReport() throws Exception {
        final LocalTaskManagerLocation taskManagerLocation1 = new LocalTaskManagerLocation();
        final LocalTaskManagerLocation taskManagerLocation2 = new LocalTaskManagerLocation();
        final SimpleSlotContext simpleSlotContext2 = createSimpleSlotContext(taskManagerLocation2);
        final Collection<SlotInfo> slotInfos =
                Arrays.asList(createSimpleSlotContext(taskManagerLocation1), simpleSlotContext2);
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService(
                        new TestingDeclarativeSlotPoolFactory(
                                new TestingDeclarativeSlotPoolBuilder()
                                        .setGetAllSlotsInformationSupplier(() -> slotInfos)))) {

            final AllocatedSlotReport allocatedSlotReport =
                    declarativeSlotPoolService.createAllocatedSlotReport(
                            taskManagerLocation2.getResourceID());

            assertThat(allocatedSlotReport.getAllocatedSlotInfos())
                    .allMatch(
                            context ->
                                    context.getAllocationId()
                                                    .equals(simpleSlotContext2.getAllocationId())
                                            && context.getSlotIndex()
                                                    == simpleSlotContext2.getPhysicalSlotNumber());
        }
    }

    @Test
    void testFailAllocationReleasesSlot() throws Exception {
        final CompletableFuture<AllocationID> releasedSlot = new CompletableFuture<>();
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService(
                        new TestingDeclarativeSlotPoolFactory(
                                new TestingDeclarativeSlotPoolBuilder()
                                        .setReleaseSlotFunction(
                                                (allocationID, exception) -> {
                                                    releasedSlot.complete(allocationID);
                                                    return ResourceCounter.empty();
                                                })))) {
            final ResourceID taskManagerId = ResourceID.generate();
            final AllocationID allocationId = new AllocationID();

            declarativeSlotPoolService.registerTaskManager(taskManagerId);

            declarativeSlotPoolService.failAllocation(
                    taskManagerId, allocationId, new FlinkException("Test cause"));

            assertThat(releasedSlot.join()).isEqualTo(allocationId);
        }
    }

    @Test
    void testFailLastAllocationOfTaskManagerReturnsIt() throws Exception {
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService()) {
            final ResourceID taskManagerId = ResourceID.generate();

            declarativeSlotPoolService.registerTaskManager(taskManagerId);
            final Optional<ResourceID> emptyTaskManager =
                    declarativeSlotPoolService.failAllocation(
                            taskManagerId, new AllocationID(), new FlinkException("Test cause"));

            assertThat(
                            emptyTaskManager.orElseThrow(
                                    () -> new Exception("Expected empty task manager")))
                    .isEqualTo(taskManagerId);
        }
    }

    @Test
    void testCloseReleasesAllSlotsForAllRegisteredTaskManagers() throws Exception {
        final Queue<ResourceID> releasedSlotsFor = new ArrayDeque<>(2);
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService(
                        new TestingDeclarativeSlotPoolFactory(
                                new TestingDeclarativeSlotPoolBuilder()
                                        .setReleaseSlotsFunction(
                                                (resourceID, e) -> {
                                                    releasedSlotsFor.offer(resourceID);
                                                    return ResourceCounter.empty();
                                                })))) {

            final List<ResourceID> taskManagerResourceIds =
                    Arrays.asList(
                            ResourceID.generate(), ResourceID.generate(), ResourceID.generate());

            for (ResourceID taskManagerResourceId : taskManagerResourceIds) {
                declarativeSlotPoolService.registerTaskManager(taskManagerResourceId);
            }

            declarativeSlotPoolService.close();

            assertThat(releasedSlotsFor)
                    .containsExactlyInAnyOrderElementsOf(taskManagerResourceIds);
        }
    }

    @Test
    void testReleaseFreeSlotsOnTaskManager() throws Exception {
        try (DeclarativeSlotPoolService slotPoolService = createDeclarativeSlotPoolService()) {
            final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
            slotPoolService.registerTaskManager(taskManagerLocation.getResourceID());

            final ResourceProfile resourceProfile =
                    ResourceProfile.newBuilder().setCpuCores(1).build();

            SlotOffer slotOffer1 = new SlotOffer(new AllocationID(), 0, resourceProfile);
            SlotOffer slotOffer2 = new SlotOffer(new AllocationID(), 1, resourceProfile);

            final DeclarativeSlotPool slotPool = slotPoolService.getDeclarativeSlotPool();
            slotPool.setResourceRequirements(ResourceCounter.withResource(resourceProfile, 2));

            final DefaultDeclarativeSlotPoolTest.FreeSlotConsumer freeSlotConsumer =
                    new DefaultDeclarativeSlotPoolTest.FreeSlotConsumer();

            final Collection<SlotOffer> slotOffers = Arrays.asList(slotOffer1, slotOffer2);

            slotPoolService.offerSlots(
                    taskManagerLocation,
                    new RpcTaskManagerGateway(
                            new TestingTaskExecutorGatewayBuilder()
                                    .setFreeSlotFunction(freeSlotConsumer)
                                    .createTestingTaskExecutorGateway(),
                            jobMasterId),
                    slotOffers);

            // slot1 is reserved, slot2 is free.
            slotPool.reserveFreeSlot(slotOffer1.getAllocationId(), resourceProfile);

            slotPoolService.releaseFreeSlotsOnTaskManager(
                    taskManagerLocation.getResourceID(), new FlinkException("Test cause"));

            assertThat(slotPool.getFreeSlotInfoTracker().getAvailableSlots()).isEmpty();
            assertThat(
                            Iterables.getOnlyElement(slotPool.getAllSlotsInformation())
                                    .getAllocationId())
                    .isEqualTo(slotOffer1.getAllocationId());
            assertThat(Iterables.getOnlyElement(freeSlotConsumer.drainFreedSlots()))
                    .isEqualTo(slotOffer2.getAllocationId());
        }
    }

    private DeclarativeSlotPoolService createDeclarativeSlotPoolService() throws Exception {
        return createDeclarativeSlotPoolService(new DefaultDeclarativeSlotPoolFactory());
    }

    private DeclarativeSlotPoolService createDeclarativeSlotPoolService(
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory) throws Exception {
        final DeclarativeSlotPoolService declarativeSlotPoolService =
                new DeclarativeSlotPoolService(
                        jobId,
                        declarativeSlotPoolFactory,
                        SystemClock.getInstance(),
                        Time.seconds(20L),
                        Time.seconds(20L));

        declarativeSlotPoolService.start(jobMasterId, address, mainThreadExecutor);

        return declarativeSlotPoolService;
    }

    @Nonnull
    private SimpleSlotContext createSimpleSlotContext(
            LocalTaskManagerLocation taskManagerLocation1) {
        return new SimpleSlotContext(
                new AllocationID(),
                taskManagerLocation1,
                0,
                new RpcTaskManagerGateway(
                        new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway(),
                        jobMasterId));
    }
}
