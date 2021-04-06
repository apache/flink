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
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotContext;
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
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.SystemClock;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link DeclarativeSlotPoolService}. */
public class DeclarativeSlotPoolServiceTest extends TestLogger {

    private static final JobID jobId = new JobID();
    private static final JobMasterId jobMasterId = JobMasterId.generate();
    private final ComponentMainThreadExecutor mainThreadExecutor =
            ComponentMainThreadExecutorServiceAdapter.forMainThread();
    private static final String address = "localhost";

    @Test
    public void testUnknownTaskManagerRegistration() throws Exception {
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService()) {
            final ResourceID unknownTaskManager = ResourceID.generate();

            assertFalse(
                    declarativeSlotPoolService.isTaskManagerRegistered(
                            unknownTaskManager.getResourceID()));
        }
    }

    @Test
    public void testKnownTaskManagerRegistration() throws Exception {
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService()) {
            final ResourceID knownTaskManager = ResourceID.generate();
            declarativeSlotPoolService.registerTaskManager(knownTaskManager);

            assertTrue(
                    declarativeSlotPoolService.isTaskManagerRegistered(
                            knownTaskManager.getResourceID()));
        }
    }

    @Test
    public void testReleaseTaskManager() throws Exception {
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService()) {
            final ResourceID knownTaskManager = ResourceID.generate();
            declarativeSlotPoolService.registerTaskManager(knownTaskManager);
            declarativeSlotPoolService.releaseTaskManager(
                    knownTaskManager, new FlinkException("Test cause"));

            assertFalse(
                    declarativeSlotPoolService.isTaskManagerRegistered(
                            knownTaskManager.getResourceID()));
        }
    }

    @Test
    public void testSlotOfferingOfUnknownTaskManagerIsIgnored() throws Exception {
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

            assertThat(acceptedSlots, is(empty()));
        }
    }

    @Test
    public void testSlotOfferingOfKnownTaskManager() throws Exception {
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

            assertThat(receivedSlotOffers.get(), is(slotOffers));
        }
    }

    @Test
    public void testConnectToResourceManagerDeclaresRequiredResources() throws Exception {
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

            assertThat(resourceRequirements.getResourceRequirements(), is(requiredResources));
            assertThat(resourceRequirements.getJobId(), is(jobId));
            assertThat(resourceRequirements.getTargetAddress(), is(address));
        }
    }

    @Test
    public void testCreateAllocatedSlotReport() throws Exception {
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

            assertThat(
                    allocatedSlotReport.getAllocatedSlotInfos(),
                    contains(matchesWithSlotContext(simpleSlotContext2)));
        }
    }

    @Test
    public void testFailAllocationReleasesSlot() throws Exception {
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

            assertThat(releasedSlot.join(), is(allocationId));
        }
    }

    @Test
    public void testFailLastAllocationOfTaskManagerReturnsIt() throws Exception {
        try (DeclarativeSlotPoolService declarativeSlotPoolService =
                createDeclarativeSlotPoolService()) {
            final ResourceID taskManagerId = ResourceID.generate();

            declarativeSlotPoolService.registerTaskManager(taskManagerId);
            final Optional<ResourceID> emptyTaskManager =
                    declarativeSlotPoolService.failAllocation(
                            taskManagerId, new AllocationID(), new FlinkException("Test cause"));

            assertThat(
                    emptyTaskManager.orElseThrow(
                            () -> new Exception("Expected empty task manager")),
                    is(taskManagerId));
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

    private Matcher<AllocatedSlotInfo> matchesWithSlotContext(SimpleSlotContext simpleSlotContext) {
        return new AllocatedSlotInfoMatcher(simpleSlotContext);
    }

    private static final class AllocatedSlotInfoMatcher extends TypeSafeMatcher<AllocatedSlotInfo> {

        private final SlotContext slotContext;

        private AllocatedSlotInfoMatcher(SlotContext slotContext) {
            this.slotContext = slotContext;
        }

        @Override
        protected boolean matchesSafely(AllocatedSlotInfo item) {
            return item.getAllocationId().equals(slotContext.getAllocationId())
                    && item.getSlotIndex() == slotContext.getPhysicalSlotNumber();
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText("expect allocated slot info with allocation id ")
                    .appendValue(slotContext.getAllocationId())
                    .appendText(" and slot index ")
                    .appendValue(slotContext.getPhysicalSlotNumber());
        }
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
