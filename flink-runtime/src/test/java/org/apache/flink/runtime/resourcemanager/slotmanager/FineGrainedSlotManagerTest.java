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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests of {@link FineGrainedSlotManager}. */
public class FineGrainedSlotManagerTest extends FineGrainedSlotManagerTestBase {

    private static final WorkerResourceSpec DEFAULT_WORKER_RESOURCE_SPEC =
            new WorkerResourceSpec.Builder()
                    .setCpuCores(10.0)
                    .setTaskHeapMemoryMB(1000)
                    .setTaskOffHeapMemoryMB(1000)
                    .setNetworkMemoryMB(1000)
                    .setManagedMemoryMB(1000)
                    .build();
    private static final WorkerResourceSpec LARGE_WORKER_RESOURCE_SPEC =
            new WorkerResourceSpec.Builder()
                    .setCpuCores(100.0)
                    .setTaskHeapMemoryMB(10000)
                    .setTaskOffHeapMemoryMB(10000)
                    .setNetworkMemoryMB(10000)
                    .setManagedMemoryMB(10000)
                    .build();
    private static final int DEFAULT_NUM_SLOTS_PER_WORKER = 2;
    private static final ResourceProfile DEFAULT_TOTAL_RESOURCE_PROFILE =
            SlotManagerUtils.generateTaskManagerTotalResourceProfile(DEFAULT_WORKER_RESOURCE_SPEC);
    private static final ResourceProfile DEFAULT_SLOT_RESOURCE_PROFILE =
            SlotManagerUtils.generateDefaultSlotResourceProfile(
                    DEFAULT_WORKER_RESOURCE_SPEC, DEFAULT_NUM_SLOTS_PER_WORKER);
    private static final ResourceProfile LARGE_TOTAL_RESOURCE_PROFILE =
            SlotManagerUtils.generateTaskManagerTotalResourceProfile(LARGE_WORKER_RESOURCE_SPEC);
    private static final ResourceProfile LARGE_SLOT_RESOURCE_PROFILE =
            SlotManagerUtils.generateDefaultSlotResourceProfile(
                    LARGE_WORKER_RESOURCE_SPEC, DEFAULT_NUM_SLOTS_PER_WORKER);

    @Override
    protected ResourceProfile getDefaultTaskManagerResourceProfile() {
        return DEFAULT_TOTAL_RESOURCE_PROFILE;
    }

    @Override
    protected ResourceProfile getDefaultSlotResourceProfile() {
        return DEFAULT_SLOT_RESOURCE_PROFILE;
    }

    @Override
    protected int getDefaultNumberSlotsPerWorker() {
        return DEFAULT_NUM_SLOTS_PER_WORKER;
    }

    @Override
    protected ResourceProfile getLargeTaskManagerResourceProfile() {
        return LARGE_TOTAL_RESOURCE_PROFILE;
    }

    @Override
    protected ResourceProfile getLargeSlotResourceProfile() {
        return LARGE_SLOT_RESOURCE_PROFILE;
    }

    @Override
    protected Optional<ResourceAllocationStrategy> getResourceAllocationStrategy() {
        return Optional.empty();
    }

    // ---------------------------------------------------------------------------------------------
    // Initialize and close
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testInitializeAndClose() throws Exception {
        new Context() {
            {
                runTest(() -> getSlotManager().close());
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Register / unregister TaskManager and and slot status reconciliation
    // ---------------------------------------------------------------------------------------------

    /** Tests that we can register task manager at the slot manager. */
    @Test
    public void testTaskManagerRegistration() throws Exception {
        final TaskExecutorConnection taskManagerConnection = createTaskExecutorConnection();
        new Context() {
            {
                runTest(
                        () -> {
                            getSlotManager()
                                    .registerTaskManager(
                                            taskManagerConnection,
                                            new SlotReport(),
                                            getDefaultTaskManagerResourceProfile(),
                                            getDefaultSlotResourceProfile());

                            assertThat(
                                    getSlotManager().getNumberRegisteredSlots(),
                                    equalTo(getDefaultNumberSlotsPerWorker()));
                            assertThat(
                                    getTaskManagerTracker().getRegisteredTaskManagers().size(),
                                    equalTo(1));
                            assertTrue(
                                    getTaskManagerTracker()
                                            .getRegisteredTaskManager(
                                                    taskManagerConnection.getInstanceID())
                                            .isPresent());
                            assertThat(
                                    getTaskManagerTracker()
                                            .getRegisteredTaskManager(
                                                    taskManagerConnection.getInstanceID())
                                            .get()
                                            .getAvailableResource(),
                                    equalTo(getDefaultTaskManagerResourceProfile()));
                            assertThat(
                                    getTaskManagerTracker()
                                            .getRegisteredTaskManager(
                                                    taskManagerConnection.getInstanceID())
                                            .get()
                                            .getTotalResource(),
                                    equalTo(getDefaultTaskManagerResourceProfile()));
                        });
            }
        };
    }

    /** Tests that un-registration of task managers will free and remove all allocated slots. */
    @Test
    public void testTaskManagerUnregistration() throws Exception {
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(tuple6 -> new CompletableFuture<>())
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        final AllocationID allocationId = new AllocationID();
        final SlotReport slotReport =
                new SlotReport(
                        createAllocatedSlotStatus(allocationId, getDefaultSlotResourceProfile()));
        new Context() {
            {
                runTest(
                        () -> {
                            getSlotManager()
                                    .registerTaskManager(
                                            taskManagerConnection,
                                            slotReport,
                                            getDefaultTaskManagerResourceProfile(),
                                            getDefaultSlotResourceProfile());
                            assertThat(
                                    getTaskManagerTracker().getRegisteredTaskManagers().size(),
                                    is(1));

                            final Optional<TaskManagerSlotInformation> slot =
                                    getTaskManagerTracker().getAllocatedOrPendingSlot(allocationId);
                            assertTrue(slot.isPresent());
                            assertTrue(slot.get().getState() == SlotState.ALLOCATED);

                            getSlotManager()
                                    .unregisterTaskManager(
                                            taskManagerConnection.getInstanceID(), TEST_EXCEPTION);
                            assertThat(
                                    getTaskManagerTracker().getRegisteredTaskManagers(),
                                    is(empty()));
                            assertFalse(
                                    getTaskManagerTracker()
                                            .getAllocatedOrPendingSlot(allocationId)
                                            .isPresent());
                        });
            }
        };
    }

    /** Tests that we can matched task manager will deduct pending task manager. */
    @Test
    public void testTaskManagerRegistrationDeductPendingTaskManager() throws Exception {
        final TaskExecutorConnection taskExecutionConnection1 = createTaskExecutorConnection();
        final TaskExecutorConnection taskExecutionConnection2 = createTaskExecutorConnection();
        final TaskExecutorConnection taskExecutionConnection3 = createTaskExecutorConnection();
        final SlotReport slotReportWithAllocatedSlot =
                new SlotReport(
                        createAllocatedSlotStatus(
                                new AllocationID(), getDefaultSlotResourceProfile()));
        new Context() {
            {
                runTest(
                        () -> {
                            getTaskManagerTracker()
                                    .addPendingTaskManager(
                                            new PendingTaskManager(
                                                    PendingTaskManagerId.generate(),
                                                    getDefaultTaskManagerResourceProfile(),
                                                    getDefaultSlotResourceProfile()));
                            // task manager with allocated slot cannot deduct pending task manager
                            getSlotManager()
                                    .registerTaskManager(
                                            taskExecutionConnection1,
                                            slotReportWithAllocatedSlot,
                                            getDefaultTaskManagerResourceProfile(),
                                            getDefaultSlotResourceProfile());
                            assertThat(
                                    getTaskManagerTracker().getPendingTaskManagers().size(), is(1));
                            // task manager with mismatched resource cannot deduct pending task
                            // manager
                            getSlotManager()
                                    .registerTaskManager(
                                            taskExecutionConnection2,
                                            new SlotReport(),
                                            getLargeTaskManagerResourceProfile(),
                                            getLargeSlotResourceProfile());
                            assertThat(
                                    getTaskManagerTracker().getPendingTaskManagers().size(), is(1));
                            getSlotManager()
                                    .registerTaskManager(
                                            taskExecutionConnection3,
                                            new SlotReport(),
                                            getDefaultTaskManagerResourceProfile(),
                                            getDefaultSlotResourceProfile());
                            assertThat(
                                    getTaskManagerTracker().getPendingTaskManagers().size(), is(0));
                        });
            }
        };
    }

    /**
     * Tests that the slot manager ignores slot reports of unknown origin (not registered task
     * managers).
     */
    @Test
    public void testReceivingUnknownSlotReport() throws Exception {
        final InstanceID unknownInstanceID = new InstanceID();
        final SlotReport unknownSlotReport = new SlotReport();
        new Context() {
            {
                runTest(
                        () -> {
                            // check that we don't have any slots registered
                            assertThat(getSlotManager().getNumberRegisteredSlots(), is(0));

                            // this should not update anything since the instance id is not known to
                            // the slot manager
                            assertFalse(
                                    getSlotManager()
                                            .reportSlotStatus(
                                                    unknownInstanceID, unknownSlotReport));

                            assertThat(getSlotManager().getNumberRegisteredSlots(), is(0));
                        });
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Handle result from ResourceAllocationStrategy
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testSlotAllocationAccordingToStrategyResult() throws Exception {
        final CompletableFuture<
                        Tuple6<
                                SlotID,
                                JobID,
                                AllocationID,
                                ResourceProfile,
                                String,
                                ResourceManagerId>>
                requestSlotFuture = new CompletableFuture<>();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    requestSlotFuture.complete(tuple6);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        final JobID jobId = new JobID();
        final SlotReport slotReport = new SlotReport();
        new Context() {
            {
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        ((jobIDCollectionMap, instanceIDTuple2Map, pendingTaskManagers) ->
                                ResourceAllocationResult.builder()
                                        .addAllocationOnRegisteredResource(
                                                jobId,
                                                taskManagerConnection.getInstanceID(),
                                                getDefaultSlotResourceProfile())
                                        .build()));
                runTest(
                        () -> {
                            getSlotManager()
                                    .registerTaskManager(
                                            taskManagerConnection,
                                            slotReport,
                                            getDefaultTaskManagerResourceProfile(),
                                            getDefaultSlotResourceProfile());
                            getSlotManager()
                                    .processResourceRequirements(
                                            createResourceRequirements(jobId, 1));
                            final Tuple6<
                                            SlotID,
                                            JobID,
                                            AllocationID,
                                            ResourceProfile,
                                            String,
                                            ResourceManagerId>
                                    requestSlot =
                                            requestSlotFuture.get(
                                                    FUTURE_TIMEOUT_SECOND, TimeUnit.SECONDS);
                            assertEquals(jobId, requestSlot.f1);
                            assertEquals(getDefaultSlotResourceProfile(), requestSlot.f3);
                        });
            }
        };
    }

    @Test
    public void testRequestNewResourcesAccordingToStrategyResult() throws Exception {
        final AtomicInteger resourceRequests = new AtomicInteger(0);
        final JobID jobId = new JobID();
        new Context() {
            {
                resourceActionsBuilder.setAllocateResourceFunction(
                        ignored -> {
                            resourceRequests.incrementAndGet();
                            return true;
                        });
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        ((jobIDCollectionMap, instanceIDTuple2Map, pendingTaskManagers) ->
                                ResourceAllocationResult.builder()
                                        .addPendingTaskManagerAllocate(
                                                new PendingTaskManager(
                                                        PendingTaskManagerId.generate(),
                                                        getDefaultTaskManagerResourceProfile(),
                                                        getDefaultSlotResourceProfile()))
                                        .build()));
                runTest(
                        () -> {
                            getSlotManager()
                                    .processResourceRequirements(
                                            createResourceRequirements(jobId, 1));
                            assertThat(resourceRequests.get(), is(1));
                        });
            }
        };
    }

    @Test
    public void testSlotAllocationForPendingTaskManagerWillBeRespected() throws Exception {
        final JobID jobId = new JobID();
        final PendingTaskManagerId pendingTaskManagerId = PendingTaskManagerId.generate();
        final CompletableFuture<
                        Tuple6<
                                SlotID,
                                JobID,
                                AllocationID,
                                ResourceProfile,
                                String,
                                ResourceManagerId>>
                requestSlotFuture = new CompletableFuture<>();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    requestSlotFuture.complete(tuple6);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        new Context() {
            {
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        ((jobIDCollectionMap, instanceIDTuple2Map, pendingTaskManagers) ->
                                ResourceAllocationResult.builder()
                                        .addPendingTaskManagerAllocate(
                                                new PendingTaskManager(
                                                        pendingTaskManagerId,
                                                        getDefaultTaskManagerResourceProfile(),
                                                        getDefaultSlotResourceProfile()))
                                        .addAllocationOnPendingResource(
                                                jobId,
                                                pendingTaskManagerId,
                                                getDefaultSlotResourceProfile())
                                        .build()));
                runTest(
                        () -> {
                            getSlotManager()
                                    .processResourceRequirements(
                                            createResourceRequirements(jobId, 1));
                            getSlotManager()
                                    .registerTaskManager(
                                            taskManagerConnection,
                                            new SlotReport(),
                                            getDefaultTaskManagerResourceProfile(),
                                            getDefaultSlotResourceProfile());
                            final Tuple6<
                                            SlotID,
                                            JobID,
                                            AllocationID,
                                            ResourceProfile,
                                            String,
                                            ResourceManagerId>
                                    requestSlot =
                                            requestSlotFuture.get(
                                                    FUTURE_TIMEOUT_SECOND, TimeUnit.SECONDS);
                            assertEquals(jobId, requestSlot.f1);
                            assertEquals(getDefaultSlotResourceProfile(), requestSlot.f3);
                        });
            }
        };
    }

    @Test
    public void testNotificationAboutNotEnoughResources() throws Exception {
        testNotificationAboutNotEnoughResources(false);
    }

    @Test
    public void testGracePeriodForNotificationAboutNotEnoughResources() throws Exception {
        testNotificationAboutNotEnoughResources(true);
    }

    private void testNotificationAboutNotEnoughResources(boolean withNotificationGracePeriod)
            throws Exception {
        final JobID jobId = new JobID();
        final List<Tuple2<JobID, Collection<ResourceRequirement>>> notEnoughResourceNotifications =
                new ArrayList<>();
        new Context() {
            {
                resourceActionsBuilder.setNotEnoughResourcesConsumer(
                        (jobId1, acquiredResources) ->
                                notEnoughResourceNotifications.add(
                                        Tuple2.of(jobId1, acquiredResources)));
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        ((jobIDCollectionMap, instanceIDTuple2Map, pendingTaskManagers) ->
                                ResourceAllocationResult.builder()
                                        .addUnfulfillableJob(jobId)
                                        .build()));

                runTest(
                        () -> {
                            if (withNotificationGracePeriod) {
                                // this should disable notifications
                                getSlotManager().setFailUnfulfillableRequest(false);
                            }

                            final ResourceRequirements resourceRequirements =
                                    createResourceRequirements(jobId, 1);
                            getSlotManager().processResourceRequirements(resourceRequirements);

                            if (withNotificationGracePeriod) {
                                assertThat(notEnoughResourceNotifications, empty());

                                // re-enable notifications which should also trigger another
                                // resource check
                                getSlotManager().setFailUnfulfillableRequest(true);
                            }

                            assertThat(notEnoughResourceNotifications, hasSize(1));
                            final Tuple2<JobID, Collection<ResourceRequirement>> notification =
                                    notEnoughResourceNotifications.get(0);
                            assertThat(notification.f0, is(jobId));
                        });
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Task manager timeout
    // ---------------------------------------------------------------------------------------------

    /**
     * Tests that formerly used task managers can timeout after all of their slots have been freed.
     */
    @Test
    public void testTimeoutForUnusedTaskManager() throws Exception {
        final Time taskManagerTimeout = Time.milliseconds(50L);

        final CompletableFuture<InstanceID> releaseResourceFuture = new CompletableFuture<>();
        final AllocationID allocationId = new AllocationID();
        final TaskExecutorConnection taskExecutionConnection = createTaskExecutorConnection();
        final InstanceID instanceId = taskExecutionConnection.getInstanceID();
        new Context() {
            {
                resourceActionsBuilder.setReleaseResourceConsumer(
                        (instanceID, e) -> releaseResourceFuture.complete(instanceID));
                slotManagerConfigurationBuilder.setTaskManagerTimeout(taskManagerTimeout);
                runTest(
                        () -> {
                            getSlotManager()
                                    .registerTaskManager(
                                            taskExecutionConnection,
                                            new SlotReport(
                                                    createAllocatedSlotStatus(
                                                            allocationId,
                                                            getDefaultSlotResourceProfile())),
                                            getDefaultTaskManagerResourceProfile(),
                                            getDefaultSlotResourceProfile());
                            assertEquals(
                                    getSlotManager().getTaskManagerIdleSince(instanceId),
                                    Long.MAX_VALUE);

                            getSlotManager()
                                    .freeSlot(
                                            new SlotID(taskExecutionConnection.getResourceID(), 0),
                                            allocationId);

                            assertThat(
                                    getSlotManager().getTaskManagerIdleSince(instanceId),
                                    not(equalTo(Long.MAX_VALUE)));
                            assertThat(
                                    releaseResourceFuture.get(
                                            FUTURE_TIMEOUT_SECOND, TimeUnit.SECONDS),
                                    is(equalTo(instanceId)));
                            // A task manager timeout does not remove the slots from the
                            // SlotManager. The receiver of the callback can then decide what to do
                            // with the TaskManager.
                            assertEquals(
                                    getDefaultNumberSlotsPerWorker(),
                                    getSlotManager().getNumberRegisteredSlots());

                            getSlotManager()
                                    .unregisterTaskManager(
                                            taskExecutionConnection.getInstanceID(),
                                            TEST_EXCEPTION);
                            assertEquals(0, getSlotManager().getNumberRegisteredSlots());
                        });
            }
        };
    }
}
