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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link SlotManagerImpl}. */
public class SlotManagerImplTest extends TestLogger {

    private static final FlinkException TEST_EXCEPTION = new FlinkException("Test exception");

    private static final WorkerResourceSpec WORKER_RESOURCE_SPEC =
            new WorkerResourceSpec.Builder()
                    .setCpuCores(100.0)
                    .setTaskHeapMemoryMB(10000)
                    .setTaskOffHeapMemoryMB(10000)
                    .setNetworkMemoryMB(10000)
                    .setManagedMemoryMB(10000)
                    .build();

    /** Tests that we can register task manager and their slots at the slot manager. */
    @Test
    public void testTaskManagerRegistration() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final ResourceID resourceId = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);

        final SlotID slotId1 = new SlotID(resourceId, 0);
        final SlotID slotId2 = new SlotID(resourceId, 1);
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
        final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
        final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

        try (SlotManagerImpl slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {
            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            assertTrue(
                    "The number registered slots does not equal the expected number.",
                    2 == slotManager.getNumberRegisteredSlots());

            assertNotNull(slotManager.getSlot(slotId1));
            assertNotNull(slotManager.getSlot(slotId2));
        }
    }

    /** Tests that un-registration of task managers will free and remove all registered slots. */
    @Test
    public void testTaskManagerUnregistration() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();
        final JobID jobId = new JobID();

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    assertThat(tuple6.f5, is(equalTo(resourceManagerId)));
                                    return new CompletableFuture<>();
                                })
                        .createTestingTaskExecutorGateway();

        final ResourceID resourceId = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);

        final SlotID slotId1 = new SlotID(resourceId, 0);
        final SlotID slotId2 = new SlotID(resourceId, 1);
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final SlotStatus slotStatus1 =
                new SlotStatus(slotId1, resourceProfile, jobId, allocationId1);
        final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
        final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

        final SlotRequest slotRequest =
                new SlotRequest(new JobID(), allocationId2, resourceProfile, "foobar");

        try (SlotManagerImpl slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {
            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            assertTrue(
                    "The number registered slots does not equal the expected number.",
                    2 == slotManager.getNumberRegisteredSlots());

            TaskManagerSlot slot1 = slotManager.getSlot(slotId1);
            TaskManagerSlot slot2 = slotManager.getSlot(slotId2);

            assertTrue(slot1.getState() == SlotState.ALLOCATED);
            assertTrue(slot2.getState() == SlotState.FREE);

            assertTrue(slotManager.registerSlotRequest(slotRequest));

            assertFalse(slot2.getState() == SlotState.FREE);
            assertTrue(slot2.getState() == SlotState.PENDING);

            PendingSlotRequest pendingSlotRequest = slotManager.getSlotRequest(allocationId2);

            assertTrue(
                    "The pending slot request should have been assigned to slot 2",
                    pendingSlotRequest.isAssigned());

            slotManager.unregisterTaskManager(
                    taskManagerConnection.getInstanceID(), TEST_EXCEPTION);

            assertTrue(0 == slotManager.getNumberRegisteredSlots());
            assertFalse(pendingSlotRequest.isAssigned());
        }
    }

    /** Tests that a slot request with no free slots will trigger the resource allocation. */
    @Test
    public void testSlotRequestWithoutFreeSlots() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final SlotRequest slotRequest =
                new SlotRequest(new JobID(), new AllocationID(), resourceProfile, "localhost");

        CompletableFuture<WorkerResourceSpec> allocateResourceFuture = new CompletableFuture<>();
        ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceConsumer(allocateResourceFuture::complete)
                        .build();

        try (SlotManager slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {

            slotManager.registerSlotRequest(slotRequest);

            allocateResourceFuture.get();
        }
    }

    /** Tests that the slot request fails if we cannot allocate more resources. */
    @Test
    public void testSlotRequestWithResourceAllocationFailure() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final SlotRequest slotRequest =
                new SlotRequest(new JobID(), new AllocationID(), resourceProfile, "localhost");

        ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceFunction(value -> false)
                        .build();

        try (SlotManager slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {

            slotManager.registerSlotRequest(slotRequest);

            fail("The slot request should have failed with a ResourceManagerException.");

        } catch (ResourceManagerException e) {
            // expected exception
        }
    }

    /** Tests that a slot request which can be fulfilled will trigger a slot allocation. */
    @Test
    public void testSlotRequestWithFreeSlot() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceID resourceID = ResourceID.generate();
        final JobID jobId = new JobID();
        final SlotID slotId = new SlotID(resourceID, 0);
        final String targetAddress = "localhost";
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final SlotRequest slotRequest =
                new SlotRequest(jobId, allocationId, resourceProfile, targetAddress);

        ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

        try (SlotManagerImpl slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {
            final CompletableFuture<
                            Tuple6<
                                    SlotID,
                                    JobID,
                                    AllocationID,
                                    ResourceProfile,
                                    String,
                                    ResourceManagerId>>
                    requestFuture = new CompletableFuture<>();
            // accept an incoming slot request
            final TaskExecutorGateway taskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setRequestSlotFunction(
                                    tuple6 -> {
                                        requestFuture.complete(
                                                Tuple6.of(
                                                        tuple6.f0, tuple6.f1, tuple6.f2, tuple6.f3,
                                                        tuple6.f4, tuple6.f5));
                                        return CompletableFuture.completedFuture(Acknowledge.get());
                                    })
                            .createTestingTaskExecutorGateway();

            final TaskExecutorConnection taskExecutorConnection =
                    new TaskExecutorConnection(resourceID, taskExecutorGateway);

            final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
            final SlotReport slotReport = new SlotReport(slotStatus);

            slotManager.registerTaskManager(
                    taskExecutorConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            assertTrue(
                    "The slot request should be accepted",
                    slotManager.registerSlotRequest(slotRequest));

            assertThat(
                    requestFuture.get(),
                    is(
                            equalTo(
                                    Tuple6.of(
                                            slotId,
                                            jobId,
                                            allocationId,
                                            resourceProfile,
                                            targetAddress,
                                            resourceManagerId))));

            TaskManagerSlot slot = slotManager.getSlot(slotId);

            assertEquals(
                    "The slot has not been allocated to the expected allocation id.",
                    allocationId,
                    slot.getAllocationId());
        }
    }

    /**
     * Checks that un-registering a pending slot request will cancel it, removing it from all
     * assigned task manager slots and then remove it from the slot manager.
     */
    @Test
    public void testUnregisterPendingSlotRequest() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();
        final ResourceID resourceID = ResourceID.generate();
        final SlotID slotId = new SlotID(resourceID, 0);
        final AllocationID allocationId = new AllocationID();

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                slotIDJobIDAllocationIDStringResourceManagerIdTuple6 ->
                                        new CompletableFuture<>())
                        .createTestingTaskExecutorGateway();

        final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1);
        final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
        final SlotReport slotReport = new SlotReport(slotStatus);

        final SlotRequest slotRequest =
                new SlotRequest(new JobID(), allocationId, resourceProfile, "foobar");

        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);

        try (SlotManagerImpl slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {
            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            TaskManagerSlot slot = slotManager.getSlot(slotId);

            slotManager.registerSlotRequest(slotRequest);

            assertNotNull(slotManager.getSlotRequest(allocationId));

            assertTrue(slot.getState() == SlotState.PENDING);

            slotManager.unregisterSlotRequest(allocationId);

            assertNull(slotManager.getSlotRequest(allocationId));

            slot = slotManager.getSlot(slotId);
            assertTrue(slot.getState() == SlotState.FREE);
        }
    }

    /** Tests that pending slot requests are tried to be fulfilled upon new slot registrations. */
    @Test
    public void testFulfillingPendingSlotRequest() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceID resourceID = ResourceID.generate();
        final JobID jobId = new JobID();
        final SlotID slotId = new SlotID(resourceID, 0);
        final String targetAddress = "localhost";
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final SlotRequest slotRequest =
                new SlotRequest(jobId, allocationId, resourceProfile, targetAddress);

        final AtomicInteger numberAllocateResourceCalls = new AtomicInteger(0);
        ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceConsumer(
                                ignored -> numberAllocateResourceCalls.incrementAndGet())
                        .build();

        final CompletableFuture<
                        Tuple6<
                                SlotID,
                                JobID,
                                AllocationID,
                                ResourceProfile,
                                String,
                                ResourceManagerId>>
                requestFuture = new CompletableFuture<>();
        // accept an incoming slot request
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    requestFuture.complete(
                                            Tuple6.of(
                                                    tuple6.f0, tuple6.f1, tuple6.f2, tuple6.f3,
                                                    tuple6.f4, tuple6.f5));
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);

        final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
        final SlotReport slotReport = new SlotReport(slotStatus);

        try (SlotManagerImpl slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {

            assertTrue(
                    "The slot request should be accepted",
                    slotManager.registerSlotRequest(slotRequest));

            assertThat(numberAllocateResourceCalls.get(), is(1));

            slotManager.registerTaskManager(
                    taskExecutorConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            assertThat(
                    requestFuture.get(),
                    is(
                            equalTo(
                                    Tuple6.of(
                                            slotId,
                                            jobId,
                                            allocationId,
                                            resourceProfile,
                                            targetAddress,
                                            resourceManagerId))));

            TaskManagerSlot slot = slotManager.getSlot(slotId);

            assertEquals(
                    "The slot has not been allocated to the expected allocation id.",
                    allocationId,
                    slot.getAllocationId());
        }
    }

    /** Tests that freeing a slot will correctly reset the slot and mark it as a free slot. */
    @Test
    public void testFreeSlot() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final JobID jobId = new JobID();

        ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

        final TaskExecutorConnection taskExecutorConnection = createTaskExecutorConnection();
        final ResourceID resourceID = taskExecutorConnection.getResourceID();
        final SlotID slotId = new SlotID(resourceID, 0);
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

        final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile, jobId, allocationId);
        final SlotReport slotReport = new SlotReport(slotStatus);

        try (SlotManagerImpl slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {

            slotManager.registerTaskManager(
                    taskExecutorConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            TaskManagerSlot slot = slotManager.getSlot(slotId);

            assertEquals(
                    "The slot has not been allocated to the expected allocation id.",
                    allocationId,
                    slot.getAllocationId());

            // this should be ignored since the allocation id does not match
            slotManager.freeSlot(slotId, new AllocationID());

            assertTrue(slot.getState() == SlotState.ALLOCATED);
            assertEquals(
                    "The slot has not been allocated to the expected allocation id.",
                    allocationId,
                    slot.getAllocationId());

            slotManager.freeSlot(slotId, allocationId);

            assertTrue(slot.getState() == SlotState.FREE);
            assertNull(slot.getAllocationId());
        }
    }

    /**
     * Tests that a second pending slot request is detected as a duplicate if the allocation ids are
     * the same.
     */
    @Test
    public void testDuplicatePendingSlotRequest() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final AtomicInteger numberAllocateResourceFunctionCalls = new AtomicInteger(0);
        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceConsumer(
                                ignored -> numberAllocateResourceFunctionCalls.incrementAndGet())
                        .build();
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1.0, 2);
        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(2.0, 1);
        final SlotRequest slotRequest1 =
                new SlotRequest(new JobID(), allocationId, resourceProfile1, "foobar");
        final SlotRequest slotRequest2 =
                new SlotRequest(new JobID(), allocationId, resourceProfile2, "barfoo");

        try (SlotManager slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {
            assertTrue(slotManager.registerSlotRequest(slotRequest1));
            assertFalse(slotManager.registerSlotRequest(slotRequest2));
        }

        // check that we have only called the resource allocation only for the first slot request,
        // since the second request is a duplicate
        assertThat(numberAllocateResourceFunctionCalls.get(), is(1));
    }

    /**
     * Tests that if we have received a slot report with some allocated slots, then we don't accept
     * slot requests with allocated allocation ids.
     */
    @Test
    public void testDuplicatePendingSlotRequestAfterSlotReport() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

        final TaskExecutorConnection taskManagerConnection = createTaskExecutorConnection();
        final ResourceID resourceID = taskManagerConnection.getResourceID();

        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1);
        final SlotID slotId = new SlotID(resourceID, 0);
        final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile, jobId, allocationId);
        final SlotReport slotReport = new SlotReport(slotStatus);

        final SlotRequest slotRequest =
                new SlotRequest(jobId, allocationId, resourceProfile, "foobar");

        try (SlotManager slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {
            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            assertFalse(slotManager.registerSlotRequest(slotRequest));
        }
    }

    /**
     * Tests that duplicate slot requests (requests with an already registered allocation id) are
     * also detected after a pending slot request has been fulfilled but not yet freed.
     */
    @Test
    public void testDuplicatePendingSlotRequestAfterSuccessfulAllocation() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final AtomicInteger allocateResourceCalls = new AtomicInteger(0);
        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceConsumer(
                                ignored -> allocateResourceCalls.incrementAndGet())
                        .build();
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1.0, 2);
        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(2.0, 1);
        final SlotRequest slotRequest1 =
                new SlotRequest(new JobID(), allocationId, resourceProfile1, "foobar");
        final SlotRequest slotRequest2 =
                new SlotRequest(new JobID(), allocationId, resourceProfile2, "barfoo");

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();

        final ResourceID resourceID = ResourceID.generate();

        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);

        final SlotID slotId = new SlotID(resourceID, 0);
        final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile1);
        final SlotReport slotReport = new SlotReport(slotStatus);

        try (SlotManagerImpl slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {
            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);
            assertTrue(slotManager.registerSlotRequest(slotRequest1));

            TaskManagerSlot slot = slotManager.getSlot(slotId);

            assertEquals(
                    "The slot has not been allocated to the expected allocation id.",
                    allocationId,
                    slot.getAllocationId());

            assertFalse(slotManager.registerSlotRequest(slotRequest2));
        }

        // check that we have only called the resource allocation only for the first slot request,
        // since the second request is a duplicate
        assertThat(allocateResourceCalls.get(), is(0));
    }

    /**
     * Tests that an already registered allocation id can be reused after the initial slot request
     * has been freed.
     */
    @Test
    public void testAcceptingDuplicateSlotRequestAfterAllocationRelease() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final AtomicInteger allocateResourceCalls = new AtomicInteger(0);
        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceConsumer(
                                ignored -> allocateResourceCalls.incrementAndGet())
                        .build();
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1.0, 2);
        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(2.0, 1);
        final SlotRequest slotRequest1 =
                new SlotRequest(new JobID(), allocationId, resourceProfile1, "foobar");
        final SlotRequest slotRequest2 =
                new SlotRequest(new JobID(), allocationId, resourceProfile2, "barfoo");

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();

        final ResourceID resourceID = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);

        final SlotID slotId = new SlotID(resourceID, 0);
        final SlotStatus slotStatus = new SlotStatus(slotId, ResourceProfile.fromResources(2.0, 2));
        final SlotReport slotReport = new SlotReport(slotStatus);

        try (SlotManagerImpl slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {
            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);
            assertTrue(slotManager.registerSlotRequest(slotRequest1));

            TaskManagerSlot slot = slotManager.getSlot(slotId);

            assertEquals(
                    "The slot has not been allocated to the expected allocation id.",
                    allocationId,
                    slot.getAllocationId());

            slotManager.freeSlot(slotId, allocationId);

            // check that the slot has been freed
            assertTrue(slot.getState() == SlotState.FREE);
            assertNull(slot.getAllocationId());

            assertTrue(slotManager.registerSlotRequest(slotRequest2));

            assertEquals(
                    "The slot has not been allocated to the expected allocation id.",
                    allocationId,
                    slot.getAllocationId());
        }

        // check that we have only called the resource allocation only for the first slot request,
        // since the second request is a duplicate
        assertThat(allocateResourceCalls.get(), is(0));
    }

    /**
     * Tests that the slot manager ignores slot reports of unknown origin (not registered task
     * managers).
     */
    @Test
    public void testReceivingUnknownSlotReport() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

        final InstanceID unknownInstanceID = new InstanceID();
        final SlotID unknownSlotId = new SlotID(ResourceID.generate(), 0);
        final ResourceProfile unknownResourceProfile = ResourceProfile.fromResources(1.0, 1);
        final SlotStatus unknownSlotStatus = new SlotStatus(unknownSlotId, unknownResourceProfile);
        final SlotReport unknownSlotReport = new SlotReport(unknownSlotStatus);

        try (SlotManager slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {
            // check that we don't have any slots registered
            assertTrue(0 == slotManager.getNumberRegisteredSlots());

            // this should not update anything since the instance id is not known to the slot
            // manager
            assertFalse(slotManager.reportSlotStatus(unknownInstanceID, unknownSlotReport));

            assertTrue(0 == slotManager.getNumberRegisteredSlots());
        }
    }

    /**
     * Tests that slots are updated with respect to the latest incoming slot report. This means that
     * slots for which a report was received are updated accordingly.
     */
    @Test
    public void testUpdateSlotReport() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();

        final TaskExecutorConnection taskManagerConnection = createTaskExecutorConnection();
        final ResourceID resourceId = taskManagerConnection.getResourceID();

        final SlotID slotId1 = new SlotID(resourceId, 0);
        final SlotID slotId2 = new SlotID(resourceId, 1);

        final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1);
        final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
        final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);

        final SlotStatus newSlotStatus2 =
                new SlotStatus(slotId2, resourceProfile, jobId, allocationId);

        final SlotReport slotReport1 = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));
        final SlotReport slotReport2 = new SlotReport(Arrays.asList(newSlotStatus2, slotStatus1));

        try (SlotManagerImpl slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {
            // check that we don't have any slots registered
            assertTrue(0 == slotManager.getNumberRegisteredSlots());

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport1, ResourceProfile.ANY, ResourceProfile.ANY);

            TaskManagerSlot slot1 = slotManager.getSlot(slotId1);
            TaskManagerSlot slot2 = slotManager.getSlot(slotId2);

            assertTrue(2 == slotManager.getNumberRegisteredSlots());

            assertTrue(slot1.getState() == SlotState.FREE);
            assertTrue(slot2.getState() == SlotState.FREE);

            assertTrue(
                    slotManager.reportSlotStatus(
                            taskManagerConnection.getInstanceID(), slotReport2));

            assertTrue(2 == slotManager.getNumberRegisteredSlots());

            assertNotNull(slotManager.getSlot(slotId1));
            assertNotNull(slotManager.getSlot(slotId2));

            // slotId2 should have been allocated for allocationId
            assertEquals(allocationId, slotManager.getSlot(slotId2).getAllocationId());
        }
    }

    /**
     * Tests that slot requests time out after the specified request timeout. If a slot request
     * times out, then the request is cancelled, removed from the slot manager and the resource
     * manager is notified about the failed allocation.
     */
    @Test
    public void testSlotRequestTimeout() throws Exception {
        final long allocationTimeout = 50L;

        final CompletableFuture<Tuple2<JobID, AllocationID>> failedAllocationFuture =
                new CompletableFuture<>();
        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setNotifyAllocationFailureConsumer(
                                tuple3 ->
                                        failedAllocationFuture.complete(
                                                Tuple2.of(tuple3.f0, tuple3.f1)))
                        .build();
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();

        final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1);
        final SlotRequest slotRequest =
                new SlotRequest(jobId, allocationId, resourceProfile, "foobar");

        final Executor mainThreadExecutor = TestingUtils.defaultExecutor();

        try (SlotManager slotManager =
                createSlotManagerBuilder()
                        .setSlotRequestTimeout(Time.milliseconds(allocationTimeout))
                        .build()) {

            slotManager.start(resourceManagerId, mainThreadExecutor, resourceManagerActions);

            final AtomicReference<Exception> atomicException = new AtomicReference<>(null);

            mainThreadExecutor.execute(
                    () -> {
                        try {
                            assertTrue(slotManager.registerSlotRequest(slotRequest));
                        } catch (Exception e) {
                            atomicException.compareAndSet(null, e);
                        }
                    });

            assertThat(failedAllocationFuture.get(), is(equalTo(Tuple2.of(jobId, allocationId))));

            if (atomicException.get() != null) {
                throw atomicException.get();
            }
        }
    }

    /** Tests that a slot request is retried if it times out on the task manager side. */
    @Test
    public void testTaskManagerSlotRequestTimeoutHandling() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final SlotRequest slotRequest =
                new SlotRequest(jobId, allocationId, resourceProfile, "foobar");
        final CompletableFuture<Acknowledge> slotRequestFuture1 = new CompletableFuture<>();
        final CompletableFuture<Acknowledge> slotRequestFuture2 = new CompletableFuture<>();
        final Iterator<CompletableFuture<Acknowledge>> slotRequestFutureIterator =
                Arrays.asList(slotRequestFuture1, slotRequestFuture2).iterator();
        final ArrayBlockingQueue<SlotID> slotIds = new ArrayBlockingQueue<>(2);

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                FunctionUtils.uncheckedFunction(
                                        requestSlotParameters -> {
                                            slotIds.put(requestSlotParameters.f0);
                                            return slotRequestFutureIterator.next();
                                        }))
                        .createTestingTaskExecutorGateway();

        final ResourceID resourceId = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);

        final SlotID slotId1 = new SlotID(resourceId, 0);
        final SlotID slotId2 = new SlotID(resourceId, 1);
        final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
        final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
        final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

        try (SlotManagerImpl slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            slotManager.registerSlotRequest(slotRequest);

            final SlotID firstSlotId = slotIds.take();
            assertThat(slotIds, is(empty()));

            TaskManagerSlot failedSlot = slotManager.getSlot(firstSlotId);

            // let the first attempt fail --> this should trigger a second attempt
            slotRequestFuture1.completeExceptionally(
                    new SlotAllocationException("Test exception."));

            // the second attempt succeeds
            slotRequestFuture2.complete(Acknowledge.get());

            final SlotID secondSlotId = slotIds.take();
            assertThat(slotIds, is(empty()));

            TaskManagerSlot slot = slotManager.getSlot(secondSlotId);

            assertTrue(slot.getState() == SlotState.ALLOCATED);
            assertEquals(allocationId, slot.getAllocationId());

            if (!failedSlot.getSlotId().equals(slot.getSlotId())) {
                assertTrue(failedSlot.getState() == SlotState.FREE);
            }
        }
    }

    /**
     * Tests that pending slot requests are rejected if a slot report with a different allocation is
     * received.
     */
    @Test
    public void testSlotReportWhileActiveSlotRequest() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final SlotRequest slotRequest =
                new SlotRequest(jobId, allocationId, resourceProfile, "foobar");
        final CompletableFuture<Acknowledge> slotRequestFuture1 = new CompletableFuture<>();

        final Iterator<CompletableFuture<Acknowledge>> slotRequestFutureIterator =
                Arrays.asList(
                                slotRequestFuture1,
                                CompletableFuture.completedFuture(Acknowledge.get()))
                        .iterator();
        final ArrayBlockingQueue<SlotID> slotIds = new ArrayBlockingQueue<>(2);

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                FunctionUtils.uncheckedFunction(
                                        requestSlotParameters -> {
                                            slotIds.put(requestSlotParameters.f0);
                                            return slotRequestFutureIterator.next();
                                        }))
                        .createTestingTaskExecutorGateway();

        final ResourceID resourceId = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);

        final SlotID slotId1 = new SlotID(resourceId, 0);
        final SlotID slotId2 = new SlotID(resourceId, 1);
        final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
        final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
        final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

        final ScheduledExecutor mainThreadExecutor = TestingUtils.defaultScheduledExecutor();

        final SlotManagerImpl slotManager =
                createSlotManagerBuilder().setScheduledExecutor(mainThreadExecutor).build();

        try {

            slotManager.start(resourceManagerId, mainThreadExecutor, resourceManagerActions);

            CompletableFuture<Void> registrationFuture =
                    CompletableFuture.supplyAsync(
                                    () -> {
                                        slotManager.registerTaskManager(
                                                taskManagerConnection,
                                                slotReport,
                                                ResourceProfile.ANY,
                                                ResourceProfile.ANY);

                                        return null;
                                    },
                                    mainThreadExecutor)
                            .thenAccept(
                                    (Object value) -> {
                                        try {
                                            slotManager.registerSlotRequest(slotRequest);
                                        } catch (ResourceManagerException e) {
                                            throw new RuntimeException(
                                                    "Could not register slots.", e);
                                        }
                                    });

            // check that no exception has been thrown
            registrationFuture.get();

            final SlotID requestedSlotId = slotIds.take();
            final SlotID freeSlotId = requestedSlotId.equals(slotId1) ? slotId2 : slotId1;

            final SlotStatus newSlotStatus1 =
                    new SlotStatus(
                            requestedSlotId, resourceProfile, new JobID(), new AllocationID());
            final SlotStatus newSlotStatus2 = new SlotStatus(freeSlotId, resourceProfile);
            final SlotReport newSlotReport =
                    new SlotReport(Arrays.asList(newSlotStatus1, newSlotStatus2));

            CompletableFuture<Boolean> reportSlotStatusFuture =
                    CompletableFuture.supplyAsync(
                            // this should update the slot with the pending slot request triggering
                            // the reassignment of it
                            () ->
                                    slotManager.reportSlotStatus(
                                            taskManagerConnection.getInstanceID(), newSlotReport),
                            mainThreadExecutor);

            assertTrue(reportSlotStatusFuture.get());

            final SlotID requestedSlotId2 = slotIds.take();

            assertEquals(freeSlotId, requestedSlotId2);
        } finally {
            CompletableFuture.runAsync(
                    ThrowingRunnable.unchecked(slotManager::close), mainThreadExecutor);
        }
    }

    /**
     * Tests that formerly used task managers can again timeout after all of their slots have been
     * freed.
     */
    @Test
    public void testTimeoutForUnusedTaskManager() throws Exception {
        final long taskManagerTimeout = 50L;

        final CompletableFuture<InstanceID> releasedResourceFuture = new CompletableFuture<>();
        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setReleaseResourceConsumer(
                                (instanceID, e) -> releasedResourceFuture.complete(instanceID))
                        .build();
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ScheduledExecutor scheduledExecutor = TestingUtils.defaultScheduledExecutor();

        final ResourceID resourceId = ResourceID.generate();

        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1);
        final SlotRequest slotRequest =
                new SlotRequest(jobId, allocationId, resourceProfile, "foobar");

        final CompletableFuture<SlotID> requestedSlotFuture = new CompletableFuture<>();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    requestedSlotFuture.complete(tuple6.f0);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);

        final SlotID slotId1 = new SlotID(resourceId, 0);
        final SlotID slotId2 = new SlotID(resourceId, 1);
        final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
        final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
        final SlotReport initialSlotReport =
                new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

        final Executor mainThreadExecutor = TestingUtils.defaultExecutor();

        try (final SlotManagerImpl slotManager =
                createSlotManagerBuilder()
                        .setTaskManagerTimeout(Time.of(taskManagerTimeout, TimeUnit.MILLISECONDS))
                        .setRedundantTaskManagerNum(0)
                        .build()) {

            slotManager.start(resourceManagerId, mainThreadExecutor, resourceManagerActions);

            CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return slotManager.registerSlotRequest(slotRequest);
                                } catch (ResourceManagerException e) {
                                    throw new CompletionException(e);
                                }
                            },
                            mainThreadExecutor)
                    .thenRun(
                            () ->
                                    slotManager.registerTaskManager(
                                            taskManagerConnection,
                                            initialSlotReport,
                                            ResourceProfile.ANY,
                                            ResourceProfile.ANY));

            final SlotID slotId = requestedSlotFuture.get();

            CompletableFuture<Boolean> idleFuture =
                    CompletableFuture.supplyAsync(
                            () ->
                                    slotManager.isTaskManagerIdle(
                                            taskManagerConnection.getInstanceID()),
                            mainThreadExecutor);

            // check that the TaskManager is not idle
            assertFalse(idleFuture.get());

            CompletableFuture<TaskManagerSlot> slotFuture =
                    CompletableFuture.supplyAsync(
                            () -> slotManager.getSlot(slotId), mainThreadExecutor);

            TaskManagerSlot slot = slotFuture.get();

            assertTrue(slot.getState() == SlotState.ALLOCATED);
            assertEquals(allocationId, slot.getAllocationId());

            CompletableFuture<Boolean> idleFuture2 =
                    CompletableFuture.runAsync(
                                    () -> slotManager.freeSlot(slotId, allocationId),
                                    mainThreadExecutor)
                            .thenApply(
                                    (Object value) ->
                                            slotManager.isTaskManagerIdle(
                                                    taskManagerConnection.getInstanceID()));

            assertTrue(idleFuture2.get());

            assertThat(
                    releasedResourceFuture.get(),
                    is(equalTo(taskManagerConnection.getInstanceID())));
        }
    }

    /**
     * Tests that a task manager timeout does not remove the slots from the SlotManager. A timeout
     * should only trigger the {@link ResourceActions#releaseResource(InstanceID, Exception)}
     * callback. The receiver of the callback can then decide what to do with the TaskManager.
     *
     * <p>See FLINK-7793
     */
    @Test
    public void testTaskManagerTimeoutDoesNotRemoveSlots() throws Exception {
        final Time taskManagerTimeout = Time.milliseconds(10L);
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceID resourceID = ResourceID.generate();
        final CompletableFuture<InstanceID> releaseResourceFuture = new CompletableFuture<>();
        final ResourceActions resourceActions =
                new TestingResourceActionsBuilder()
                        .setReleaseResourceConsumer(
                                (instanceId, ignored) -> releaseResourceFuture.complete(instanceId))
                        .build();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();

        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);
        final SlotStatus slotStatus =
                createEmptySlotStatus(
                        new SlotID(resourceID, 0), ResourceProfile.fromResources(1.0, 1));
        final SlotReport initialSlotReport = new SlotReport(slotStatus);

        try (final SlotManager slotManager =
                createSlotManagerBuilder()
                        .setTaskManagerTimeout(taskManagerTimeout)
                        .setRedundantTaskManagerNum(0)
                        .buildAndStartWithDirectExec(resourceManagerId, resourceActions)) {

            slotManager.registerTaskManager(
                    taskExecutorConnection,
                    initialSlotReport,
                    ResourceProfile.ANY,
                    ResourceProfile.ANY);

            assertEquals(1, slotManager.getNumberRegisteredSlots());

            // wait for the timeout call to happen
            assertThat(releaseResourceFuture.get(), is(taskExecutorConnection.getInstanceID()));

            assertEquals(1, slotManager.getNumberRegisteredSlots());

            slotManager.unregisterTaskManager(
                    taskExecutorConnection.getInstanceID(), TEST_EXCEPTION);

            assertEquals(0, slotManager.getNumberRegisteredSlots());
        }
    }

    /**
     * Tests that free slots which are reported as allocated won't be considered for fulfilling
     * other pending slot requests.
     *
     * <p>See: FLINK-8505
     */
    @Test
    public void testReportAllocatedSlot() throws Exception {
        final ResourceID taskManagerId = ResourceID.generate();
        final ResourceActions resourceActions = new TestingResourceActionsBuilder().build();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(taskManagerId, taskExecutorGateway);

        try (final SlotManagerImpl slotManager =
                createSlotManager(ResourceManagerId.generate(), resourceActions)) {

            // initially report a single slot as free
            final SlotID slotId = new SlotID(taskManagerId, 0);
            final SlotStatus initialSlotStatus = new SlotStatus(slotId, ResourceProfile.ANY);
            final SlotReport initialSlotReport = new SlotReport(initialSlotStatus);

            slotManager.registerTaskManager(
                    taskExecutorConnection,
                    initialSlotReport,
                    ResourceProfile.ANY,
                    ResourceProfile.ANY);

            assertThat(slotManager.getNumberRegisteredSlots(), is(equalTo(1)));

            // Now report this slot as allocated
            final SlotStatus slotStatus =
                    new SlotStatus(slotId, ResourceProfile.ANY, new JobID(), new AllocationID());
            final SlotReport slotReport = new SlotReport(slotStatus);

            slotManager.reportSlotStatus(taskExecutorConnection.getInstanceID(), slotReport);

            // this slot request should not be fulfilled
            final AllocationID allocationId = new AllocationID();
            final SlotRequest slotRequest =
                    new SlotRequest(new JobID(), allocationId, ResourceProfile.UNKNOWN, "foobar");

            // This triggered an IllegalStateException before
            slotManager.registerSlotRequest(slotRequest);

            assertThat(slotManager.getSlotRequest(allocationId).isAssigned(), is(false));
        }
    }

    /**
     * Tests that the SlotManager retries allocating a slot if the TaskExecutor#requestSlot call
     * fails.
     */
    @Test
    public void testSlotRequestFailure() throws Exception {
        try (final SlotManagerImpl slotManager =
                createSlotManager(
                        ResourceManagerId.generate(),
                        new TestingResourceActionsBuilder().build())) {

            final SlotRequest slotRequest =
                    new SlotRequest(
                            new JobID(), new AllocationID(), ResourceProfile.UNKNOWN, "foobar");
            slotManager.registerSlotRequest(slotRequest);

            final BlockingQueue<
                            Tuple6<
                                    SlotID,
                                    JobID,
                                    AllocationID,
                                    ResourceProfile,
                                    String,
                                    ResourceManagerId>>
                    requestSlotQueue = new ArrayBlockingQueue<>(1);
            final BlockingQueue<CompletableFuture<Acknowledge>> responseQueue =
                    new ArrayBlockingQueue<>(1);

            final TestingTaskExecutorGateway testingTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setRequestSlotFunction(
                                    slotIDJobIDAllocationIDStringResourceManagerIdTuple6 -> {
                                        requestSlotQueue.offer(
                                                slotIDJobIDAllocationIDStringResourceManagerIdTuple6);
                                        try {
                                            return responseQueue.take();
                                        } catch (InterruptedException ignored) {
                                            return FutureUtils.completedExceptionally(
                                                    new FlinkException(
                                                            "Response queue was interrupted."));
                                        }
                                    })
                            .createTestingTaskExecutorGateway();

            final ResourceID taskExecutorResourceId = ResourceID.generate();
            final TaskExecutorConnection taskExecutionConnection =
                    new TaskExecutorConnection(taskExecutorResourceId, testingTaskExecutorGateway);
            final SlotReport slotReport =
                    new SlotReport(
                            createEmptySlotStatus(
                                    new SlotID(taskExecutorResourceId, 0), ResourceProfile.ANY));

            final CompletableFuture<Acknowledge> firstManualSlotRequestResponse =
                    new CompletableFuture<>();
            responseQueue.offer(firstManualSlotRequestResponse);

            slotManager.registerTaskManager(
                    taskExecutionConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>
                    firstRequest = requestSlotQueue.take();

            final CompletableFuture<Acknowledge> secondManualSlotRequestResponse =
                    new CompletableFuture<>();
            responseQueue.offer(secondManualSlotRequestResponse);

            // fail first request
            firstManualSlotRequestResponse.completeExceptionally(
                    new SlotAllocationException("Test exception"));

            final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>
                    secondRequest = requestSlotQueue.take();

            assertThat(secondRequest.f2, equalTo(firstRequest.f2));
            assertThat(secondRequest.f0, equalTo(firstRequest.f0));

            secondManualSlotRequestResponse.complete(Acknowledge.get());

            final TaskManagerSlot slot = slotManager.getSlot(secondRequest.f0);
            assertThat(slot.getState(), equalTo(SlotState.ALLOCATED));
            assertThat(slot.getAllocationId(), equalTo(secondRequest.f2));
        }
    }

    /**
     * Tests that pending request is removed if task executor reports a slot with its allocation id.
     */
    @Test
    public void testSlotRequestRemovedIfTMReportAllocation() throws Exception {
        try (final SlotManagerImpl slotManager =
                createSlotManager(
                        ResourceManagerId.generate(),
                        new TestingResourceActionsBuilder().build())) {

            final JobID jobID = new JobID();
            final SlotRequest slotRequest1 =
                    new SlotRequest(jobID, new AllocationID(), ResourceProfile.UNKNOWN, "foobar");
            slotManager.registerSlotRequest(slotRequest1);

            final BlockingQueue<
                            Tuple6<
                                    SlotID,
                                    JobID,
                                    AllocationID,
                                    ResourceProfile,
                                    String,
                                    ResourceManagerId>>
                    requestSlotQueue = new ArrayBlockingQueue<>(1);
            final BlockingQueue<CompletableFuture<Acknowledge>> responseQueue =
                    new ArrayBlockingQueue<>(1);

            final TestingTaskExecutorGateway testingTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setRequestSlotFunction(
                                    slotIDJobIDAllocationIDStringResourceManagerIdTuple6 -> {
                                        requestSlotQueue.offer(
                                                slotIDJobIDAllocationIDStringResourceManagerIdTuple6);
                                        try {
                                            return responseQueue.take();
                                        } catch (InterruptedException ignored) {
                                            return FutureUtils.completedExceptionally(
                                                    new FlinkException(
                                                            "Response queue was interrupted."));
                                        }
                                    })
                            .createTestingTaskExecutorGateway();

            final ResourceID taskExecutorResourceId = ResourceID.generate();
            final TaskExecutorConnection taskExecutionConnection =
                    new TaskExecutorConnection(taskExecutorResourceId, testingTaskExecutorGateway);
            final SlotReport slotReport =
                    new SlotReport(
                            createEmptySlotStatus(
                                    new SlotID(taskExecutorResourceId, 0), ResourceProfile.ANY));

            final CompletableFuture<Acknowledge> firstManualSlotRequestResponse =
                    new CompletableFuture<>();
            responseQueue.offer(firstManualSlotRequestResponse);

            slotManager.registerTaskManager(
                    taskExecutionConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>
                    firstRequest = requestSlotQueue.take();

            final CompletableFuture<Acknowledge> secondManualSlotRequestResponse =
                    new CompletableFuture<>();
            responseQueue.offer(secondManualSlotRequestResponse);

            final SlotRequest slotRequest2 =
                    new SlotRequest(jobID, new AllocationID(), ResourceProfile.UNKNOWN, "foobar");
            slotManager.registerSlotRequest(slotRequest2);

            // fail first request
            firstManualSlotRequestResponse.completeExceptionally(
                    new TimeoutException("Test exception to fail first allocation"));

            final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>
                    secondRequest = requestSlotQueue.take();

            // fail second request
            secondManualSlotRequestResponse.completeExceptionally(
                    new SlotOccupiedException(
                            "Test exception", slotRequest1.getAllocationId(), jobID));

            assertThat(firstRequest.f2, equalTo(slotRequest1.getAllocationId()));
            assertThat(secondRequest.f2, equalTo(slotRequest2.getAllocationId()));
            assertThat(secondRequest.f0, equalTo(firstRequest.f0));

            secondManualSlotRequestResponse.complete(Acknowledge.get());

            final TaskManagerSlot slot = slotManager.getSlot(secondRequest.f0);
            assertThat(slot.getState(), equalTo(SlotState.ALLOCATED));
            assertThat(slot.getAllocationId(), equalTo(firstRequest.f2));

            assertThat(slotManager.getNumberRegisteredSlots(), is(1));
        }
    }

    /** Tests notify the job manager of the allocations when the task manager is failed/killed. */
    @Test
    public void testNotifyFailedAllocationWhenTaskManagerTerminated() throws Exception {

        final Queue<Tuple2<JobID, AllocationID>> allocationFailures = new ArrayDeque<>(5);

        final TestingResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setNotifyAllocationFailureConsumer(
                                (Tuple3<JobID, AllocationID, Exception> failureMessage) ->
                                        allocationFailures.offer(
                                                Tuple2.of(failureMessage.f0, failureMessage.f1)))
                        .build();

        try (final SlotManager slotManager =
                createSlotManager(ResourceManagerId.generate(), resourceManagerActions)) {

            // register slot request for job1.
            JobID jobId1 = new JobID();
            final SlotRequest slotRequest11 = createSlotRequest(jobId1);
            final SlotRequest slotRequest12 = createSlotRequest(jobId1);
            slotManager.registerSlotRequest(slotRequest11);
            slotManager.registerSlotRequest(slotRequest12);

            // create task-manager-1 with 2 slots.
            final ResourceID taskExecutorResourceId1 = ResourceID.generate();
            final TestingTaskExecutorGateway testingTaskExecutorGateway1 =
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
            final TaskExecutorConnection taskExecutionConnection1 =
                    new TaskExecutorConnection(
                            taskExecutorResourceId1, testingTaskExecutorGateway1);
            final SlotReport slotReport1 = createSlotReport(taskExecutorResourceId1, 2);

            // register the task-manager-1 to the slot manager, this will trigger the slot
            // allocation for job1.
            slotManager.registerTaskManager(
                    taskExecutionConnection1,
                    slotReport1,
                    ResourceProfile.ANY,
                    ResourceProfile.ANY);

            // register slot request for job2.
            JobID jobId2 = new JobID();
            final SlotRequest slotRequest21 = createSlotRequest(jobId2);
            final SlotRequest slotRequest22 = createSlotRequest(jobId2);
            slotManager.registerSlotRequest(slotRequest21);
            slotManager.registerSlotRequest(slotRequest22);

            // register slot request for job3.
            JobID jobId3 = new JobID();
            final SlotRequest slotRequest31 = createSlotRequest(jobId3);
            slotManager.registerSlotRequest(slotRequest31);

            // create task-manager-2 with 3 slots.
            final ResourceID taskExecutorResourceId2 = ResourceID.generate();
            final TestingTaskExecutorGateway testingTaskExecutorGateway2 =
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
            final TaskExecutorConnection taskExecutionConnection2 =
                    new TaskExecutorConnection(
                            taskExecutorResourceId2, testingTaskExecutorGateway2);
            final SlotReport slotReport2 = createSlotReport(taskExecutorResourceId2, 3);

            // register the task-manager-2 to the slot manager, this will trigger the slot
            // allocation for job2 and job3.
            slotManager.registerTaskManager(
                    taskExecutionConnection2,
                    slotReport2,
                    ResourceProfile.ANY,
                    ResourceProfile.ANY);

            // validate for job1.
            slotManager.unregisterTaskManager(
                    taskExecutionConnection1.getInstanceID(), TEST_EXCEPTION);

            assertThat(allocationFailures, hasSize(2));

            Tuple2<JobID, AllocationID> allocationFailure;
            final Set<AllocationID> failedAllocations = new HashSet<>(2);

            while ((allocationFailure = allocationFailures.poll()) != null) {
                assertThat(allocationFailure.f0, equalTo(jobId1));
                failedAllocations.add(allocationFailure.f1);
            }

            assertThat(
                    failedAllocations,
                    containsInAnyOrder(
                            slotRequest11.getAllocationId(), slotRequest12.getAllocationId()));

            // validate the result for job2 and job3.
            slotManager.unregisterTaskManager(
                    taskExecutionConnection2.getInstanceID(), TEST_EXCEPTION);

            assertThat(allocationFailures, hasSize(3));

            Map<JobID, List<Tuple2<JobID, AllocationID>>> job2AndJob3FailedAllocationInfo =
                    allocationFailures.stream().collect(Collectors.groupingBy(tuple -> tuple.f0));

            assertThat(job2AndJob3FailedAllocationInfo.entrySet(), hasSize(2));

            final Set<AllocationID> job2FailedAllocations =
                    extractFailedAllocationsForJob(jobId2, job2AndJob3FailedAllocationInfo);
            final Set<AllocationID> job3FailedAllocations =
                    extractFailedAllocationsForJob(jobId3, job2AndJob3FailedAllocationInfo);

            assertThat(
                    job2FailedAllocations,
                    containsInAnyOrder(
                            slotRequest21.getAllocationId(), slotRequest22.getAllocationId()));
            assertThat(job3FailedAllocations, containsInAnyOrder(slotRequest31.getAllocationId()));
        }
    }

    private Set<AllocationID> extractFailedAllocationsForJob(
            JobID jobId2,
            Map<JobID, List<Tuple2<JobID, AllocationID>>> job2AndJob3FailedAllocationInfo) {
        return job2AndJob3FailedAllocationInfo.get(jobId2).stream()
                .map(t -> t.f1)
                .collect(Collectors.toSet());
    }

    @Nonnull
    private SlotReport createSlotReport(ResourceID taskExecutorResourceId, int numberSlots) {
        return createSlotReport(
                taskExecutorResourceId,
                numberSlots,
                ResourceProfile.ANY,
                SlotManagerImplTest::createEmptySlotStatus);
    }

    @Nonnull
    private SlotReport createSlotReport(
            ResourceID taskExecutorResourceId,
            int numberSlots,
            ResourceProfile resourceProfile,
            BiFunction<SlotID, ResourceProfile, SlotStatus> slotStatusFactory) {
        final Set<SlotStatus> slotStatusSet = new HashSet<>(numberSlots);
        for (int i = 0; i < numberSlots; i++) {
            slotStatusSet.add(
                    slotStatusFactory.apply(
                            new SlotID(taskExecutorResourceId, i), resourceProfile));
        }

        return new SlotReport(slotStatusSet);
    }

    private static SlotStatus createEmptySlotStatus(
            SlotID slotId, ResourceProfile resourceProfile) {
        return new SlotStatus(slotId, resourceProfile);
    }

    @Nonnull
    private SlotRequest createSlotRequest(JobID jobId) {
        return createSlotRequest(jobId, ResourceProfile.UNKNOWN);
    }

    @Nonnull
    private SlotRequest createSlotRequest(JobID jobId, ResourceProfile resourceProfile) {
        return new SlotRequest(jobId, new AllocationID(), resourceProfile, "foobar1");
    }

    private SlotManagerImpl createSlotManager(
            ResourceManagerId resourceManagerId, ResourceActions resourceManagerActions) {
        return createSlotManager(resourceManagerId, resourceManagerActions, 1);
    }

    private SlotManagerImpl createSlotManager(
            ResourceManagerId resourceManagerId,
            ResourceActions resourceManagerActions,
            int numSlotsPerWorker) {
        SlotManagerImpl slotManager =
                createSlotManagerBuilder()
                        .setNumSlotsPerWorker(numSlotsPerWorker)
                        .setRedundantTaskManagerNum(0)
                        .buildAndStartWithDirectExec(resourceManagerId, resourceManagerActions);
        return slotManager;
    }

    private SlotManagerBuilder createSlotManagerBuilder() {
        return SlotManagerBuilder.newBuilder().setDefaultWorkerResourceSpec(WORKER_RESOURCE_SPEC);
    }

    /**
     * Tests that we only request new resources/containers once we have assigned all pending task
     * manager slots.
     */
    @Test
    public void testRequestNewResources() throws Exception {
        final int numberSlots = 2;
        final AtomicInteger resourceRequests = new AtomicInteger(0);
        final TestingResourceActions testingResourceActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceFunction(
                                ignored -> {
                                    resourceRequests.incrementAndGet();
                                    return true;
                                })
                        .build();

        try (final SlotManagerImpl slotManager =
                createSlotManager(
                        ResourceManagerId.generate(), testingResourceActions, numberSlots)) {

            final JobID jobId = new JobID();
            assertThat(slotManager.registerSlotRequest(createSlotRequest(jobId)), is(true));
            assertThat(resourceRequests.get(), is(1));

            // the second slot request should not try to allocate a new resource because the
            // previous resource was started with 2 slots.
            assertThat(slotManager.registerSlotRequest(createSlotRequest(jobId)), is(true));
            assertThat(resourceRequests.get(), is(1));

            assertThat(slotManager.getNumberAssignedPendingTaskManagerSlots(), is(2));

            assertThat(slotManager.registerSlotRequest(createSlotRequest(jobId)), is(true));
            assertThat(resourceRequests.get(), is(2));
        }
    }

    /** Tests that a failing allocation/slot request will return the pending task manager slot. */
    @Test
    public void testFailingAllocationReturnsPendingTaskManagerSlot() throws Exception {
        final int numberSlots = 2;
        final TestingResourceActions resourceActions = new TestingResourceActionsBuilder().build();
        try (final SlotManagerImpl slotManager =
                createSlotManager(ResourceManagerId.generate(), resourceActions, numberSlots)) {
            final JobID jobId = new JobID();

            final SlotRequest slotRequest = createSlotRequest(jobId);
            assertThat(slotManager.registerSlotRequest(slotRequest), is(true));

            assertThat(slotManager.getNumberPendingTaskManagerSlots(), is(numberSlots));
            assertThat(slotManager.getNumberAssignedPendingTaskManagerSlots(), is(1));

            slotManager.unregisterSlotRequest(slotRequest.getAllocationId());

            assertThat(slotManager.getNumberPendingTaskManagerSlots(), is(numberSlots));
            assertThat(slotManager.getNumberAssignedPendingTaskManagerSlots(), is(0));
        }
    }

    /** Tests the completion of pending task manager slots by registering a TaskExecutor. */
    @Test
    public void testPendingTaskManagerSlotCompletion() throws Exception {
        final int numberSlots = 3;
        final TestingResourceActions resourceActions = new TestingResourceActionsBuilder().build();
        final ResourceProfile resourceProfile =
                SlotManagerImpl.generateDefaultSlotResourceProfile(
                        WORKER_RESOURCE_SPEC, numberSlots);

        try (final SlotManagerImpl slotManager =
                createSlotManager(ResourceManagerId.generate(), resourceActions, numberSlots)) {
            final JobID jobId = new JobID();
            assertThat(
                    slotManager.registerSlotRequest(createSlotRequest(jobId, resourceProfile)),
                    is(true));

            assertThat(slotManager.getNumberPendingTaskManagerSlots(), is(numberSlots));
            assertThat(slotManager.getNumberAssignedPendingTaskManagerSlots(), is(1));
            assertThat(slotManager.getNumberRegisteredSlots(), is(0));

            final TaskExecutorConnection taskExecutorConnection = createTaskExecutorConnection();
            final SlotReport slotReport =
                    createSlotReport(
                            taskExecutorConnection.getResourceID(),
                            numberSlots - 1,
                            resourceProfile,
                            SlotManagerImplTest::createEmptySlotStatus);

            slotManager.registerTaskManager(
                    taskExecutorConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            assertThat(slotManager.getNumberRegisteredSlots(), is(numberSlots - 1));
            assertThat(slotManager.getNumberPendingTaskManagerSlots(), is(1));
        }
    }

    private TaskExecutorConnection createTaskExecutorConnection() {
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        return createTaskExecutorConnection(taskExecutorGateway);
    }

    private TaskExecutorConnection createTaskExecutorConnection(
            TaskExecutorGateway taskExecutorGateway) {
        return new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
    }

    /**
     * Tests that a different slot can fulfill a pending slot request. If the pending slot request
     * has a pending task manager slot assigned, it should be freed.
     */
    @Test
    public void testRegistrationOfDifferentSlot() throws Exception {
        final int numberSlots = 1;
        final TestingResourceActions resourceActions = new TestingResourceActionsBuilder().build();

        try (final SlotManagerImpl slotManager =
                createSlotManager(ResourceManagerId.generate(), resourceActions, numberSlots)) {
            final JobID jobId = new JobID();
            final ResourceProfile requestedSlotProfile = ResourceProfile.fromResources(1.0, 1);

            assertThat(
                    slotManager.registerSlotRequest(createSlotRequest(jobId, requestedSlotProfile)),
                    is(true));

            assertThat(slotManager.getNumberPendingTaskManagerSlots(), is(numberSlots));

            final int numberOfferedSlots = 1;
            final TaskExecutorConnection taskExecutorConnection = createTaskExecutorConnection();
            final ResourceProfile offeredSlotProfile = ResourceProfile.fromResources(2.0, 2);
            final SlotReport slotReport =
                    createSlotReport(
                            taskExecutorConnection.getResourceID(),
                            numberOfferedSlots,
                            offeredSlotProfile,
                            SlotManagerImplTest::createEmptySlotStatus);

            slotManager.registerTaskManager(
                    taskExecutorConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            assertThat(slotManager.getNumberRegisteredSlots(), is(numberOfferedSlots));
            assertThat(slotManager.getNumberPendingTaskManagerSlots(), is(numberSlots));
            assertThat(slotManager.getNumberAssignedPendingTaskManagerSlots(), is(0));
        }
    }

    /** Tests that only free slots can fulfill/complete a pending task manager slot. */
    @Test
    public void testOnlyFreeSlotsCanFulfillPendingTaskManagerSlot() throws Exception {
        final int numberSlots = 1;
        final TestingResourceActions resourceActions = new TestingResourceActionsBuilder().build();

        try (final SlotManagerImpl slotManager =
                createSlotManager(ResourceManagerId.generate(), resourceActions, numberSlots)) {
            final JobID jobId = new JobID();
            assertThat(slotManager.registerSlotRequest(createSlotRequest(jobId)), is(true));

            final TaskExecutorConnection taskExecutorConnection = createTaskExecutorConnection();
            final SlotID slotId = new SlotID(taskExecutorConnection.getResourceID(), 0);
            final SlotStatus slotStatus =
                    new SlotStatus(slotId, ResourceProfile.ANY, jobId, new AllocationID());
            final SlotReport slotReport = new SlotReport(slotStatus);

            slotManager.registerTaskManager(
                    taskExecutorConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            assertThat(slotManager.getNumberRegisteredSlots(), is(1));
            assertThat(slotManager.getNumberPendingTaskManagerSlots(), is(numberSlots));
            assertThat(slotManager.getNumberAssignedPendingTaskManagerSlots(), is(1));
        }
    }

    /** Tests that the unregister cause is being forwarded when failing allocations. */
    @Test
    public void unregisterTaskManager_withAllocatedSlot_failsAllocationsWithCause()
            throws Exception {
        CompletableFuture<Exception> allocationFailureCause = new CompletableFuture<>();
        TestingResourceActions resourceActions =
                new TestingResourceActionsBuilder()
                        .setNotifyAllocationFailureConsumer(
                                jobIDAllocationIDExceptionTuple3 ->
                                        allocationFailureCause.complete(
                                                jobIDAllocationIDExceptionTuple3.f2))
                        .build();

        FlinkException failureCause = new FlinkException("unregisterTaskManager test exception.");

        try (SlotManagerImpl slotManager =
                createSlotManager(ResourceManagerId.generate(), resourceActions)) {
            TaskExecutorConnection taskExecutorConnection = createTaskExecutorConnection();
            SlotReport slotReport =
                    createSingleAllocatedSlotReport(
                            taskExecutorConnection.getResourceID(), new JobID());
            slotManager.registerTaskManager(
                    taskExecutorConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);
            slotManager.unregisterTaskManager(taskExecutorConnection.getInstanceID(), failureCause);

            assertThat(allocationFailureCause.get(), FlinkMatchers.containsCause(failureCause));
        }
    }

    private SlotReport createSingleAllocatedSlotReport(ResourceID resourceID, JobID jobId) {
        return createSlotReport(
                resourceID,
                1,
                ResourceProfile.ANY,
                (slotId, resourceProfile) ->
                        new SlotStatus(slotId, resourceProfile, jobId, new AllocationID()));
    }

    /**
     * The spread out slot allocation strategy should spread out the allocated slots across all
     * available TaskExecutors. See FLINK-12122.
     */
    @Test
    public void testSpreadOutSlotAllocationStrategy() throws Exception {
        try (SlotManagerImpl slotManager =
                createSlotManagerBuilder()
                        .setSlotMatchingStrategy(LeastUtilizationSlotMatchingStrategy.INSTANCE)
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(),
                                new TestingResourceActionsBuilder().build())) {

            final List<CompletableFuture<JobID>> requestSlotFutures = new ArrayList<>();

            final int numberTaskExecutors = 5;

            // register n TaskExecutors with 2 slots each
            for (int i = 0; i < numberTaskExecutors; i++) {
                final CompletableFuture<JobID> requestSlotFuture = new CompletableFuture<>();
                requestSlotFutures.add(requestSlotFuture);
                registerTaskExecutorWithTwoSlots(slotManager, requestSlotFuture);
            }

            final JobID jobId = new JobID();

            // request n slots
            for (int i = 0; i < numberTaskExecutors; i++) {
                assertTrue(slotManager.registerSlotRequest(createSlotRequest(jobId)));
            }

            // check that every TaskExecutor has received a slot request
            final Set<JobID> jobIds =
                    new HashSet<>(
                            FutureUtils.combineAll(requestSlotFutures).get(10L, TimeUnit.SECONDS));
            assertThat(jobIds, hasSize(1));
            assertThat(jobIds, containsInAnyOrder(jobId));
        }
    }

    /**
     * Test that the slot manager respect the max limitation of the number of slots when allocate
     * new resource.
     */
    @Test
    public void testMaxSlotLimitAllocateResource() throws Exception {
        final int numberSlots = 1;
        final int maxSlotNum = 1;

        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final JobID jobId = new JobID();

        final AtomicInteger resourceRequests = new AtomicInteger(0);
        ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceFunction(
                                ignored -> {
                                    resourceRequests.incrementAndGet();
                                    return true;
                                })
                        .build();

        try (SlotManagerImpl slotManager =
                createSlotManagerBuilder()
                        .setNumSlotsPerWorker(numberSlots)
                        .setMaxSlotNum(maxSlotNum)
                        .buildAndStartWithDirectExec(resourceManagerId, resourceManagerActions)) {

            assertTrue(
                    "The slot request should be accepted",
                    slotManager.registerSlotRequest(createSlotRequest(jobId)));
            assertThat(resourceRequests.get(), is(1));

            // The second slot request should not try to allocate a new resource because of the max
            // limitation.
            assertTrue(
                    "The slot request should be accepted",
                    slotManager.registerSlotRequest(createSlotRequest(jobId)));
            assertThat(resourceRequests.get(), is(1));
        }
    }

    /**
     * Test that the slot manager release resource when the number of slots exceed max limit when
     * new TaskExecutor registered.
     */
    @Test
    public void testMaxSlotLimitRegisterResource() throws Exception {
        final int numberSlots = 1;
        final int maxSlotNum = 1;
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();

        final CompletableFuture<InstanceID> releasedResourceFuture = new CompletableFuture<>();
        ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setReleaseResourceConsumer(
                                (instanceID, e) -> releasedResourceFuture.complete(instanceID))
                        .build();

        final TaskExecutorGateway taskExecutorGateway1 =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final TaskExecutorGateway taskExecutorGateway2 =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final ResourceID resourceId1 = ResourceID.generate();
        final ResourceID resourceId2 = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection1 =
                new TaskExecutorConnection(resourceId1, taskExecutorGateway1);
        final TaskExecutorConnection taskManagerConnection2 =
                new TaskExecutorConnection(resourceId2, taskExecutorGateway2);

        final SlotID slotId1 = new SlotID(resourceId1, 0);
        final SlotID slotId2 = new SlotID(resourceId1, 0);
        final SlotStatus slotStatus1 = new SlotStatus(slotId1, ResourceProfile.UNKNOWN);
        final SlotStatus slotStatus2 = new SlotStatus(slotId2, ResourceProfile.UNKNOWN);
        final SlotReport slotReport1 = new SlotReport(Collections.singletonList(slotStatus1));
        final SlotReport slotReport2 = new SlotReport(Collections.singletonList(slotStatus2));

        try (SlotManagerImpl slotManager =
                createSlotManagerBuilder()
                        .setNumSlotsPerWorker(numberSlots)
                        .setMaxSlotNum(maxSlotNum)
                        .setRedundantTaskManagerNum(0)
                        .buildAndStartWithDirectExec(resourceManagerId, resourceManagerActions)) {
            slotManager.registerTaskManager(
                    taskManagerConnection1, slotReport1, ResourceProfile.ANY, ResourceProfile.ANY);
            slotManager.registerTaskManager(
                    taskManagerConnection2, slotReport2, ResourceProfile.ANY, ResourceProfile.ANY);

            assertThat(
                    "The number registered slots does not equal the expected number.",
                    slotManager.getNumberRegisteredSlots(),
                    is(1));
            assertNotNull(slotManager.getSlot(slotId1));

            // The second registered task manager should be released.
            assertThat(
                    releasedResourceFuture.get(),
                    is(equalTo(taskManagerConnection2.getInstanceID())));
        }
    }

    private void registerTaskExecutorWithTwoSlots(
            SlotManagerImpl slotManager, CompletableFuture<JobID> firstRequestSlotFuture) {
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                slotIDJobIDAllocationIDStringResourceManagerIdTuple6 -> {
                                    firstRequestSlotFuture.complete(
                                            slotIDJobIDAllocationIDStringResourceManagerIdTuple6
                                                    .f1);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection firstTaskExecutorConnection =
                createTaskExecutorConnection(taskExecutorGateway);
        final SlotReport firstSlotReport =
                createSlotReport(firstTaskExecutorConnection.getResourceID(), 2);
        slotManager.registerTaskManager(
                firstTaskExecutorConnection,
                firstSlotReport,
                ResourceProfile.ANY,
                ResourceProfile.ANY);
    }

    @Test
    public void testGenerateDefaultSlotProfile() {
        final int numSlots = 5;
        final ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(1)
                        .setTaskOffHeapMemoryMB(2)
                        .setNetworkMemoryMB(3)
                        .setManagedMemoryMB(4)
                        .build();
        final WorkerResourceSpec workerResourceSpec =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0 * numSlots)
                        .setTaskHeapMemoryMB(1 * numSlots)
                        .setTaskOffHeapMemoryMB(2 * numSlots)
                        .setNetworkMemoryMB(3 * numSlots)
                        .setManagedMemoryMB(4 * numSlots)
                        .build();

        assertThat(
                SlotManagerImpl.generateDefaultSlotResourceProfile(workerResourceSpec, numSlots),
                is(resourceProfile));
    }
}
