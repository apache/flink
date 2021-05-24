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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.exceptions.UnfulfillableSlotRequestException;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for setting the SlotManager to eagerly fail unfulfillable requests. */
public class SlotManagerFailUnfulfillableTest extends TestLogger {

    private static final WorkerResourceSpec WORKER_RESOURCE_SPEC =
            new WorkerResourceSpec.Builder()
                    .setCpuCores(100.0)
                    .setTaskHeapMemoryMB(10000)
                    .setTaskOffHeapMemoryMB(10000)
                    .setNetworkMemoryMB(10000)
                    .setManagedMemoryMB(10000)
                    .build();

    @Test
    public void testTurnOnKeepsPendingFulfillableRequests() throws Exception {
        // setup
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(2.0, 100);

        final SlotManager slotManager = createSlotManagerNotStartingNewTMs();
        slotManager.setFailUnfulfillableRequest(false);
        registerFreeSlot(slotManager, resourceProfile);

        slotManager.registerSlotRequest(slotRequest(resourceProfile));
        slotManager.registerSlotRequest(slotRequest(resourceProfile));

        // test
        slotManager.setFailUnfulfillableRequest(true);

        // assert
        assertEquals(1, slotManager.getNumberPendingSlotRequests());
    }

    @Test
    public void testTurnOnCancelsPendingUnFulfillableRequests() throws Exception {
        // setup
        final ResourceProfile availableProfile = ResourceProfile.fromResources(2.0, 100);
        final ResourceProfile unfulfillableProfile = ResourceProfile.fromResources(1.0, 200);

        final List<Tuple3<JobID, AllocationID, Exception>> allocationFailures = new ArrayList<>();
        final SlotManager slotManager = createSlotManagerNotStartingNewTMs(allocationFailures);
        slotManager.setFailUnfulfillableRequest(false);
        registerFreeSlot(slotManager, availableProfile);

        // test
        final SlotRequest request = slotRequest(unfulfillableProfile);
        slotManager.registerSlotRequest(request);
        slotManager.setFailUnfulfillableRequest(true);

        // assert
        assertEquals(1, allocationFailures.size());
        assertEquals(request.getAllocationId(), allocationFailures.get(0).f1);
        assertTrue(
                ExceptionUtils.findThrowable(
                                allocationFailures.get(0).f2,
                                UnfulfillableSlotRequestException.class)
                        .isPresent());
        assertEquals(0, slotManager.getNumberPendingSlotRequests());
    }

    @Test
    public void testTurnOnKeepsRequestsWithStartingTMs() throws Exception {
        // setup
        final ResourceProfile availableProfile = ResourceProfile.fromResources(2.0, 100);
        final ResourceProfile newTmProfile =
                SlotManagerUtils.generateDefaultSlotResourceProfile(WORKER_RESOURCE_SPEC, 1);

        final SlotManager slotManager = createSlotManagerStartingNewTMs();
        slotManager.setFailUnfulfillableRequest(false);
        registerFreeSlot(slotManager, availableProfile);

        // test
        slotManager.registerSlotRequest(slotRequest(newTmProfile));
        slotManager.setFailUnfulfillableRequest(true);

        // assert
        assertEquals(1, slotManager.getNumberPendingSlotRequests());
    }

    @Test
    public void testFulfillableRequestsKeepPendingWhenOn() throws Exception {
        // setup
        final ResourceProfile availableProfile = ResourceProfile.fromResources(2.0, 100);

        final SlotManager slotManager = createSlotManagerNotStartingNewTMs();
        registerFreeSlot(slotManager, availableProfile);

        // test
        slotManager.registerSlotRequest(slotRequest(availableProfile));
        slotManager.registerSlotRequest(slotRequest(availableProfile));

        // assert
        assertEquals(1, slotManager.getNumberPendingSlotRequests());
    }

    @Test
    public void testUnfulfillableRequestsFailWhenOn() {
        // setup
        final ResourceProfile availableProfile = ResourceProfile.fromResources(2.0, 100);
        final ResourceProfile unfulfillableProfile = ResourceProfile.fromResources(2.0, 200);

        final List<Tuple3<JobID, AllocationID, Exception>> notifiedAllocationFailures =
                new ArrayList<>();
        final SlotManager slotManager =
                createSlotManagerNotStartingNewTMs(notifiedAllocationFailures);
        registerFreeSlot(slotManager, availableProfile);

        // test
        try {
            slotManager.registerSlotRequest(slotRequest(unfulfillableProfile));
            fail("this should cause an exception");
        } catch (ResourceManagerException exception) {
            assertTrue(
                    ExceptionUtils.findThrowable(exception, UnfulfillableSlotRequestException.class)
                            .isPresent());
        }

        // assert
        assertEquals(0, notifiedAllocationFailures.size());
        assertEquals(0, slotManager.getNumberPendingSlotRequests());
    }

    @Test
    public void testStartingTmKeepsSlotPendingWhenOn() throws Exception {
        // setup
        final ResourceProfile availableProfile = ResourceProfile.fromResources(2.0, 100);
        final ResourceProfile newTmProfile =
                SlotManagerUtils.generateDefaultSlotResourceProfile(WORKER_RESOURCE_SPEC, 1);

        final SlotManager slotManager = createSlotManagerStartingNewTMs();
        registerFreeSlot(slotManager, availableProfile);

        // test
        slotManager.registerSlotRequest(slotRequest(newTmProfile));

        // assert
        assertEquals(1, slotManager.getNumberPendingSlotRequests());
    }

    // ------------------------------------------------------------------------
    //  helper
    // ------------------------------------------------------------------------

    private static SlotManager createSlotManagerNotStartingNewTMs() {
        return createSlotManager(new ArrayList<>(), false);
    }

    private static SlotManager createSlotManagerNotStartingNewTMs(
            List<Tuple3<JobID, AllocationID, Exception>> notifiedAllocationFailures) {
        return createSlotManager(notifiedAllocationFailures, false);
    }

    private static SlotManager createSlotManagerStartingNewTMs() {
        return createSlotManager(new ArrayList<>(), true);
    }

    private static SlotManager createSlotManager(
            List<Tuple3<JobID, AllocationID, Exception>> notifiedAllocationFailures,
            boolean startNewTMs) {

        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceFunction(ignored -> startNewTMs)
                        .setNotifyAllocationFailureConsumer(
                                tuple3 -> notifiedAllocationFailures.add(tuple3))
                        .build();

        SlotManager slotManager =
                SlotManagerBuilder.newBuilder()
                        .setDefaultWorkerResourceSpec(WORKER_RESOURCE_SPEC)
                        .build();
        slotManager.start(
                ResourceManagerId.generate(), Executors.directExecutor(), resourceManagerActions);

        return slotManager;
    }

    private static void registerFreeSlot(SlotManager slotManager, ResourceProfile slotProfile) {
        final ResourceID resourceID = ResourceID.generate();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);

        final SlotReport slotReport =
                new SlotReport(
                        Collections.singleton(
                                new SlotStatus(new SlotID(resourceID, 0), slotProfile)));

        slotManager.registerTaskManager(
                taskExecutorConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);
    }

    private static SlotRequest slotRequest(ResourceProfile profile) {
        return new SlotRequest(new JobID(), new AllocationID(), profile, "foobar");
    }
}
