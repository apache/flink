/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.runtime.jobmaster.slotpool.PreferredAllocationRequestSlotMatchingStrategyTest.createSlot;
import static org.assertj.core.api.Assertions.assertThat;

/** Testing for {@link TasksBalancedRequestSlotMatchingStrategy}. */
class TasksBalancedRequestSlotMatchingStrategyTest {

    private static final ResourceProfile smallFineGrainedProfile =
            ResourceProfile.newBuilder().setCpuCores(1d).build();
    private static final ResourceProfile bigFineGrainedProfile =
            ResourceProfile.newBuilder().setCpuCores(2d).build();

    private static final TaskManagerLocation tmLocation1 = new LocalTaskManagerLocation();
    private static final TaskManagerLocation tmLocation2 = new LocalTaskManagerLocation();

    @Test
    void testMatchRequestsAndSlotsRiskOfFineGrainedResourcesMatchedToUnknownProfile() {
        // The case is aiming to check when the numbers of requests and resources are equals but
        // having the risk of matching resources that would be matched with fine-grained request
        // with ResourceProfile>UNKNOWN.
        final Collection<PendingRequest> pendingRequests =
                Arrays.asList(
                        createRequest(ResourceProfile.UNKNOWN, 100),
                        createRequest(bigFineGrainedProfile, 1));
        List<TestingPhysicalSlot> slots =
                Arrays.asList(
                        createSlot(bigFineGrainedProfile, new AllocationID(), tmLocation1),
                        createSlot(smallFineGrainedProfile, new AllocationID(), tmLocation2));
        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                TasksBalancedRequestSlotMatchingStrategy.INSTANCE.matchRequestsAndSlots(
                        slots,
                        pendingRequests,
                        new HashMap<>() {
                            {
                                put(tmLocation1.getResourceID(), DefaultLoadingWeight.EMPTY);
                                put(tmLocation2.getResourceID(), new DefaultLoadingWeight(9));
                            }
                        });
        assertThat(requestSlotMatches).hasSize(2);
    }

    @Test
    void testMatchRequestsAndSlotsMissingFineGrainedResources() {

        PendingRequest requestWithBigProfile = createRequest(bigFineGrainedProfile, 6);
        PendingRequest requestWithUnknownProfile = createRequest(ResourceProfile.UNKNOWN, 6);
        PendingRequest requestWithSmallProfile = createRequest(smallFineGrainedProfile, 6);

        final Collection<PendingRequest> pendingRequests =
                Arrays.asList(
                        requestWithSmallProfile, requestWithUnknownProfile, requestWithBigProfile);
        List<TestingPhysicalSlot> slots =
                Arrays.asList(
                        createSlot(
                                bigFineGrainedProfile,
                                new AllocationID(),
                                new LocalTaskManagerLocation()),
                        createSlot(
                                bigFineGrainedProfile,
                                new AllocationID(),
                                new LocalTaskManagerLocation()),
                        createSlot(
                                bigFineGrainedProfile,
                                new AllocationID(),
                                new LocalTaskManagerLocation()));
        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                TasksBalancedRequestSlotMatchingStrategy.INSTANCE.matchRequestsAndSlots(
                        slots, pendingRequests, new HashMap<>());
        assertThat(requestSlotMatches).isEmpty();
    }

    private static PendingRequest createRequest(ResourceProfile requestProfile, float loading) {
        return PendingRequest.createNormalRequest(
                new SlotRequestId(),
                requestProfile,
                new DefaultLoadingWeight(loading),
                Collections.emptyList());
    }
}
