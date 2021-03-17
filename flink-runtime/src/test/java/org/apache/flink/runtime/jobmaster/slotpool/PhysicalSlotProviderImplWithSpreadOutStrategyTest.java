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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link PhysicalSlotProviderImpl} using {@link
 * EvenlySpreadOutLocationPreferenceSlotSelectionStrategy}.
 */
public class PhysicalSlotProviderImplWithSpreadOutStrategyTest {

    @Rule
    public PhysicalSlotProviderResource physicalSlotProviderResource =
            new PhysicalSlotProviderResource(
                    LocationPreferenceSlotSelectionStrategy.createEvenlySpreadOut());

    @Test
    public void testSlotAllocationFulfilledWithWorkloadSpreadOut()
            throws InterruptedException, ExecutionException {
        physicalSlotProviderResource.registerSlotOffersFromNewTaskExecutor(
                ResourceProfile.ANY, ResourceProfile.ANY, ResourceProfile.ANY, ResourceProfile.ANY);
        physicalSlotProviderResource.registerSlotOffersFromNewTaskExecutor(
                ResourceProfile.ANY, ResourceProfile.ANY, ResourceProfile.ANY, ResourceProfile.ANY);

        PhysicalSlotRequest request0 = physicalSlotProviderResource.createSimpleRequest();
        PhysicalSlotRequest request1 = physicalSlotProviderResource.createSimpleRequest();

        PhysicalSlotRequest.Result result0 =
                physicalSlotProviderResource.allocateSlot(request0).get();
        PhysicalSlotRequest.Result result1 =
                physicalSlotProviderResource.allocateSlot(request1).get();

        assertThat(
                result0.getPhysicalSlot().getTaskManagerLocation(),
                not(result1.getPhysicalSlot().getTaskManagerLocation()));
    }

    @Test
    public void testSlotAllocationFulfilledWithPreferredInputOverwrittingSpreadOut()
            throws ExecutionException, InterruptedException {
        physicalSlotProviderResource.registerSlotOffersFromNewTaskExecutor(
                ResourceProfile.ANY, ResourceProfile.ANY);
        physicalSlotProviderResource.registerSlotOffersFromNewTaskExecutor(
                ResourceProfile.ANY, ResourceProfile.ANY);

        PhysicalSlotRequest request0 = physicalSlotProviderResource.createSimpleRequest();
        PhysicalSlotRequest.Result result0 =
                physicalSlotProviderResource.allocateSlot(request0).get();
        TaskManagerLocation preferredTaskManagerLocation =
                result0.getPhysicalSlot().getTaskManagerLocation();

        PhysicalSlotRequest request1 =
                new PhysicalSlotRequest(
                        new SlotRequestId(),
                        SlotProfile.preferredLocality(
                                ResourceProfile.ANY,
                                Collections.singleton(preferredTaskManagerLocation)),
                        false);
        PhysicalSlotRequest.Result result1 =
                physicalSlotProviderResource.allocateSlot(request1).get();

        assertThat(
                result1.getPhysicalSlot().getTaskManagerLocation(),
                is(preferredTaskManagerLocation));
    }

    @Test
    public void testSlotAllocationFulfilledWithNewSlots()
            throws ExecutionException, InterruptedException {
        final CompletableFuture<PhysicalSlotRequest.Result> slotFuture =
                physicalSlotProviderResource.allocateSlot(
                        physicalSlotProviderResource.createSimpleRequest());
        assertThat(slotFuture.isDone(), is(false));
        physicalSlotProviderResource.registerSlotOffersFromNewTaskExecutor(ResourceProfile.ANY);
        slotFuture.get();
    }

    @Test
    public void testIndividualBatchSlotRequestTimeoutCheckIsDisabledOnAllocatingNewSlots()
            throws Exception {
        TestingSlotPoolImpl slotPool =
                new SlotPoolBuilder(physicalSlotProviderResource.getMainThreadExecutor()).build();
        assertThat(slotPool.isBatchSlotRequestTimeoutCheckEnabled(), is(true));

        new PhysicalSlotProviderImpl(
                LocationPreferenceSlotSelectionStrategy.createEvenlySpreadOut(), slotPool);
        assertThat(slotPool.isBatchSlotRequestTimeoutCheckEnabled(), is(false));
    }
}
