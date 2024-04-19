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
import org.apache.flink.runtime.clusterframework.types.SlotProfileTestingUtils;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PhysicalSlotProviderImpl} using {@link
 * EvenlySpreadOutLocationPreferenceSlotSelectionStrategy}.
 */
class PhysicalSlotProviderImplWithSpreadOutStrategyTest {

    @RegisterExtension
    private PhysicalSlotProviderExtension physicalSlotProviderExtension =
            new PhysicalSlotProviderExtension(
                    LocationPreferenceSlotSelectionStrategy.createEvenlySpreadOut());

    @Test
    void testSlotAllocationFulfilledWithWorkloadSpreadOut()
            throws InterruptedException, ExecutionException {
        physicalSlotProviderExtension.registerSlotOffersFromNewTaskExecutor(
                ResourceProfile.ANY, ResourceProfile.ANY, ResourceProfile.ANY, ResourceProfile.ANY);
        physicalSlotProviderExtension.registerSlotOffersFromNewTaskExecutor(
                ResourceProfile.ANY, ResourceProfile.ANY, ResourceProfile.ANY, ResourceProfile.ANY);

        PhysicalSlotRequest request0 = physicalSlotProviderExtension.createSimpleRequest();
        PhysicalSlotRequest request1 = physicalSlotProviderExtension.createSimpleRequest();

        PhysicalSlotRequest.Result result0 =
                physicalSlotProviderExtension.allocateSlot(request0).get();
        PhysicalSlotRequest.Result result1 =
                physicalSlotProviderExtension.allocateSlot(request1).get();

        assertThat(result0.getPhysicalSlot().getTaskManagerLocation())
                .isNotEqualTo(result1.getPhysicalSlot().getTaskManagerLocation());
    }

    @Test
    void testSlotAllocationFulfilledWithPreferredInputOverwrittingSpreadOut()
            throws ExecutionException, InterruptedException {
        physicalSlotProviderExtension.registerSlotOffersFromNewTaskExecutor(
                ResourceProfile.ANY, ResourceProfile.ANY);
        physicalSlotProviderExtension.registerSlotOffersFromNewTaskExecutor(
                ResourceProfile.ANY, ResourceProfile.ANY);

        PhysicalSlotRequest request0 = physicalSlotProviderExtension.createSimpleRequest();
        PhysicalSlotRequest.Result result0 =
                physicalSlotProviderExtension.allocateSlot(request0).get();
        TaskManagerLocation preferredTaskManagerLocation =
                result0.getPhysicalSlot().getTaskManagerLocation();

        PhysicalSlotRequest request1 =
                new PhysicalSlotRequest(
                        new SlotRequestId(),
                        SlotProfileTestingUtils.preferredLocality(
                                ResourceProfile.ANY,
                                Collections.singleton(preferredTaskManagerLocation)),
                        false);
        PhysicalSlotRequest.Result result1 =
                physicalSlotProviderExtension.allocateSlot(request1).get();

        assertThat(result1.getPhysicalSlot().getTaskManagerLocation())
                .isEqualTo(preferredTaskManagerLocation);
    }

    @Test
    void testSlotAllocationFulfilledWithNewSlots() throws ExecutionException, InterruptedException {
        final CompletableFuture<PhysicalSlotRequest.Result> slotFuture =
                physicalSlotProviderExtension.allocateSlot(
                        physicalSlotProviderExtension.createSimpleRequest());
        assertThatFuture(slotFuture).isNotDone();
        physicalSlotProviderExtension.registerSlotOffersFromNewTaskExecutor(ResourceProfile.ANY);
        slotFuture.get();
    }

    @Test
    void testIndividualBatchSlotRequestTimeoutCheckIsDisabledOnAllocatingNewSlots()
            throws Exception {
        DeclarativeSlotPoolBridge slotPool =
                new DeclarativeSlotPoolBridgeBuilder()
                        .buildAndStart(physicalSlotProviderExtension.getMainThreadExecutor());
        assertThat(slotPool.isBatchSlotRequestTimeoutCheckEnabled()).isTrue();

        final PhysicalSlotProvider slotProvider =
                new PhysicalSlotProviderImpl(
                        LocationPreferenceSlotSelectionStrategy.createEvenlySpreadOut(), slotPool);
        slotProvider.disableBatchSlotRequestTimeoutCheck();
        assertThat(slotPool.isBatchSlotRequestTimeoutCheckEnabled()).isFalse();
    }
}
