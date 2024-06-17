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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PhysicalSlotProviderImpl} using {@link
 * DefaultLocationPreferenceSlotSelectionStrategy}.
 */
class PhysicalSlotProviderImplWithDefaultSlotSelectionStrategyTest {

    @RegisterExtension
    private final PhysicalSlotProviderExtension physicalSlotProviderExtension =
            new PhysicalSlotProviderExtension(
                    LocationPreferenceSlotSelectionStrategy.createDefault());

    @Test
    void testSlotAllocationFulfilledWithAvailableSlots()
            throws InterruptedException, ExecutionException {
        PhysicalSlotRequest request = physicalSlotProviderExtension.createSimpleRequest();
        physicalSlotProviderExtension.registerSlotOffersFromNewTaskExecutor(ResourceProfile.ANY);
        CompletableFuture<PhysicalSlotRequest.Result> slotFuture =
                physicalSlotProviderExtension.allocateSlot(request);
        PhysicalSlotRequest.Result result = slotFuture.get();
        assertThat(result.getSlotRequestId()).isEqualTo(request.getSlotRequestId());
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
                        LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
        slotProvider.disableBatchSlotRequestTimeoutCheck();
        assertThat(slotPool.isBatchSlotRequestTimeoutCheckEnabled()).isFalse();
    }
}
