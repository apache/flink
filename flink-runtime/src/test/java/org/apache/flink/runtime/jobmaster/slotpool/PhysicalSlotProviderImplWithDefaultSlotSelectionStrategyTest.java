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
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link PhysicalSlotProviderImpl} using {@link
 * DefaultLocationPreferenceSlotSelectionStrategy}.
 */
public class PhysicalSlotProviderImplWithDefaultSlotSelectionStrategyTest extends TestLogger {

    @Rule
    public final PhysicalSlotProviderResource physicalSlotProviderResource =
            new PhysicalSlotProviderResource(
                    LocationPreferenceSlotSelectionStrategy.createDefault());

    @Test
    public void testSlotAllocationFulfilledWithAvailableSlots()
            throws InterruptedException, ExecutionException {
        PhysicalSlotRequest request = physicalSlotProviderResource.createSimpleRequest();
        physicalSlotProviderResource.registerSlotOffersFromNewTaskExecutor(ResourceProfile.ANY);
        CompletableFuture<PhysicalSlotRequest.Result> slotFuture =
                physicalSlotProviderResource.allocateSlot(request);
        PhysicalSlotRequest.Result result = slotFuture.get();
        assertThat(result.getSlotRequestId(), is(request.getSlotRequestId()));
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
        DeclarativeSlotPoolBridge slotPool =
                new DeclarativeSlotPoolBridgeBuilder()
                        .buildAndStart(physicalSlotProviderResource.getMainThreadExecutor());
        assertThat(slotPool.isBatchSlotRequestTimeoutCheckEnabled(), is(true));

        new PhysicalSlotProviderImpl(
                LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
        assertThat(slotPool.isBatchSlotRequestTimeoutCheckEnabled(), is(false));
    }
}
