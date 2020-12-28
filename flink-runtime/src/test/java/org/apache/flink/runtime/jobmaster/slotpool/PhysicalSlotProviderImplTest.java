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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link PhysicalSlotProviderImpl}. */
public class PhysicalSlotProviderImplTest {
    private static ScheduledExecutorService singleThreadScheduledExecutorService;

    private static ComponentMainThreadExecutor mainThreadExecutor;

    private TestingSlotPoolImpl slotPool;

    private PhysicalSlotProvider physicalSlotProvider;

    @BeforeClass
    public static void setupClass() {
        singleThreadScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        singleThreadScheduledExecutorService);
    }

    @AfterClass
    public static void teardownClass() {
        if (singleThreadScheduledExecutorService != null) {
            singleThreadScheduledExecutorService.shutdownNow();
        }
    }

    @Before
    public void setup() throws Exception {
        slotPool = new SlotPoolBuilder(mainThreadExecutor).build();
        physicalSlotProvider =
                new PhysicalSlotProviderImpl(
                        LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
    }

    @After
    public void teardown() {
        CompletableFuture.runAsync(() -> slotPool.close(), mainThreadExecutor).join();
    }

    @Test
    public void testSlotAllocationFulfilledWithAvailableSlots()
            throws InterruptedException, ExecutionException {
        PhysicalSlotRequest request = createPhysicalSlotRequest();
        addSlotToSlotPool();
        CompletableFuture<PhysicalSlotRequest.Result> slotFuture = allocateSlot(request);
        PhysicalSlotRequest.Result result = slotFuture.get();
        assertThat(result.getSlotRequestId(), is(request.getSlotRequestId()));
    }

    @Test
    public void testSlotAllocationFulfilledWithNewSlots()
            throws ExecutionException, InterruptedException {
        final CompletableFuture<PhysicalSlotRequest.Result> slotFuture =
                allocateSlot(createPhysicalSlotRequest());
        assertThat(slotFuture.isDone(), is(false));
        addSlotToSlotPool();
        slotFuture.get();
    }

    @Test
    public void testIndividualBatchSlotRequestTimeoutCheckIsDisabledOnAllocatingNewSlots()
            throws Exception {
        TestingSlotPoolImpl slotPool = new SlotPoolBuilder(mainThreadExecutor).build();
        assertThat(slotPool.isBatchSlotRequestTimeoutCheckEnabled(), is(true));

        new PhysicalSlotProviderImpl(
                LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
        assertThat(slotPool.isBatchSlotRequestTimeoutCheckEnabled(), is(false));
    }

    private CompletableFuture<PhysicalSlotRequest.Result> allocateSlot(
            PhysicalSlotRequest request) {
        return CompletableFuture.supplyAsync(
                        () -> physicalSlotProvider.allocatePhysicalSlot(request),
                        mainThreadExecutor)
                .thenCompose(Function.identity());
    }

    private void addSlotToSlotPool() {
        SlotPoolUtils.offerSlots(
                slotPool, mainThreadExecutor, Collections.singletonList(ResourceProfile.ANY));
    }

    private static PhysicalSlotRequest createPhysicalSlotRequest() {
        return new PhysicalSlotRequest(
                new SlotRequestId(), SlotProfile.noLocality(ResourceProfile.UNKNOWN), false);
    }
}
