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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.TestingComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DeclarativeSlotPoolBridge} interactions. */
class SlotPoolInteractionsTest {

    private static final Time fastTimeout = Time.milliseconds(1L);

    @RegisterExtension
    private static final TestingComponentMainThreadExecutor.Extension EXECUTOR_EXTENSION =
            new TestingComponentMainThreadExecutor.Extension(10L);

    private final TestingComponentMainThreadExecutor testMainThreadExecutor =
            EXECUTOR_EXTENSION.getComponentMainThreadTestExecutor();

    // ------------------------------------------------------------------------
    //  tests
    // ------------------------------------------------------------------------

    @Test
    void testSlotAllocationNoResourceManager() throws Exception {

        try (SlotPool pool = createAndSetUpSlotPoolWithoutResourceManager()) {

            final CompletableFuture<PhysicalSlot> future =
                    testMainThreadExecutor.execute(
                            () ->
                                    pool.requestNewAllocatedSlot(
                                            new SlotRequestId(),
                                            ResourceProfile.UNKNOWN,
                                            fastTimeout));

            assertThatThrownBy(future::get)
                    .withFailMessage("We expected an ExecutionException.")
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void testCancelSlotAllocationWithoutResourceManager() throws Exception {

        try (DeclarativeSlotPoolBridge pool = createAndSetUpSlotPoolWithoutResourceManager()) {

            final CompletableFuture<PhysicalSlot> future =
                    testMainThreadExecutor.execute(
                            () ->
                                    pool.requestNewAllocatedSlot(
                                            new SlotRequestId(),
                                            ResourceProfile.UNKNOWN,
                                            fastTimeout));

            assertThatThrownBy(future::get)
                    .withFailMessage("We expected a TimeoutException.")
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TimeoutException.class);

            CommonTestUtils.waitUntilCondition(() -> pool.getNumPendingRequests() == 0);
        }
    }

    /** Tests that a slot allocation times out wrt to the specified time out. */
    @Test
    void testSlotAllocationTimeout() throws Exception {

        try (DeclarativeSlotPoolBridge pool = createAndSetUpSlotPool()) {

            final CompletableFuture<PhysicalSlot> future =
                    testMainThreadExecutor.execute(
                            () ->
                                    pool.requestNewAllocatedSlot(
                                            new SlotRequestId(),
                                            ResourceProfile.UNKNOWN,
                                            fastTimeout));

            assertThatThrownBy(future::get)
                    .withFailMessage("We expected a TimeoutException.")
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TimeoutException.class);

            CommonTestUtils.waitUntilCondition(() -> pool.getNumPendingRequests() == 0);
        }
    }

    private DeclarativeSlotPoolBridge createAndSetUpSlotPool() throws Exception {
        return new DeclarativeSlotPoolBridgeBuilder()
                .buildAndStart(testMainThreadExecutor.getMainThreadExecutor());
    }

    private DeclarativeSlotPoolBridge createAndSetUpSlotPoolWithoutResourceManager()
            throws Exception {
        return new DeclarativeSlotPoolBridgeBuilder()
                .setResourceManagerGateway(null)
                .buildAndStart(testMainThreadExecutor.getMainThreadExecutor());
    }
}
