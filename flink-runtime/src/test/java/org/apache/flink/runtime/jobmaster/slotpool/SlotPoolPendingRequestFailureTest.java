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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolUtils.requestNewAllocatedSlot;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.fail;

/** Tests for the failing of pending slot requests at the {@link SlotPool}. */
public class SlotPoolPendingRequestFailureTest extends TestLogger {

    private TestingResourceManagerGateway resourceManagerGateway;

    @Before
    public void setup() {
        resourceManagerGateway = new TestingResourceManagerGateway();
    }

    /** Tests that a pending slot request is failed with a timeout. */
    @Test
    public void testPendingSlotRequestTimeout() throws Exception {
        final ScheduledExecutorService singleThreadExecutor =
                Executors.newSingleThreadScheduledExecutor();
        final ComponentMainThreadExecutor componentMainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        singleThreadExecutor);

        final SlotPool slotPool =
                new SlotPoolBuilder(componentMainThreadExecutor)
                        .setResourceManagerGateway(resourceManagerGateway)
                        .build();

        try {
            final Time timeout = Time.milliseconds(5L);

            final CompletableFuture<PhysicalSlot> slotFuture =
                    CompletableFuture.supplyAsync(
                                    () ->
                                            requestNewAllocatedSlot(
                                                    slotPool, new SlotRequestId(), timeout),
                                    componentMainThreadExecutor)
                            .thenCompose(Function.identity());

            try {
                slotFuture.get();
                fail("Expected that the future completes with a TimeoutException.");
            } catch (ExecutionException ee) {
                assertThat(
                        ExceptionUtils.stripExecutionException(ee),
                        instanceOf(TimeoutException.class));
            }
        } finally {
            CompletableFuture.runAsync(
                            ThrowingRunnable.unchecked(slotPool::close),
                            componentMainThreadExecutor)
                    .get();
            singleThreadExecutor.shutdownNow();
        }
    }
}
