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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link DefaultTimerService}. */
public class DefaultTimerServiceTest extends TestLogger {
    /** Tests that all registered timeouts can be unregistered. */
    @Test
    public void testUnregisterAllTimeouts() throws Exception {
        final ManuallyTriggeredScheduledExecutorService scheduledExecutorService =
                new ManuallyTriggeredScheduledExecutorService();
        DefaultTimerService<AllocationID> timerService =
                new DefaultTimerService<>(scheduledExecutorService, 100L);
        timerService.start((ignoredA, ignoredB) -> {});

        timerService.registerTimeout(new AllocationID(), 10, TimeUnit.SECONDS);
        timerService.registerTimeout(new AllocationID(), 10, TimeUnit.SECONDS);

        timerService.unregisterAllTimeouts();

        Map<?, ?> timeouts = timerService.getTimeouts();
        assertTrue(timeouts.isEmpty());

        for (ScheduledFuture<?> scheduledTask : scheduledExecutorService.getAllScheduledTasks()) {
            assertThat(scheduledTask.isCancelled(), is(true));
        }
    }

    @Test
    public void testIsValidInitiallyReturnsFalse() {
        final DefaultTimerService<AllocationID> timerService = createAndStartTimerService();

        assertThat(timerService.isValid(new AllocationID(), UUID.randomUUID()), is(false));
    }

    @Test
    public void testIsValidReturnsTrueForActiveTimeout() throws Exception {
        final DefaultTimerService<AllocationID> timerService = createAndStartTimerService();

        final AllocationID allocationId = new AllocationID();
        timerService.registerTimeout(allocationId, 10, TimeUnit.SECONDS);

        final DefaultTimerService.Timeout<AllocationID> timeout =
                timerService.getTimeouts().get(allocationId);
        final UUID ticket = timeout.getTicket();

        assertThat(timerService.isValid(allocationId, ticket), is(true));
    }

    @Test
    public void testIsValidReturnsFalseIfEitherKeyOrTicketDoesNotMatch() {
        final DefaultTimerService<AllocationID> timerService = createAndStartTimerService();

        final AllocationID allocationId = new AllocationID();
        timerService.registerTimeout(allocationId, 10, TimeUnit.SECONDS);

        final DefaultTimerService.Timeout<AllocationID> timeout =
                timerService.getTimeouts().get(allocationId);
        final UUID ticket = timeout.getTicket();

        assertThat(timerService.isValid(new AllocationID(), ticket), is(false));
        assertThat(timerService.isValid(allocationId, UUID.randomUUID()), is(false));
    }

    private static DefaultTimerService<AllocationID> createAndStartTimerService() {
        final ManuallyTriggeredScheduledExecutorService scheduledExecutorService =
                new ManuallyTriggeredScheduledExecutorService();
        DefaultTimerService<AllocationID> timerService =
                new DefaultTimerService<>(scheduledExecutorService, 100L);
        timerService.start((ignoredA, ignoredB) -> {});
        return timerService;
    }
}
