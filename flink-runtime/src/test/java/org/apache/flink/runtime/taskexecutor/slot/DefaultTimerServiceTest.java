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

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DefaultTimerService}. */
class DefaultTimerServiceTest {
    /** Tests that all registered timeouts can be unregistered. */
    @Test
    void testUnregisterAllTimeouts() {
        final ManuallyTriggeredScheduledExecutorService scheduledExecutorService =
                new ManuallyTriggeredScheduledExecutorService();
        DefaultTimerService<AllocationID> timerService =
                new DefaultTimerService<>(scheduledExecutorService, 100L);
        timerService.start((ignoredA, ignoredB) -> {});

        timerService.registerTimeout(new AllocationID(), 10, TimeUnit.SECONDS);
        timerService.registerTimeout(new AllocationID(), 10, TimeUnit.SECONDS);

        timerService.unregisterAllTimeouts();

        Map<?, ?> timeouts = timerService.getTimeouts();
        assertThat(timeouts).isEmpty();

        for (ScheduledFuture<?> scheduledTask : scheduledExecutorService.getAllScheduledTasks()) {
            assertThat(scheduledTask.isCancelled()).isTrue();
        }
    }

    @Test
    void testIsValidInitiallyReturnsFalse() {
        final DefaultTimerService<AllocationID> timerService = createAndStartTimerService();

        assertThat(timerService.isValid(new AllocationID(), UUID.randomUUID())).isFalse();
    }

    @Test
    void testIsValidReturnsTrueForActiveTimeout() throws Exception {
        final DefaultTimerService<AllocationID> timerService = createAndStartTimerService();

        final AllocationID allocationId = new AllocationID();
        timerService.registerTimeout(allocationId, 10, TimeUnit.SECONDS);

        final DefaultTimerService.Timeout<AllocationID> timeout =
                timerService.getTimeouts().get(allocationId);
        final UUID ticket = timeout.getTicket();

        assertThat(timerService.isValid(allocationId, ticket)).isTrue();
    }

    @Test
    void testIsValidReturnsFalseIfEitherKeyOrTicketDoesNotMatch() {
        final DefaultTimerService<AllocationID> timerService = createAndStartTimerService();

        final AllocationID allocationId = new AllocationID();
        timerService.registerTimeout(allocationId, 10, TimeUnit.SECONDS);

        final DefaultTimerService.Timeout<AllocationID> timeout =
                timerService.getTimeouts().get(allocationId);
        final UUID ticket = timeout.getTicket();

        assertThat(timerService.isValid(new AllocationID(), ticket)).isFalse();
        assertThat(timerService.isValid(allocationId, UUID.randomUUID())).isFalse();
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
