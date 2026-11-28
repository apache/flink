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

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.util.ratelimit.DynamicRateLimitParams;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.DynamicRateLimitEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.ReportedRecordsEvent;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for the dynamic, checkpoint-duration-driven rate limit feature of {@link
 * SourceCoordinator}.
 */
@SuppressWarnings("serial")
class SourceCoordinatorDynamicRateLimitTest extends SourceCoordinatorTestBase {

    private final ManualClock clock = new ManualClock();

    @Test
    void testDisabledSendsNoDynamicRateLimitEvents() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(
                            DynamicRateLimitParams.DISABLED, closeableRegistry);
            registerReader(coordinator, 0);

            completeCheckpointCycle(coordinator, 100L, 0);

            assertThat(getSentDynamicRateLimitEvents(0)).isEmpty();
        }
    }

    @Test
    void testReaderRegisteringBeforeAnyAdjustmentDoesNotReceiveTheMaxRate() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 10, 250.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);

            registerReader(coordinator, 0);

            // The max rate must not override the rate a restored reader recovered itself.
            assertThat(getSentDynamicRateLimitEvents(0)).isEmpty();
        }
    }

    @Test
    void testNewlyRegisteredReaderImmediatelyReceivesTheCurrentRate() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 10, 250.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);
            registerReader(coordinator, 0);
            completeCheckpointCycle(coordinator, 1L, 0, 90, 0, 0);

            registerReader(coordinator, 1);

            assertThat(getLatestDynamicRateLimitEvent(1))
                    .isEqualTo(new DynamicRateLimitEvent(90.0 / NUM_SUBTASKS));
        }
    }

    @Test
    void testNoRateIsBroadcastWithoutObservedRecordRates() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 10, 100.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);
            registerReader(coordinator, 0);

            completeCheckpointCycle(coordinator, 1L, 0);

            assertThat(getSentDynamicRateLimitEvents(0)).isEmpty();
        }
    }

    @Test
    void testAdjustmentUsesObservedThroughputRatherThanThePreviousTarget() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 4, 1000.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);
            registerReader(coordinator, 0);

            completeCheckpointCycle(coordinator, 1L, 5);

            // 40 records over the 1000ms reported period is 40 records/s, far below the initial max
            // rate (1000.0): if the adjustment were still scaling off
            // the previous target instead of this observation, the result would be 1000.0 * 0.5 =
            // 500.0 rather than 40.0 * 0.5 = 20.0 (split evenly over the subtasks). The slower
            // second checkpoint is a worsening trend, so the rate is throttled down.
            completeCheckpointCycle(coordinator, 2L, 200, 40, 0, 0);

            assertThat(getLatestDynamicRateLimitEvent(0).getRecordsPerSecond())
                    .isEqualTo(40.0 * 0.5 / NUM_SUBTASKS);
        }
    }

    @Test
    void testAdjustmentSumsThroughputAcrossAllReportingSubtasks() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 4, 1000.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);
            registerReader(coordinator, 0);
            registerReader(coordinator, 1);

            completeCheckpointCycle(coordinator, 1L, 5);
            completeCheckpointCycle(coordinator, 2L, 300, 30, 20, 0);

            assertThat(getLatestDynamicRateLimitEvent(0).getRecordsPerSecond())
                    .isEqualTo((30.0 + 20.0) * 0.5 / NUM_SUBTASKS);
        }
    }

    @Test
    void testRecordsReportedForTheFirstCheckpointAreUsed() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 4, 1000.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);
            registerReader(coordinator, 0);

            // a single duration shows no trend, so the observed throughput is kept as the target
            completeCheckpointCycle(coordinator, 1L, 5, 40, 0, 0);

            assertThat(getLatestDynamicRateLimitEvent(0).getRecordsPerSecond())
                    .isEqualTo(40.0 / NUM_SUBTASKS);
        }
    }

    @Test
    void testThroughputIsRecordsOverReportedElapsedTime() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 4, 1000.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);
            registerReader(coordinator, 0);

            coordinator.checkpointCoordinator(1L, new CompletableFuture<>());
            CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());
            reportRecords(coordinator, 0, 1L, 100, 2000);
            reportRecords(coordinator, 1, 1L, 0, 1000);
            reportRecords(coordinator, 2, 1L, 0, 1000);
            clock.advanceTime(Duration.ofMillis(5));
            coordinator.notifyCheckpointComplete(1L);
            CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());

            // 100 records over 2000ms
            assertThat(getLatestDynamicRateLimitEvent(0).getRecordsPerSecond())
                    .isEqualTo(50.0 / NUM_SUBTASKS);
        }
    }

    @Test
    void testReportWithoutElapsedTimeIsIgnored() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 4, 1000.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);
            registerReader(coordinator, 0);

            coordinator.checkpointCoordinator(1L, new CompletableFuture<>());
            CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());
            reportRecords(coordinator, 0, 1L, 100, 0);
            coordinator.notifyCheckpointComplete(1L);
            CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());

            // no usable record rate was observed, so the rate is not adjusted
            assertThat(getSentDynamicRateLimitEvents(0)).isEmpty();
        }
    }

    @Test
    void testRecordsReportedForOtherCheckpointsAreIgnored() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 4, 1000.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);
            registerReader(coordinator, 0);

            completeCheckpointCycle(coordinator, 1L, 5, 40, 40, 40);
            reportRecords(coordinator, 0, 99L, 400, 1000);
            completeCheckpointCycle(coordinator, 2L, 200, 40, 40, 40);

            assertThat(getLatestDynamicRateLimitEvent(0).getRecordsPerSecond())
                    .isEqualTo(40.0 * 0.5);
        }
    }

    @Test
    void testIdleSourceDoesNotAdjustTheRate() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 4, 1000.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);
            for (int subtask = 0; subtask < NUM_SUBTASKS; subtask++) {
                registerReader(coordinator, subtask);
            }

            completeCheckpointCycle(coordinator, 1L, 5, 0);
            completeCheckpointCycle(coordinator, 2L, 5, 0);

            // zero observed throughput is treated as unknown, so no rate is adjusted at all
            for (int subtask = 0; subtask < NUM_SUBTASKS; subtask++) {
                assertThat(getSentDynamicRateLimitEvents(subtask)).isEmpty();
            }
        }
    }

    @Test
    void testRecordsReportedForAnAbortedCheckpointAreDropped() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 4, 1000.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);
            registerReader(coordinator, 0);

            coordinator.checkpointCoordinator(1L, new CompletableFuture<>());
            CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());
            reportRecords(coordinator, 0, 1L, 40, 1000);
            coordinator.notifyCheckpointAborted(1L);
            CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());

            // checkpoint 1 was aborted, so its report is not used for checkpoint 2
            completeCheckpointCycle(coordinator, 2L, 5, 20, 20, 20);
            completeCheckpointCycle(coordinator, 3L, 200, 20, 20, 20);

            assertThat(getLatestDynamicRateLimitEvent(0).getRecordsPerSecond())
                    .isEqualTo(20.0 * 0.5);
        }
    }

    @Test
    void testAllocatorMovesUnusedBudgetToSaturatedSubtasks() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 4, 300.0, 0.5, 2.0, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(params, closeableRegistry);
            for (int subtask = 0; subtask < NUM_SUBTASKS; subtask++) {
                registerReader(coordinator, subtask);
            }

            // Everyone starts with an even share of 100. Subtasks 1 and 2 only manage 10 records/s
            // (skewed away from them), subtask 0 uses its whole share.
            completeCheckpointCycle(coordinator, 1L, 5, 100, 100, 100);
            completeCheckpointCycle(coordinator, 2L, 5, 100, 10, 10);

            // total = 120 * ramp-up 2.0 = 240; idle subtasks need 10 * 1.2 = 12 each
            assertThat(getLatestDynamicRateLimitEvent(1).getRecordsPerSecond())
                    .isCloseTo(12.0, within(1e-9));
            assertThat(getLatestDynamicRateLimitEvent(2).getRecordsPerSecond())
                    .isCloseTo(12.0, within(1e-9));
            assertThat(getLatestDynamicRateLimitEvent(0).getRecordsPerSecond())
                    .isCloseTo(240.0 - 24.0, within(1e-9));
        }
    }

    @Test
    void testFirstCheckpointAfterRestoreReportsTheRateWithoutObservedThroughput() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 4, 1000.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getRestoredAndStartedSourceCoordinator(params, closeableRegistry);
            registerReader(coordinator, 0);

            // With restored state the rate is reported right away, based on the current target,
            // even though no throughput was observed yet.
            completeCheckpointCycle(coordinator, 101L, 5);

            assertThat(getLatestDynamicRateLimitEvent(0).getRecordsPerSecond())
                    .isEqualTo(1000.0 / NUM_SUBTASKS);
        }
    }

    @Test
    void testAdjustsAgainOnceThroughputIsObservedAfterRestore() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            DynamicRateLimitParams params =
                    new DynamicRateLimitParams(true, 4, 1000.0, 0.5, 1.1, 0.1, 0.5, 0.8);
            SourceCoordinator<?, ?> coordinator =
                    getRestoredAndStartedSourceCoordinator(params, closeableRegistry);
            registerReader(coordinator, 0);
            completeCheckpointCycle(coordinator, 101L, 5);

            completeCheckpointCycle(coordinator, 102L, 5, 40, 40, 40);

            // 40 records/s observed, ramped up by 1.1 and split over the subtasks
            assertThat(getLatestDynamicRateLimitEvent(0).getRecordsPerSecond())
                    .isCloseTo(40.0 * 1.1, within(1e-9));
        }
    }

    @Test
    void testReportedRecordsAreNotRetainedWhenDisabled() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            SourceCoordinator<?, ?> coordinator =
                    getAndStartNewSourceCoordinator(
                            DynamicRateLimitParams.DISABLED, closeableRegistry);
            registerReader(coordinator, 0);

            completeCheckpointCycle(coordinator, 1L, 5, 40);
            completeCheckpointCycle(coordinator, 2L, 5, 40);

            assertThat(coordinator.getNumCheckpointsWithReportedRecords()).isZero();
        }
    }

    // ------------------------------------------------------------------------

    private SourceCoordinator<?, ?> getAndStartNewSourceCoordinator(
            DynamicRateLimitParams dynamicRateLimitParams, AutoCloseableRegistry closeableRegistry)
            throws Exception {
        SourceCoordinator<?, ?> coordinator =
                getNewSourceCoordinator(dynamicRateLimitParams, clock);
        closeableRegistry.registerCloseable(coordinator);
        coordinator.start();
        setAllReaderTasksReady(coordinator);
        return coordinator;
    }

    /** Starts a coordinator that was restored from a checkpoint of checkpoint id 100. */
    private SourceCoordinator<?, ?> getRestoredAndStartedSourceCoordinator(
            DynamicRateLimitParams dynamicRateLimitParams, AutoCloseableRegistry closeableRegistry)
            throws Exception {
        final SourceCoordinator<?, ?> previous =
                getNewSourceCoordinator(dynamicRateLimitParams, clock);
        closeableRegistry.registerCloseable(previous);
        previous.start();
        final CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();
        previous.checkpointCoordinator(100L, checkpoint);
        CoordinatorTestUtils.waitForCoordinatorToProcessActions(previous.getContext());

        final SourceCoordinator<?, ?> restored =
                getNewSourceCoordinator(dynamicRateLimitParams, clock);
        closeableRegistry.registerCloseable(restored);
        restored.resetToCheckpoint(100L, checkpoint.get());
        restored.start();
        setAllReaderTasksReady(restored);
        return restored;
    }

    private void registerReader(SourceCoordinator<?, ?> coordinator, int subtask) {
        coordinator.handleEventFromOperator(
                subtask, 0, new ReaderRegistrationEvent(subtask, createLocationFor(subtask, 0)));
        CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());
    }

    private void reportRecords(
            SourceCoordinator<?, ?> coordinator,
            int subtask,
            long checkpointId,
            long numRecords,
            long elapsedMillis) {
        coordinator.handleEventFromOperator(
                subtask, 0, new ReportedRecordsEvent(checkpointId, numRecords, elapsedMillis));
        CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());
    }

    /**
     * Triggers a checkpoint, lets each subtask report the given number of records over a period of
     * one second, and completes the checkpoint after {@code durationMillis}.
     */
    private void completeCheckpointCycle(
            SourceCoordinator<?, ?> coordinator,
            long checkpointId,
            long durationMillis,
            long... recordsPerSubtask)
            throws Exception {
        coordinator.checkpointCoordinator(checkpointId, new CompletableFuture<>());
        CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());
        for (int subtask = 0; subtask < recordsPerSubtask.length; subtask++) {
            reportRecords(coordinator, subtask, checkpointId, recordsPerSubtask[subtask], 1000);
        }
        clock.advanceTime(Duration.ofMillis(durationMillis));
        coordinator.notifyCheckpointComplete(checkpointId);
        CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());
    }

    private List<DynamicRateLimitEvent> getSentDynamicRateLimitEvents(int subtask) {
        List<OperatorEvent> events = receivingTasks.getSentEventsForSubtask(subtask);
        return events.stream()
                .filter(DynamicRateLimitEvent.class::isInstance)
                .map(DynamicRateLimitEvent.class::cast)
                .collect(Collectors.toList());
    }

    private DynamicRateLimitEvent getLatestDynamicRateLimitEvent(int subtask) {
        List<DynamicRateLimitEvent> events = getSentDynamicRateLimitEvents(subtask);
        assertThat(events).isNotEmpty();
        return events.get(events.size() - 1);
    }
}
