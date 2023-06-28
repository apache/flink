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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.runtime.operators.coordination.CoordinatorStoreImpl;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.ReportedWatermarkEvent;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for watermark alignment of the {@link SourceCoordinator}. */
@SuppressWarnings("serial")
class SourceCoordinatorAlignmentTest extends SourceCoordinatorTestBase {

    private static final Random RANDOM = new Random();

    @Test
    void testWatermarkAlignment() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            SourceCoordinator<?, ?> sourceCoordinator1 =
                    getAndStartNewSourceCoordinator(
                            new WatermarkAlignmentParams(1000L, "group1", Long.MAX_VALUE),
                            closeableRegistry);

            int subtask0 = 0;
            int subtask1 = 1;
            reportWatermarkEvent(sourceCoordinator1, subtask0, 42);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);

            reportWatermarkEvent(sourceCoordinator1, subtask1, 44);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1042);

            reportWatermarkEvent(sourceCoordinator1, subtask0, 5000);
            assertLatestWatermarkAlignmentEvent(subtask0, 1044);
            assertLatestWatermarkAlignmentEvent(subtask1, 1044);
        }
    }

    @Test
    void testWatermarkAlignmentWithIdleness() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            SourceCoordinator<?, ?> sourceCoordinator1 =
                    getAndStartNewSourceCoordinator(
                            new WatermarkAlignmentParams(1000L, "group1", Long.MAX_VALUE),
                            closeableRegistry);

            int subtask0 = 0;
            int subtask1 = 1;
            reportWatermarkEvent(sourceCoordinator1, subtask0, 42);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);

            reportWatermarkEvent(sourceCoordinator1, subtask1, 44);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1042);

            // subtask0 becomes idle
            reportWatermarkEvent(sourceCoordinator1, subtask0, Long.MAX_VALUE);
            assertLatestWatermarkAlignmentEvent(subtask0, 1044);
            assertLatestWatermarkAlignmentEvent(subtask1, 1044);

            // subtask0 becomes active again
            reportWatermarkEvent(sourceCoordinator1, subtask0, 42);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1042);

            // all subtask becomes idle
            reportWatermarkEvent(sourceCoordinator1, subtask0, Long.MAX_VALUE);
            reportWatermarkEvent(sourceCoordinator1, subtask1, Long.MAX_VALUE);
            assertLatestWatermarkAlignmentEvent(subtask0, Long.MAX_VALUE);
            assertLatestWatermarkAlignmentEvent(subtask1, Long.MAX_VALUE);

            // subtask0 becomes active again
            reportWatermarkEvent(sourceCoordinator1, subtask0, 42);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1042);

            // subtask1 becomes active again
            reportWatermarkEvent(sourceCoordinator1, subtask1, 46);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1042);
        }
    }

    @Test
    void testWatermarkAlignmentWithTwoGroups() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            long maxDrift = 1000L;
            SourceCoordinator<?, ?> sourceCoordinator1 =
                    getAndStartNewSourceCoordinator(
                            new WatermarkAlignmentParams(maxDrift, "group1", Long.MAX_VALUE),
                            closeableRegistry);

            SourceCoordinator<?, ?> sourceCoordinator2 =
                    getAndStartNewSourceCoordinator(
                            new WatermarkAlignmentParams(maxDrift, "group2", Long.MAX_VALUE),
                            closeableRegistry);

            int subtask0 = 0;
            int subtask1 = 1;
            reportWatermarkEvent(sourceCoordinator1, subtask0, 42);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);

            reportWatermarkEvent(sourceCoordinator2, subtask1, 44);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1044);

            reportWatermarkEvent(sourceCoordinator1, subtask0, 5000);
            assertLatestWatermarkAlignmentEvent(subtask0, 6000);
            assertLatestWatermarkAlignmentEvent(subtask1, 1044);
        }
    }

    /**
     * When JobManager failover and auto recover job, SourceCoordinator will reset twice: 1. Create
     * JobMaster --> Create Scheduler --> Create DefaultExecutionGraph --> Init
     * SourceCoordinator(but will not start it) 2. JobMaster call
     * restoreLatestCheckpointedStateInternal, which will call {@link
     * org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator#resetToCheckpoint(long,byte[])}
     * and reset SourceCoordinator. Because the first SourceCoordinator is not be started, so the
     * period task can't be stopped.
     */
    @Test
    void testAnnounceCombinedWatermarkWithoutStart() throws Exception {
        long maxDrift = 1000L;
        WatermarkAlignmentParams params =
                new WatermarkAlignmentParams(maxDrift, "group1", maxDrift);

        final Source<Integer, MockSourceSplit, Set<MockSourceSplit>> mockSource =
                createMockSource();

        // First to init a SourceCoordinator to simulate JobMaster init SourceCoordinator
        AtomicInteger counter1 = new AtomicInteger(0);
        sourceCoordinator =
                new SourceCoordinator<MockSourceSplit, Set<MockSourceSplit>>(
                        OPERATOR_NAME,
                        mockSource,
                        getNewSourceCoordinatorContext(),
                        new CoordinatorStoreImpl(),
                        params,
                        null) {
                    @Override
                    void announceCombinedWatermark() {
                        counter1.incrementAndGet();
                    }
                };

        // Second we call SourceCoordinator::close and re-init SourceCoordinator to simulate
        // RecreateOnResetOperatorCoordinator::resetToCheckpoint
        sourceCoordinator.close();
        CountDownLatch latch = new CountDownLatch(2);
        sourceCoordinator =
                new SourceCoordinator<MockSourceSplit, Set<MockSourceSplit>>(
                        OPERATOR_NAME,
                        mockSource,
                        getNewSourceCoordinatorContext(),
                        new CoordinatorStoreImpl(),
                        params,
                        null) {
                    @Override
                    void announceCombinedWatermark() {
                        latch.countDown();
                    }
                };

        sourceCoordinator.start();
        setReaderTaskReady(sourceCoordinator, 0, 0);

        latch.await();
        assertThat(counter1.get()).isZero();

        sourceCoordinator.close();
    }

    @Test
    void testSendWatermarkAlignmentEventFailed() throws Exception {
        long maxDrift = 1000L;

        WatermarkAlignmentParams params =
                new WatermarkAlignmentParams(maxDrift, "group1", maxDrift);

        final Source<Integer, MockSourceSplit, Set<MockSourceSplit>> mockSource =
                createMockSource();

        CountDownLatch latch = new CountDownLatch(1);
        sourceCoordinator =
                new SourceCoordinator<MockSourceSplit, Set<MockSourceSplit>>(
                        OPERATOR_NAME,
                        mockSource,
                        getNewSourceCoordinatorContext(),
                        new CoordinatorStoreImpl(),
                        params,
                        null) {
                    @Override
                    void announceCombinedWatermark() {
                        // announceCombinedWatermark maybe throw some RuntimeException, we catch
                        // these exception first and rethrow it after latch.countDown() to make
                        // latch.wait return
                        RuntimeException exception = null;
                        try {
                            super.announceCombinedWatermark();
                        } catch (RuntimeException t) {
                            exception = t;
                        }

                        latch.countDown();
                        if (exception != null) {
                            throw exception;
                        }
                    }
                };
        sourceCoordinator.start();

        final int subtask = 0;
        int attemptNumber = 0;
        sourceCoordinator.handleEventFromOperator(
                subtask,
                attemptNumber,
                new ReaderRegistrationEvent(subtask, createLocationFor(subtask, attemptNumber)));
        // SubTask ReportedWatermarkEvent before setReaderTaskReady to simulate task failover
        sourceCoordinator.handleEventFromOperator(
                subtask, attemptNumber, new ReportedWatermarkEvent(1000));

        // wait the period task is called at least once
        latch.await();

        setReaderTaskReady(sourceCoordinator, subtask, attemptNumber);

        waitForSentEvents(5);
        // SourceAlignment will ignore the task not ready error, so job will not fail
        assertThat(operatorCoordinatorContext.isJobFailed()).isFalse();
    }

    @Test
    void testWatermarkAggregator() {
        final SourceCoordinator.WatermarkAggregator<Integer> combinedWatermark =
                new SourceCoordinator.WatermarkAggregator<>();

        combinedWatermark.aggregate(0, new Watermark(10));
        assertThat(combinedWatermark.getAggregatedWatermark().getTimestamp()).isEqualTo(10);

        combinedWatermark.aggregate(1, new Watermark(12));
        assertThat(combinedWatermark.getAggregatedWatermark().getTimestamp()).isEqualTo(10);

        combinedWatermark.aggregate(2, new Watermark(13));
        assertThat(combinedWatermark.getAggregatedWatermark().getTimestamp()).isEqualTo(10);

        combinedWatermark.aggregate(1, new Watermark(9));
        assertThat(combinedWatermark.getAggregatedWatermark().getTimestamp()).isEqualTo(9);

        // The watermark of idle source
        combinedWatermark.aggregate(1, new Watermark(Long.MAX_VALUE));
        assertThat(combinedWatermark.getAggregatedWatermark().getTimestamp()).isEqualTo(10);

        combinedWatermark.aggregate(1, new Watermark(8));
        assertThat(combinedWatermark.getAggregatedWatermark().getTimestamp()).isEqualTo(8);

        combinedWatermark.aggregate(1, new Watermark(20));
        assertThat(combinedWatermark.getAggregatedWatermark().getTimestamp()).isEqualTo(10);

        combinedWatermark.aggregate(0, new Watermark(23));
        assertThat(combinedWatermark.getAggregatedWatermark().getTimestamp()).isEqualTo(13);

        combinedWatermark.aggregate(2, new Watermark(22));
        assertThat(combinedWatermark.getAggregatedWatermark().getTimestamp()).isEqualTo(20);
    }

    @Test
    void testWatermarkAggregatorRandomly() {
        testWatermarkAggregatorRandomly(20, 1000, true, true);
        testWatermarkAggregatorRandomly(20, 1000, true, false);
        testWatermarkAggregatorRandomly(20, 2000, true, true);
        testWatermarkAggregatorRandomly(20, 2000, true, false);
        testWatermarkAggregatorRandomly(20, 5000, true, true);
        testWatermarkAggregatorRandomly(20, 5000, true, false);
        testWatermarkAggregatorRandomly(10, 10000, true, true);
        testWatermarkAggregatorRandomly(10, 10000, true, false);
    }

    private void testWatermarkAggregatorRandomly(
            int roundNumber, int keyNumber, boolean checkResult, boolean testSourceIdle) {
        final SourceCoordinator.WatermarkAggregator<Integer> combinedWatermark =
                new SourceCoordinator.WatermarkAggregator<>();
        final Map<Integer, Long> latestWatermarks = new HashMap<>();

        for (long round = 0; round < roundNumber; round++) {
            for (int key = 0; key < keyNumber; key++) {
                long timestamp = getRandomTimestamp(testSourceIdle);
                combinedWatermark.aggregate(key, new Watermark(timestamp));

                if (checkResult) {
                    latestWatermarks.put(key, timestamp);
                    // Disable the check for benchmark
                    assertAggregatedWatermark(combinedWatermark, latestWatermarks);
                }
            }
        }
    }

    // Randomizes the last three digits of the timestamp
    private long getRandomTimestamp(boolean testSourceIdle) {
        if (testSourceIdle && RANDOM.nextInt(100) == 0) {
            // Simulate the source idle and the default watermark
            return RANDOM.nextBoolean() ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
        return System.currentTimeMillis() / 1000 * 1000 + RANDOM.nextInt(1000);
    }

    private void assertAggregatedWatermark(
            SourceCoordinator.WatermarkAggregator<Integer> combinedWatermark,
            Map<Integer, Long> latestWatermarks) {
        long expectAggregatedWatermark =
                latestWatermarks.values().stream()
                        .min(Comparable::compareTo)
                        .orElseThrow(IllegalStateException::new);
        assertThat(combinedWatermark.getAggregatedWatermark().getTimestamp())
                .isEqualTo(expectAggregatedWatermark);
    }

    private SourceCoordinator<?, ?> getAndStartNewSourceCoordinator(
            WatermarkAlignmentParams watermarkAlignmentParams,
            AutoCloseableRegistry closeableRegistry)
            throws Exception {
        SourceCoordinator<?, ?> sourceCoordinator =
                getNewSourceCoordinator(watermarkAlignmentParams);
        closeableRegistry.registerCloseable(sourceCoordinator);
        sourceCoordinator.start();
        setAllReaderTasksReady(sourceCoordinator);

        return sourceCoordinator;
    }

    private void reportWatermarkEvent(
            SourceCoordinator<?, ?> sourceCoordinator1, int subtask, long watermark) {
        sourceCoordinator1.handleEventFromOperator(
                subtask, 0, new ReportedWatermarkEvent(watermark));
        CoordinatorTestUtils.waitForCoordinatorToProcessActions(sourceCoordinator1.getContext());
        sourceCoordinator1.announceCombinedWatermark();
    }

    private void assertLatestWatermarkAlignmentEvent(int subtask, long expectedWatermark) {
        List<OperatorEvent> events = receivingTasks.getSentEventsForSubtask(subtask);
        assertThat(events).isNotEmpty();
        assertThat(events.get(events.size() - 1))
                .isEqualTo(new WatermarkAlignmentEvent(expectedWatermark));
    }
}
