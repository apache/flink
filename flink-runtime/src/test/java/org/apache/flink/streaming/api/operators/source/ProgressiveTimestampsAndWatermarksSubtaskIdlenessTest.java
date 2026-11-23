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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.clock.RelativeClock;
import org.apache.flink.util.clock.SystemClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the subtask-level idleness signal that {@link ProgressiveTimestampsAndWatermarks}
 * announces downstream.
 *
 * <p>That signal is the conjunction of two independent branches - the merged per-split outputs and
 * the main output - so these tests pin down when the conjunction may and may not conclude that the
 * whole subtask is idle.
 */
class ProgressiveTimestampsAndWatermarksSubtaskIdlenessTest {

    private static final Duration IDLE_TIMEOUT = Duration.ofMillis(10);

    /** Timestamp of the record that a split emits before it falls idle. */
    private static final long FIRST_EVENT_TIMESTAMP = 100L;

    /** Timestamp of the record the split resumes with, behind {@link #FIRST_EVENT_TIMESTAMP}. */
    private static final long RESUMED_EVENT_TIMESTAMP = 50L;

    /**
     * A subtask must not announce idleness while one of its splits is still producing records, even
     * if that split's watermark never advances. The main output of a split-based source never sees
     * a record, so its activity timer always expires; the split branch is what has to hold the
     * subtask active.
     */
    @Test
    void subtaskMustNotGoIdleWhileRecordsFlowThroughItsSplits() {
        final ManualClock mainInputActivityClock = new ManualClock(System.nanoTime());
        final RecordingListener listener = new RecordingListener();
        // a generator that never emits: the split stays active but its watermark stays at MIN_VALUE
        final TimestampsAndWatermarks<Long> eventTimeLogic =
                createEventTimeLogic(
                        WatermarkStrategy.<Long>forGenerator(context -> new NeverEmits())
                                .withTimestampAssigner((event, timestamp) -> event)
                                .withIdleness(IDLE_TIMEOUT),
                        mainInputActivityClock,
                        SystemClock.getInstance());

        final ReaderOutput<Long> mainOutput =
                eventTimeLogic.createMainOutput(new CollectingDataOutput<>(), listener);
        final SourceOutput<Long> splitOutput = mainOutput.createOutputForSplit("split-A");

        splitOutput.collect(1L, 1L);
        eventTimeLogic.emitImmediateWatermark(0L);
        assertThat(listener.idleUpdates).isEmpty();

        mainInputActivityClock.advanceTime(IDLE_TIMEOUT.plusMillis(1));
        splitOutput.collect(2L, 2L);
        eventTimeLogic.emitImmediateWatermark(0L);

        assertThat(listener.idleUpdates)
                .as("records kept arriving on split-A, so the subtask is not idle")
                .isEmpty();
    }

    /** Once every split really is idle, the subtask must still stand down. */
    @Test
    void subtaskGoesIdleOnceAllOfItsSplitsAreIdle() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final RecordingListener listener = new RecordingListener();
        final TimestampsAndWatermarks<Long> eventTimeLogic = createEventTimeLogic(clock, clock);

        final ReaderOutput<Long> mainOutput =
                eventTimeLogic.createMainOutput(new CollectingDataOutput<>(), listener);
        final SourceOutput<Long> splitOutput = mainOutput.createOutputForSplit("split-A");

        splitOutput.collect(1L, 1L);
        eventTimeLogic.emitImmediateWatermark(0L);
        assertThat(listener.idleUpdates).isEmpty();

        // the idleness timers need one emit to start counting and one more to expire
        for (int i = 0; i < 2; i++) {
            clock.advanceTime(IDLE_TIMEOUT.plusMillis(1));
            eventTimeLogic.emitImmediateWatermark(0L);
        }

        assertThat(listener.idleUpdates)
                .as("split-A stopped producing, so the subtask has nothing left to hold it active")
                .containsExactly(true);
    }

    /**
     * A subtask that was never assigned a split has no split branch to speak for it, so the main
     * output's activity timer alone has to make it stand down - otherwise it would pin the
     * downstream watermark forever.
     */
    @Test
    void subtaskWithoutSplitsGoesIdleViaTheMainOutputTimer() {
        final ManualClock mainInputActivityClock = new ManualClock(System.nanoTime());
        final RecordingListener listener = new RecordingListener();
        final TimestampsAndWatermarks<Long> eventTimeLogic =
                createEventTimeLogic(mainInputActivityClock, SystemClock.getInstance());

        eventTimeLogic.createMainOutput(new CollectingDataOutput<>(), listener);

        eventTimeLogic.emitImmediateWatermark(0L);
        assertThat(listener.idleUpdates).isEmpty();

        mainInputActivityClock.advanceTime(IDLE_TIMEOUT.plusMillis(1));
        eventTimeLogic.emitImmediateWatermark(0L);

        assertThat(listener.idleUpdates)
                .as("a subtask without splits must still signal idleness downstream")
                .containsExactly(true);
    }

    /**
     * When all splits fall idle their watermarks are flushed to the maximum seen so far. A split
     * that resumes behind that maximum therefore produces no advancing watermark, and the subtask
     * would stay announced as idle even though it is emitting records again.
     */
    @Test
    void subtaskGoesActiveAgainWhenAnIdleSplitResumesBehindTheFlushedWatermark() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final RecordingListener listener = new RecordingListener();
        final TimestampsAndWatermarks<Long> eventTimeLogic = createEventTimeLogic(clock, clock);

        final ReaderOutput<Long> mainOutput =
                eventTimeLogic.createMainOutput(new CollectingDataOutput<>(), listener);
        final SourceOutput<Long> splitOutput = mainOutput.createOutputForSplit("split-A");

        splitOutput.collect(FIRST_EVENT_TIMESTAMP, FIRST_EVENT_TIMESTAMP);
        eventTimeLogic.emitImmediateWatermark(0L);

        // the idleness timers need one emit to start counting and one more to expire
        for (int i = 0; i < 2; i++) {
            clock.advanceTime(IDLE_TIMEOUT.plusMillis(1));
            eventTimeLogic.emitImmediateWatermark(0L);
        }
        assertThat(listener.idleUpdates).containsExactly(true);

        splitOutput.collect(RESUMED_EVENT_TIMESTAMP, RESUMED_EVENT_TIMESTAMP);
        eventTimeLogic.emitImmediateWatermark(0L);

        assertThat(listener.idleUpdates)
                .as("split-A produces records again, so the subtask has to be announced as active")
                .containsExactly(true, false);
    }

    @Test
    void testRegisteredSplitBlockWatermarkUntilReportedIdle() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final RecordingListener listener = new RecordingListener();
        final TimestampsAndWatermarks<Long> eventTimeLogic =
                createEventTimeLogicWithIdleGenerator(clock, clock);
        final ReaderOutput<Long> mainOutput =
                eventTimeLogic.createMainOutput(new CollectingDataOutput<>(), listener);

        eventTimeLogic.emitImmediateWatermark(0L); // starts the main output's timer
        clock.advanceTime(IDLE_TIMEOUT.minusMillis(1));
        // A split is registered just before the main input is about to become idle
        mainOutput.createOutputForSplit("split-A");
        clock.advanceTime(Duration.ofMillis(2));
        eventTimeLogic.emitImmediateWatermark(0L); // main timer expires, split-A's timer starts

        assertThat(listener.idleUpdates).isEmpty();

        clock.advanceTime(IDLE_TIMEOUT.plusMillis(1));
        eventTimeLogic.emitImmediateWatermark(0L);
        assertThat(listener.idleUpdates).containsExactly(true);
    }

    /**
     * Once the last split is released nothing holds the subtask active, so it stands down if the
     * main output is idle. That timer reports idleness only once, so the release itself must
     * conclude when the timer already expired while the split was around.
     */
    @Test
    void subtaskGoesIdleWhenItsLastSplitIsReleasedAfterTheMainOutputTimerExpired() {
        final ManualClock mainInputActivityClock = new ManualClock(System.nanoTime());
        final RecordingListener listener = new RecordingListener();
        final TimestampsAndWatermarks<Long> eventTimeLogic =
                createEventTimeLogicWithIdleGenerator(
                        mainInputActivityClock, SystemClock.getInstance());

        final ReaderOutput<Long> mainOutput =
                eventTimeLogic.createMainOutput(new CollectingDataOutput<>(), listener);
        final SourceOutput<Long> splitOutput = mainOutput.createOutputForSplit("split-A");
        splitOutput.collect(1L, 1L);

        for (int i = 0; i < 2; i++) {
            mainInputActivityClock.advanceTime(IDLE_TIMEOUT.plusMillis(1));
            eventTimeLogic.emitImmediateWatermark(0L);
        }
        assertThat(listener.idleUpdates)
                .as("split-A is active, so the main output alone must not idle the subtask")
                .isEmpty();

        mainOutput.releaseOutputForSplit("split-A");

        assertThat(listener.idleUpdates)
                .as("no split is left to hold the subtask active and the main output is idle")
                .containsExactly(true);
    }

    /**
     * Before the main output has reported idleness, releasing the last split is no verdict: the
     * strategy may not detect idleness, or a wrapping strategy may still suppress its generators.
     * The main output's own timer then stands the subtask down.
     */
    @Test
    void releasingTheLastSplitDoesNotIdleTheSubtaskBeforeTheMainOutputReportedIdle() {
        final ManualClock mainInputActivityClock = new ManualClock(System.nanoTime());
        final RecordingListener listener = new RecordingListener();
        final TimestampsAndWatermarks<Long> eventTimeLogic =
                createEventTimeLogicWithIdleGenerator(
                        mainInputActivityClock, SystemClock.getInstance());

        final ReaderOutput<Long> mainOutput =
                eventTimeLogic.createMainOutput(new CollectingDataOutput<>(), listener);
        final SourceOutput<Long> splitOutput = mainOutput.createOutputForSplit("split-A");
        splitOutput.collect(1L, 1L);
        eventTimeLogic.emitImmediateWatermark(0L);
        mainOutput.releaseOutputForSplit("split-A");

        assertThat(listener.idleUpdates)
                .as("the main output has not reported yet, so the release alone must not idle it")
                .isEmpty();

        // the main output's timer started counting at the emit above and expires now
        mainInputActivityClock.advanceTime(IDLE_TIMEOUT.plusMillis(1));
        eventTimeLogic.emitImmediateWatermark(0L);

        assertThat(listener.idleUpdates)
                .as("no split is left, so the main output's timer stands the subtask down")
                .containsExactly(true);
    }

    /**
     * A registered split blocks the watermark until it is proven idle or active, so a subtask that
     * already stood down is announced active again when a split is registered.
     */
    @Test
    void registeringASplitReactivatesAnIdleSubtask() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final RecordingListener listener = new RecordingListener();
        final TimestampsAndWatermarks<Long> eventTimeLogic =
                createEventTimeLogicWithIdleGenerator(clock, clock);

        final ReaderOutput<Long> mainOutput =
                eventTimeLogic.createMainOutput(new CollectingDataOutput<>(), listener);

        // no split is assigned yet, so the subtask stands down through the main output's timer
        for (int i = 0; i < 2; i++) {
            clock.advanceTime(IDLE_TIMEOUT.plusMillis(1));
            eventTimeLogic.emitImmediateWatermark(0L);
        }
        assertThat(listener.idleUpdates).containsExactly(true);

        // one split never reports anything, the other only reports that it is idle
        mainOutput.createOutputForSplit("silent");
        mainOutput.createOutputForSplit("idle").markIdle();
        eventTimeLogic.emitImmediateWatermark(0L);

        assertThat(listener.idleUpdates)
                .as("a registered split is unproven, so the subtask is active again")
                .containsExactly(true, false);
    }

    /**
     * Without idleness detection nothing ever marks the subtask idle, so releasing its last split
     * must not either: the next split's records emit no watermark that could re-activate it.
     */
    @Test
    void subtaskWithoutIdlenessDetectionDoesNotGoIdleWhenItsLastSplitIsReleased() {
        final CollectingDataOutput<Long> output = new CollectingDataOutput<>();
        final RecordingListener listener = new RecordingListener();
        final TimestampsAndWatermarks<Long> eventTimeLogic =
                createEventTimeLogic(
                        WatermarkStrategy.<Long>noWatermarks()
                                .withTimestampAssigner((event, timestamp) -> event),
                        SystemClock.getInstance(),
                        SystemClock.getInstance());

        final ReaderOutput<Long> mainOutput = eventTimeLogic.createMainOutput(output, listener);
        final SourceOutput<Long> splitOutput = mainOutput.createOutputForSplit("split-A");
        splitOutput.collect(1L, 1L);
        eventTimeLogic.emitImmediateWatermark(0L);
        mainOutput.releaseOutputForSplit("split-A");

        assertThat(output.events).doesNotContain(WatermarkStatus.IDLE);
        assertThat(listener.idleUpdates).isEmpty();
    }

    /**
     * A reader that reports idleness through the main output speaks for itself: the operator
     * registers a split output per assigned split, and one the reader never reports through must
     * not hold that report back. A split that reported activity does, as before.
     */
    @Test
    void mainOutputIdleReportIsHeldBackOnlyBySplitsThatReportedActivity() {
        final RecordingListener listener = new RecordingListener();
        final TimestampsAndWatermarks<Long> eventTimeLogic =
                createEventTimeLogic(
                        WatermarkStrategy.<Long>noWatermarks()
                                .withTimestampAssigner((event, timestamp) -> event),
                        SystemClock.getInstance(),
                        SystemClock.getInstance());

        final ReaderOutput<Long> mainOutput =
                eventTimeLogic.createMainOutput(new CollectingDataOutput<>(), listener);
        mainOutput.createOutputForSplit("never-reports");
        mainOutput.markIdle();

        assertThat(listener.idleUpdates)
                .as("a split output that never reported says nothing against the reader")
                .containsExactly(true);

        mainOutput.createOutputForSplit("active").markActive();
        mainOutput.markIdle();

        assertThat(listener.idleUpdates)
                .as("a split that reported activity holds the main output's idle report back")
                .containsExactly(true, false);
    }

    /**
     * A split that is registered after the reader reported idleness through the main output
     * re-activates the subtask. Without idleness detection no timer proves that split idle, so the
     * subtask stays active until the reader reports idleness again.
     */
    @Test
    void splitRegisteredAfterMainOutputIdleReportHoldsTheSubtaskActiveUntilTheNextReport() {
        final RecordingListener listener = new RecordingListener();
        final TimestampsAndWatermarks<Long> eventTimeLogic =
                createEventTimeLogic(
                        WatermarkStrategy.<Long>noWatermarks()
                                .withTimestampAssigner((event, timestamp) -> event),
                        SystemClock.getInstance(),
                        SystemClock.getInstance());

        final ReaderOutput<Long> mainOutput =
                eventTimeLogic.createMainOutput(new CollectingDataOutput<>(), listener);
        mainOutput.markIdle();
        mainOutput.createOutputForSplit("split-A");
        eventTimeLogic.emitImmediateWatermark(0L);

        assertThat(listener.idleUpdates)
                .as("the reader has not reported since split-A was registered")
                .containsExactly(true, false);

        mainOutput.markIdle();

        assertThat(listener.idleUpdates)
                .as("split-A never reported activity, so the new report is not held back")
                .containsExactly(true, false, true);
    }

    // ------------------------------------------------------------------------

    private static TimestampsAndWatermarks<Long> createEventTimeLogic(
            RelativeClock mainInputActivityClock, Clock clock) {
        return createEventTimeLogic(
                WatermarkStrategy.<Long>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event)
                        .withIdleness(IDLE_TIMEOUT),
                mainInputActivityClock,
                clock);
    }

    /**
     * Generators that emit nothing until they see a record: a registered split then reports
     * activity or idleness only through collected records and its idleness timer.
     */
    private static TimestampsAndWatermarks<Long> createEventTimeLogicWithIdleGenerator(
            RelativeClock mainInputActivityClock, Clock clock) {
        return createEventTimeLogic(
                WatermarkStrategy.<Long>forGenerator(context -> new NeverEmits())
                        .withTimestampAssigner((event, timestamp) -> event)
                        .withIdleness(IDLE_TIMEOUT),
                mainInputActivityClock,
                clock);
    }

    private static TimestampsAndWatermarks<Long> createEventTimeLogic(
            WatermarkStrategy<Long> strategy, RelativeClock mainInputActivityClock, Clock clock) {
        return TimestampsAndWatermarks.createProgressiveEventTimeLogic(
                strategy,
                new UnregisteredMetricsGroup(),
                new TestProcessingTimeService(),
                0L,
                mainInputActivityClock,
                clock,
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());
    }

    private static final class NeverEmits implements WatermarkGenerator<Long> {
        @Override
        public void onEvent(Long event, long eventTimestamp, WatermarkOutput output) {}

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }

    private static final class RecordingListener
            implements TimestampsAndWatermarks.WatermarkUpdateListener {
        private final List<Boolean> idleUpdates = new ArrayList<>();

        @Override
        public void updateIdle(boolean isIdle) {
            idleUpdates.add(isIdle);
        }

        @Override
        public void updateCurrentEffectiveWatermark(long watermark) {}

        @Override
        public void updateCurrentSplitWatermark(String splitId, long watermark) {}

        @Override
        public void updateCurrentSplitIdle(String splitId, boolean idle) {}

        @Override
        public void splitFinished(String splitId) {}
    }
}
