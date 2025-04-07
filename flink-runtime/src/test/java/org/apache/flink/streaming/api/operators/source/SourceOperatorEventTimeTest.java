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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.operators.source.TestingSourceOperator.createTestOperator;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that validate correct handling of watermark generation in the {@link ReaderOutput} as
 * created by the {@link ProgressiveTimestampsAndWatermarks}.
 */
class SourceOperatorEventTimeTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMainOutputPeriodicWatermarks(boolean emitProgressiveWatermarks) throws Exception {
        final WatermarkStrategy<Integer> watermarkStrategy =
                WatermarkStrategy.forGenerator((ctx) -> new OnPeriodicTestWatermarkGenerator<>());

        final List<Watermark> result =
                testSequenceOfWatermarks(
                        emitProgressiveWatermarks,
                        watermarkStrategy,
                        (output) -> output.collect(0, 100L),
                        (output) -> output.collect(0, 120L),
                        (output) -> output.collect(0, 110L));

        assertWatermarksOrEmpty(
                emitProgressiveWatermarks, result, new Watermark(100L), new Watermark(120L));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMainOutputEventWatermarks(boolean emitProgressiveWatermarks) throws Exception {
        final WatermarkStrategy<Integer> watermarkStrategy =
                WatermarkStrategy.forGenerator((ctx) -> new OnEventTestWatermarkGenerator<>());

        final List<Watermark> result =
                testSequenceOfWatermarks(
                        emitProgressiveWatermarks,
                        watermarkStrategy,
                        (output) -> output.collect(0, 100L),
                        (output) -> output.collect(0, 120L),
                        (output) -> output.collect(0, 110L));

        assertWatermarksOrEmpty(
                emitProgressiveWatermarks, result, new Watermark(100L), new Watermark(120L));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPerSplitOutputPeriodicWatermarks(boolean emitProgressiveWatermarks) throws Exception {
        final WatermarkStrategy<Integer> watermarkStrategy =
                WatermarkStrategy.forGenerator((ctx) -> new OnPeriodicTestWatermarkGenerator<>());

        final List<Watermark> result =
                testSequenceOfWatermarks(
                        emitProgressiveWatermarks,
                        watermarkStrategy,
                        (output) -> {
                            output.createOutputForSplit("A");
                            output.createOutputForSplit("B");
                        },
                        (output) -> output.createOutputForSplit("A").collect(0, 100L),
                        (output) -> output.createOutputForSplit("B").collect(0, 200L),
                        (output) -> output.createOutputForSplit("A").collect(0, 150L),
                        (output) -> output.releaseOutputForSplit("A"),
                        (output) -> output.createOutputForSplit("B").collect(0, 200L));

        assertWatermarksOrEmpty(
                emitProgressiveWatermarks,
                result,
                new Watermark(100L),
                new Watermark(150L),
                new Watermark(200L));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPerSplitOutputEventWatermarks(boolean emitProgressiveWatermarks) throws Exception {
        final WatermarkStrategy<Integer> watermarkStrategy =
                WatermarkStrategy.forGenerator((ctx) -> new OnEventTestWatermarkGenerator<>());

        final List<Watermark> result =
                testSequenceOfWatermarks(
                        emitProgressiveWatermarks,
                        watermarkStrategy,
                        (output) -> {
                            output.createOutputForSplit("one");
                            output.createOutputForSplit("two");
                        },
                        (output) -> output.createOutputForSplit("one").collect(0, 100L),
                        (output) -> output.createOutputForSplit("two").collect(0, 200L),
                        (output) -> output.createOutputForSplit("one").collect(0, 150L),
                        (output) -> output.releaseOutputForSplit("one"),
                        (output) -> output.createOutputForSplit("two").collect(0, 200L));

        assertWatermarksOrEmpty(
                emitProgressiveWatermarks,
                result,
                new Watermark(100L),
                new Watermark(150L),
                new Watermark(200L));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCreatingPerSplitOutputOnSplitAddition(boolean emitProgressiveWatermarks)
            throws Exception {
        final WatermarkStrategy<Integer> watermarkStrategy =
                WatermarkStrategy.forGenerator((ctx) -> new OnEventTestWatermarkGenerator<>());

        InterpretingSourceReader reader =
                new InterpretingSourceReader(
                        // No watermark (no record from split 2, whose watermark is Long.MIN_VALUE)
                        (output) -> output.createOutputForSplit("1").collect(0, 100L),
                        (output) -> output.createOutputForSplit("1").collect(0, 200L),
                        (output) -> output.createOutputForSplit("1").collect(0, 300L),
                        // Emit watermark 150 (from the 1st record of split 2)
                        (output) -> output.createOutputForSplit("2").collect(0, 150L),
                        // Emit watermark 300 (from the 3rd record in split 1)
                        (output) -> output.createOutputForSplit("2").collect(0, 400L));
        SourceOperator<Integer, MockSourceSplit> sourceOperator =
                createTestOperator(reader, watermarkStrategy, emitProgressiveWatermarks);

        // Add two splits to SourceOperator. Output for two splits should be created during event
        // handling.
        sourceOperator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Arrays.asList(new MockSourceSplit(1), new MockSourceSplit(2)),
                        new MockSourceSplitSerializer()));

        final List<Watermark> result = testSequenceOfWatermarks(sourceOperator);
        assertWatermarksOrEmpty(
                emitProgressiveWatermarks, result, new Watermark(150L), new Watermark(300L));
    }

    @Test
    void testMainAndPerSplitWatermarkIdleness() throws Exception {
        final WatermarkStrategy<Integer> watermarkStrategy =
                WatermarkStrategy.forGenerator((ctx) -> new OnEventTestWatermarkGenerator<>());

        InterpretingSourceReader reader =
                new InterpretingSourceReader(
                        // Emit from main output
                        output -> output.collect(0, 100L),
                        // Mark main output as idle
                        // This should not generate IDLE event as per-split output is active
                        ReaderOutput::markIdle,
                        // Emit from per-split output
                        output -> output.createOutputForSplit("1").collect(0, 150L),
                        output -> output.createOutputForSplit("2").collect(0, 200L),
                        // Mark output of split 1 as idle
                        // This should not generate IDLE event as output of split 2 is active
                        output -> output.createOutputForSplit("1").markIdle(),
                        // Mark output of split 2 as idle
                        // Expect to have an IDLE event as all outputs (main, split 1 and split 2)
                        // are idle
                        output -> output.createOutputForSplit("2").markIdle(),
                        // Emit from main output
                        // Expect to have an ACTIVE event
                        output -> output.collect(0, 250L),
                        // Mark output 1 and 2 as idle again
                        // This should not generate IDLE event as main output is active
                        output -> output.createOutputForSplit("1").markIdle(),
                        output -> output.createOutputForSplit("2").markIdle(),
                        // Mark main output as idle again to test active from per-split output
                        // Expect to have an IDLE event
                        ReaderOutput::markIdle,
                        // Emit from per-split output
                        // Expect to have an ACTIVE event
                        output -> output.createOutputForSplit("1").collect(0, 300L));

        SourceOperator<Integer, MockSourceSplit> sourceOperator =
                createTestOperator(reader, watermarkStrategy, true);

        List<Object> events = testSequenceOfEvents(sourceOperator);

        assertThat(events)
                .containsExactly(
                        // Record and watermark from main output
                        new StreamRecord<>(0, 100L),
                        new Watermark(100L),
                        new WatermarkStatus(WatermarkStatus.IDLE_STATUS),
                        // Record and watermark from split 1
                        new StreamRecord<>(0, 150L),
                        new WatermarkStatus(WatermarkStatus.ACTIVE_STATUS),
                        new Watermark(150L),
                        // Record and watermark from split 2
                        new StreamRecord<>(0, 200L),
                        new Watermark(200L),
                        // IDLE event as output of main, split 1 and split 2 are idle
                        new WatermarkStatus(WatermarkStatus.IDLE_STATUS),
                        // Record and watermark from main output, together with an ACTIVE event
                        new StreamRecord<>(0, 250L),
                        new WatermarkStatus(WatermarkStatus.ACTIVE_STATUS),
                        new Watermark(250L),
                        // IDLE event as output of main, split 1 and split 2 are idle
                        new WatermarkStatus(WatermarkStatus.IDLE_STATUS),
                        // Record and watermark from split 1, together with an ACTIVE event
                        new StreamRecord<>(0, 300L),
                        new WatermarkStatus(WatermarkStatus.ACTIVE_STATUS),
                        new Watermark(300));
    }

    // ------------------------------------------------------------------------
    //   test execution helpers
    // ------------------------------------------------------------------------

    /**
     * Asserts that the given expected watermarks are present in the actual watermarks in STREAMING
     * mode. Otherwise, asserts that the list of actual watermarks is empty in BATCH mode.
     */
    private void assertWatermarksOrEmpty(
            boolean emitProgressiveWatermarks,
            List<Watermark> actualWatermarks,
            Watermark... expectedWatermarks) {
        // We add the expected Long.MAX_VALUE watermark to the end. We expect that for both
        // "STREAMING" and "BATCH" mode.
        if (emitProgressiveWatermarks) {
            assertThat(actualWatermarks).contains(expectedWatermarks);
        } else {
            assertThat(actualWatermarks).isEmpty();
        }
    }

    @SafeVarargs
    private final List<Watermark> testSequenceOfWatermarks(
            final boolean emitProgressiveWatermarks,
            final WatermarkStrategy<Integer> watermarkStrategy,
            final Consumer<ReaderOutput<Integer>>... actions)
            throws Exception {
        final SourceReader<Integer, MockSourceSplit> reader = new InterpretingSourceReader(actions);
        final SourceOperator<Integer, MockSourceSplit> sourceOperator =
                createTestOperator(reader, watermarkStrategy, emitProgressiveWatermarks);

        return testSequenceOfWatermarks(sourceOperator);
    }

    @SuppressWarnings("FinalPrivateMethod")
    private final List<Watermark> testSequenceOfWatermarks(
            SourceOperator<Integer, MockSourceSplit> sourceOperator) throws Exception {

        final List<Object> allEvents = testSequenceOfEvents(sourceOperator);

        return allEvents.stream()
                .filter((evt) -> evt instanceof Watermark)
                .map((evt) -> (Watermark) evt)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("FinalPrivateMethod")
    private final List<Object> testSequenceOfEvents(
            final SourceOperator<Integer, MockSourceSplit> sourceOperator) throws Exception {

        final CollectingDataOutput<Integer> out = new CollectingDataOutput<>();

        final TestProcessingTimeService timeService =
                ((TestProcessingTimeService) sourceOperator.getProcessingTimeService());

        while (sourceOperator.emitNext(out) != DataInputStatus.END_OF_INPUT) {
            timeService.setCurrentTime(timeService.getCurrentProcessingTime() + 100);
        }

        return out.events;
    }

    // ------------------------------------------------------------------------
    //   test mocks
    // ------------------------------------------------------------------------

    private static final class InterpretingSourceReader
            implements SourceReader<Integer, MockSourceSplit> {

        private final Iterator<Consumer<ReaderOutput<Integer>>> actions;

        @SafeVarargs
        private InterpretingSourceReader(Consumer<ReaderOutput<Integer>>... actions) {
            this.actions = Arrays.asList(actions).iterator();
        }

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<Integer> output) {
            if (actions.hasNext()) {
                actions.next().accept(output);
                return InputStatus.MORE_AVAILABLE;
            } else {
                return InputStatus.END_OF_INPUT;
            }
        }

        @Override
        public List<MockSourceSplit> snapshotState(long checkpointId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void addSplits(List<MockSourceSplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {}
    }
}
