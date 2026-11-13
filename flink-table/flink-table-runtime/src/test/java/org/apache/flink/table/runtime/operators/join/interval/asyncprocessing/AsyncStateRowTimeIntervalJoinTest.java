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

package org.apache.flink.table.runtime.operators.join.interval.asyncprocessing;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.runtime.asyncprocessing.operators.co.AsyncKeyedCoProcessOperatorWithWatermarkDelay;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.asyncprocessing.AsyncKeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.KeyedCoProcessOperatorWithWatermarkDelay;
import org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction;
import org.apache.flink.table.runtime.operators.join.interval.RowTimeIntervalJoin;
import org.apache.flink.table.runtime.operators.join.interval.TimeIntervalStreamJoinTestBase;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AsyncStateRowTimeIntervalJoin} alone: its fixes over the synchronous version and
 * its async cleanup, restore and failure paths. The cases both versions share run against both in
 * {@code RowTimeIntervalJoinTest}.
 *
 * <p>All joins are {@code l.rowtime BETWEEN r.rowtime - 5 AND r.rowtime + 9}.
 */
class AsyncStateRowTimeIntervalJoinTest extends TimeIntervalStreamJoinTestBase {

    private final RowDataKeySelector keySelector =
            HandwrittenSelectorUtil.getRowDataSelector(new int[] {1}, rowType.toRowFieldTypes());

    @Test
    void testRowsAtTimeZeroAndEarlier() throws Exception {
        AsyncKeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                testHarness = createTestHarness(FlinkJoinType.INNER, 0, joinFunction);
        testHarness.open();

        testHarness.processElement2(insertRecord(-2L, "k"));
        // Not cached, as its window has passed the operator time 0, but joined.
        testHarness.processElement1(insertRecord(-6L, "k"));
        // Cached; registers the cleanup timer at -4 + 5 + 1 = 2.
        testHarness.processElement1(insertRecord(-4L, "k"));
        testHarness.processElement1(insertRecord(0L, "k"));

        // The timer at 2 expires the left rows up to 3 - 5 - 1 = -3, keeping the row at 0.
        testHarness.processWatermark1(new Watermark(3));
        testHarness.processWatermark2(new Watermark(3));
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(4);

        testHarness.processElement2(insertRecord(3L, "k"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(-6L, "k", -2L, "k"));
        expectedOutput.add(insertRecord(-4L, "k", -2L, "k"));
        expectedOutput.add(insertRecord(0L, "k", -2L, "k"));
        expectedOutput.add(new Watermark(3 - 9));
        expectedOutput.add(insertRecord(0L, "k", 3L, "k"));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    void testCleanUpTimeSaturates() throws Exception {
        AsyncKeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                testHarness = createTestHarness(FlinkJoinType.LEFT, Long.MAX_VALUE, joinFunction);
        testHarness.open();

        // The cleanup time 1 + 5 + Long.MAX_VALUE + 1 is clamped to Long.MAX_VALUE. A wrapped,
        // negative time would fire at the next watermark and pad the row there.
        testHarness.processElement1(insertRecord(1L, "k"));
        testHarness.processWatermark1(new Watermark(100));
        testHarness.processWatermark2(new Watermark(100));
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(2);

        testHarness.processWatermark1(new Watermark(Long.MAX_VALUE));
        testHarness.processWatermark2(new Watermark(Long.MAX_VALUE));
        assertThat(testHarness.numKeyedStateEntries()).isZero();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(100 - 9));
        expectedOutput.add(insertRecord(1L, "k", null, null));
        expectedOutput.add(new Watermark(Long.MAX_VALUE - 9));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    void testPartialExpiryPadsEachRowOnce() throws Exception {
        AsyncKeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                testHarness = createTestHarness(FlinkJoinType.LEFT, 10, joinFunction);
        testHarness.open();

        // Registers the cleanup timer at 1 + 5 + 10 + 1 = 17.
        testHarness.processElement1(insertRecord(1L, "k"));
        testHarness.processElement1(insertRecord(2L, "k"));
        testHarness.processElement1(insertRecord(12L, "k"));
        testHarness.processElement1(insertRecord(40L, "k"));
        testHarness.processWatermark1(new Watermark(15));
        testHarness.processWatermark2(new Watermark(15));

        // Expires the left rows up to 15 - 5 - 1 = 9 while joining, before their timer fires.
        testHarness.processElement2(insertRecord(30L, "k"));

        // The timer at 17 expires the left rows up to 14. The rows at 1 and 2 must be gone already,
        // or they would be padded a second time. The row at 12 is padded and deleted, or it would
        // be padded again at the next timer. The row at 40 is kept.
        testHarness.processWatermark1(new Watermark(20));
        testHarness.processWatermark2(new Watermark(20));

        testHarness.processWatermark1(new Watermark(60));
        testHarness.processWatermark2(new Watermark(60));
        assertThat(testHarness.numKeyedStateEntries()).isZero();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(15 - 9));
        expectedOutput.add(insertRecord(1L, "k", null, null));
        expectedOutput.add(insertRecord(2L, "k", null, null));
        expectedOutput.add(insertRecord(12L, "k", null, null));
        expectedOutput.add(new Watermark(20 - 9));
        expectedOutput.add(insertRecord(40L, "k", null, null));
        expectedOutput.add(new Watermark(60 - 9));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    /**
     * Full expiry leaves no state or timer at the bulk-delete limit of 65,536 row times and above
     * it.
     */
    @ParameterizedTest(name = "rowTimes={0}")
    @ValueSource(ints = {65_536, 65_537})
    void testFullExpiry(int rowTimes) throws Exception {
        AsyncKeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                testHarness = createTestHarness(FlinkJoinType.INNER, 0, joinFunction);
        testHarness.open();

        for (long rowTime = 1; rowTime <= rowTimes; rowTime++) {
            testHarness.processElement1(insertRecord(rowTime, "k"));
        }
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(2);

        testHarness.processWatermark1(new Watermark(Long.MAX_VALUE));
        testHarness.processWatermark2(new Watermark(Long.MAX_VALUE));
        assertThat(testHarness.numKeyedStateEntries()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isZero();
        testHarness.close();
    }

    /** Restores the state of the synchronous version, as when enabling async state for a job. */
    @Test
    void testRestoreSyncSnapshot() throws Exception {
        RowTimeIntervalJoin syncJoin =
                new RowTimeIntervalJoin(
                        FlinkJoinType.LEFT,
                        -5,
                        9,
                        0,
                        0,
                        rowType,
                        rowType,
                        joinFunction,
                        0,
                        0,
                        -1L,
                        false);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> syncHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new KeyedCoProcessOperatorWithWatermarkDelay<>(
                                syncJoin, syncJoin.getMaxOutputDelay()),
                        keySelector,
                        keySelector,
                        keySelector.getProducedType());
        syncHarness.open();
        syncHarness.processElement1(insertRecord(1L, "a"));
        syncHarness.processElement1(insertRecord(2L, "b"));
        syncHarness.processElement2(insertRecord(3L, "a"));
        OperatorSubtaskState snapshot = syncHarness.snapshot(0L, 0L);
        syncHarness.close();

        AsyncKeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                testHarness = createTestHarness(FlinkJoinType.LEFT, 0, newJoinFunction());
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        // Joins the restored left row at 1, which is not padded as it was joined before.
        testHarness.processElement2(insertRecord(4L, "a"));
        // Fires the restored timers, which pad the restored left row at 2.
        testHarness.processWatermark1(new Watermark(100));
        testHarness.processWatermark2(new Watermark(100));
        assertThat(testHarness.numKeyedStateEntries()).isZero();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1L, "a", 4L, "a"));
        expectedOutput.add(insertRecord(2L, "b", null, null));
        expectedOutput.add(new Watermark(100 - 9));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    void testFailureInStateCallbackFailsOperator() throws Exception {
        funcCode =
                funcCode.replace(
                        "return true;", "throw new RuntimeException(\"Expected test failure\");");
        try (AsyncKeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                testHarness = createTestHarness(FlinkJoinType.INNER, 0, newJoinFunction())) {
            testHarness.open();

            testHarness.processElement1(insertRecord(1L, "k"));
            // The join condition runs in the callback of the request that reads the left rows.
            assertThatThrownBy(() -> testHarness.processElement2(insertRecord(1L, "k")))
                    .hasStackTraceContaining("Expected test failure");
        }
    }

    @Test
    void testCrossDomainEarlyFireUsesStartProcessingTime() throws Exception {
        AtomicReference<RunnableWithException> onJoin = new AtomicReference<>();
        IntervalJoinFunction nonMatchingJoin =
                new IntervalJoinFunction(
                        null,
                        (InternalTypeInfo<RowData>) joinFunction.getProducedType(),
                        new boolean[] {true}) {
                    @Override
                    public void open(OpenContext openContext) {}

                    @Override
                    public void join(
                            RowData first, RowData second, RowData joinKey, Collector<RowData> out)
                            throws Exception {
                        RunnableWithException hook = onJoin.get();
                        if (hook != null) {
                            hook.run();
                        }
                    }
                };
        try (AsyncKeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                testHarness =
                        createTestHarness(FlinkJoinType.LEFT, 0, nonMatchingJoin, 10L, true)) {
            testHarness.open();
            testHarness.setProcessingTime(100L);

            testHarness.processElement2(insertRecord(10L, "k"));
            testHarness.processElement1(insertRecord(10L, "k"));
            testHarness.processElement1(insertRecord(10L, "k"));
            // Advance processing time during the right-cache scan callback; the deadline must
            // still be 100 + 10 = 110, not 105 + 10 = 115.
            onJoin.set(() -> testHarness.getProcessingTimeService().setCurrentTime(105L));
            testHarness.processElement1(insertRecord(12L, "k"));

            testHarness.setProcessingTime(110L);
            List<Object> expectedOutput = new ArrayList<>();
            expectedOutput.add(insertRecord(10L, "k", null, null));
            expectedOutput.add(insertRecord(10L, "k", null, null));
            expectedOutput.add(insertRecord(12L, "k", null, null));
            assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

            testHarness.processWatermark1(new Watermark(30));
            testHarness.processWatermark2(new Watermark(30));
            expectedOutput.add(new Watermark(30 - 9));
            assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
            assertThat(testHarness.numKeyedStateEntries()).isZero();
        }
    }

    private AsyncKeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createTestHarness(
                    FlinkJoinType joinType, long minCleanUpInterval, IntervalJoinFunction joinFunc)
                    throws Exception {
        return createTestHarness(joinType, minCleanUpInterval, joinFunc, -1L, false);
    }

    private AsyncKeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createTestHarness(
                    FlinkJoinType joinType,
                    long minCleanUpInterval,
                    IntervalJoinFunction joinFunc,
                    long earlyFireDelay,
                    boolean earlyFireCrossDomain)
                    throws Exception {
        AsyncStateRowTimeIntervalJoin join =
                new AsyncStateRowTimeIntervalJoin(
                        joinType,
                        -5,
                        9,
                        0,
                        minCleanUpInterval,
                        rowType,
                        rowType,
                        joinFunc,
                        0,
                        0,
                        earlyFireDelay,
                        earlyFireCrossDomain);
        return AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                new AsyncKeyedCoProcessOperatorWithWatermarkDelay<>(join, join.getMaxOutputDelay()),
                keySelector,
                keySelector,
                keySelector.getProducedType());
    }
}
