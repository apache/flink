/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OutputTag;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IntervalJoinOperator}. Those tests cover correctness and cleaning of state */
@ExtendWith(ParameterizedTestExtension.class)
class IntervalJoinOperatorTest {

    private final boolean lhsFasterThanRhs;

    @Parameters(name = "lhs faster than rhs: {0}")
    private static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {{true}, {false}});
    }

    public IntervalJoinOperatorTest(boolean lhsFasterThanRhs) {
        this.lhsFasterThanRhs = lhsFasterThanRhs;
    }

    @TestTemplate
    void testImplementationMirrorsCorrectly() throws Exception {

        long lowerBound = 1;
        long upperBound = 3;

        boolean lowerBoundInclusive = true;
        boolean upperBoundInclusive = false;

        setupHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)
                .processElementsAndWatermarks(1, 4)
                .andExpect(
                        streamRecordOf(1, 2),
                        streamRecordOf(1, 3),
                        streamRecordOf(2, 3),
                        streamRecordOf(2, 4),
                        streamRecordOf(3, 4))
                .noLateRecords()
                .close();

        setupHarness(-1 * upperBound, upperBoundInclusive, -1 * lowerBound, lowerBoundInclusive)
                .processElementsAndWatermarks(1, 4)
                .andExpect(
                        streamRecordOf(2, 1),
                        streamRecordOf(3, 1),
                        streamRecordOf(3, 2),
                        streamRecordOf(4, 2),
                        streamRecordOf(4, 3))
                .noLateRecords()
                .close();
    }

    @TestTemplate // lhs - 2 <= rhs <= rhs + 2
    void testNegativeInclusiveAndNegativeInclusive() throws Exception {

        setupHarness(-2, true, -1, true)
                .processElementsAndWatermarks(1, 4)
                .andExpect(
                        streamRecordOf(2, 1),
                        streamRecordOf(3, 1),
                        streamRecordOf(3, 2),
                        streamRecordOf(4, 2),
                        streamRecordOf(4, 3))
                .noLateRecords()
                .close();
    }

    @TestTemplate // lhs - 1 <= rhs <= rhs + 1
    void testNegativeInclusiveAndPositiveInclusive() throws Exception {

        setupHarness(-1, true, 1, true)
                .processElementsAndWatermarks(1, 4)
                .andExpect(
                        streamRecordOf(1, 1),
                        streamRecordOf(1, 2),
                        streamRecordOf(2, 1),
                        streamRecordOf(2, 2),
                        streamRecordOf(2, 3),
                        streamRecordOf(3, 2),
                        streamRecordOf(3, 3),
                        streamRecordOf(3, 4),
                        streamRecordOf(4, 3),
                        streamRecordOf(4, 4))
                .noLateRecords()
                .close();
    }

    @TestTemplate // lhs + 1 <= rhs <= lhs + 2
    void testPositiveInclusiveAndPositiveInclusive() throws Exception {

        setupHarness(1, true, 2, true)
                .processElementsAndWatermarks(1, 4)
                .andExpect(
                        streamRecordOf(1, 2),
                        streamRecordOf(1, 3),
                        streamRecordOf(2, 3),
                        streamRecordOf(2, 4),
                        streamRecordOf(3, 4))
                .noLateRecords()
                .close();
    }

    @TestTemplate
    void testNegativeExclusiveAndNegativeExlusive() throws Exception {

        setupHarness(-3, false, -1, false)
                .processElementsAndWatermarks(1, 4)
                .andExpect(streamRecordOf(3, 1), streamRecordOf(4, 2))
                .noLateRecords()
                .close();
    }

    @TestTemplate
    void testNegativeExclusiveAndPositiveExlusive() throws Exception {

        setupHarness(-1, false, 1, false)
                .processElementsAndWatermarks(1, 4)
                .andExpect(
                        streamRecordOf(1, 1),
                        streamRecordOf(2, 2),
                        streamRecordOf(3, 3),
                        streamRecordOf(4, 4))
                .noLateRecords()
                .close();
    }

    @TestTemplate
    void testPositiveExclusiveAndPositiveExlusive() throws Exception {

        setupHarness(1, false, 3, false)
                .processElementsAndWatermarks(1, 4)
                .andExpect(streamRecordOf(1, 3), streamRecordOf(2, 4))
                .noLateRecords()
                .close();
    }

    @TestTemplate
    void testStateCleanupNegativeInclusiveNegativeInclusive() throws Exception {

        setupHarness(-1, true, 0, true)
                .processElement1(1)
                .processElement1(2)
                .processElement1(3)
                .processElement1(4)
                .processElement1(5)
                .processElement2(1)
                .processElement2(2)
                .processElement2(3)
                .processElement2(4)
                .processElement2(5) // fill both buffers with values
                .processWatermark1(1)
                .processWatermark2(1) // set common watermark to 1 and check that data is cleaned
                .assertLeftBufferContainsOnly(2, 3, 4, 5)
                .assertRightBufferContainsOnly(1, 2, 3, 4, 5)
                .processWatermark1(4) // set common watermark to 4 and check that data is cleaned
                .processWatermark2(4)
                .assertLeftBufferContainsOnly(5)
                .assertRightBufferContainsOnly(4, 5)
                .processWatermark1(
                        6) // set common watermark to 6 and check that data all buffers are empty
                .processWatermark2(6)
                .assertLeftBufferEmpty()
                .assertRightBufferEmpty()
                .close();
    }

    @TestTemplate
    void testStateCleanupNegativePositiveNegativeExlusive() throws Exception {
        setupHarness(-2, false, 1, false)
                .processElement1(1)
                .processElement1(2)
                .processElement1(3)
                .processElement1(4)
                .processElement1(5)
                .processElement2(1)
                .processElement2(2)
                .processElement2(3)
                .processElement2(4)
                .processElement2(5) // fill both buffers with values
                .processWatermark1(1)
                .processWatermark2(1) // set common watermark to 1 and check that data is cleaned
                .assertLeftBufferContainsOnly(2, 3, 4, 5)
                .assertRightBufferContainsOnly(1, 2, 3, 4, 5)
                .processWatermark1(4) // set common watermark to 4 and check that data is cleaned
                .processWatermark2(4)
                .assertLeftBufferContainsOnly(5)
                .assertRightBufferContainsOnly(4, 5)
                .processWatermark1(
                        6) // set common watermark to 6 and check that data all buffers are empty
                .processWatermark2(6)
                .assertLeftBufferEmpty()
                .assertRightBufferEmpty()
                .close();
    }

    @TestTemplate
    void testStateCleanupPositiveInclusivePositiveInclusive() throws Exception {
        setupHarness(0, true, 1, true)
                .processElement1(1)
                .processElement1(2)
                .processElement1(3)
                .processElement1(4)
                .processElement1(5)
                .processElement2(1)
                .processElement2(2)
                .processElement2(3)
                .processElement2(4)
                .processElement2(5) // fill both buffers with values
                .processWatermark1(1)
                .processWatermark2(1) // set common watermark to 1 and check that data is cleaned
                .assertLeftBufferContainsOnly(1, 2, 3, 4, 5)
                .assertRightBufferContainsOnly(2, 3, 4, 5)
                .processWatermark1(4) // set common watermark to 4 and check that data is cleaned
                .processWatermark2(4)
                .assertLeftBufferContainsOnly(4, 5)
                .assertRightBufferContainsOnly(5)
                .processWatermark1(
                        6) // set common watermark to 6 and check that data all buffers are empty
                .processWatermark2(6)
                .assertLeftBufferEmpty()
                .assertRightBufferEmpty()
                .close();
    }

    @TestTemplate
    void testStateCleanupPositiveExlusivePositiveExclusive() throws Exception {
        setupHarness(-1, false, 2, false)
                .processElement1(1)
                .processElement1(2)
                .processElement1(3)
                .processElement1(4)
                .processElement1(5)
                .processElement2(1)
                .processElement2(2)
                .processElement2(3)
                .processElement2(4)
                .processElement2(5) // fill both buffers with values
                .processWatermark1(1)
                .processWatermark2(1) // set common watermark to 1 and check that data is cleaned
                .assertLeftBufferContainsOnly(1, 2, 3, 4, 5)
                .assertRightBufferContainsOnly(2, 3, 4, 5)
                .processWatermark1(4) // set common watermark to 4 and check that data is cleaned
                .processWatermark2(4)
                .assertLeftBufferContainsOnly(4, 5)
                .assertRightBufferContainsOnly(5)
                .processWatermark1(
                        6) // set common watermark to 6 and check that data all buffers are empty
                .processWatermark2(6)
                .assertLeftBufferEmpty()
                .assertRightBufferEmpty()
                .close();
    }

    @TestTemplate
    void testRestoreFromSnapshot() throws Exception {

        // config
        int lowerBound = -1;
        boolean lowerBoundInclusive = true;
        int upperBound = 1;
        boolean upperBoundInclusive = true;

        // create first test harness
        OperatorSubtaskState handles;
        List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput;

        try (TestHarness testHarness =
                createTestHarness(
                        lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

            testHarness.setup();
            testHarness.open();

            // process elements with first test harness
            testHarness.processElement1(createStreamRecord(1, "lhs"));
            testHarness.processWatermark1(new Watermark(1));

            testHarness.processElement2(createStreamRecord(1, "rhs"));
            testHarness.processWatermark2(new Watermark(1));

            testHarness.processElement1(createStreamRecord(2, "lhs"));
            testHarness.processWatermark1(new Watermark(2));

            testHarness.processElement2(createStreamRecord(2, "rhs"));
            testHarness.processWatermark2(new Watermark(2));

            testHarness.processElement1(createStreamRecord(3, "lhs"));
            testHarness.processWatermark1(new Watermark(3));

            testHarness.processElement2(createStreamRecord(3, "rhs"));
            testHarness.processWatermark2(new Watermark(3));

            // snapshot and validate output
            handles = testHarness.snapshot(0, 0);
            testHarness.close();

            expectedOutput =
                    Lists.newArrayList(
                            streamRecordOf(1, 1),
                            streamRecordOf(1, 2),
                            streamRecordOf(2, 1),
                            streamRecordOf(2, 2),
                            streamRecordOf(2, 3),
                            streamRecordOf(3, 2),
                            streamRecordOf(3, 3));

            TestHarnessUtil.assertNoLateRecords(testHarness.getOutput());
            assertOutput(expectedOutput, testHarness.getOutput());
        }

        try (TestHarness newTestHarness =
                createTestHarness(
                        lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {
            // create new test harness from snapshpt

            newTestHarness.setup();
            newTestHarness.initializeState(handles);
            newTestHarness.open();

            // process elements
            newTestHarness.processElement1(createStreamRecord(4, "lhs"));
            newTestHarness.processWatermark1(new Watermark(4));

            newTestHarness.processElement2(createStreamRecord(4, "rhs"));
            newTestHarness.processWatermark2(new Watermark(4));

            // assert expected output
            expectedOutput =
                    Lists.newArrayList(
                            streamRecordOf(3, 4), streamRecordOf(4, 3), streamRecordOf(4, 4));

            TestHarnessUtil.assertNoLateRecords(newTestHarness.getOutput());
            assertOutput(expectedOutput, newTestHarness.getOutput());
        }
    }

    @TestTemplate
    void testContextCorrectLeftTimestamp() throws Exception {

        IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> op =
                new IntervalJoinOperator<>(
                        -1,
                        1,
                        true,
                        true,
                        null,
                        null,
                        TestElem.serializer(),
                        TestElem.serializer(),
                        new ProcessJoinFunction<TestElem, TestElem, Tuple2<TestElem, TestElem>>() {
                            @Override
                            public void processElement(
                                    TestElem left,
                                    TestElem right,
                                    Context ctx,
                                    Collector<Tuple2<TestElem, TestElem>> out)
                                    throws Exception {
                                assertThat(ctx.getLeftTimestamp()).isEqualTo(left.ts);
                            }
                        });

        try (TestHarness testHarness =
                new TestHarness(
                        op,
                        (elem) -> elem.key,
                        (elem) -> elem.key,
                        TypeInformation.of(String.class))) {

            testHarness.setup();
            testHarness.open();

            processElementsAndWatermarks(testHarness);
        }
    }

    @TestTemplate
    void testReturnsCorrectTimestamp() throws Exception {
        IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> op =
                new IntervalJoinOperator<>(
                        -1,
                        1,
                        true,
                        true,
                        null,
                        null,
                        TestElem.serializer(),
                        TestElem.serializer(),
                        new ProcessJoinFunction<TestElem, TestElem, Tuple2<TestElem, TestElem>>() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            public void processElement(
                                    TestElem left,
                                    TestElem right,
                                    Context ctx,
                                    Collector<Tuple2<TestElem, TestElem>> out)
                                    throws Exception {
                                assertThat(ctx.getTimestamp())
                                        .isEqualTo(Math.max(left.ts, right.ts));
                            }
                        });

        try (TestHarness testHarness =
                new TestHarness(
                        op,
                        (elem) -> elem.key,
                        (elem) -> elem.key,
                        TypeInformation.of(String.class))) {

            testHarness.setup();
            testHarness.open();

            processElementsAndWatermarks(testHarness);
        }
    }

    @TestTemplate
    void testContextCorrectRightTimestamp() throws Exception {

        IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> op =
                new IntervalJoinOperator<>(
                        -1,
                        1,
                        true,
                        true,
                        null,
                        null,
                        TestElem.serializer(),
                        TestElem.serializer(),
                        new ProcessJoinFunction<TestElem, TestElem, Tuple2<TestElem, TestElem>>() {
                            @Override
                            public void processElement(
                                    TestElem left,
                                    TestElem right,
                                    Context ctx,
                                    Collector<Tuple2<TestElem, TestElem>> out)
                                    throws Exception {
                                assertThat(ctx.getRightTimestamp()).isEqualTo(right.ts);
                            }
                        });

        try (TestHarness testHarness =
                new TestHarness(
                        op,
                        (elem) -> elem.key,
                        (elem) -> elem.key,
                        TypeInformation.of(String.class))) {

            testHarness.setup();
            testHarness.open();

            processElementsAndWatermarks(testHarness);
        }
    }

    @TestTemplate
    void testFailsWithNoTimestampsLeft() throws Exception {
        try (TestHarness newTestHarness = createTestHarness(0L, true, 0L, true)) {

            newTestHarness.setup();
            newTestHarness.open();

            // note that the StreamRecord has no timestamp in constructor
            assertThatThrownBy(
                            () ->
                                    newTestHarness.processElement1(
                                            new StreamRecord<>(new TestElem(0, "lhs"))))
                    .isInstanceOf(FlinkException.class);
        }
    }

    @TestTemplate // (expected = FlinkException.class)
    void testFailsWithNoTimestampsRight() throws Exception {
        try (TestHarness newTestHarness = createTestHarness(0L, true, 0L, true)) {

            newTestHarness.setup();
            newTestHarness.open();

            // note that the StreamRecord has no timestamp in constructor
            assertThatThrownBy(
                            () ->
                                    newTestHarness.processElement2(
                                            new StreamRecord<>(new TestElem(0, "rhs"))))
                    .isInstanceOf(FlinkException.class);
        }
    }

    @TestTemplate
    void testDiscardsLateData() throws Exception {
        setupHarness(-1, true, 1, true)
                .processElement1(1)
                .processElement2(1)
                .processElement1(2)
                .processElement2(2)
                .processElement1(3)
                .processElement2(3)
                .processWatermark1(3)
                .processWatermark2(3)
                .processElement1(1) // this element is late and should not be joined again
                .processElement1(4)
                .processElement2(4)
                .processElement1(5)
                .processElement2(5)
                .andExpect(
                        streamRecordOf(1, 1),
                        streamRecordOf(1, 2),
                        streamRecordOf(2, 1),
                        streamRecordOf(2, 2),
                        streamRecordOf(2, 3),
                        streamRecordOf(3, 2),
                        streamRecordOf(3, 3),
                        streamRecordOf(3, 4),
                        streamRecordOf(4, 3),
                        streamRecordOf(4, 4),
                        streamRecordOf(4, 5),
                        streamRecordOf(5, 4),
                        streamRecordOf(5, 5))
                .noLateRecords()
                .close();
    }

    @TestTemplate
    void testLateData() throws Exception {
        OutputTag<TestElem> leftLateTag = new OutputTag<TestElem>("left_late") {};
        OutputTag<TestElem> rightLateTag = new OutputTag<TestElem>("right_late") {};
        setupHarness(-1, true, 1, true, leftLateTag, rightLateTag)
                .processElement1(3)
                .processElement2(3)
                .processWatermark1(3)
                .processWatermark2(3)
                .processElement1(4)
                .processElement2(4)
                .processElement1(1) // the left side element is late
                .processElement2(2) // the right side element is late
                .processElement1(5)
                .processElement2(5)
                .andExpect(
                        streamRecordOf(3, 3),
                        streamRecordOf(3, 4),
                        streamRecordOf(4, 3),
                        streamRecordOf(4, 4),
                        streamRecordOf(4, 5),
                        streamRecordOf(5, 4),
                        streamRecordOf(5, 5))
                .expectLateRecords(leftLateTag, createStreamRecord(1, "lhs"))
                .expectLateRecords(rightLateTag, createStreamRecord(2, "rhs"))
                .close();
    }

    private void assertEmpty(MapState<Long, ?> state) throws Exception {
        assertThat(state.keys()).isEmpty();
    }

    private void assertContainsOnly(MapState<Long, ?> state, long... ts) throws Exception {
        for (long t : ts) {
            String message =
                    "Keys not found in state. \n Expected: "
                            + Arrays.toString(ts)
                            + "\n Actual:   "
                            + state.keys();
            assertThat(state.contains(t)).as(message).isTrue();
        }

        String message =
                "Too many objects in state. \n Expected: "
                        + Arrays.toString(ts)
                        + "\n Actual:   "
                        + state.keys();
        assertThat(state.keys()).as(message).hasSize(ts.length);
    }

    private <T1, T2> void assertOutput(
            Iterable<StreamRecord<T1>> expectedOutput, Queue<T2> actualOutput) {

        int actualSize =
                actualOutput.stream()
                        .filter(elem -> elem instanceof StreamRecord)
                        .collect(Collectors.toList())
                        .size();

        int expectedSize = Iterables.size(expectedOutput);

        assertThat(actualSize)
                .as("Expected and actual size of stream records different")
                .isEqualTo(expectedSize);

        for (StreamRecord<T1> record : expectedOutput) {
            assertThat(actualOutput.contains(record)).isTrue();
        }
    }

    private TestHarness createTestHarness(
            long lowerBound,
            boolean lowerBoundInclusive,
            long upperBound,
            boolean upperBoundInclusive)
            throws Exception {

        IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> operator =
                new IntervalJoinOperator<>(
                        lowerBound,
                        upperBound,
                        lowerBoundInclusive,
                        upperBoundInclusive,
                        null,
                        null,
                        TestElem.serializer(),
                        TestElem.serializer(),
                        new PassthroughFunction());

        return new TestHarness(
                operator,
                (elem) -> elem.key, // key
                (elem) -> elem.key, // key
                TypeInformation.of(String.class));
    }

    private JoinTestBuilder setupHarness(
            long lowerBound,
            boolean lowerBoundInclusive,
            long upperBound,
            boolean upperBoundInclusive,
            OutputTag<TestElem> leftLateDataOutputTag,
            OutputTag<TestElem> rightLateDataOutputTag)
            throws Exception {

        IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> operator =
                new IntervalJoinOperator<>(
                        lowerBound,
                        upperBound,
                        lowerBoundInclusive,
                        upperBoundInclusive,
                        leftLateDataOutputTag,
                        rightLateDataOutputTag,
                        TestElem.serializer(),
                        TestElem.serializer(),
                        new PassthroughFunction());

        TestHarness t =
                new TestHarness(
                        operator,
                        (elem) -> elem.key, // key
                        (elem) -> elem.key, // key
                        TypeInformation.of(String.class));

        return new JoinTestBuilder(t, operator);
    }

    private JoinTestBuilder setupHarness(
            long lowerBound,
            boolean lowerBoundInclusive,
            long upperBound,
            boolean upperBoundInclusive)
            throws Exception {

        return setupHarness(
                lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive, null, null);
    }

    private class JoinTestBuilder {

        private IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>>
                operator;
        private TestHarness testHarness;

        public JoinTestBuilder(
                TestHarness t,
                IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>>
                        operator)
                throws Exception {

            this.testHarness = t;
            this.operator = operator;
            t.open();
            t.setup();
        }

        public TestHarness get() {
            return testHarness;
        }

        public JoinTestBuilder processElement1(int ts) throws Exception {
            testHarness.processElement1(createStreamRecord(ts, "lhs"));
            return this;
        }

        public JoinTestBuilder processElement2(int ts) throws Exception {
            testHarness.processElement2(createStreamRecord(ts, "rhs"));
            return this;
        }

        public JoinTestBuilder processWatermark1(int ts) throws Exception {
            testHarness.processWatermark1(new Watermark(ts));
            return this;
        }

        public JoinTestBuilder processWatermark2(int ts) throws Exception {
            testHarness.processWatermark2(new Watermark(ts));
            return this;
        }

        public JoinTestBuilder processElementsAndWatermarks(int from, int to) throws Exception {
            if (lhsFasterThanRhs) {
                // add to lhs
                for (int i = from; i <= to; i++) {
                    testHarness.processElement1(createStreamRecord(i, "lhs"));
                    testHarness.processWatermark1(new Watermark(i));
                }

                // add to rhs
                for (int i = from; i <= to; i++) {
                    testHarness.processElement2(createStreamRecord(i, "rhs"));
                    testHarness.processWatermark2(new Watermark(i));
                }
            } else {
                // add to rhs
                for (int i = from; i <= to; i++) {
                    testHarness.processElement2(createStreamRecord(i, "rhs"));
                    testHarness.processWatermark2(new Watermark(i));
                }

                // add to lhs
                for (int i = from; i <= to; i++) {
                    testHarness.processElement1(createStreamRecord(i, "lhs"));
                    testHarness.processWatermark1(new Watermark(i));
                }
            }

            return this;
        }

        @SafeVarargs
        public final JoinTestBuilder andExpect(StreamRecord<Tuple2<TestElem, TestElem>>... elems) {
            assertOutput(Lists.newArrayList(elems), testHarness.getOutput());
            return this;
        }

        public JoinTestBuilder assertLeftBufferContainsOnly(long... timestamps) {

            try {
                assertContainsOnly(operator.getLeftBuffer(), timestamps);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public JoinTestBuilder assertRightBufferContainsOnly(long... timestamps) {

            try {
                assertContainsOnly(operator.getRightBuffer(), timestamps);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public JoinTestBuilder assertLeftBufferEmpty() {
            try {
                assertEmpty(operator.getLeftBuffer());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public JoinTestBuilder assertRightBufferEmpty() {
            try {
                assertEmpty(operator.getRightBuffer());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        @SafeVarargs
        public final JoinTestBuilder expectLateRecords(
                OutputTag<TestElem> tag, StreamRecord<TestElem>... elems) {
            assertOutput(Lists.newArrayList(elems), testHarness.getSideOutput(tag));
            return this;
        }

        public JoinTestBuilder noLateRecords() {
            TestHarnessUtil.assertNoLateRecords(this.testHarness.getOutput());
            return this;
        }

        public void close() throws Exception {
            testHarness.close();
        }
    }

    private static class PassthroughFunction
            extends ProcessJoinFunction<TestElem, TestElem, Tuple2<TestElem, TestElem>> {

        @Override
        public void processElement(
                TestElem left,
                TestElem right,
                Context ctx,
                Collector<Tuple2<TestElem, TestElem>> out)
                throws Exception {
            out.collect(Tuple2.of(left, right));
        }
    }

    private StreamRecord<Tuple2<TestElem, TestElem>> streamRecordOf(long lhsTs, long rhsTs) {
        TestElem lhs = new TestElem(lhsTs, "lhs");
        TestElem rhs = new TestElem(rhsTs, "rhs");

        long ts = Math.max(lhsTs, rhsTs);
        return new StreamRecord<>(Tuple2.of(lhs, rhs), ts);
    }

    private static class TestElem {
        String key;
        long ts;
        String source;

        public TestElem(long ts, String source) {
            this.key = "key";
            this.ts = ts;
            this.source = source;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestElem testElem = (TestElem) o;

            if (ts != testElem.ts) {
                return false;
            }

            if (key != null ? !key.equals(testElem.key) : testElem.key != null) {
                return false;
            }

            return source != null ? source.equals(testElem.source) : testElem.source == null;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (int) (ts ^ (ts >>> 32));
            result = 31 * result + (source != null ? source.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return this.source + ":" + this.ts;
        }

        public static TypeSerializer<TestElem> serializer() {
            return TypeInformation.of(new TypeHint<TestElem>() {})
                    .createSerializer(new SerializerConfigImpl());
        }
    }

    private static StreamRecord<TestElem> createStreamRecord(long ts, String source) {
        TestElem testElem = new TestElem(ts, source);
        return new StreamRecord<>(testElem, ts);
    }

    private void processElementsAndWatermarks(TestHarness testHarness) throws Exception {
        if (lhsFasterThanRhs) {
            // add to lhs
            for (int i = 1; i <= 4; i++) {
                testHarness.processElement1(createStreamRecord(i, "lhs"));
                testHarness.processWatermark1(new Watermark(i));
            }

            // add to rhs
            for (int i = 1; i <= 4; i++) {
                testHarness.processElement2(createStreamRecord(i, "rhs"));
                testHarness.processWatermark2(new Watermark(i));
            }
        } else {
            // add to rhs
            for (int i = 1; i <= 4; i++) {
                testHarness.processElement2(createStreamRecord(i, "rhs"));
                testHarness.processWatermark2(new Watermark(i));
            }

            // add to lhs
            for (int i = 1; i <= 4; i++) {
                testHarness.processElement1(createStreamRecord(i, "lhs"));
                testHarness.processWatermark1(new Watermark(i));
            }
        }
    }

    /** Custom test harness to avoid endless generics in all of the test code. */
    private static class TestHarness
            extends KeyedTwoInputStreamOperatorTestHarness<
                    String, TestElem, TestElem, Tuple2<TestElem, TestElem>> {

        TestHarness(
                TwoInputStreamOperator<TestElem, TestElem, Tuple2<TestElem, TestElem>> operator,
                KeySelector<TestElem, String> keySelector1,
                KeySelector<TestElem, String> keySelector2,
                TypeInformation<String> keyType)
                throws Exception {
            super(operator, keySelector1, keySelector2, keyType);
        }
    }
}
