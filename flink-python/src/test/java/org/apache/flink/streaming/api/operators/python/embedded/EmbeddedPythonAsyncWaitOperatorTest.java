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

package org.apache.flink.streaming.api.operators.python.embedded;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.runtime.operators.python.scalar.PythonScalarFunctionOperatorTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link EmbeddedPythonAsyncWaitOperator}. The Python side is replaced by a fake that
 * records the invocations, so that each test decides when and how the handlers complete.
 */
class EmbeddedPythonAsyncWaitOperatorTest {

    private static final long TIMEOUT = 1000L;

    @Test
    void testOrderedOutput() throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 10, 0L, true);
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(operator)) {
            harness.open();
            harness.processElement(1, 1L);
            harness.processElement(2, 2L);
            harness.processWatermark(new Watermark(2L));
            harness.processElement(3, 3L);

            // complete in reverse order
            operator.complete(2, 30L);
            operator.complete(1, 20L);
            operator.complete(0, 10L);
            harness.endInput();

            assertThat(harness.getOutput())
                    .containsExactly(
                            new StreamRecord<>(10, 1L),
                            new StreamRecord<>(20, 2L),
                            new Watermark(2L),
                            new StreamRecord<>(30, 3L));
        }
    }

    @Test
    void testUnorderedOutput() throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 10, 0L, false);
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(operator)) {
            harness.open();
            harness.processElement(1, 1L);
            harness.processElement(2, 2L);
            harness.processWatermark(new Watermark(2L));
            harness.processElement(3, 3L);

            // the record after the watermark completes first, but stays behind the watermark
            operator.complete(2, 30L);
            operator.complete(1, 20L);
            drainMailbox(harness);
            assertThat(harness.getOutput()).containsExactly(new StreamRecord<>(20, 2L));

            operator.complete(0, 10L);
            harness.endInput();
            assertThat(harness.getOutput())
                    .containsExactly(
                            new StreamRecord<>(20, 2L),
                            new StreamRecord<>(10, 1L),
                            new Watermark(2L),
                            new StreamRecord<>(30, 3L));
        }
    }

    @Test
    void testMultipleResultsOfOneInputAreCopied() throws Exception {
        FakeOperator<Row> operator = new FakeOperator<>(Types.ROW(Types.INT), 10, 0L, true);
        try (OneInputStreamOperatorTestHarness<Integer, Row> harness = createHarness(operator)) {
            harness.open();
            harness.processElement(1, 1L);
            harness.processElement(2, 2L);

            // the row converter reuses its output row
            operator.complete(0, externalRow(10L), externalRow(11L), externalRow(12L));
            operator.complete(1, (Object) externalRow(20L));
            harness.endInput();

            assertThat(harness.getOutput())
                    .containsExactly(
                            new StreamRecord<>(Row.of(10), 1L),
                            new StreamRecord<>(Row.of(11), 1L),
                            new StreamRecord<>(Row.of(12), 1L),
                            new StreamRecord<>(Row.of(20), 2L));
        }
    }

    @Test
    void testOnlyFirstCompletionCounts() throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 10, 0L, true);
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(operator)) {
            harness.open();
            harness.processElement(1, 1L);

            operator.complete(0, 10L);
            operator.complete(0, 11L);
            operator.handler(0).completeExceptionally("late failure");
            harness.endInput();

            assertThat(harness.getOutput()).containsExactly(new StreamRecord<>(10, 1L));
            assertThat(harness.getEnvironment().getActualExternalFailureCause()).isEmpty();
        }
    }

    @Test
    void testTimeoutResult() throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 10, TIMEOUT, true);
        operator.onTimeout =
                (handler, value) -> handler.complete(Collections.singletonList(-((Long) value)));
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(operator)) {
            harness.open();
            harness.setProcessingTime(0L);
            harness.processElement(1, 1L);
            harness.processElement(2, 2L);

            operator.complete(1, 20L);
            harness.setProcessingTime(TIMEOUT);
            harness.endInput();

            // only the input without a result timed out
            assertThat(operator.timeoutCalls).isEqualTo(1);
            assertThat(harness.getOutput())
                    .containsExactly(new StreamRecord<>(-1, 1L), new StreamRecord<>(20, 2L));
        }
    }

    @Test
    void testTimeoutAfterCompletionDoesNotCallPython() throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 10, TIMEOUT, true);
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(operator)) {
            harness.open();
            harness.setProcessingTime(0L);
            harness.processElement(1, 1L);

            // the result is still in the mailbox when the timer fires
            operator.complete(0, 10L);
            harness.setProcessingTime(TIMEOUT);
            harness.endInput();

            assertThat(operator.timeoutCalls).isZero();
            assertThat(harness.getOutput()).containsExactly(new StreamRecord<>(10, 1L));
        }
    }

    @Test
    void testResultThatArrivesDuringTimeoutWins() throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 10, TIMEOUT, true);
        operator.onTimeout =
                (handler, value) -> {
                    // the event loop thread delivers the result while timeout() runs
                    handler.complete(Collections.singletonList(10L));
                    handler.complete(Collections.singletonList(-1L));
                };
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(operator)) {
            harness.open();
            harness.setProcessingTime(0L);
            harness.processElement(1, 1L);
            harness.setProcessingTime(TIMEOUT);
            harness.endInput();

            assertThat(operator.timeoutCalls).isEqualTo(1);
            assertThat(harness.getOutput()).containsExactly(new StreamRecord<>(10, 1L));
        }
    }

    @Test
    void testRaisingTimeoutFailsTaskAndDrainsQueue() throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 10, TIMEOUT, true);
        operator.onTimeout =
                (handler, value) -> handler.completeExceptionally("TimeoutError: input " + value);
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(operator)) {
            harness.getEnvironment().setExpectedExternalFailureCause(Throwable.class);
            harness.open();
            harness.setProcessingTime(0L);
            harness.processElement(1, 1L);
            harness.processElement(2, 2L);

            operator.complete(1, 20L);
            harness.setProcessingTime(TIMEOUT);
            harness.endInput();

            assertThat(harness.getOutput()).containsExactly(new StreamRecord<>(20, 2L));
            assertFailedWith(harness, "TimeoutError: input 1");
        }
    }

    @ParameterizedTest(name = "ordered = {0}")
    @ValueSource(booleans = {true, false})
    void testAsyncFailureFailsTaskAndDrainsQueue(boolean ordered) throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 10, 0L, ordered);
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(operator)) {
            harness.getEnvironment().setExpectedExternalFailureCause(Throwable.class);
            harness.open();
            harness.processElement(1, 1L);
            harness.processElement(2, 2L);

            operator.handler(0).completeExceptionally("ValueError: boom");
            operator.complete(1, 20L);
            harness.endInput();

            assertThat(harness.getOutput()).containsExactly(new StreamRecord<>(20, 2L));
            assertFailedWith(harness, "ValueError: boom");
        }
    }

    @ParameterizedTest(name = "ordered = {0}")
    @ValueSource(booleans = {true, false})
    void testSnapshotAndRestore(boolean ordered) throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 10, 0L, ordered);
        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(operator)) {
            harness.open();
            harness.processElement(1, 1L);
            harness.processElement(2, 2L);
            harness.processWatermark(new Watermark(2L));
            harness.processElement(3, 3L);
            // its result is still in the mailbox, so the input is in the snapshot as well
            operator.complete(1, 20L);

            snapshot = harness.snapshot(1L, 1L);
        }

        FakeOperator<Integer> restored = new FakeOperator<>(Types.INT, 10, 0L, ordered);
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(restored)) {
            harness.initializeState(snapshot);
            harness.open();

            // the unordered queue keeps no order within the elements before a watermark
            assertThat(restored.invokedValues())
                    .containsExactlyInAnyOrder(1L, 2L, 3L)
                    .last()
                    .isEqualTo(3L);
            for (int i = 0; i < 3; i++) {
                restored.complete(i, (Long) restored.invokedValues().get(i) * 10);
            }
            harness.endInput();

            List<Object> output = new ArrayList<>(harness.getOutput());
            assertThat(output).hasSize(4);
            if (ordered) {
                assertThat(output.subList(0, 2))
                        .containsExactly(new StreamRecord<>(10, 1L), new StreamRecord<>(20, 2L));
            } else {
                assertThat(output.subList(0, 2))
                        .containsExactlyInAnyOrder(
                                new StreamRecord<>(10, 1L), new StreamRecord<>(20, 2L));
            }
            assertThat(output.subList(2, 4))
                    .containsExactly(new Watermark(2L), new StreamRecord<>(30, 3L));
        }
    }

    @Test
    void testResultsAfterCloseAreDropped() throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 10, TIMEOUT, true);
        OneInputStreamOperatorTestHarness<Integer, Integer> harness = createHarness(operator);
        harness.open();
        harness.setProcessingTime(0L);
        harness.processElement(1, 1L);
        harness.processElement(2, 2L);
        harness.close();

        // late callbacks from the event loop thread must neither fail nor reach the mailbox
        operator.complete(0, 10L);
        operator.handler(1).completeExceptionally("CancelledError");

        assertThat(operator.closeCalls).isPositive();
        assertThat(harness.getTaskMailbox().drain())
                .noneMatch(mail -> mail.toString().contains("EmbeddedPythonAsyncWaitOperator"));
        assertThat(harness.getOutput()).isEmpty();
        assertThat(harness.getEnvironment().getActualExternalFailureCause()).isEmpty();
    }

    @Test
    void testAvailabilityFollowsQueueCapacity() throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 2, 0L, true);
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(operator)) {
            harness.open();
            assertThat(operator.isAvailable()).isTrue();

            harness.processElement(1, 1L);
            harness.processElement(2, 2L);
            assertThat(operator.isAvailable()).isFalse();

            operator.complete(0, 10L);
            drainMailbox(harness);
            assertThat(operator.isAvailable()).isTrue();

            operator.complete(1, 20L);
            harness.endInput();
            assertThat(harness.getOutput())
                    .containsExactly(new StreamRecord<>(10, 1L), new StreamRecord<>(20, 2L));
        }
    }

    @Test
    void testEmissionWaitsForDownstreamAvailability() throws Exception {
        FakeOperator<Integer> operator = new FakeOperator<>(Types.INT, 10, 0L, true);
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(operator)) {
            harness.open();
            CompletableFuture<Void> downstream = new CompletableFuture<>();
            operator.setDownstreamAvailabilityProvider(() -> downstream);

            harness.processElement(1, 1L);
            operator.complete(0, 10L);
            drainMailbox(harness);
            assertThat(harness.getOutput()).isEmpty();

            downstream.complete(null);
            drainMailbox(harness);
            assertThat(harness.getOutput()).containsExactly(new StreamRecord<>(10, 1L));
            harness.endInput();
        }
    }

    private static <OUT> OneInputStreamOperatorTestHarness<Integer, OUT> createHarness(
            FakeOperator<OUT> operator) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new EmbeddedPythonAsyncWaitOperatorFactory<>(operator), IntSerializer.INSTANCE);
    }

    private static void drainMailbox(OneInputStreamOperatorTestHarness<?, ?> harness)
            throws Exception {
        for (Mail mail : harness.getTaskMailbox().drain()) {
            mail.run();
        }
    }

    private static void assertFailedWith(
            OneInputStreamOperatorTestHarness<?, ?> harness, String pythonError) {
        assertThat(harness.getEnvironment().getActualExternalFailureCause())
                .hasValueSatisfying(
                        t ->
                                assertThat(ExceptionUtils.findThrowableWithMessage(t, pythonError))
                                        .isPresent());
    }

    /** The form in which the Python row converter hands a row with one INT field to Java. */
    private static Object[] externalRow(long value) {
        return new Object[] {0L, new Object[] {value}};
    }

    /** Replaces the calls into Python with a recording fake. */
    private static class FakeOperator<OUT> extends EmbeddedPythonAsyncWaitOperator<Integer, OUT> {

        private static final long serialVersionUID = 1L;

        private final transient List<Handler> handlers = new ArrayList<>();

        private final transient List<Object> values = new ArrayList<>();

        /** Mimics the default Python {@code timeout}, which raises {@code TimeoutError}. */
        transient BiConsumer<Handler, Object> onTimeout =
                (handler, value) -> handler.completeExceptionally("TimeoutError");

        transient int timeoutCalls;

        transient int closeCalls;

        FakeOperator(TypeInformation<OUT> outputType, int capacity, long timeout, boolean ordered) {
            super(
                    new Configuration(),
                    new DataStreamPythonFunctionInfo(
                            PythonScalarFunctionOperatorTestBase.DummyPythonFunction.INSTANCE, 0),
                    Types.INT,
                    outputType,
                    capacity,
                    timeout,
                    ordered);
        }

        @Override
        public void open() throws Exception {
            // no interpreter: set up what the embedded base classes would and skip Python
            collector = new TimestampedCollector<>(output);
            outputDataConverter =
                    PythonTypeUtils.TypeInfoToDataConverter.typeInfoDataConverter(outputTypeInfo);
            openAsyncWait();
        }

        @Override
        void invokeAsync(Handler handler, Object value) {
            handlers.add(handler);
            values.add(value);
        }

        @Override
        void invokeTimeout(Handler handler, Object value) {
            timeoutCalls++;
            onTimeout.accept(handler, value);
        }

        @Override
        void closePythonOperation() {
            closeCalls++;
        }

        Handler handler(int index) {
            return handlers.get(index);
        }

        List<Object> invokedValues() {
            return values;
        }

        void complete(int index, Object... results) {
            handlers.get(index).complete(Arrays.asList(results));
        }
    }
}
