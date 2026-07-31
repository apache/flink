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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.generated.GeneratedResultFutureWrapper;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the timeout handling in {@link AsyncLookupJoinRunner}. */
public class AsyncLookupJoinRunnerTimeoutTest {

    private static final int ASYNC_BUFFER_CAPACITY = 100;

    private final TypeSerializer<RowData> inSerializer =
            new RowDataSerializer(
                    DataTypes.INT().getLogicalType(), DataTypes.STRING().getLogicalType());

    private final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.STRING().getLogicalType()
                    });

    private final DataType rightRowDataType =
            DataTypes.ROW(
                            DataTypes.FIELD("f0", DataTypes.INT()),
                            DataTypes.FIELD("f1", DataTypes.STRING()))
                    .bridgedTo(RowData.class);

    @SuppressWarnings({"unchecked", "rawtypes"})
    private final DataStructureConverter<RowData, Object> fetcherConverter =
            (DataStructureConverter) DataStructureConverters.getConverter(rightRowDataType);

    private final RowDataSerializer rightRowSerializer =
            (RowDataSerializer)
                    InternalSerializers.<RowData>create(rightRowDataType.getLogicalType());

    // ---------------------------------------------------------------------------------
    // Unit-level tests (direct asyncInvoke/timeout calls)
    // ---------------------------------------------------------------------------------

    @Test
    public void testTimeoutPropagatesException() throws Exception {
        AsyncLookupJoinRunner joinRunner =
                createRunner(new NeverCompletingFetcherFunction(), false);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);
        joinRunner.timeout(input, resultFuture);

        assertThat(resultFuture.error).isInstanceOf(TimeoutException.class);
        assertThat(resultFuture.result).isNull();

        joinRunner.close();
    }

    @Test
    public void testTimeoutWithFallbackResultsInnerJoin() throws Exception {
        AsyncLookupJoinRunner joinRunner =
                createRunner(new FallbackOnTimeoutFetcherFunction(), false);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);
        joinRunner.timeout(input, resultFuture);

        assertThat(resultFuture.error).isNull();
        assertThat(resultFuture.result).hasSize(1);
        RowData joined = resultFuture.result.iterator().next();
        assertThat(joined.getInt(0)).isEqualTo(1);
        assertThat(joined.getString(1).toString()).isEqualTo("a");
        assertThat(joined.getInt(2)).isEqualTo(1);
        assertThat(joined.getString(3).toString()).isEqualTo("fallback");

        joinRunner.close();
    }

    @Test
    public void testTimeoutWithFallbackResultsLeftJoin() throws Exception {
        AsyncLookupJoinRunner joinRunner =
                createRunner(new FallbackOnTimeoutFetcherFunction(), true);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);
        joinRunner.timeout(input, resultFuture);

        assertThat(resultFuture.error).isNull();
        assertThat(resultFuture.result).hasSize(1);
        RowData joined = resultFuture.result.iterator().next();
        assertThat(joined.getInt(0)).isEqualTo(1);
        assertThat(joined.getString(1).toString()).isEqualTo("a");
        assertThat(joined.getInt(2)).isEqualTo(1);
        assertThat(joined.getString(3).toString()).isEqualTo("fallback");

        joinRunner.close();
    }

    @Test
    public void testTimeoutWithEmptyFallbackInnerJoin() throws Exception {
        AsyncLookupJoinRunner joinRunner =
                createRunner(new EmptyFallbackOnTimeoutFetcherFunction(), false);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);
        joinRunner.timeout(input, resultFuture);

        assertThat(resultFuture.error).isNull();
        assertThat(resultFuture.result).isEmpty();

        joinRunner.close();
    }

    @Test
    public void testTimeoutWithEmptyFallbackLeftJoin() throws Exception {
        AsyncLookupJoinRunner joinRunner =
                createRunner(new EmptyFallbackOnTimeoutFetcherFunction(), true);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);
        joinRunner.timeout(input, resultFuture);

        assertThat(resultFuture.error).isNull();
        assertThat(resultFuture.result).hasSize(1);
        RowData joined = resultFuture.result.iterator().next();
        assertThat(joined.getInt(0)).isEqualTo(1);
        assertThat(joined.getString(1).toString()).isEqualTo("a");
        assertThat(joined.isNullAt(2)).isTrue();
        assertThat(joined.isNullAt(3)).isTrue();

        joinRunner.close();
    }

    @Test
    public void testTimeoutWhenFutureAlreadyCompleted() throws Exception {
        AsyncLookupJoinRunner joinRunner = createRunner(new SyncCompletingFetcherFunction(), false);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);

        assertThat(resultFuture.result).hasSize(1);
        RowData joined = resultFuture.result.iterator().next();
        assertThat(joined.getInt(2)).isEqualTo(1);
        assertThat(joined.getString(3).toString()).isEqualTo("sync");

        joinRunner.timeout(input, resultFuture);

        assertThat(resultFuture.error).isNull();
        assertThat(resultFuture.result).hasSize(1);
        RowData resultAfterTimeout = resultFuture.result.iterator().next();
        assertThat(resultAfterTimeout.getInt(2)).isEqualTo(1);
        assertThat(resultAfterTimeout.getString(3).toString()).isEqualTo("sync");

        joinRunner.close();
    }

    @Test
    public void testTimeoutFetcherThrowsException() throws Exception {
        AsyncLookupJoinRunner joinRunner =
                createRunner(new ThrowingTimeoutFetcherFunction(), false);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);
        joinRunner.timeout(input, resultFuture);

        assertThat(resultFuture.error)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("fetcher timeout failed");

        joinRunner.close();
    }

    @Test
    public void testTimeoutExceptionDoesNotExhaustBuffer() throws Exception {
        AsyncLookupJoinRunner joinRunner =
                createRunner(new ThrowingTimeoutFetcherFunction(), false);

        for (int i = 0; i < ASYNC_BUFFER_CAPACITY + 2; i++) {
            final int id = i;
            final CapturingResultFuture resultFuture = new CapturingResultFuture();
            CompletableFuture<Void> invocation =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    RowData input = row(id, "v" + id);
                                    joinRunner.asyncInvoke(input, resultFuture);
                                    joinRunner.timeout(input, resultFuture);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            invocation.get(5, TimeUnit.SECONDS);
            assertThat(resultFuture.error)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("fetcher timeout failed");
            assertThat(resultFuture.result).isNull();
        }

        joinRunner.close();
    }

    @Test
    public void testNonIdentityResultConverter() throws Exception {
        DataType externalRightRowDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.INT()),
                        DataTypes.FIELD("f1", DataTypes.STRING()));
        @SuppressWarnings({"unchecked", "rawtypes"})
        DataStructureConverter<RowData, Object> externalConverter =
                (DataStructureConverter)
                        DataStructureConverters.getConverter(externalRightRowDataType);

        AsyncLookupJoinRunner joinRunner =
                createRunner(
                        new ExternalTypeSyncCompletingFetcher(),
                        externalConverter,
                        new AsyncLookupJoinHarnessTest.TestingFetcherResultFuture(),
                        false);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);

        assertThat(resultFuture.error).isNull();
        assertThat(resultFuture.result).hasSize(1);
        RowData joined = resultFuture.result.iterator().next();
        assertThat(joined.getInt(0)).isEqualTo(1);
        assertThat(joined.getString(1).toString()).isEqualTo("a");
        assertThat(joined.getInt(2)).isEqualTo(1);
        assertThat(joined.getString(3).toString()).isEqualTo("converted");

        joinRunner.close();
    }

    @Test
    public void testFetcherCompletesExceptionally() throws Exception {
        AsyncLookupJoinRunner joinRunner =
                createRunner(new SyncExceptionalFetcherFunction(), false);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);

        assertThat(resultFuture.result).isNull();
        assertThat(resultFuture.error)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("async fetch failed");

        joinRunner.close();
    }

    @Test
    public void testLateCompleteIgnoredAfterTimeout() throws Exception {
        AsyncLookupJoinRunner joinRunner = createRunner(new StoringFetcherFunction(), false);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);
        ResultFuture<RowData> innerFuture = StoringFetcherFunction.lastStoredFuture;

        joinRunner.timeout(input, resultFuture);
        assertThat(resultFuture.error).isInstanceOf(TimeoutException.class);

        resultFuture.error = null;
        resultFuture.result = null;

        innerFuture.complete(Collections.singletonList(GenericRowData.of(1, fromString("late"))));

        assertThat(resultFuture.result).isNull();
        assertThat(resultFuture.error).isNull();

        joinRunner.close();
    }

    @Test
    public void testLateCompleteExceptionallyIgnoredAfterTimeout() throws Exception {
        AsyncLookupJoinRunner joinRunner = createRunner(new StoringFetcherFunction(), false);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);
        ResultFuture<RowData> innerFuture = StoringFetcherFunction.lastStoredFuture;

        joinRunner.timeout(input, resultFuture);
        assertThat(resultFuture.error).isInstanceOf(TimeoutException.class);

        resultFuture.error = null;
        resultFuture.result = null;

        innerFuture.completeExceptionally(new RuntimeException("late error"));

        assertThat(resultFuture.result).isNull();
        assertThat(resultFuture.error).isNull();

        joinRunner.close();
    }

    @Test
    public void testJoinConditionDelegateCompletesExceptionally() throws Exception {
        AsyncLookupJoinRunner joinRunner =
                createRunner(
                        new SyncCompletingFetcherFunction(),
                        fetcherConverter,
                        new ExceptionallyCompletingResultFuture(),
                        false);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);

        assertThat(resultFuture.result).isNull();
        assertThat(resultFuture.error)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("join condition failed");

        joinRunner.close();
    }

    @Test
    public void testJoinConditionThrowsException() throws Exception {
        AsyncLookupJoinRunner joinRunner =
                createRunner(
                        new SyncCompletingFetcherFunction(),
                        fetcherConverter,
                        new ThrowingConditionResultFuture(),
                        false);

        RowData input = row(1, "a");
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        joinRunner.asyncInvoke(input, resultFuture);

        assertThat(resultFuture.result).isNull();
        assertThat(resultFuture.error)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("join condition threw");

        joinRunner.close();
    }

    // ---------------------------------------------------------------------------------
    // Integration-level tests (via OneInputStreamOperatorTestHarness)
    // ---------------------------------------------------------------------------------

    @Test
    public void testTimeoutInnerJoinWithFallback() throws Exception {
        SlowFetcherFunction.latch = new CountDownLatch(1);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarnessWithTimeout(false, 10L);

        testHarness.open();
        testHarness.setProcessingTime(0L);

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(insertRecord(1, "a"));
            testHarness.processElement(insertRecord(3, "c"));
            testHarness.processElement(insertRecord(5, "e"));
        }

        testHarness.setProcessingTime(11L);
        SlowFetcherFunction.latch.countDown();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Timeout_1"));
        expectedOutput.add(insertRecord(3, "c", 3, "Timeout_3"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testTimeoutLeftJoinWithFallback() throws Exception {
        SlowFetcherFunction.latch = new CountDownLatch(1);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarnessWithTimeout(true, 10L);

        testHarness.open();
        testHarness.setProcessingTime(0L);

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(insertRecord(1, "a"));
            testHarness.processElement(insertRecord(5, "e"));
        }

        testHarness.setProcessingTime(11L);
        SlowFetcherFunction.latch.countDown();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Timeout_1"));
        expectedOutput.add(insertRecord(5, "e", null, null));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testMixedNormalAndTimeoutInnerJoin() throws Exception {
        MixedSpeedFetcherFunction.latch = new CountDownLatch(1);
        MixedSpeedFetcherFunction.completionLatch = new CountDownLatch(2);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarnessWithTimeout(new MixedSpeedFetcherFunction(), false, 10L);

        testHarness.open();
        testHarness.setProcessingTime(0L);

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(insertRecord(1, "a"));
            testHarness.processElement(insertRecord(3, "c"));
            testHarness.processElement(insertRecord(4, "d"));
            testHarness.processElement(insertRecord(5, "e"));
        }

        testHarness.setProcessingTime(11L);

        MixedSpeedFetcherFunction.latch.countDown();
        assertThat(MixedSpeedFetcherFunction.completionLatch.await(5, TimeUnit.SECONDS)).isTrue();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(3, "c", 3, "Timeout_3"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testMixedNormalAndTimeoutLeftJoin() throws Exception {
        MixedSpeedFetcherFunction.latch = new CountDownLatch(1);
        MixedSpeedFetcherFunction.completionLatch = new CountDownLatch(2);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarnessWithTimeout(new MixedSpeedFetcherFunction(), true, 10L);

        testHarness.open();
        testHarness.setProcessingTime(0L);

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(insertRecord(1, "a"));
            testHarness.processElement(insertRecord(3, "c"));
            testHarness.processElement(insertRecord(4, "d"));
            testHarness.processElement(insertRecord(5, "e"));
        }

        testHarness.setProcessingTime(11L);

        MixedSpeedFetcherFunction.latch.countDown();
        assertThat(MixedSpeedFetcherFunction.completionLatch.await(5, TimeUnit.SECONDS)).isTrue();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(3, "c", 3, "Timeout_3"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(insertRecord(5, "e", null, null));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testTimeoutLateResultIgnored() throws Exception {
        SlowFetcherFunction.latch = new CountDownLatch(1);
        SlowFetcherFunction.completionLatch = new CountDownLatch(2);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarnessWithTimeout(false, 10L);

        testHarness.open();
        testHarness.setProcessingTime(0L);

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(insertRecord(1, "a"));
            testHarness.processElement(insertRecord(3, "c"));
        }

        testHarness.setProcessingTime(11L);

        SlowFetcherFunction.latch.countDown();
        assertThat(SlowFetcherFunction.completionLatch.await(5, TimeUnit.SECONDS)).isTrue();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Timeout_1"));
        expectedOutput.add(insertRecord(3, "c", 3, "Timeout_3"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
    }

    // ---------------------------------------------------------------------------------
    // Helper methods
    // ---------------------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "rawtypes"})
    private AsyncLookupJoinRunner createRunner(
            AsyncFunction<RowData, ?> fetcherFunction, boolean isLeftOuterJoin) throws Exception {
        return createRunner(
                fetcherFunction,
                fetcherConverter,
                new AsyncLookupJoinHarnessTest.TestingFetcherResultFuture(),
                isLeftOuterJoin);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private AsyncLookupJoinRunner createRunner(
            AsyncFunction<RowData, ?> fetcherFunction,
            DataStructureConverter<RowData, Object> converter,
            TableFunctionResultFuture<RowData> resultFutureTemplate,
            boolean isLeftOuterJoin)
            throws Exception {
        AsyncLookupJoinRunner joinRunner =
                new AsyncLookupJoinRunner(
                        new GeneratedFunctionWrapper(fetcherFunction),
                        converter,
                        new GeneratedResultFutureWrapper<>(resultFutureTemplate),
                        new GeneratedFunctionWrapper(
                                new LookupJoinHarnessTest.TestingPreFilterCondition()),
                        rightRowSerializer,
                        isLeftOuterJoin,
                        ASYNC_BUFFER_CAPACITY);
        joinRunner.setRuntimeContext(new MockStreamingRuntimeContext(1, 0));
        joinRunner.open(DefaultOpenContext.INSTANCE);
        return joinRunner;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private OneInputStreamOperatorTestHarness<RowData, RowData> createHarnessWithTimeout(
            boolean isLeftJoin, long timeoutMs) throws Exception {
        return createHarnessWithTimeout(new SlowFetcherFunction(), isLeftJoin, timeoutMs);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private OneInputStreamOperatorTestHarness<RowData, RowData> createHarnessWithTimeout(
            AsyncFunction<RowData, RowData> fetcher, boolean isLeftJoin, long timeoutMs)
            throws Exception {
        RichAsyncFunction<RowData, RowData> joinRunner =
                new AsyncLookupJoinRunner(
                        new GeneratedFunctionWrapper(fetcher),
                        fetcherConverter,
                        new GeneratedResultFutureWrapper<>(
                                new AsyncLookupJoinHarnessTest.TestingFetcherResultFuture()),
                        new GeneratedFunctionWrapper(
                                new LookupJoinHarnessTest.TestingPreFilterCondition()),
                        rightRowSerializer,
                        isLeftJoin,
                        ASYNC_BUFFER_CAPACITY);

        return new OneInputStreamOperatorTestHarness<>(
                new AsyncWaitOperatorFactory<>(
                        joinRunner,
                        timeoutMs,
                        ASYNC_BUFFER_CAPACITY,
                        AsyncDataStream.OutputMode.ORDERED),
                inSerializer);
    }

    // ---------------------------------------------------------------------------------
    // Test utilities
    // ---------------------------------------------------------------------------------

    private static final class CapturingResultFuture implements ResultFuture<RowData> {
        Collection<RowData> result;
        Throwable error;

        @Override
        public void complete(Collection<RowData> result) {
            this.result = result;
        }

        @Override
        public void complete(CollectionSupplier<RowData> supplier) {
            try {
                this.result = supplier.get();
            } catch (Exception e) {
                this.error = e;
            }
        }

        @Override
        public void completeExceptionally(Throwable error) {
            this.error = error;
        }
    }

    // ---------------------------------------------------------------------------------
    // Test fetcher functions
    // ---------------------------------------------------------------------------------

    /** Never completes asyncInvoke; uses default timeout (TimeoutException). */
    public static final class NeverCompletingFetcherFunction extends AbstractRichFunction
            implements AsyncFunction<RowData, RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {}
    }

    /** Never completes asyncInvoke; provides fallback results on timeout. */
    public static final class FallbackOnTimeoutFetcherFunction extends AbstractRichFunction
            implements AsyncFunction<RowData, RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {}

        @Override
        public void timeout(RowData input, ResultFuture<RowData> resultFuture) {
            int id = input.getInt(0);
            resultFuture.complete(
                    Collections.singletonList(GenericRowData.of(id, fromString("fallback"))));
        }
    }

    /** Never completes asyncInvoke; returns empty collection on timeout. */
    public static final class EmptyFallbackOnTimeoutFetcherFunction extends AbstractRichFunction
            implements AsyncFunction<RowData, RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {}

        @Override
        public void timeout(RowData input, ResultFuture<RowData> resultFuture) {
            resultFuture.complete(Collections.emptyList());
        }
    }

    /** Completes asyncInvoke synchronously. */
    public static final class SyncCompletingFetcherFunction extends AbstractRichFunction
            implements AsyncFunction<RowData, RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {
            int id = input.getInt(0);
            resultFuture.complete(
                    Collections.singletonList(GenericRowData.of(id, fromString("sync"))));
        }
    }

    /** Never completes asyncInvoke; throws exception on timeout. */
    public static final class ThrowingTimeoutFetcherFunction extends AbstractRichFunction
            implements AsyncFunction<RowData, RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {}

        @Override
        public void timeout(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
            throw new RuntimeException("fetcher timeout failed");
        }
    }

    /** Completes asyncInvoke synchronously with external Row objects. */
    public static final class ExternalTypeSyncCompletingFetcher extends AbstractRichFunction
            implements AsyncFunction<RowData, Object> {
        private static final long serialVersionUID = 1L;

        @Override
        public void asyncInvoke(RowData input, ResultFuture<Object> resultFuture) {
            resultFuture.complete(Collections.singletonList(Row.of(input.getInt(0), "converted")));
        }
    }

    /** Completes asyncInvoke synchronously with completeExceptionally. */
    public static final class SyncExceptionalFetcherFunction extends AbstractRichFunction
            implements AsyncFunction<RowData, RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {
            resultFuture.completeExceptionally(new RuntimeException("async fetch failed"));
        }
    }

    /** Stores inner ResultFuture reference for late-completion tests. */
    public static final class StoringFetcherFunction extends AbstractRichFunction
            implements AsyncFunction<RowData, RowData> {
        private static final long serialVersionUID = 1L;
        static volatile ResultFuture<RowData> lastStoredFuture;

        @Override
        public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {
            lastStoredFuture = resultFuture;
        }
    }

    /**
     * A fetcher that blocks all async lookups on a latch, providing fallback data on timeout. Used
     * to test the timeout handling in {@link AsyncLookupJoinRunner} via the operator harness.
     */
    public static final class SlowFetcherFunction extends AbstractRichFunction
            implements AsyncFunction<RowData, RowData> {

        private static final long serialVersionUID = 1L;

        private static final Map<Integer, List<RowData>> normalData = new HashMap<>();
        private static final Map<Integer, List<RowData>> fallbackData = new HashMap<>();

        static volatile CountDownLatch latch;
        static volatile CountDownLatch completionLatch;

        static {
            normalData.put(
                    1, Collections.singletonList(GenericRowData.of(1, fromString("Julian"))));
            normalData.put(
                    3,
                    Arrays.asList(
                            GenericRowData.of(3, fromString("Jark")),
                            GenericRowData.of(3, fromString("Jackson"))));
            normalData.put(
                    4, Collections.singletonList(GenericRowData.of(4, fromString("Fabian"))));

            fallbackData.put(
                    1, Collections.singletonList(GenericRowData.of(1, fromString("Timeout_1"))));
            fallbackData.put(
                    3, Collections.singletonList(GenericRowData.of(3, fromString("Timeout_3"))));
            fallbackData.put(
                    4, Collections.singletonList(GenericRowData.of(4, fromString("Timeout_4"))));
        }

        private transient ExecutorService executor;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            this.executor = Executors.newFixedThreadPool(2);
        }

        @Override
        public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {
            int id = input.getInt(0);
            executor.submit(
                    () -> {
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            return;
                        }
                        Collection<RowData> result = normalData.get(id);
                        resultFuture.complete(result != null ? result : Collections.emptyList());
                        if (completionLatch != null) {
                            completionLatch.countDown();
                        }
                    });
        }

        @Override
        public void timeout(RowData input, ResultFuture<RowData> resultFuture) {
            int id = input.getInt(0);
            Collection<RowData> result = fallbackData.get(id);
            resultFuture.complete(result != null ? result : Collections.emptyList());
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (executor != null && !executor.isShutdown()) {
                executor.shutdownNow();
            }
        }
    }

    /**
     * A fetcher with mixed behavior: some keys complete immediately, others block on a latch. On
     * timeout, returns fallback data for key 3 and empty for others.
     */
    public static final class MixedSpeedFetcherFunction extends AbstractRichFunction
            implements AsyncFunction<RowData, RowData> {

        private static final long serialVersionUID = 1L;

        private static final Map<Integer, List<RowData>> fastData = new HashMap<>();
        private static final Map<Integer, List<RowData>> slowData = new HashMap<>();
        private static final Map<Integer, List<RowData>> fallbackData = new HashMap<>();

        static volatile CountDownLatch latch;
        static volatile CountDownLatch completionLatch;

        static {
            fastData.put(1, Collections.singletonList(GenericRowData.of(1, fromString("Julian"))));
            fastData.put(4, Collections.singletonList(GenericRowData.of(4, fromString("Fabian"))));

            slowData.put(
                    3,
                    Arrays.asList(
                            GenericRowData.of(3, fromString("Jark")),
                            GenericRowData.of(3, fromString("Jackson"))));
            slowData.put(5, Collections.singletonList(GenericRowData.of(5, fromString("Slow_5"))));

            fallbackData.put(
                    3, Collections.singletonList(GenericRowData.of(3, fromString("Timeout_3"))));
        }

        private transient ExecutorService executor;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            this.executor = Executors.newFixedThreadPool(2);
        }

        @Override
        public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {
            int id = input.getInt(0);
            List<RowData> fast = fastData.get(id);
            if (fast != null) {
                resultFuture.complete(fast);
                return;
            }
            executor.submit(
                    () -> {
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            return;
                        }
                        Collection<RowData> result = slowData.get(id);
                        resultFuture.complete(result != null ? result : Collections.emptyList());
                        if (completionLatch != null) {
                            completionLatch.countDown();
                        }
                    });
        }

        @Override
        public void timeout(RowData input, ResultFuture<RowData> resultFuture) {
            int id = input.getInt(0);
            Collection<RowData> result = fallbackData.get(id);
            resultFuture.complete(result != null ? result : Collections.emptyList());
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (executor != null && !executor.isShutdown()) {
                executor.shutdownNow();
            }
        }
    }

    /** Calls completeExceptionally on the delegate in complete(). */
    public static final class ExceptionallyCompletingResultFuture
            extends TableFunctionResultFuture<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        @SuppressWarnings("unchecked")
        public void complete(Collection<RowData> result) {
            getResultFuture().completeExceptionally(new RuntimeException("join condition failed"));
        }
    }

    /** Throws exception in complete(). */
    public static final class ThrowingConditionResultFuture
            extends TableFunctionResultFuture<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public void complete(Collection<RowData> result) {
            throw new RuntimeException("join condition threw");
        }
    }
}
