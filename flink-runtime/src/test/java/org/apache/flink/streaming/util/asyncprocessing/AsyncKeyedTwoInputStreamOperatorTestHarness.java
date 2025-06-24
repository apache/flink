/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.asyncprocessing;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateStreamOperator;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordProcessorUtils;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncKeyOrderedProcessingOperator;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.streaming.util.asyncprocessing.AsyncProcessingTestUtil.drain;
import static org.apache.flink.streaming.util.asyncprocessing.AsyncProcessingTestUtil.execute;
import static org.apache.flink.streaming.util.asyncprocessing.AsyncProcessingTestUtil.unwrapAsyncException;
import static org.assertj.core.api.Assertions.fail;

/**
 * A test harness for testing a {@link OneInputStreamOperator} which uses async state.
 *
 * <p>All methods that interact with the operator need to be executed in another thread to simulate
 * async processing, please use methods of test harness instead of operator.
 */
public class AsyncKeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT>
        extends KeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT> {

    private final TwoInputStreamOperator<IN1, IN2, OUT> twoInputOperator;

    private ThrowingConsumer<StreamRecord<IN1>, Exception> processor1;
    private ThrowingConsumer<StreamRecord<IN2>, Exception> processor2;

    /** The executor service for async state processing. */
    private final ExecutorService executor;

    /** Create an instance of the subclass of this class. */
    public static <
                    K,
                    IN1,
                    IN2,
                    OUT,
                    OP extends AsyncKeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT>>
            OP create(FunctionWithException<ExecutorService, OP, Exception> constructor)
                    throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CompletableFuture<OP> future = new CompletableFuture<>();
        executor.execute(
                () -> {
                    try {
                        future.complete(constructor.apply(executor));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        return future.get();
    }

    public static <K, IN1, IN2, OUT>
            AsyncKeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT> create(
                    TwoInputStreamOperator<IN1, IN2, OUT> operator,
                    KeySelector<IN1, K> keySelector1,
                    KeySelector<IN2, K> keySelector2,
                    TypeInformation<K> keyType)
                    throws Exception {
        return create(operator, keySelector1, keySelector2, keyType, 1, 1, 0);
    }

    public static <K, IN1, IN2, OUT>
            AsyncKeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT> create(
                    TwoInputStreamOperator<IN1, IN2, OUT> operator,
                    KeySelector<IN1, K> keySelector1,
                    KeySelector<IN2, K> keySelector2,
                    TypeInformation<K> keyType,
                    int maxParallelism,
                    int numSubtasks,
                    int subtaskIndex)
                    throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CompletableFuture<AsyncKeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT>> future =
                new CompletableFuture<>();
        executor.execute(
                () -> {
                    try {
                        future.complete(
                                new AsyncKeyedTwoInputStreamOperatorTestHarness<>(
                                        executor,
                                        operator,
                                        keySelector1,
                                        keySelector2,
                                        keyType,
                                        maxParallelism,
                                        numSubtasks,
                                        subtaskIndex));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        return future.get();
    }

    public AsyncKeyedTwoInputStreamOperatorTestHarness(
            ExecutorService executor,
            TwoInputStreamOperator<IN1, IN2, OUT> operator,
            KeySelector<IN1, K> keySelector1,
            KeySelector<IN2, K> keySelector2,
            TypeInformation<K> keyType,
            int maxParallelism,
            int numSubtasks,
            int subtaskIndex)
            throws Exception {
        super(
                operator,
                keySelector1,
                keySelector2,
                keyType,
                maxParallelism,
                numSubtasks,
                subtaskIndex);

        Preconditions.checkState(
                operator instanceof AsyncKeyOrderedProcessingOperator,
                "Operator is not an AsyncKeyOrderedProcessingOperator");
        this.twoInputOperator = operator;
        this.executor = executor;
        // Make environment record any failure
        getEnvironment().setExpectedExternalFailureCause(Throwable.class);
    }

    private ThrowingConsumer<StreamRecord<IN1>, Exception> getRecordProcessor1() {
        if (processor1 == null) {
            processor1 = RecordProcessorUtils.getRecordProcessor1(twoInputOperator);
        }
        return processor1;
    }

    private ThrowingConsumer<StreamRecord<IN2>, Exception> getRecordProcessor2() {
        if (processor2 == null) {
            processor2 = RecordProcessorUtils.getRecordProcessor2(twoInputOperator);
        }
        return processor2;
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        executeAndGet(() -> getRecordProcessor1().accept(element));
    }

    @Override
    public void processElement1(IN1 value, long timestamp) throws Exception {
        processElement1(new StreamRecord<>(value, timestamp));
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        executeAndGet(() -> getRecordProcessor2().accept(element));
    }

    @Override
    public void processElement2(IN2 value, long timestamp) throws Exception {
        processElement2(new StreamRecord<>(value, timestamp));
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        executeAndGet(() -> twoInputOperator.processWatermark1(mark));
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        executeAndGet(() -> twoInputOperator.processWatermark2(mark));
    }

    @Override
    public void processBothWatermarks(Watermark mark) throws Exception {
        executeAndGet(() -> twoInputOperator.processWatermark1(mark));
        executeAndGet(() -> twoInputOperator.processWatermark2(mark));
    }

    @Override
    public void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception {
        executeAndGet(() -> twoInputOperator.processWatermarkStatus1(watermarkStatus));
    }

    @Override
    public void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception {
        executeAndGet(() -> twoInputOperator.processWatermarkStatus2(watermarkStatus));
    }

    @Override
    public void processRecordAttributes1(RecordAttributes recordAttributes) throws Exception {
        executeAndGet(() -> twoInputOperator.processRecordAttributes1(recordAttributes));
    }

    @Override
    public void processRecordAttributes2(RecordAttributes recordAttributes) throws Exception {
        executeAndGet(() -> twoInputOperator.processRecordAttributes2(recordAttributes));
    }

    public void endInput1() throws Exception {
        if (operator instanceof BoundedMultiInput) {
            executeAndGet(() -> ((BoundedMultiInput) operator).endInput(1));
        }
    }

    public void endInput2() throws Exception {
        if (operator instanceof BoundedMultiInput) {
            executeAndGet(() -> ((BoundedMultiInput) operator).endInput(2));
        }
    }

    public void drainStateRequests() throws Exception {
        executeAndGet(() -> drain(operator));
    }

    @Override
    public void close() throws Exception {
        executeAndGet(super::close);
        executor.shutdown();
    }

    @Override
    public int numKeyedStateEntries() {
        AbstractAsyncStateStreamOperator<OUT> asyncOp =
                (AbstractAsyncStateStreamOperator<OUT>) operator;
        AsyncKeyedStateBackend<Object> asyncKeyedStateBackend = asyncOp.getAsyncKeyedStateBackend();
        KeyedStateBackend<?> keyedStateBackend;
        if (asyncKeyedStateBackend instanceof AsyncKeyedStateBackendAdaptor) {
            keyedStateBackend =
                    ((AsyncKeyedStateBackendAdaptor<?>) asyncKeyedStateBackend)
                            .getKeyedStateBackend();

        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported async keyed state backend: %s",
                            asyncKeyedStateBackend.getClass().getCanonicalName()));
        }

        if (keyedStateBackend instanceof HeapKeyedStateBackend) {
            return ((HeapKeyedStateBackend) keyedStateBackend).numKeyValueStateEntries();
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported keyed state backend: %s",
                            keyedStateBackend.getClass().getCanonicalName()));
        }
    }

    private void executeAndGet(RunnableWithException runnable) throws Exception {
        try {
            execute(
                            executor,
                            () -> {
                                checkEnvState();
                                runnable.run();
                            })
                    .get();
            checkEnvState();
        } catch (Exception e) {
            execute(executor, () -> mockTask.cleanUp(e)).get();
            throw unwrapAsyncException(e);
        }
    }

    private void checkEnvState() {
        if (getEnvironment().getActualExternalFailureCause().isPresent()) {
            fail(
                    "There is an error on other threads",
                    getEnvironment().getActualExternalFailureCause().get());
        }
    }
}
