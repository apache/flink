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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordProcessorUtils;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncStateProcessingOperator;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.streaming.util.asyncprocessing.AsyncProcessingTestUtil.drain;
import static org.apache.flink.streaming.util.asyncprocessing.AsyncProcessingTestUtil.execute;

/**
 * A test harness for testing a {@link OneInputStreamOperator} which uses async state.
 *
 * <p>All methods that interact with the operator need to be executed in another thread to simulate
 * async processing, please use methods of test harness instead of operator.
 */
public class AsyncKeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT>
        extends AbstractStreamOperatorTestHarness<OUT> {

    private final TwoInputStreamOperator<IN1, IN2, OUT> twoInputOperator;

    private ThrowingConsumer<StreamRecord<IN1>, Exception> processor1;
    private ThrowingConsumer<StreamRecord<IN2>, Exception> processor2;

    /** The executor service for async state processing. */
    private ExecutorService executor;

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
        super(operator, maxParallelism, numSubtasks, subtaskIndex);

        ClosureCleaner.clean(keySelector1, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
        ClosureCleaner.clean(keySelector2, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
        config.setStatePartitioner(0, keySelector1);
        config.setStatePartitioner(1, keySelector2);
        config.setStateKeySerializer(
                keyType.createSerializer(executionConfig.getSerializerConfig()));
        config.serializeAllConfigs();

        Preconditions.checkState(
                operator instanceof AsyncStateProcessingOperator,
                "Operator is not an AsyncStateProcessingOperator");
        this.twoInputOperator = operator;
        this.executor = executor;
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

    public void processElement1(StreamRecord<IN1> element) throws Exception {
        execute(executor, (ignore) -> getRecordProcessor1().accept(element)).get();
    }

    public void processElement1(IN1 value, long timestamp) throws Exception {
        processElement1(new StreamRecord<>(value, timestamp));
    }

    public void processElement2(StreamRecord<IN2> element) throws Exception {
        execute(executor, (ignore) -> getRecordProcessor2().accept(element)).get();
    }

    public void processElement2(IN2 value, long timestamp) throws Exception {
        processElement2(new StreamRecord<>(value, timestamp));
    }

    public void processWatermark1(Watermark mark) throws Exception {
        execute(executor, (ignore) -> twoInputOperator.processWatermark1(mark)).get();
    }

    public void processWatermark2(Watermark mark) throws Exception {
        execute(executor, (ignore) -> twoInputOperator.processWatermark2(mark)).get();
    }

    public void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception {
        execute(executor, (ignore) -> twoInputOperator.processWatermarkStatus1(watermarkStatus))
                .get();
    }

    public void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception {
        execute(executor, (ignore) -> twoInputOperator.processWatermarkStatus2(watermarkStatus))
                .get();
    }

    public void processRecordAttributes1(RecordAttributes recordAttributes) throws Exception {
        execute(executor, (ignore) -> twoInputOperator.processRecordAttributes1(recordAttributes))
                .get();
    }

    public void processRecordAttributes2(RecordAttributes recordAttributes) throws Exception {
        execute(executor, (ignore) -> twoInputOperator.processRecordAttributes2(recordAttributes))
                .get();
    }

    public void drainStateRequests() throws Exception {
        execute(executor, (ignore) -> drain(operator)).get();
    }

    @Override
    public void close() throws Exception {
        execute(
                        executor,
                        (ignore) -> {
                            super.close();
                        })
                .get();
        executor.shutdown();
    }
}
