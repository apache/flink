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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordProcessorUtils;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A test harness for testing a {@link OneInputStreamOperator} which uses async state.
 *
 * <p>All methods that interact with the operator need to be executed in another thread to simulate
 * async processing, please use methods of test harness instead of operator.
 */
public class AsyncKeyedOneInputStreamOperatorTestHarness<K, IN, OUT>
        extends AbstractStreamOperatorTestHarness<OUT> {

    /** Empty if the {@link #operator} is not {@link MultipleInputStreamOperator}. */
    private final List<Input> inputs = new ArrayList<>();

    /** The executor service for async state processing. */
    private ExecutorService executor;

    public static <K, IN, OUT> AsyncKeyedOneInputStreamOperatorTestHarness<K, IN, OUT> create(
            OneInputStreamOperator<IN, OUT> operator,
            final KeySelector<IN, K> keySelector,
            TypeInformation<K> keyType,
            int maxParallelism,
            int numSubtasks,
            int subtaskIndex)
            throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        CompletableFuture<AsyncKeyedOneInputStreamOperatorTestHarness<K, IN, OUT>> future =
                new CompletableFuture<>();
        executorService.execute(
                () -> {
                    try {
                        future.complete(
                                new AsyncKeyedOneInputStreamOperatorTestHarness<>(
                                        executorService,
                                        SimpleOperatorFactory.of(operator),
                                        keySelector,
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

    protected AsyncKeyedOneInputStreamOperatorTestHarness(
            ExecutorService executor,
            StreamOperatorFactory<OUT> operatorFactory,
            final KeySelector<IN, K> keySelector,
            TypeInformation<K> keyType,
            int maxParallelism,
            int numSubtasks,
            int subtaskIndex)
            throws Exception {
        super(operatorFactory, maxParallelism, numSubtasks, subtaskIndex);

        ClosureCleaner.clean(keySelector, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
        config.setStatePartitioner(0, keySelector);
        config.setStateKeySerializer(
                keyType.createSerializer(executionConfig.getSerializerConfig()));
        config.serializeAllConfigs();
        this.executor = executor;
    }

    @Override
    public void setup(TypeSerializer<OUT> outputSerializer) {
        super.setup(outputSerializer);
        if (operator instanceof MultipleInputStreamOperator) {
            checkState(inputs.isEmpty());
            inputs.addAll(((MultipleInputStreamOperator) operator).getInputs());
        }
    }

    public OneInputStreamOperator<IN, OUT> getOneInputOperator() {
        return (OneInputStreamOperator<IN, OUT>) this.operator;
    }

    public void processElement(StreamRecord<IN> element) throws Exception {
        processElementInternal(element).get();
    }

    /**
     * Submit an element processing in an executor thread. This method is mainly used for internal
     * testing, please use {@link #processElement} for common operator testing.
     */
    public CompletableFuture<Void> processElementInternal(StreamRecord<IN> element)
            throws Exception {
        if (inputs.isEmpty()) {
            return execute(
                    (ignore) ->
                            RecordProcessorUtils.getRecordProcessor(getOneInputOperator())
                                    .accept(element));
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            return execute(
                    (ignore) ->
                            ((ThrowingConsumer<StreamRecord, Exception>)
                                            RecordProcessorUtils.getRecordProcessor(input))
                                    .accept(element));
        }
    }

    public void processWatermark(long watermark) throws Exception {
        processWatermarkInternal(watermark).get();
    }

    /** For internal testing. */
    public CompletableFuture<Void> processWatermarkInternal(long watermark) throws Exception {
        return processWatermarkInternal(new Watermark(watermark));
    }

    public void processWatermarkStatus(WatermarkStatus status) throws Exception {
        processWatermarkStatusInternal(status).get();
    }

    /** For internal testing. */
    public CompletableFuture<Void> processWatermarkStatusInternal(WatermarkStatus status)
            throws Exception {
        if (inputs.isEmpty()) {
            return execute((ignore) -> getOneInputOperator().processWatermarkStatus(status));
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            return execute((ignore) -> input.processWatermarkStatus(status));
        }
    }

    public void processWatermark(Watermark mark) throws Exception {
        processWatermarkInternal(mark).get();
    }

    /** For internal testing. */
    public CompletableFuture<Void> processWatermarkInternal(Watermark mark) throws Exception {
        if (inputs.isEmpty()) {
            return execute((ignore) -> getOneInputOperator().processWatermark(mark));
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            return execute((ignore) -> input.processWatermark(mark));
        }
    }

    public void processLatencyMarker(LatencyMarker marker) throws Exception {
        processLatencyMarkerInternal(marker).get();
    }

    /** For internal testing. */
    public CompletableFuture<Void> processLatencyMarkerInternal(LatencyMarker marker) {
        if (inputs.isEmpty()) {
            return execute((ignore) -> getOneInputOperator().processLatencyMarker(marker));
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            return execute((ignore) -> input.processLatencyMarker(marker));
        }
    }

    public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        processRecordAttributesInternal(recordAttributes).get();
    }

    /** For internal testing. */
    public CompletableFuture<Void> processRecordAttributesInternal(
            RecordAttributes recordAttributes) {
        if (inputs.isEmpty()) {
            return execute(
                    (ignore) -> getOneInputOperator().processRecordAttributes(recordAttributes));
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            return execute((ignore) -> input.processRecordAttributes(recordAttributes));
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        execute((ignore) -> operator.prepareSnapshotPreBarrier(checkpointId)).get();
    }

    @Override
    public void close() throws Exception {
        execute(
                        (ignore) -> {
                            super.close();
                        })
                .get();
        executor.shutdown();
    }

    private CompletableFuture<Void> execute(ThrowingConsumer<Void, Exception> processor) {
        CompletableFuture<Void> future = new CompletableFuture();
        executor.execute(
                () -> {
                    try {
                        processor.accept(null);
                        future.complete(null);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        return future;
    }
}
