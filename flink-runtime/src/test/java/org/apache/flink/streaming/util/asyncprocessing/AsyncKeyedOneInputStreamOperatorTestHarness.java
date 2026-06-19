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
import org.apache.flink.streaming.api.operators.BoundedOneInput;
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
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.streaming.util.asyncprocessing.AsyncProcessingTestUtil.drain;
import static org.apache.flink.streaming.util.asyncprocessing.AsyncProcessingTestUtil.unwrapAsyncException;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.fail;

/**
 * A test harness for testing a {@link OneInputStreamOperator} which uses async state.
 *
 * <p>All methods that interact with the operator need to be executed in another thread to simulate
 * async processing, please use methods of test harness instead of operator.
 */
public class AsyncKeyedOneInputStreamOperatorTestHarness<K, IN, OUT>
        extends KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> {

    /** Empty if the {@link #operator} is not {@link MultipleInputStreamOperator}. */
    private final List<Input<IN>> inputs = new ArrayList<>();

    private long currentWatermark;

    /** The executor service for async state processing. */
    private final ExecutorService executor;

    /** Create an instance of the subclass of this class. */
    public static <K, IN, OUT, OP extends AsyncKeyedOneInputStreamOperatorTestHarness<K, IN, OUT>>
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

    public static <K, IN, OUT> AsyncKeyedOneInputStreamOperatorTestHarness<K, IN, OUT> create(
            OneInputStreamOperator<IN, OUT> operator,
            final KeySelector<IN, K> keySelector,
            TypeInformation<K> keyType)
            throws Exception {
        return create(operator, keySelector, keyType, 1, 1, 0);
    }

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
        super(operatorFactory, keySelector, keyType, maxParallelism, numSubtasks, subtaskIndex);

        ClosureCleaner.clean(keySelector, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
        config.setStatePartitioner(0, keySelector);
        config.setStateKeySerializer(
                keyType.createSerializer(executionConfig.getSerializerConfig()));
        config.serializeAllConfigs();
        this.executor = executor;
        // Make environment record any failure
        getEnvironment().setExpectedExternalFailureCause(Throwable.class);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void setup(TypeSerializer<OUT> outputSerializer) {
        super.setup(outputSerializer);
        if (operator instanceof MultipleInputStreamOperator) {
            checkState(inputs.isEmpty());
            inputs.addAll(((MultipleInputStreamOperator) operator).getInputs());
        }
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        finishFuture(processElementInternal(element));
    }

    /**
     * Submit an element processing in an executor thread. This method is mainly used for internal
     * testing, please use {@link #processElement} for common operator testing.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CompletableFuture<Void> processElementInternal(StreamRecord<IN> element)
            throws Exception {
        if (inputs.isEmpty()) {
            return execute(
                    () ->
                            RecordProcessorUtils.getRecordProcessor(getOneInputOperator())
                                    .accept(element));
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            return execute(
                    () ->
                            ((ThrowingConsumer<StreamRecord, Exception>)
                                            RecordProcessorUtils.getRecordProcessor(input))
                                    .accept(element));
        }
    }

    @Override
    public void processWatermark(long watermark) throws Exception {
        finishFuture(processWatermarkInternal(watermark));
    }

    /** For internal testing. */
    public CompletableFuture<Void> processWatermarkInternal(long watermark) {
        return processWatermarkInternal(new Watermark(watermark));
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus status) throws Exception {
        finishFuture(processWatermarkStatusInternal(status));
    }

    /** For internal testing. */
    @SuppressWarnings("rawtypes")
    public CompletableFuture<Void> processWatermarkStatusInternal(WatermarkStatus status) {
        if (inputs.isEmpty()) {
            return execute(() -> getOneInputOperator().processWatermarkStatus(status));
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            return execute(() -> input.processWatermarkStatus(status));
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        finishFuture(processWatermarkInternal(mark));
    }

    @Override
    public void endInput() throws Exception {
        if (operator instanceof BoundedOneInput) {
            executeAndGet(() -> ((BoundedOneInput) operator).endInput());
        }
    }

    /** For internal testing. */
    @SuppressWarnings("rawtypes")
    public CompletableFuture<Void> processWatermarkInternal(Watermark mark) {
        currentWatermark = mark.getTimestamp();
        if (inputs.isEmpty()) {
            return execute(() -> getOneInputOperator().processWatermark(mark));
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            return execute(() -> input.processWatermark(mark));
        }
    }

    public void processLatencyMarker(LatencyMarker marker) throws Exception {
        finishFuture(processLatencyMarkerInternal(marker));
    }

    /** For internal testing. */
    @SuppressWarnings("rawtypes")
    public CompletableFuture<Void> processLatencyMarkerInternal(LatencyMarker marker) {
        if (inputs.isEmpty()) {
            return execute(() -> getOneInputOperator().processLatencyMarker(marker));
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            return execute(() -> input.processLatencyMarker(marker));
        }
    }

    @Override
    public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        finishFuture(processRecordAttributesInternal(recordAttributes));
    }

    @Override
    public long getCurrentWatermark() {
        return currentWatermark;
    }

    /** For internal testing. */
    @SuppressWarnings("rawtypes")
    public CompletableFuture<Void> processRecordAttributesInternal(
            RecordAttributes recordAttributes) {
        if (inputs.isEmpty()) {
            return execute(() -> getOneInputOperator().processRecordAttributes(recordAttributes));
        } else {
            checkState(inputs.size() == 1);
            Input input = inputs.get(0);
            return execute(() -> input.processRecordAttributes(recordAttributes));
        }
    }

    public void drainStateRequests() throws Exception {
        executeAndGet(() -> drain(operator));
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        executeAndGet(() -> operator.prepareSnapshotPreBarrier(checkpointId));
    }

    @Override
    public void close() throws Exception {
        executeAndGet(super::close);
        executor.shutdown();
    }

    private CompletableFuture<Void> execute(RunnableWithException runnable) {
        return AsyncProcessingTestUtil.execute(
                executor,
                () -> {
                    checkEnvState();
                    runnable.run();
                });
    }

    private void executeAndGet(RunnableWithException runnable) throws Exception {
        finishFuture(execute(runnable));
    }

    private void finishFuture(CompletableFuture<Void> future) throws Exception {
        try {
            future.get();
            checkEnvState();
        } catch (Exception e) {
            AsyncProcessingTestUtil.execute(executor, () -> mockTask.cleanUp(e)).get();
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
