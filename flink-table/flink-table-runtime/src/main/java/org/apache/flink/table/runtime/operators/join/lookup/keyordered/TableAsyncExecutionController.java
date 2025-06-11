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

package org.apache.flink.table.runtime.operators.join.lookup.keyordered;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueueEntry;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The {@link TableAsyncExecutionController} is used to keep key ordered process mode for async
 * operator. It allows for out of order processing on different keys and sequential processing of
 * {@link StreamElement} on the same key.
 *
 * <p>Existing {@link AsyncExecutionController} is tightly coupled with the concept of state and
 * this is why we add {@link TableAsyncExecutionController}.
 *
 * <p>TODO: Refactor this for less deduplication in the FLINK-37921.
 *
 * @param <IN> Input type for the controller.
 * @param <OUT> Output type for the controller.
 * @param <KEY> The key type for the controller.
 */
public class TableAsyncExecutionController<IN, OUT, KEY> {

    private static final Logger LOG = LoggerFactory.getLogger(TableAsyncExecutionController.class);

    /** Consumer to actually call async invoke method. */
    private final ThrowingConsumer<AecRecord<IN, OUT>, Exception> asyncInvoke;

    /** Consumer to emit watermark. */
    private final Consumer<Watermark> emitWatermark;

    /** Consumer to emit results wrapped in a {@link StreamElementQueueEntry}. */
    private final Consumer<StreamElementQueueEntry<OUT>> emitResult;

    /**
     * Function to infer which side drives this {@link StreamElementQueueEntry}.
     *
     * <p>The returned input index starts with 0.
     */
    private final Function<StreamElementQueueEntry<OUT>, Integer> inferDrivenInputIndex;

    /**
     * Function to infer which key should be blocked by this {@link TableAsyncExecutionController}.
     *
     * <p>Input args: {@code <Record, InputIndex that starts with 0>}
     */
    private final BiFunctionWithException<StreamRecord<IN>, Integer, KEY, Exception>
            inferBlockingKey;

    /** The key accounting unit which is used to detect the key conflict. */
    private final KeyAccountingUnit<KEY> keyAccountingUnit;

    /** The buffer to store the stream elements which keeps the order of process on the same key. */
    private final RecordsBuffer<AecRecord<IN, OUT>, KEY> recordsBuffer;

    /** The epochManager to manage the order of input. */
    private final EpochManager<OUT> epochManager;

    private final AecRecord<IN, OUT> reusedRecord;

    public TableAsyncExecutionController(
            ThrowingConsumer<AecRecord<IN, OUT>, Exception> asyncInvoke,
            Consumer<Watermark> emitWatermark,
            Consumer<StreamElementQueueEntry<OUT>> emitResult,
            Function<StreamElementQueueEntry<OUT>, Integer> inferDrivenInputIndex,
            BiFunctionWithException<StreamRecord<IN>, Integer, KEY, Exception> inferBlockingKey) {
        this.asyncInvoke = asyncInvoke;
        this.emitWatermark = emitWatermark;
        this.emitResult = emitResult;
        this.inferDrivenInputIndex = inferDrivenInputIndex;
        this.inferBlockingKey = inferBlockingKey;

        this.keyAccountingUnit = new KeyAccountingUnit<>();
        this.recordsBuffer = new RecordsBuffer<>();
        this.epochManager = new EpochManager<>();
        this.reusedRecord = new AecRecord<>();
    }

    public void registerMetrics(MetricGroup metricGroup) {
        metricGroup.gauge("aec_inflight_size", recordsBuffer::getActiveSize);
        metricGroup.gauge("aec_blocking_size", recordsBuffer::getBlockingSize);
        metricGroup.gauge("aec_finish_size", recordsBuffer::getFinishSize);
    }

    /**
     * Used for an element to be completed. This is used to release the occupied key and notify the
     * epoch of the element to collect the result future.
     */
    public void completeRecord(
            StreamElementQueueEntry<OUT> resultFuture, AecRecord<IN, OUT> aecRecord)
            throws Exception {
        KEY key = getKey(aecRecord);
        recordsBuffer.finish(key, aecRecord);
        keyAccountingUnit.release(aecRecord.getRecord(), key);
        Epoch<OUT> epoch = aecRecord.getEpoch();
        epoch.collect(resultFuture);
        epoch.setOutput(
                element -> {
                    emitResult.accept(element);
                    outputRecord(
                            (StreamRecord<IN>) element.getInputElement(),
                            epoch,
                            inferDrivenInputIndex.apply(element));
                });
        // trigger the oldest epoch to output the result
        epochManager.completeOneRecord(epoch);
        trigger(key);
    }

    public void recovery(StreamRecord<IN> record, Watermark watermark, int inputIndex)
            throws Exception {
        Optional<Epoch<OUT>> epoch = epochManager.getProperEpoch(watermark);
        if (epoch.isPresent()) {
            submitRecord(record, epoch.get(), inputIndex);
        } else {
            submitWatermark(watermark);
            submitRecord(record, null, inputIndex);
        }
    }

    public void submitRecord(StreamRecord<IN> record, @Nullable Epoch<OUT> epoch, int inputIndex)
            throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("size in records buffer:  %s", recordsBuffer.sizeToString()));
        }
        Epoch<OUT> currentEpoch;
        if (epoch != null) {
            // only for recovery in case finding proper epoch in epochManager
            currentEpoch = epoch;
        } else {
            currentEpoch = epochManager.onRecord();
        }
        AecRecord<IN, OUT> aecRecord = new AecRecord<>(record, currentEpoch, inputIndex);
        KEY key = getKey(record, inputIndex);
        recordsBuffer.enqueueRecord(key, aecRecord);
        trigger(key);
    }

    public void submitWatermark(Watermark watermark) {
        epochManager.onNonRecord(
                watermark,
                () -> {
                    try {
                        emitWatermark.accept(watermark);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to emit watermark", e);
                    }
                });
    }

    public Map<KEY, Deque<AecRecord<IN, OUT>>> pendingElements() {
        return recordsBuffer.pendingElements();
    }

    public void outputRecord(StreamRecord<IN> record, Epoch<OUT> epoch, int inputIndex) {
        reusedRecord.reset(record, epoch, inputIndex);
        recordsBuffer.output(getKey(record, inputIndex), reusedRecord);
    }

    public void close() {
        epochManager.close();
        recordsBuffer.close();
    }

    @VisibleForTesting
    public Epoch<OUT> getActiveEpoch() {
        return epochManager.getActiveEpoch();
    }

    @VisibleForTesting
    public int getBlockingSize() {
        return recordsBuffer.getBlockingSize();
    }

    @VisibleForTesting
    public int getFinishSize() {
        return recordsBuffer.getFinishSize();
    }

    @VisibleForTesting
    public int getInFlightSize() {
        return recordsBuffer.getActiveSize();
    }

    @VisibleForTesting
    public RecordsBuffer<AecRecord<IN, OUT>, KEY> getRecordsBuffer() {
        return recordsBuffer;
    }

    @VisibleForTesting
    public ThrowingConsumer<AecRecord<IN, OUT>, Exception> getAsyncInvoke() {
        return asyncInvoke;
    }

    @VisibleForTesting
    public Consumer<Watermark> getEmitWatermark() {
        return emitWatermark;
    }

    @VisibleForTesting
    public Consumer<StreamElementQueueEntry<OUT>> getEmitResult() {
        return emitResult;
    }

    @VisibleForTesting
    public Function<StreamElementQueueEntry<OUT>, Integer> getInferDrivenInputIndex() {
        return inferDrivenInputIndex;
    }

    @VisibleForTesting
    public BiFunctionWithException<StreamRecord<IN>, Integer, KEY, Exception>
            getInferBlockingKey() {
        return inferBlockingKey;
    }

    private void trigger(KEY key) throws Exception {
        if (ifOccupy(key)) {
            Optional<AecRecord<IN, OUT>> element = recordsBuffer.pop(key);
            if (element.isPresent() && tryOccupyKey(element.get().getRecord(), key)) {
                try {
                    asyncInvoke.accept(element.get());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to async invoke the record", e);
                }
            }
        }
    }

    private boolean tryOccupyKey(StreamRecord<IN> record, KEY key) {
        return keyAccountingUnit.occupy(record, key);
    }

    private boolean ifOccupy(KEY key) {
        return keyAccountingUnit.ifOccupy(key);
    }

    private KEY getKey(StreamRecord<IN> record, int inputIndex) {
        try {
            return inferBlockingKey.apply(record, inputIndex);
        } catch (Exception e) {
            throw new RuntimeException("Unable to retrieve key from record " + record, e);
        }
    }

    private KEY getKey(AecRecord<IN, OUT> aecRecord) {
        StreamRecord<IN> record = aecRecord.getRecord();
        return getKey(record, aecRecord.getInputIndex());
    }
}
