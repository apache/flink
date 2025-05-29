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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueueEntry;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The {@link AsyncExecutionController} is used to keep key ordered process mode for async lookup
 * join. It allows for out of order processing on different keys and sequential processing of {@link
 * StreamElement} on the same key.
 *
 * @param <IN> Input type for the controller.
 * @param <OUT> Output type for the controller.
 * @param <KEY> The key type for the controller.
 */
public class AsyncExecutionController<IN, OUT, KEY> {

    protected static final Logger LOG = LoggerFactory.getLogger(AsyncExecutionController.class);

    /** Consumer to actually call async invoke method. */
    private final Consumer<AecRecord<IN, OUT>> asyncInvoke;

    /** Consumer to emit watermark. */
    private final Consumer<Watermark> emitWatermark;

    /** Consumer to emit results wrapped in a {@link StreamElementQueueEntry}. */
    private final Consumer<StreamElementQueueEntry<OUT>> emitResult;

    /** Selector to get key from input. */
    private final KeySelector<IN, KEY> keySelector;

    /** The key accounting unit which is used to detect the key conflict. */
    private final KeyAccountingUnit<KEY> keyAccountingUnit;

    /** The buffer to store the stream elements which keeps the order of process on the same key. */
    private final RecordsBuffer<AecRecord<IN, OUT>, KEY> recordsBuffer;

    /** The epochManager to manage the order of input. */
    private final EpochManager<OUT> epochManager;

    private final AecRecord<IN, OUT> reusedRecord;

    public AsyncExecutionController(
            KeySelector<IN, KEY> keySelector,
            Consumer<AecRecord<IN, OUT>> asyncInvoke,
            Consumer<Watermark> emitWatermark,
            Consumer<StreamElementQueueEntry<OUT>> emitResult) {
        this.keySelector = keySelector;
        this.asyncInvoke = asyncInvoke;
        this.emitWatermark = emitWatermark;
        this.emitResult = emitResult;
        this.keyAccountingUnit = new KeyAccountingUnit<>();
        this.recordsBuffer = new RecordsBuffer<>();
        this.epochManager = new EpochManager<>();
        this.reusedRecord = new AecRecord<>();
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
                    outputRecord((StreamRecord<IN>) element.getInputElement(), epoch);
                });
        // trigger the oldest epoch to output the result
        epochManager.completeOneRecord(epoch);
        trigger(key);
    }

    public void recovery(StreamRecord<IN> record, Watermark watermark) throws Exception {
        Optional<Epoch<OUT>> epoch = epochManager.getProperEpoch(watermark);
        if (epoch.isPresent()) {
            submitRecord(record, epoch.get());
        } else {
            submitWatermark(watermark);
            submitRecord(record, null);
        }
    }

    public void submitRecord(StreamRecord<IN> record, @Nullable Epoch<OUT> epoch) throws Exception {
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
        AecRecord<IN, OUT> aecRecord = new AecRecord<>(record, currentEpoch);
        KEY key = getKey(record);
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

    public void outputRecord(StreamRecord<IN> record, Epoch<OUT> epoch) {
        reusedRecord.setRecord(record).setEpoch(epoch);
        recordsBuffer.output(getKey(record), reusedRecord);
    }

    public void close() {
        epochManager.close();
        recordsBuffer.close();
    }

    @VisibleForTesting
    public Epoch<OUT> getActiveEpoch() {
        return epochManager.getActiveEpoch();
    }

    private void trigger(KEY key) throws Exception {
        if (ifOccupy(key)) {
            Optional<AecRecord<IN, OUT>> element = recordsBuffer.pop(key);
            if (element.isPresent() && tryOccupyKey(element.get().getRecord(), key)) {
                asyncInvoke.accept(element.get());
            }
        }
    }

    private boolean tryOccupyKey(StreamRecord<IN> record, KEY key) {
        return keyAccountingUnit.occupy(record, key);
    }

    private boolean ifOccupy(KEY key) {
        return keyAccountingUnit.ifOccupy(key);
    }

    private KEY getKey(StreamRecord<IN> record) {
        try {
            return keySelector.getKey(record.getValue());
        } catch (Exception e) {
            throw new RuntimeException("Unable to retrieve key from record " + record, e);
        }
    }

    private KEY getKey(AecRecord<IN, OUT> aecRecord) {
        StreamRecord<IN> record = aecRecord.getRecord();
        return getKey(record);
    }
}
