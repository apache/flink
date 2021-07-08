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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MutableObjectIterator;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link StreamTaskInput} which sorts in the incoming records from a chained input. It postpones
 * emitting the records until it receives {@link InputStatus#END_OF_INPUT} from the chained input.
 * After it is done it emits a single record at a time from the sorter.
 *
 * <p>The sorter uses binary comparison of keys, which are extracted and serialized when received
 * from the chained input. Moreover the timestamps of incoming records are used for secondary
 * ordering. For the comparison it uses either {@link FixedLengthByteKeyComparator} if the length of
 * the serialized key is constant, or {@link VariableLengthByteKeyComparator} otherwise.
 *
 * <p>Watermarks, stream statuses, nor latency markers are propagated downstream as they do not make
 * sense with buffered records. The input emits the largest watermark seen after all records.
 *
 * @param <T> The type of the value in incoming {@link StreamRecord StreamRecords}.
 * @param <K> The type of the key.
 */
public final class SortingDataInput<T, K> implements StreamTaskInput<T> {

    private final StreamTaskInput<T> wrappedInput;
    private final PushSorter<Tuple2<byte[], StreamRecord<T>>> sorter;
    private final KeySelector<T, K> keySelector;
    private final TypeSerializer<K> keySerializer;
    private final DataOutputSerializer dataOutputSerializer;
    private final ForwardingDataOutput forwardingDataOutput;
    private MutableObjectIterator<Tuple2<byte[], StreamRecord<T>>> sortedInput = null;
    private boolean emittedLast;
    private long watermarkSeen = Long.MIN_VALUE;

    public SortingDataInput(
            StreamTaskInput<T> wrappedInput,
            TypeSerializer<T> typeSerializer,
            TypeSerializer<K> keySerializer,
            KeySelector<T, K> keySelector,
            MemoryManager memoryManager,
            IOManager ioManager,
            boolean objectReuse,
            double managedMemoryFraction,
            Configuration jobConfiguration,
            AbstractInvokable containingTask) {
        try {
            this.forwardingDataOutput = new ForwardingDataOutput();
            this.keySelector = keySelector;
            this.keySerializer = keySerializer;
            int keyLength = keySerializer.getLength();
            final TypeComparator<Tuple2<byte[], StreamRecord<T>>> comparator;
            if (keyLength > 0) {
                this.dataOutputSerializer = new DataOutputSerializer(keyLength);
                comparator = new FixedLengthByteKeyComparator<>(keyLength);
            } else {
                this.dataOutputSerializer = new DataOutputSerializer(64);
                comparator = new VariableLengthByteKeyComparator<>();
            }
            KeyAndValueSerializer<T> keyAndValueSerializer =
                    new KeyAndValueSerializer<>(typeSerializer, keyLength);
            this.wrappedInput = wrappedInput;
            this.sorter =
                    ExternalSorter.newBuilder(
                                    memoryManager,
                                    containingTask,
                                    keyAndValueSerializer,
                                    comparator)
                            .memoryFraction(managedMemoryFraction)
                            .enableSpilling(
                                    ioManager,
                                    jobConfiguration.get(AlgorithmOptions.SORT_SPILLING_THRESHOLD))
                            .maxNumFileHandles(
                                    jobConfiguration.get(AlgorithmOptions.SPILLING_MAX_FAN))
                            .objectReuse(objectReuse)
                            .largeRecords(
                                    jobConfiguration.get(
                                            AlgorithmOptions.USE_LARGE_RECORDS_HANDLER))
                            .build();
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getInputIndex() {
        return wrappedInput.getInputIndex();
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        throw new UnsupportedOperationException(
                "Checkpoints are not supported with sorted inputs" + " in the BATCH runtime.");
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;
        try {
            wrappedInput.close();
        } catch (IOException e) {
            ex = ExceptionUtils.firstOrSuppressed(e, ex);
        }

        try {
            sorter.close();
        } catch (IOException e) {
            ex = ExceptionUtils.firstOrSuppressed(e, ex);
        }

        if (ex != null) {
            throw ex;
        }
    }

    private class ForwardingDataOutput implements DataOutput<T> {
        @Override
        public void emitRecord(StreamRecord<T> streamRecord) throws Exception {
            K key = keySelector.getKey(streamRecord.getValue());

            keySerializer.serialize(key, dataOutputSerializer);
            byte[] serializedKey = dataOutputSerializer.getCopyOfBuffer();
            dataOutputSerializer.clear();

            sorter.writeRecord(Tuple2.of(serializedKey, streamRecord));
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            watermarkSeen = Math.max(watermarkSeen, watermark.getTimestamp());
        }

        @Override
        public void emitStreamStatus(StreamStatus streamStatus) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}
    }

    @Override
    public InputStatus emitNext(DataOutput<T> output) throws Exception {
        if (sortedInput != null) {
            return emitNextSortedRecord(output);
        }

        InputStatus inputStatus = wrappedInput.emitNext(forwardingDataOutput);
        if (inputStatus == InputStatus.END_OF_INPUT) {
            endSorting();
            return emitNextSortedRecord(output);
        }

        return inputStatus;
    }

    @Nonnull
    private InputStatus emitNextSortedRecord(DataOutput<T> output) throws Exception {
        if (emittedLast) {
            return InputStatus.END_OF_INPUT;
        }

        Tuple2<byte[], StreamRecord<T>> next = sortedInput.next();
        if (next != null) {
            output.emitRecord(next.f1);
            return InputStatus.MORE_AVAILABLE;
        } else {
            emittedLast = true;
            if (watermarkSeen > Long.MIN_VALUE) {
                output.emitWatermark(new Watermark(watermarkSeen));
            }
            return InputStatus.END_OF_INPUT;
        }
    }

    private void endSorting() throws Exception {
        this.sorter.finishReading();
        this.sortedInput = sorter.getIterator();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (sortedInput != null) {
            return AvailabilityProvider.AVAILABLE;
        } else {
            return wrappedInput.getAvailableFuture();
        }
    }
}
