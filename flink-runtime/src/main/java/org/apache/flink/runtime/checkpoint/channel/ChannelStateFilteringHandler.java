/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilter;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilterContext;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannel;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannelRecordFilterFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Filters recovered channel state buffers during the channel-state-unspilling phase, removing
 * records that do not belong to the current subtask after rescaling.
 *
 * <p>Uses a per-gate architecture: each {@link InputGate} gets its own {@link GateFilterHandler}
 * with the correct serializer, so multi-input tasks (e.g., TwoInputStreamTask) correctly
 * deserialize different record types on different gates.
 */
@Internal
public class ChannelStateFilteringHandler implements Closeable {

    // Wildcard allows heterogeneous record types across gates.
    private final GateFilterHandler<?>[] gateHandlers;

    ChannelStateFilteringHandler(GateFilterHandler<?>[] gateHandlers) {
        this.gateHandlers = checkNotNull(gateHandlers);
    }

    /**
     * Creates a handler from the recovery context, building per-gate virtual channels based on
     * rescaling descriptors. Returns {@code null} if no filtering is needed (e.g., source tasks or
     * no rescaling).
     */
    @Nullable
    public static ChannelStateFilteringHandler createFromContext(
            RecordFilterContext filterContext, InputGate[] inputGates) {
        // Source tasks have no network inputs
        if (filterContext.getNumberOfGates() == 0) {
            return null;
        }

        InflightDataRescalingDescriptor rescalingDescriptor =
                filterContext.getRescalingDescriptor();

        GateFilterHandler<?>[] gateHandlers = new GateFilterHandler<?>[inputGates.length];
        boolean hasAnyVirtualChannels = false;

        for (int gateIndex = 0; gateIndex < inputGates.length; gateIndex++) {
            gateHandlers[gateIndex] =
                    createGateHandler(filterContext, inputGates, rescalingDescriptor, gateIndex);
            if (gateHandlers[gateIndex] != null) {
                hasAnyVirtualChannels = true;
            }
        }

        if (!hasAnyVirtualChannels) {
            return null;
        }

        return new ChannelStateFilteringHandler(gateHandlers);
    }

    /**
     * Filters {@code sourceBuffer} through the virtual channel identified by {@code gateIndex} /
     * {@code oldChannelIndex}, appending each surviving record (length-prefixed) into {@code
     * outputSerializer}. One call may emit 0..N records depending on the filter result and whether
     * records spanning previous buffers complete here. The caller owns the segment boundary.
     */
    public void filterAndRewrite(
            int gateIndex,
            int oldSubtaskIndex,
            int oldChannelIndex,
            Buffer sourceBuffer,
            DataOutputSerializer outputSerializer)
            throws IOException {

        if (gateIndex < 0 || gateIndex >= gateHandlers.length) {
            throw new IllegalStateException(
                    "Invalid gateIndex: "
                            + gateIndex
                            + ", number of gates: "
                            + gateHandlers.length);
        }

        GateFilterHandler<?> gateHandler = gateHandlers[gateIndex];
        if (gateHandler == null) {
            throw new IllegalStateException(
                    "No handler for gateIndex "
                            + gateIndex
                            + ". This gate is not a network input and should not have recovered buffers.");
        }
        gateHandler.filterAndRewrite(
                oldSubtaskIndex, oldChannelIndex, sourceBuffer, outputSerializer);
    }

    /**
     * FLINK-38544 transitional: removed when the spilling backend lands (dies together with the
     * in-memory {@code FilteringHandler}, its only caller).
     *
     * <p>Filters a recovered buffer from the specified virtual channel, returning new buffers
     * containing only the records that belong to the current subtask.
     *
     * <p>One source buffer may produce 0 to N result buffers: 0 if all records are filtered out,
     * and potentially more than 1 when a spanning record completes in this buffer. The deserializer
     * caches partial record data from previous buffers, so the output may contain data that was not
     * in the current source buffer, causing the total output size to exceed one buffer capacity.
     * This can happen with any spanning record regardless of its size.
     *
     * @return filtered buffers, possibly empty if all records were filtered out.
     */
    public List<Buffer> filterAndRewrite(
            int gateIndex,
            int oldSubtaskIndex,
            int oldChannelIndex,
            Buffer sourceBuffer,
            BufferSupplier bufferSupplier)
            throws IOException, InterruptedException {

        if (gateIndex < 0 || gateIndex >= gateHandlers.length) {
            throw new IllegalStateException(
                    "Invalid gateIndex: "
                            + gateIndex
                            + ", number of gates: "
                            + gateHandlers.length);
        }

        GateFilterHandler<?> gateHandler = gateHandlers[gateIndex];
        if (gateHandler == null) {
            throw new IllegalStateException(
                    "No handler for gateIndex "
                            + gateIndex
                            + ". This gate is not a network input and should not have recovered buffers.");
        }
        return gateHandler.filterAndRewrite(
                oldSubtaskIndex, oldChannelIndex, sourceBuffer, bufferSupplier);
    }

    /** Returns {@code true} if any virtual channel has a partial (spanning) record pending. */
    public boolean hasPartialData() {
        for (GateFilterHandler<?> handler : gateHandlers) {
            if (handler != null && handler.hasPartialData()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        for (GateFilterHandler<?> handler : gateHandlers) {
            if (handler != null) {
                handler.clear();
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Private static helper methods
    // -------------------------------------------------------------------------------------------

    /**
     * Creates a {@link GateFilterHandler} for a single gate. The method-level type parameter
     * ensures type safety within each gate while allowing different gates to have different types.
     */
    @SuppressWarnings("unchecked")
    @Nullable
    private static <T> GateFilterHandler<T> createGateHandler(
            RecordFilterContext filterContext,
            InputGate[] inputGates,
            InflightDataRescalingDescriptor rescalingDescriptor,
            int gateIndex) {
        RecordFilterContext.InputFilterConfig inputConfig = filterContext.getInputConfig(gateIndex);
        if (inputConfig == null) {
            throw new IllegalStateException(
                    "No InputFilterConfig for gateIndex "
                            + gateIndex
                            + ". This indicates a bug in RecordFilterContext initialization.");
        }

        InputGate gate = inputGates[gateIndex];
        int[] oldSubtaskIndexes = rescalingDescriptor.getOldSubtaskIndexes(gateIndex);
        RescaleMappings channelMapping = rescalingDescriptor.getChannelMapping(gateIndex);

        TypeSerializer<T> typeSerializer = (TypeSerializer<T>) inputConfig.getTypeSerializer();
        StreamElementSerializer<T> elementSerializer =
                new StreamElementSerializer<>(typeSerializer);

        VirtualChannelRecordFilterFactory<T> filterFactory =
                VirtualChannelRecordFilterFactory.fromContext(filterContext, gateIndex);

        Map<SubtaskConnectionDescriptor, VirtualChannel<T>> gateVirtualChannels = new HashMap<>();

        for (int oldSubtaskIndex : oldSubtaskIndexes) {
            int numChannels = gate.getNumberOfInputChannels();
            int[] oldChannelIndexes = getOldChannelIndexes(channelMapping, numChannels);

            for (int oldChannelIndex : oldChannelIndexes) {
                SubtaskConnectionDescriptor key =
                        new SubtaskConnectionDescriptor(oldSubtaskIndex, oldChannelIndex);

                if (gateVirtualChannels.containsKey(key)) {
                    continue;
                }

                // Only ambiguous channels need actual filtering; non-ambiguous ones pass through
                boolean isAmbiguous = rescalingDescriptor.isAmbiguous(gateIndex, oldSubtaskIndex);

                RecordFilter<T> recordFilter =
                        isAmbiguous
                                ? filterFactory.createFilter()
                                : VirtualChannelRecordFilterFactory.createPassThroughFilter();

                RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer =
                        new SpillingAdaptiveSpanningRecordDeserializer<>(
                                filterContext.getTmpDirectories());

                VirtualChannel<T> vc = new VirtualChannel<>(deserializer, recordFilter);
                gateVirtualChannels.put(key, vc);
            }
        }

        if (gateVirtualChannels.isEmpty()) {
            return null;
        }

        return new GateFilterHandler<>(gateVirtualChannels, elementSerializer);
    }

    /**
     * Collects all old channel indexes that are mapped from any new channel index in this gate.
     * channelMapping is new-to-old, so we iterate new indexes and collect their old counterparts.
     */
    private static int[] getOldChannelIndexes(RescaleMappings channelMapping, int numChannels) {
        List<Integer> oldIndexes = new ArrayList<>();
        for (int newIndex = 0; newIndex < numChannels; newIndex++) {
            int[] mapped = channelMapping.getMappedIndexes(newIndex);
            for (int oldIndex : mapped) {
                if (!oldIndexes.contains(oldIndex)) {
                    oldIndexes.add(oldIndex);
                }
            }
        }
        return oldIndexes.stream().mapToInt(Integer::intValue).toArray();
    }

    // -------------------------------------------------------------------------------------------
    // Inner classes
    // -------------------------------------------------------------------------------------------

    /**
     * FLINK-38544 transitional: removed when the spilling backend lands. Provides buffers for
     * re-serializing filtered records on the in-memory delivery path. Implementations may block.
     */
    @FunctionalInterface
    public interface BufferSupplier {
        Buffer requestBufferBlocking() throws IOException, InterruptedException;
    }

    /**
     * Handles record filtering for a single input gate. Each gate has its own serializer and set of
     * virtual channels, allowing different gates to handle different record types independently.
     */
    static class GateFilterHandler<T> {

        private final Map<SubtaskConnectionDescriptor, VirtualChannel<T>> virtualChannels;
        private final StreamElementSerializer<T> serializer;
        private final DeserializationDelegate<StreamElement> deserializationDelegate;

        // FLINK-38544 transitional: removed when the spilling backend lands (used only by the
        // in-memory buffer-delivering filterAndRewrite overload below).
        private final DataOutputSerializer outputSerializer;
        private final byte[] lengthBuffer = new byte[4];

        GateFilterHandler(
                Map<SubtaskConnectionDescriptor, VirtualChannel<T>> virtualChannels,
                StreamElementSerializer<T> serializer) {
            this.virtualChannels = checkNotNull(virtualChannels);
            this.serializer = checkNotNull(serializer);
            this.deserializationDelegate = new NonReusingDeserializationDelegate<>(serializer);
            this.outputSerializer = new DataOutputSerializer(128);
        }

        /**
         * Deserializes records from {@code sourceBuffer}, applies the virtual channel's record
         * filter, and re-serializes each surviving record into {@code outputSerializer}. No
         * intermediate network buffer is used; the caller owns the segment boundary.
         */
        void filterAndRewrite(
                int oldSubtaskIndex,
                int oldChannelIndex,
                Buffer sourceBuffer,
                DataOutputSerializer outputSerializer)
                throws IOException {

            boolean sourceBufferOwnershipTransferred = false;
            try {
                SubtaskConnectionDescriptor key =
                        new SubtaskConnectionDescriptor(oldSubtaskIndex, oldChannelIndex);
                VirtualChannel<T> vc = virtualChannels.get(key);
                if (vc == null) {
                    throw new IllegalStateException(
                            "No VirtualChannel found for key: "
                                    + key
                                    + "; known channels are "
                                    + virtualChannels.keySet());
                }

                vc.setNextBuffer(sourceBuffer);
                sourceBufferOwnershipTransferred = true;

                while (true) {
                    DeserializationResult result = vc.getNextRecord(deserializationDelegate);
                    if (result.isFullRecord()) {
                        serializeElement(deserializationDelegate.getInstance(), outputSerializer);
                    }
                    if (result.isBufferConsumed()) {
                        break;
                    }
                }
            } catch (Throwable t) {
                if (!sourceBufferOwnershipTransferred) {
                    sourceBuffer.recycleBuffer();
                }
                throw t;
            }
        }

        /**
         * Appends one stream element as a length-prefixed record. Reserves the 4B prefix,
         * serializes the element, then backfills the length, because {@code outputSerializer}
         * already holds the segment header and earlier records, so the prefix cannot be written
         * from a fixed offset.
         */
        private void serializeElement(StreamElement element, DataOutputSerializer outputSerializer)
                throws IOException {
            int startPos = outputSerializer.length();
            outputSerializer.writeInt(0); // length placeholder
            serializer.serialize(element, outputSerializer);
            int recordLength = outputSerializer.length() - startPos - Integer.BYTES;
            outputSerializer.writeIntUnsafe(recordLength, startPos);
        }

        // -----------------------------------------------------------------------------------
        // FLINK-38544 transitional: everything below until hasPartialData() is the in-memory
        // buffer-delivering filter path, kept alive for the v1 FilteringHandler; removed when
        // the spilling backend lands.
        // -----------------------------------------------------------------------------------

        /**
         * Deserializes records from {@code sourceBuffer}, applies the virtual channel's record
         * filter, and immediately re-serializes each surviving record into output buffers.
         */
        List<Buffer> filterAndRewrite(
                int oldSubtaskIndex,
                int oldChannelIndex,
                Buffer sourceBuffer,
                BufferSupplier bufferSupplier)
                throws IOException, InterruptedException {

            boolean sourceBufferOwnershipTransferred = false;
            List<Buffer> resultBuffers = new ArrayList<>();
            Buffer currentBuffer = null;
            try {
                SubtaskConnectionDescriptor key =
                        new SubtaskConnectionDescriptor(oldSubtaskIndex, oldChannelIndex);
                VirtualChannel<T> vc = virtualChannels.get(key);
                if (vc == null) {
                    throw new IllegalStateException(
                            "No VirtualChannel found for key: "
                                    + key
                                    + "; known channels are "
                                    + virtualChannels.keySet());
                }

                vc.setNextBuffer(sourceBuffer);
                sourceBufferOwnershipTransferred = true;

                while (true) {
                    DeserializationResult result = vc.getNextRecord(deserializationDelegate);
                    if (result.isFullRecord()) {
                        if (currentBuffer == null) {
                            currentBuffer = bufferSupplier.requestBufferBlocking();
                        }
                        currentBuffer =
                                serializeElement(
                                        deserializationDelegate.getInstance(),
                                        currentBuffer,
                                        resultBuffers,
                                        bufferSupplier);
                    }
                    if (result.isBufferConsumed()) {
                        break;
                    }
                }

                if (currentBuffer != null) {
                    if (currentBuffer.readableBytes() > 0) {
                        resultBuffers.add(currentBuffer);
                    } else {
                        currentBuffer.recycleBuffer();
                    }
                    currentBuffer = null;
                }

                return resultBuffers;
            } catch (Throwable t) {
                if (!sourceBufferOwnershipTransferred) {
                    sourceBuffer.recycleBuffer();
                }
                // Avoid double-recycle: currentBuffer may already be the last element in
                // resultBuffers if serializeElement added it before the exception.
                if (currentBuffer != null
                        && (resultBuffers.isEmpty()
                                || resultBuffers.get(resultBuffers.size() - 1) != currentBuffer)) {
                    currentBuffer.recycleBuffer();
                }
                for (Buffer buf : resultBuffers) {
                    buf.recycleBuffer();
                }
                resultBuffers.clear();
                throw t;
            }
        }

        /**
         * Serializes a single stream element into the current buffer using the length-prefixed
         * format (4-byte big-endian length + record bytes) expected by Flink's record
         * deserializers. Spills into new buffers from {@code bufferSupplier} when needed.
         *
         * @return the buffer to continue writing into (may differ from the input buffer).
         */
        private Buffer serializeElement(
                StreamElement element,
                Buffer currentBuffer,
                List<Buffer> resultBuffers,
                BufferSupplier bufferSupplier)
                throws IOException, InterruptedException {
            outputSerializer.clear();
            serializer.serialize(element, outputSerializer);
            int recordLength = outputSerializer.length();

            writeLengthToBuffer(recordLength);
            currentBuffer =
                    writeDataToBuffer(
                            lengthBuffer, 0, 4, currentBuffer, resultBuffers, bufferSupplier);

            byte[] serializedData = outputSerializer.getSharedBuffer();
            currentBuffer =
                    writeDataToBuffer(
                            serializedData,
                            0,
                            recordLength,
                            currentBuffer,
                            resultBuffers,
                            bufferSupplier);
            return currentBuffer;
        }

        private void writeLengthToBuffer(int length) {
            lengthBuffer[0] = (byte) (length >> 24);
            lengthBuffer[1] = (byte) (length >> 16);
            lengthBuffer[2] = (byte) (length >> 8);
            lengthBuffer[3] = (byte) length;
        }

        /**
         * Writes data to the current buffer, spilling into new buffers from {@code bufferSupplier}
         * when the current one is full.
         *
         * @return the buffer to continue writing into (may differ from the input buffer).
         */
        private Buffer writeDataToBuffer(
                byte[] data,
                int dataOffset,
                int dataLength,
                Buffer currentBuffer,
                List<Buffer> resultBuffers,
                BufferSupplier bufferSupplier)
                throws IOException, InterruptedException {
            int offset = dataOffset;
            int remaining = dataLength;

            while (remaining > 0) {
                int writableBytes = currentBuffer.getMaxCapacity() - currentBuffer.getSize();

                if (writableBytes == 0) {
                    // Buffer is full, transfer ownership to resultBuffers
                    resultBuffers.add(currentBuffer);
                    currentBuffer = bufferSupplier.requestBufferBlocking();
                    writableBytes = currentBuffer.getMaxCapacity();
                }

                int bytesToWrite = Math.min(remaining, writableBytes);
                currentBuffer
                        .getMemorySegment()
                        .put(
                                currentBuffer.getMemorySegmentOffset() + currentBuffer.getSize(),
                                data,
                                offset,
                                bytesToWrite);
                currentBuffer.setSize(currentBuffer.getSize() + bytesToWrite);

                offset += bytesToWrite;
                remaining -= bytesToWrite;
            }
            return currentBuffer;
        }

        // ------------------------ end of FLINK-38544 transitional block -----------------------

        boolean hasPartialData() {
            return virtualChannels.values().stream().anyMatch(VirtualChannel::hasPartialData);
        }

        void clear() {
            virtualChannels.values().forEach(VirtualChannel::clear);
        }
    }
}
