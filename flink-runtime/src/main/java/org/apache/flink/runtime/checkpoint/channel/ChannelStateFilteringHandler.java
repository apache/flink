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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Filters recovered channel state buffers in the channel-state-unspilling thread.
 *
 * <p>This handler uses a layered design where each input gate has its own {@link GateFilterHandler}
 * that manages the gate's virtual channels with the correct serializer. This ensures that
 * multi-input tasks (e.g., TwoInputStreamTask) correctly use different serializers for different
 * input gates.
 *
 * <p>The design follows the pattern used in {@code DemultiplexingRecordDeserializer} where each
 * physical channel maintains its own map of virtual channels keyed by {@link
 * SubtaskConnectionDescriptor}.
 */
@Internal
public class ChannelStateFilteringHandler {

    /**
     * Handles filtering for a single input gate.
     *
     * <p>Each gate has its own serializer and virtual channels. This ensures correct serialization
     * for multi-input tasks where different gates have different record types.
     */
    static class GateFilterHandler<T> {

        /** Virtual channels for this gate, keyed by (oldSubtaskIndex, oldChannelIndex). */
        private final Map<SubtaskConnectionDescriptor, VirtualChannel<T>> virtualChannels;

        /** Serializer for this gate's element type. */
        private final StreamElementSerializer<T> serializer;

        /** Deserialization delegate for this gate. */
        private final DeserializationDelegate<StreamElement> deserializationDelegate;

        /** Reusable output serializer for writing records. */
        private final DataOutputSerializer outputSerializer;

        /** Temporary buffer for writing 4-byte length prefix (big-endian). */
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
         * Filters a buffer from a specific virtual channel.
         *
         * @param oldSubtaskIndex The old subtask index.
         * @param oldChannelIndex The old channel index.
         * @param sourceBuffer Original buffer to filter.
         * @param bufferSupplier Supplier for new buffers.
         * @return Filtered buffers.
         */
        List<Buffer> filterAndRewrite(
                int oldSubtaskIndex,
                int oldChannelIndex,
                Buffer sourceBuffer,
                BufferSupplier bufferSupplier)
                throws IOException, InterruptedException {

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

            List<StreamElement> filteredElements = new ArrayList<>();

            while (true) {
                DeserializationResult result = vc.getNextRecord(deserializationDelegate);
                if (result.isFullRecord()) {
                    filteredElements.add(deserializationDelegate.getInstance());
                }
                if (result.isBufferConsumed()) {
                    break;
                }
            }

            return serializeToBuffers(filteredElements, bufferSupplier);
        }

        /**
         * Serializes filtered stream elements to new buffers.
         *
         * <p>Each record is written with a 4-byte big-endian length prefix followed by the record
         * content. This matches the format expected by Flink's record deserializers.
         *
         * @param elements The filtered stream elements to serialize.
         * @param bufferSupplier Supplier for new buffers.
         * @return List of buffers containing the serialized elements.
         */
        private List<Buffer> serializeToBuffers(
                List<StreamElement> elements, BufferSupplier bufferSupplier)
                throws IOException, InterruptedException {

            List<Buffer> resultBuffers = new ArrayList<>();

            if (elements.isEmpty()) {
                return resultBuffers;
            }

            Buffer currentBuffer = bufferSupplier.requestBufferBlocking();

            for (StreamElement element : elements) {
                outputSerializer.clear();
                serializer.serialize(element, outputSerializer);
                int recordLength = outputSerializer.length();

                // Write 4-byte length prefix first (big-endian)
                writeLengthToBuffer(recordLength);
                currentBuffer =
                        writeDataToBuffer(
                                lengthBuffer, 0, 4, currentBuffer, resultBuffers, bufferSupplier);

                // Then write record content
                byte[] serializedData = outputSerializer.getSharedBuffer();
                currentBuffer =
                        writeDataToBuffer(
                                serializedData,
                                0,
                                recordLength,
                                currentBuffer,
                                resultBuffers,
                                bufferSupplier);
            }

            if (currentBuffer.readableBytes() > 0) {
                resultBuffers.add(currentBuffer.retainBuffer());
            }
            currentBuffer.recycleBuffer();

            return resultBuffers;
        }

        /** Writes a 4-byte big-endian length prefix to the lengthBuffer. */
        private void writeLengthToBuffer(int length) {
            lengthBuffer[0] = (byte) (length >> 24);
            lengthBuffer[1] = (byte) (length >> 16);
            lengthBuffer[2] = (byte) (length >> 8);
            lengthBuffer[3] = (byte) length;
        }

        /**
         * Writes data to buffer, handling buffer overflow by requesting new buffers.
         *
         * @return The current buffer after writing (may be different from input if overflow
         *     occurred).
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
                    if (currentBuffer.readableBytes() > 0) {
                        resultBuffers.add(currentBuffer.retainBuffer());
                    }
                    currentBuffer.recycleBuffer();
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

        boolean hasPartialData() {
            return virtualChannels.values().stream().anyMatch(VirtualChannel::hasPartialData);
        }

        void clear() {
            virtualChannels.values().forEach(VirtualChannel::clear);
        }
    }

    /**
     * Per-gate filter handlers indexed by gate index. Each gate may have a different record type,
     * so wildcard is used to allow heterogeneous types across gates.
     */
    private final GateFilterHandler<?>[] gateHandlers;

    /**
     * Creates a new ChannelStateFilteringHandler.
     *
     * @param gateHandlers Array of per-gate filter handlers indexed by gate index. Each element
     *     handles a specific gate with its own record type.
     */
    ChannelStateFilteringHandler(GateFilterHandler<?>[] gateHandlers) {
        this.gateHandlers = checkNotNull(gateHandlers);
    }

    /**
     * Creates a ChannelStateFilteringHandler from a RecordFilterContext and InputGates.
     *
     * <p>This factory method creates a {@link GateFilterHandler} for each input gate, with each
     * handler using the correct serializer for that gate's input type. Virtual channels are keyed
     * by {@link SubtaskConnectionDescriptor} within each gate.
     *
     * @param filterContext The filter context containing input configs and rescaling info.
     * @param inputGates The input gates being recovered.
     * @return A new ChannelStateFilteringHandler instance, or null if no filtering is needed.
     */
    @Nullable
    public static ChannelStateFilteringHandler createFromContext(
            RecordFilterContext filterContext, InputGate[] inputGates) {
        // Source tasks have no network inputs, so no filtering is needed
        if (filterContext.getNumberOfGates() == 0) {
            return null;
        }

        InflightDataRescalingDescriptor rescalingDescriptor =
                filterContext.getRescalingDescriptor();

        // Use array indexed by gateIndex.
        GateFilterHandler<?>[] gateHandlers = new GateFilterHandler<?>[inputGates.length];
        boolean hasAnyVirtualChannels = false;

        // Process all gates. Each gate must have a corresponding InputFilterConfig.
        // Source tasks (with no input gates) are already handled above by returning null.
        for (int gateIndex = 0; gateIndex < inputGates.length; gateIndex++) {
            gateHandlers[gateIndex] =
                    createGateHandler(filterContext, inputGates, rescalingDescriptor, gateIndex);
            if (gateHandlers[gateIndex] != null) {
                hasAnyVirtualChannels = true;
            }
        }

        // Return null if no virtual channels were created
        if (!hasAnyVirtualChannels) {
            return null;
        }

        return new ChannelStateFilteringHandler(gateHandlers);
    }

    /**
     * Creates a GateFilterHandler for a single gate. Uses a method-level type parameter to ensure
     * type safety within each gate, while allowing different gates to have different types.
     */
    @SuppressWarnings("unchecked")
    @Nullable
    private static <T> GateFilterHandler<T> createGateHandler(
            RecordFilterContext filterContext,
            InputGate[] inputGates,
            InflightDataRescalingDescriptor rescalingDescriptor,
            int gateIndex) {
        RecordFilterContext.InputFilterConfig inputConfig = filterContext.getInputConfig(gateIndex);
        // Every physical gate must have a config. Null indicates a bug in initialization.
        if (inputConfig == null) {
            throw new IllegalStateException(
                    "No InputFilterConfig for gateIndex "
                            + gateIndex
                            + ". This indicates a bug in RecordFilterContext initialization.");
        }

        InputGate gate = inputGates[gateIndex];
        int[] oldSubtaskIndexes = rescalingDescriptor.getOldSubtaskIndexes(gateIndex);
        RescaleMappings channelMapping = rescalingDescriptor.getChannelMapping(gateIndex);

        // Create serializer for this gate's input type
        TypeSerializer<T> typeSerializer = (TypeSerializer<T>) inputConfig.getTypeSerializer();
        StreamElementSerializer<T> elementSerializer =
                new StreamElementSerializer<>(typeSerializer);

        // Create filter factory for this gate's input
        VirtualChannelRecordFilterFactory<T> filterFactory =
                VirtualChannelRecordFilterFactory.fromContext(filterContext, gateIndex);

        // Build virtual channels for this gate
        Map<SubtaskConnectionDescriptor, VirtualChannel<T>> gateVirtualChannels = new HashMap<>();

        // For each old subtask that contributed state
        for (int oldSubtaskIndex : oldSubtaskIndexes) {
            // For each channel in the gate
            int numChannels = gate.getNumberOfInputChannels();
            int[] oldChannelIndexes = getOldChannelIndexes(channelMapping, numChannels);

            for (int oldChannelIndex : oldChannelIndexes) {
                SubtaskConnectionDescriptor key =
                        new SubtaskConnectionDescriptor(oldSubtaskIndex, oldChannelIndex);

                // Avoid creating duplicate channels
                if (gateVirtualChannels.containsKey(key)) {
                    continue;
                }

                // Determine if this channel needs filtering
                boolean isAmbiguous = rescalingDescriptor.isAmbiguous(gateIndex, oldSubtaskIndex);

                RecordFilter<T> recordFilter =
                        isAmbiguous
                                ? filterFactory.createFilter()
                                : VirtualChannelRecordFilterFactory.createPassThroughFilter();

                // Create deserializer for this virtual channel
                RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer =
                        createDeserializer(filterContext.getTmpDirectories());

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
     * Gets all old channel indexes that map to channels in this gate.
     *
     * @param channelMapping The channel mapping from rescaling.
     * @param numChannels Number of channels in the current gate.
     * @return Array of old channel indexes.
     */
    private static int[] getOldChannelIndexes(RescaleMappings channelMapping, int numChannels) {
        // channelMapping is new -> old mapping, so getMappedIndexes(newIndex) returns old indexes
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

    /**
     * Creates a RecordDeserializer for a Virtual Channel.
     *
     * @param tmpDirectories Temporary directories for spilling.
     * @return A new RecordDeserializer instance.
     */
    private static RecordDeserializer<DeserializationDelegate<StreamElement>> createDeserializer(
            String[] tmpDirectories) {
        if (tmpDirectories != null && tmpDirectories.length > 0) {
            return new SpillingAdaptiveSpanningRecordDeserializer<>(tmpDirectories);
        } else {
            // Use default temp directories if not provided
            String[] defaultDirs = new String[] {System.getProperty("java.io.tmpdir")};
            return new SpillingAdaptiveSpanningRecordDeserializer<>(defaultDirs);
        }
    }

    /**
     * Process a buffer from a specific Virtual Channel, filtering records and rewriting to new
     * buffers.
     *
     * @param gateIndex The input gate index.
     * @param oldSubtaskIndex The old subtask index from rescaling.
     * @param oldChannelIndex The old channel index from rescaling.
     * @param sourceBuffer Original buffer to filter.
     * @param bufferSupplier Supplier for new buffers (will block if no buffer available).
     * @return Filtered buffers (may be empty if all records were filtered out).
     * @throws IOException If an I/O error occurs during deserialization or serialization.
     * @throws InterruptedException If the thread is interrupted while waiting for buffers.
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

    /**
     * Checks if there is any partial (spanning) record remaining in any VirtualChannel.
     *
     * @return true if any VirtualChannel has partial data.
     */
    public boolean hasPartialData() {
        for (GateFilterHandler<?> handler : gateHandlers) {
            if (handler != null && handler.hasPartialData()) {
                return true;
            }
        }
        return false;
    }

    /** Clears the state of all VirtualChannels. */
    public void clear() {
        for (GateFilterHandler<?> handler : gateHandlers) {
            if (handler != null) {
                handler.clear();
            }
        }
    }

    /**
     * Interface for supplying buffers. Implementations should block until a buffer is available
     * (Phase 1 strategy).
     */
    @FunctionalInterface
    public interface BufferSupplier {
        /**
         * Requests a Buffer, blocking until one is available.
         *
         * @return A Buffer for writing data (should be writable with size 0).
         * @throws IOException If an I/O error occurs.
         * @throws InterruptedException If the thread is interrupted while waiting.
         */
        Buffer requestBufferBlocking() throws IOException, InterruptedException;
    }
}
