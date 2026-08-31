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
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilter;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilterContext;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannel;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannelRecordFilterFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

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

        return new GateFilterHandler<>(gateVirtualChannels, elementSerializer, channelMapping);
    }

    /**
     * Maps each old-channel key to the virtual channels that fold into its new channel, so
     * watermarks/statuses can be aggregated across old channels merged on a fan-in rescale. Without
     * a rescale each group is a singleton and aggregation is verbatim pass-through.
     */
    private static <T>
            Map<SubtaskConnectionDescriptor, List<VirtualChannel<T>>> buildWatermarkMergeGroups(
                    Map<SubtaskConnectionDescriptor, VirtualChannel<T>> gateVirtualChannels,
                    RescaleMappings channelMapping) {
        RescaleMappings oldToNewMapping = channelMapping.invert();
        Map<Integer, List<VirtualChannel<T>>> byNewChannel = new HashMap<>();
        Map<SubtaskConnectionDescriptor, List<VirtualChannel<T>>> mergeGroups = new HashMap<>();
        gateVirtualChannels.forEach(
                (key, vc) -> {
                    List<VirtualChannel<T>> group =
                            byNewChannel.computeIfAbsent(
                                    newChannelIndexOf(key, oldToNewMapping),
                                    idx -> new ArrayList<>());
                    group.add(vc);
                    mergeGroups.put(key, group);
                });
        return mergeGroups;
    }

    /**
     * Resolves the new channel index the given old-channel descriptor is routed to. Its {@code
     * outputSubtaskIndex} field carries the old channel index.
     */
    private static int newChannelIndexOf(
            SubtaskConnectionDescriptor oldChannelKey, RescaleMappings oldToNewMapping) {
        int oldChannelIndex = oldChannelKey.getOutputSubtaskIndex();
        int[] mapped = oldToNewMapping.getMappedIndexes(oldChannelIndex);
        checkState(
                mapped.length == 1,
                "One old channel is expected to fold into exactly one new channel, but %s mapped to %s new channels.",
                oldChannelKey,
                mapped.length);
        return mapped[0];
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
     * Handles record filtering for a single input gate. Each gate has its own serializer and set of
     * virtual channels, allowing different gates to handle different record types independently.
     */
    static class GateFilterHandler<T> {

        private final Map<SubtaskConnectionDescriptor, VirtualChannel<T>> virtualChannels;

        /**
         * For each old-channel key, the virtual channels folding into the same new channel, used to
         * aggregate watermarks/statuses across old channels merged on a fan-in rescale.
         */
        private final Map<SubtaskConnectionDescriptor, List<VirtualChannel<T>>>
                watermarkMergeGroups;

        private final StreamElementSerializer<T> serializer;
        private final DeserializationDelegate<StreamElement> deserializationDelegate;

        GateFilterHandler(
                Map<SubtaskConnectionDescriptor, VirtualChannel<T>> virtualChannels,
                StreamElementSerializer<T> serializer,
                RescaleMappings channelMapping) {
            this.virtualChannels = checkNotNull(virtualChannels);
            this.serializer = checkNotNull(serializer);
            this.watermarkMergeGroups = buildWatermarkMergeGroups(virtualChannels, channelMapping);
            this.deserializationDelegate = new NonReusingDeserializationDelegate<>(serializer);
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

                List<VirtualChannel<T>> mergeGroup = watermarkMergeGroups.get(key);
                checkNotNull(mergeGroup, "No watermark merge group for key: %s", key);

                vc.setNextBuffer(sourceBuffer);
                sourceBufferOwnershipTransferred = true;

                while (true) {
                    DeserializationResult result = vc.getNextRecord(deserializationDelegate);
                    if (result.isFullRecord()) {
                        // vc.getNextRecord has already updated the source channel's lastWatermark /
                        // watermarkStatus, so aggregation below reads the up-to-date group state.
                        emitAggregated(
                                deserializationDelegate.getInstance(),
                                mergeGroup,
                                outputSerializer);
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
         * Writes one element, aggregating non-records across the {@code mergeGroup}: emit the group
         * min watermark (suppressed until every merged channel has one), and {@code ACTIVE} status
         * while any merged channel is active. Records and latency markers pass through verbatim.
         */
        private void emitAggregated(
                StreamElement element,
                List<VirtualChannel<T>> mergeGroup,
                DataOutputSerializer outputSerializer)
                throws IOException {
            if (element.isWatermark()) {
                Watermark minWatermark = null;
                for (VirtualChannel<T> channel : mergeGroup) {
                    Watermark candidate = channel.getLastWatermark();
                    if (minWatermark == null
                            || candidate.getTimestamp() < minWatermark.getTimestamp()) {
                        minWatermark = candidate;
                    }
                }
                checkState(minWatermark != null, "Should always have a watermark");
                // min == UNINITIALIZED only when some merged old channel has no watermark yet;
                // hold the group's watermark back until every one of them has produced one.
                if (!minWatermark.equals(Watermark.UNINITIALIZED)) {
                    serializeElement(minWatermark, outputSerializer);
                }
            } else if (element.isWatermarkStatus()) {
                boolean anyActive = false;
                for (VirtualChannel<T> channel : mergeGroup) {
                    if (channel.getWatermarkStatus().isActive()) {
                        anyActive = true;
                        break;
                    }
                }
                serializeElement(
                        anyActive ? WatermarkStatus.ACTIVE : element.asWatermarkStatus(),
                        outputSerializer);
            } else {
                serializeElement(element, outputSerializer);
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

        boolean hasPartialData() {
            return virtualChannels.values().stream().anyMatch(VirtualChannel::hasPartialData);
        }

        void clear() {
            virtualChannels.values().forEach(VirtualChannel::clear);
        }
    }
}
