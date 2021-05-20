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

package org.apache.flink.streaming.runtime.io.recovery;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Demultiplexes buffers on subtask-level.
 *
 * <p>Example: If the current task has been downscaled from 2 to 1. Then the only new subtask needs
 * to handle data originating from old subtasks 0 and 1.
 *
 * <p>It is also responsible for summarizing watermark and stream statuses of the virtual channels.
 */
class DemultiplexingRecordDeserializer<T>
        implements RecordDeserializer<DeserializationDelegate<StreamElement>> {
    public static final DemultiplexingRecordDeserializer UNMAPPED =
            new DemultiplexingRecordDeserializer(Collections.emptyMap());
    private final Map<SubtaskConnectionDescriptor, VirtualChannel<T>> channels;

    private VirtualChannel<T> currentVirtualChannel;

    static class VirtualChannel<T> {
        private final RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer;
        private final Predicate<StreamRecord<T>> recordFilter;
        Watermark lastWatermark = Watermark.UNINITIALIZED;
        StreamStatus streamStatus = StreamStatus.ACTIVE;
        private DeserializationResult lastResult;

        VirtualChannel(
                RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer,
                Predicate<StreamRecord<T>> recordFilter) {
            this.deserializer = deserializer;
            this.recordFilter = recordFilter;
        }

        public DeserializationResult getNextRecord(DeserializationDelegate<StreamElement> delegate)
                throws IOException {
            do {
                lastResult = deserializer.getNextRecord(delegate);

                if (lastResult.isFullRecord()) {
                    final StreamElement element = delegate.getInstance();
                    // test if record belongs to this subtask if it comes from ambiguous channel
                    if (element.isRecord() && recordFilter.test(element.asRecord())) {
                        return lastResult;
                    } else if (element.isWatermark()) {
                        lastWatermark = element.asWatermark();
                        return lastResult;
                    } else if (element.isStreamStatus()) {
                        streamStatus = element.asStreamStatus();
                        return lastResult;
                    }
                }
                // loop is only re-executed for filtered full records
            } while (!lastResult.isBufferConsumed());
            return DeserializationResult.PARTIAL_RECORD;
        }

        public void setNextBuffer(Buffer buffer) throws IOException {
            deserializer.setNextBuffer(buffer);
        }

        public void clear() {
            deserializer.clear();
        }

        public boolean hasPartialData() {
            return lastResult != null && !lastResult.isBufferConsumed();
        }
    }

    public DemultiplexingRecordDeserializer(
            Map<SubtaskConnectionDescriptor, VirtualChannel<T>> channels) {
        this.channels = checkNotNull(channels);
    }

    public void select(SubtaskConnectionDescriptor descriptor) {
        currentVirtualChannel = channels.get(descriptor);
        if (currentVirtualChannel == null) {
            throw new IllegalStateException(
                    "Cannot select " + descriptor + "; known channels are " + channels.keySet());
        }
    }

    public boolean hasMappings() {
        return !channels.isEmpty();
    }

    @VisibleForTesting
    Collection<SubtaskConnectionDescriptor> getVirtualChannelSelectors() {
        return channels.keySet();
    }

    @Override
    public void setNextBuffer(Buffer buffer) throws IOException {
        currentVirtualChannel.setNextBuffer(buffer);
    }

    @Override
    public CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException {
        throw new IllegalStateException("Cannot checkpoint while recovering");
    }

    public boolean hasPartialData() {
        return channels.values().stream().anyMatch(VirtualChannel::hasPartialData);
    }

    /** Summarizes the status and watermarks of all virtual channels. */
    @Override
    public DeserializationResult getNextRecord(DeserializationDelegate<StreamElement> delegate)
            throws IOException {
        DeserializationResult result;
        do {
            result = currentVirtualChannel.getNextRecord(delegate);

            if (result.isFullRecord()) {
                final StreamElement element = delegate.getInstance();
                if (element.isRecord() || element.isLatencyMarker()) {
                    return result;
                } else if (element.isWatermark()) {
                    // basically, do not emit a watermark if not all virtual channel are past it
                    final Watermark minWatermark =
                            channels.values().stream()
                                    .map(virtualChannel -> virtualChannel.lastWatermark)
                                    .min(Comparator.comparing(Watermark::getTimestamp))
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            "Should always have a watermark"));
                    // at least one virtual channel has no watermark, don't emit any watermark yet
                    if (minWatermark.equals(Watermark.UNINITIALIZED)) {
                        continue;
                    }
                    delegate.setInstance(minWatermark);
                    return result;
                } else if (element.isStreamStatus()) {
                    // summarize statuses across all virtual channels
                    // duplicate statuses are filtered in StatusWatermarkValve
                    if (channels.values().stream().anyMatch(d -> d.streamStatus.isActive())) {
                        delegate.setInstance(StreamStatus.ACTIVE);
                    }
                    return result;
                }
            }

            // loop is only re-executed for suppressed watermark
        } while (!result.isBufferConsumed());
        return DeserializationResult.PARTIAL_RECORD;
    }

    public void clear() {
        channels.values().forEach(d -> d.clear());
    }

    static <T> DemultiplexingRecordDeserializer<T> create(
            InputChannelInfo channelInfo,
            InflightDataRescalingDescriptor rescalingDescriptor,
            Function<Integer, RecordDeserializer<DeserializationDelegate<StreamElement>>>
                    deserializerFactory,
            Function<InputChannelInfo, Predicate<StreamRecord<T>>> recordFilterFactory) {
        int[] oldSubtaskIndexes =
                rescalingDescriptor.getOldSubtaskIndexes(channelInfo.getGateIdx());
        if (oldSubtaskIndexes.length == 0) {
            return UNMAPPED;
        }
        final int[] oldChannelIndexes =
                rescalingDescriptor
                        .getChannelMapping(channelInfo.getGateIdx())
                        .getMappedIndexes(channelInfo.getInputChannelIdx());
        if (oldChannelIndexes.length == 0) {
            return UNMAPPED;
        }
        int totalChannels = oldSubtaskIndexes.length * oldChannelIndexes.length;
        Map<SubtaskConnectionDescriptor, VirtualChannel<T>> virtualChannels =
                Maps.newHashMapWithExpectedSize(totalChannels);
        for (int subtask : oldSubtaskIndexes) {
            for (int channel : oldChannelIndexes) {
                SubtaskConnectionDescriptor descriptor =
                        new SubtaskConnectionDescriptor(subtask, channel);
                virtualChannels.put(
                        descriptor,
                        new VirtualChannel<>(
                                deserializerFactory.apply(totalChannels),
                                rescalingDescriptor.isAmbiguous(channelInfo.getGateIdx(), subtask)
                                        ? recordFilterFactory.apply(channelInfo)
                                        : RecordFilter.all()));
            }
        }

        return new DemultiplexingRecordDeserializer(virtualChannels);
    }

    @Override
    public String toString() {
        return "DemultiplexingRecordDeserializer{" + "channels=" + channels.keySet() + '}';
    }
}
