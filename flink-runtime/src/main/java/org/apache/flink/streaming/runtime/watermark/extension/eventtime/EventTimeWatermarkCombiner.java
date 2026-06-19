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

package org.apache.flink.streaming.runtime.watermark.extension.eventtime;

import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermark.WatermarkCombiner;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.util.function.Consumer;

/**
 * A {@link WatermarkCombiner} used to combine {@link EventTimeExtension} related watermarks in
 * input channels.
 */
public class EventTimeWatermarkCombiner extends StatusWatermarkValve implements WatermarkCombiner {

    private WrappedDataOutput<?> output;

    public EventTimeWatermarkCombiner(int numInputChannels) {
        super(numInputChannels);
        this.output = new WrappedDataOutput<>();
    }

    @Override
    public void combineWatermark(
            Watermark watermark, int channelIndex, Consumer<Watermark> watermarkEmitter)
            throws Exception {
        output.setWatermarkEmitter(watermarkEmitter);

        if (EventTimeExtension.isEventTimeWatermark(watermark)) {
            inputWatermark(
                    new org.apache.flink.streaming.api.watermark.Watermark(
                            ((LongWatermark) watermark).getValue()),
                    channelIndex,
                    output);
        } else if (EventTimeExtension.isIdleStatusWatermark(watermark.getIdentifier())) {
            inputWatermarkStatus(
                    new WatermarkStatus(
                            ((BoolWatermark) watermark).getValue()
                                    ? WatermarkStatus.IDLE_STATUS
                                    : WatermarkStatus.ACTIVE_STATUS),
                    channelIndex,
                    output);
        }
    }

    /** Wrap {@link DataOutput} to emit watermarks using {@code watermarkEmitter}. */
    static class WrappedDataOutput<T> implements DataOutput<T> {

        private Consumer<Watermark> watermarkEmitter;

        public WrappedDataOutput() {}

        public void setWatermarkEmitter(Consumer<Watermark> watermarkEmitter) {
            this.watermarkEmitter = watermarkEmitter;
        }

        @Override
        public void emitRecord(StreamRecord<T> streamRecord) throws Exception {
            throw new RuntimeException("Should not emit records with this output.");
        }

        @Override
        public void emitWatermark(org.apache.flink.streaming.api.watermark.Watermark watermark)
                throws Exception {
            watermarkEmitter.accept(
                    EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(
                            watermark.getTimestamp()));
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            watermarkEmitter.accept(
                    EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(
                            watermarkStatus.isIdle()));
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            throw new RuntimeException("Should not emit LatencyMarker with this output.");
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) throws Exception {
            throw new RuntimeException("Should not emit RecordAttributes with this output.");
        }

        @Override
        public void emitWatermark(WatermarkEvent watermark) throws Exception {
            throw new RuntimeException("Should not emit WatermarkEvent with this output.");
        }
    }
}
