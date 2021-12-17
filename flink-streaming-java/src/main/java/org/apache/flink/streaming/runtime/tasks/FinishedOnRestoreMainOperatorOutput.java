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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

/**
 * A fake main operator output that skips all the following operators for finished on restored
 * tasks.
 */
public class FinishedOnRestoreMainOperatorOutput<OUT> implements WatermarkGaugeExposingOutput<OUT> {

    private final RecordWriterOutput<?>[] streamOutputs;

    private final WatermarkGauge watermarkGauge = new WatermarkGauge();

    public FinishedOnRestoreMainOperatorOutput(RecordWriterOutput<?>[] streamOutputs) {
        this.streamOutputs = streamOutputs;
    }

    @Override
    public void collect(OUT record) {
        throw new IllegalStateException();
    }

    @Override
    public void emitWatermark(Watermark mark) {
        watermarkGauge.setCurrentWatermark(mark.getTimestamp());
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.emitWatermark(mark);
        }
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        throw new IllegalStateException();
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        throw new IllegalStateException();
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        throw new IllegalStateException();
    }

    @Override
    public void close() {}

    @Override
    public Gauge<Long> getWatermarkGauge() {
        return watermarkGauge;
    }
}
