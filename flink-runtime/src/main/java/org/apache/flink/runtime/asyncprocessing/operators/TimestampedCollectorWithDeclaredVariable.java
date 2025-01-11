/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.asyncprocessing.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclaredVariable;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

/**
 * Wrapper around an {@link Output} for user functions that expect a {@link Output}. This collector
 * is mostly like the {@link org.apache.flink.streaming.api.operators.TimestampedCollector} but it
 * keeps the timestamp in {@link DeclaredVariable} and give it to output finally. Most operators
 * would set the timestamp of the incoming {@link StreamRecord} here.
 *
 * @param <T> The type of the elements that can be emitted.
 */
@Internal
public final class TimestampedCollectorWithDeclaredVariable<T> implements Output<T> {

    private final Output<StreamRecord<T>> output;

    private final StreamRecord<T> reuse;

    private final DeclaredVariable<Long> timestamp;

    /**
     * Creates a new {@link TimestampedCollectorWithDeclaredVariable} that wraps the given {@link
     * Output}.
     */
    public TimestampedCollectorWithDeclaredVariable(
            Output<StreamRecord<T>> output, DeclarationContext declarationContext) {
        this(
                output,
                declarationContext.declareVariable(
                        LongSerializer.INSTANCE, "_TCollector$timestamp", null));
    }

    /**
     * Creates a new {@link TimestampedCollectorWithDeclaredVariable} that wraps the given {@link
     * Output} and given {@link DeclaredVariable} that holds the timestamp.
     */
    public TimestampedCollectorWithDeclaredVariable(
            Output<StreamRecord<T>> output, DeclaredVariable<Long> timestamp) {
        this.output = output;
        this.timestamp = timestamp;
        this.reuse = new StreamRecord<>(null);
    }

    @Override
    public void collect(T record) {
        Long time = timestamp.get();
        if (time == null) {
            reuse.eraseTimestamp();
        } else {
            reuse.setTimestamp(time);
        }
        output.collect(reuse.replace(record));
    }

    public void setTimestamp(StreamRecord<?> timestampBase) {
        if (timestampBase.hasTimestamp()) {
            timestamp.set(timestampBase.getTimestamp());
        } else {
            timestamp.set(null);
        }
    }

    public void setAbsoluteTimestamp(long time) {
        timestamp.set(time);
    }

    public void eraseTimestamp() {
        timestamp.set(null);
    }

    @Override
    public void close() {
        output.close();
    }

    @Override
    public void emitWatermark(Watermark mark) {
        output.emitWatermark(mark);
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        output.emitWatermarkStatus(watermarkStatus);
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        output.collect(outputTag, record);
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        output.emitLatencyMarker(latencyMarker);
    }

    @Override
    public void emitRecordAttributes(RecordAttributes recordAttributes) {
        output.emitRecordAttributes(recordAttributes);
    }

    @Override
    public void emitWatermark(WatermarkEvent watermark) {
        output.emitWatermark(watermark);
    }
}
