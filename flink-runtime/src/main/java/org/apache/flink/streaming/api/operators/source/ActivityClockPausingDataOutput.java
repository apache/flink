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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.util.PausableRelativeClock;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PushingAsyncDataInput.DataOutput} decorator that pauses the source's input activity
 * clock while the downstream operators process what was emitted.
 *
 * <p>Everything emitted by a source is processed synchronously by the chained operators on the task
 * thread before control returns to the source reader. During that time no split can be polled, so
 * the elapsed time says nothing about whether a split has records. Without this decorator a slow
 * chained operator, or a window firing triggered by a watermark, can exceed the idleness timeout
 * and get a split with pending records marked idle, after which those records arrive behind an
 * already advanced watermark and are dropped as late.
 *
 * <p>This is the same idea as pausing the clock during backpressure (FLIP-471): the idleness
 * timeout only counts time during which the source was actually able to make progress on its input.
 *
 * @param <T> The type of the emitted records.
 */
@Internal
public final class ActivityClockPausingDataOutput<T>
        implements PushingAsyncDataInput.DataOutput<T> {

    private final PushingAsyncDataInput.DataOutput<T> delegate;
    private final PausableRelativeClock inputActivityClock;

    public ActivityClockPausingDataOutput(
            PushingAsyncDataInput.DataOutput<T> delegate,
            PausableRelativeClock inputActivityClock) {
        this.delegate = checkNotNull(delegate);
        this.inputActivityClock = checkNotNull(inputActivityClock);
    }

    @Override
    public void emitRecord(StreamRecord<T> streamRecord) throws Exception {
        inputActivityClock.pause();
        try {
            delegate.emitRecord(streamRecord);
        } finally {
            inputActivityClock.unPause();
        }
    }

    @Override
    public void emitWatermark(Watermark watermark) throws Exception {
        inputActivityClock.pause();
        try {
            delegate.emitWatermark(watermark);
        } finally {
            inputActivityClock.unPause();
        }
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        inputActivityClock.pause();
        try {
            delegate.emitWatermarkStatus(watermarkStatus);
        } finally {
            inputActivityClock.unPause();
        }
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        inputActivityClock.pause();
        try {
            delegate.emitLatencyMarker(latencyMarker);
        } finally {
            inputActivityClock.unPause();
        }
    }

    @Override
    public void emitRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        inputActivityClock.pause();
        try {
            delegate.emitRecordAttributes(recordAttributes);
        } finally {
            inputActivityClock.unPause();
        }
    }

    @Override
    public void emitWatermark(WatermarkEvent watermark) throws Exception {
        inputActivityClock.pause();
        try {
            delegate.emitWatermark(watermark);
        } finally {
            inputActivityClock.unPause();
        }
    }
}
