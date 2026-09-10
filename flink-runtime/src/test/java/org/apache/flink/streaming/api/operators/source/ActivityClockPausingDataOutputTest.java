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

import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.util.PausableRelativeClock;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ActivityClockPausingDataOutput}. */
class ActivityClockPausingDataOutputTest {

    private static final long BUSY_MILLIS = 250;

    @Test
    void downstreamTimeDoesNotAdvanceActivityClock() throws Exception {
        final ManualClock baseClock = new ManualClock();
        final PausableRelativeClock activityClock = new PausableRelativeClock(baseClock);
        final CollectingDataOutput<Integer> collected = new CollectingDataOutput<>();
        final BusyDelegate<Integer> busyDelegate = new BusyDelegate<>(collected, baseClock);
        final ActivityClockPausingDataOutput<Integer> output =
                new ActivityClockPausingDataOutput<>(busyDelegate, activityClock);

        final long start = activityClock.relativeTimeMillis();

        output.emitRecord(new StreamRecord<>(1, 1L));
        output.emitWatermark(new Watermark(1L));
        output.emitWatermarkStatus(WatermarkStatus.IDLE);
        output.emitLatencyMarker(new LatencyMarker(1L, null, 0));
        output.emitRecordAttributes(new RecordAttributesBuilder(Collections.emptyList()).build());
        output.emitWatermark(new WatermarkEvent(null, false));

        assertThat(activityClock.relativeTimeMillis() - start).isZero();
        assertThat(baseClock.relativeTimeMillis()).isEqualTo(6 * BUSY_MILLIS);
        assertThat(collected.getEvents()).hasSize(6);
    }

    @Test
    void clockResumesBetweenEmits() throws Exception {
        final ManualClock baseClock = new ManualClock();
        final PausableRelativeClock activityClock = new PausableRelativeClock(baseClock);
        final ActivityClockPausingDataOutput<Integer> output =
                new ActivityClockPausingDataOutput<>(
                        new BusyDelegate<>(new CollectingDataOutput<>(), baseClock), activityClock);

        final long start = activityClock.relativeTimeMillis();
        output.emitRecord(new StreamRecord<>(1, 1L));
        baseClock.advanceTime(Duration.ofMillis(40)); // reader time between records: counts
        output.emitRecord(new StreamRecord<>(2, 2L));

        assertThat(activityClock.relativeTimeMillis() - start).isEqualTo(40);
    }

    @Test
    void clockResumesWhenDelegateThrows() {
        final ManualClock baseClock = new ManualClock();
        final PausableRelativeClock activityClock = new PausableRelativeClock(baseClock);
        final ActivityClockPausingDataOutput<Integer> output =
                new ActivityClockPausingDataOutput<>(
                        new BusyDelegate<Integer>(new CollectingDataOutput<>(), baseClock) {
                            @Override
                            public void emitRecord(StreamRecord<Integer> streamRecord)
                                    throws Exception {
                                throw new Exception("downstream failure");
                            }
                        },
                        activityClock);

        assertThatThrownBy(() -> output.emitRecord(new StreamRecord<>(1, 1L)))
                .hasMessage("downstream failure");

        final long afterFailure = activityClock.relativeTimeMillis();
        baseClock.advanceTime(Duration.ofMillis(10));
        assertThat(activityClock.relativeTimeMillis() - afterFailure).isEqualTo(10);
    }

    /** A downstream output that burns {@link #BUSY_MILLIS} of wall-clock time on every call. */
    private static class BusyDelegate<E> implements PushingAsyncDataInput.DataOutput<E> {
        private final PushingAsyncDataInput.DataOutput<E> delegate;
        private final ManualClock clock;

        BusyDelegate(PushingAsyncDataInput.DataOutput<E> delegate, ManualClock clock) {
            this.delegate = delegate;
            this.clock = clock;
        }

        private void burn() {
            clock.advanceTime(Duration.ofMillis(BUSY_MILLIS));
        }

        @Override
        public void emitRecord(StreamRecord<E> streamRecord) throws Exception {
            burn();
            delegate.emitRecord(streamRecord);
        }

        @Override
        public void emitWatermark(Watermark watermark) throws Exception {
            burn();
            delegate.emitWatermark(watermark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            burn();
            delegate.emitWatermarkStatus(watermarkStatus);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            burn();
            delegate.emitLatencyMarker(latencyMarker);
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) throws Exception {
            burn();
            delegate.emitRecordAttributes(recordAttributes);
        }

        @Override
        public void emitWatermark(WatermarkEvent watermark) throws Exception {
            burn();
            delegate.emitWatermark(watermark);
        }
    }
}
