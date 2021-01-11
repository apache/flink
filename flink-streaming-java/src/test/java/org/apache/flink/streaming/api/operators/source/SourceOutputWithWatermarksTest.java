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

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for the {@link SourceOutputWithWatermarks}. */
public class SourceOutputWithWatermarksTest {

    @Test
    public void testNoTimestampValue() {
        final CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();
        final SourceOutputWithWatermarks<Integer> out =
                SourceOutputWithWatermarks.createWithSameOutputs(
                        dataOutput, new RecordTimestampAssigner<>(), new NoWatermarksGenerator<>());

        out.collect(17);

        final Object event = dataOutput.events.get(0);
        assertThat(event, instanceOf(StreamRecord.class));
        assertEquals(TimestampAssigner.NO_TIMESTAMP, ((StreamRecord<?>) event).getTimestamp());
    }

    @Test
    public void eventsAreBeforeWatermarks() {
        final CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();
        final SourceOutputWithWatermarks<Integer> out =
                SourceOutputWithWatermarks.createWithSameOutputs(
                        dataOutput,
                        new RecordTimestampAssigner<>(),
                        new TestWatermarkGenerator<>());

        out.collect(42, 12345L);

        assertThat(
                dataOutput.events,
                contains(
                        new StreamRecord<>(42, 12345L),
                        new org.apache.flink.streaming.api.watermark.Watermark(12345L)));
    }

    // ------------------------------------------------------------------------

    private static final class TestWatermarkGenerator<T> implements WatermarkGenerator<T> {

        private long lastTimestamp;

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            lastTimestamp = eventTimestamp;
            output.emitWatermark(new Watermark(eventTimestamp));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(lastTimestamp));
        }
    }
}
