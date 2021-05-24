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

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import org.junit.Test;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

/** Unit tests for the {@link WatermarkToDataOutput}. */
public class WatermarkToDataOutputTest {

    @Test
    public void testInitialZeroWatermark() {
        final CollectingDataOutput<Object> testingOutput = new CollectingDataOutput<>();
        final WatermarkToDataOutput wmOutput = new WatermarkToDataOutput(testingOutput);

        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(0L));

        assertThat(testingOutput.events, contains(new Watermark(0L)));
    }

    @Test
    public void testWatermarksDoNotRegress() {
        final CollectingDataOutput<Object> testingOutput = new CollectingDataOutput<>();
        final WatermarkToDataOutput wmOutput = new WatermarkToDataOutput(testingOutput);

        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(12L));
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(17L));
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(10L));
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(18L));
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(17L));
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(18L));

        assertThat(
                testingOutput.events,
                contains(new Watermark(12L), new Watermark(17L), new Watermark(18L)));
    }

    @Test
    public void becomingActiveEmitsStatus() {
        final CollectingDataOutput<Object> testingOutput = new CollectingDataOutput<>();
        final WatermarkToDataOutput wmOutput = new WatermarkToDataOutput(testingOutput);

        wmOutput.markIdle();
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(100L));

        assertThat(
                testingOutput.events,
                contains(StreamStatus.IDLE, StreamStatus.ACTIVE, new Watermark(100L)));
    }
}
