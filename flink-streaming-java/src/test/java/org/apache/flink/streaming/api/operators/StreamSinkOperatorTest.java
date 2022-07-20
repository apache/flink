/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

/** Tests for {@link StreamSink}. */
public class StreamSinkOperatorTest extends TestLogger {

    @Rule public ExpectedException expectedException = ExpectedException.none();

    /**
     * Verify that we can correctly query watermark, processing time and the timestamp from the
     * context.
     */
    @Test
    public void testTimeQuerying() throws Exception {

        BufferingQueryingSink<String> bufferingSink = new BufferingQueryingSink<>();

        StreamSink<String> operator = new StreamSink<>(bufferingSink);

        OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.open();

        testHarness.processWatermark(new Watermark(17));
        testHarness.setProcessingTime(12);
        testHarness.processElement(new StreamRecord<>("Hello", 12L));

        testHarness.processWatermark(new Watermark(42));
        testHarness.setProcessingTime(15);
        testHarness.processElement(new StreamRecord<>("Ciao", 13L));

        testHarness.processWatermark(new Watermark(42));
        testHarness.setProcessingTime(15);
        testHarness.processElement(new StreamRecord<>("Ciao"));

        assertThat(bufferingSink.data.size(), is(3));

        assertThat(
                bufferingSink.data,
                contains(
                        new Tuple4<>(17L, 12L, 12L, "Hello"),
                        new Tuple4<>(42L, 15L, 13L, "Ciao"),
                        new Tuple4<>(42L, 15L, null, "Ciao")));

        assertThat(bufferingSink.watermarks.size(), is(3));

        assertThat(
                bufferingSink.watermarks,
                contains(
                        new org.apache.flink.api.common.eventtime.Watermark(17L),
                        new org.apache.flink.api.common.eventtime.Watermark(42L),
                        new org.apache.flink.api.common.eventtime.Watermark(42L)));

        testHarness.close();
    }

    private static class BufferingQueryingSink<T> implements SinkFunction<T> {

        // watermark, processing-time, timestamp, event
        private final List<Tuple4<Long, Long, Long, T>> data;

        private final List<org.apache.flink.api.common.eventtime.Watermark> watermarks;

        public BufferingQueryingSink() {
            data = new ArrayList<>();
            watermarks = new ArrayList<>();
        }

        @Override
        public void invoke(T value, Context context) throws Exception {
            Long timestamp = context.timestamp();
            if (timestamp != null) {
                data.add(
                        new Tuple4<>(
                                context.currentWatermark(),
                                context.currentProcessingTime(),
                                context.timestamp(),
                                value));
            } else {
                data.add(
                        new Tuple4<>(
                                context.currentWatermark(),
                                context.currentProcessingTime(),
                                null,
                                value));
            }
        }

        @Override
        public void writeWatermark(org.apache.flink.api.common.eventtime.Watermark watermark)
                throws Exception {
            watermarks.add(watermark);
        }
    }
}
