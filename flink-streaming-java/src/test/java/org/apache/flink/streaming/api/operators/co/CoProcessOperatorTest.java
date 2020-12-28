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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

/** Tests {@link CoProcessOperator}. */
public class CoProcessOperatorTest extends TestLogger {

    @Test
    public void testTimestampAndWatermarkQuerying() throws Exception {

        CoProcessOperator<Integer, String, String> operator =
                new CoProcessOperator<>(new WatermarkQueryingProcessFunction());

        TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
                new TwoInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.open();

        testHarness.processWatermark1(new Watermark(17));
        testHarness.processWatermark2(new Watermark(17));
        testHarness.processElement1(new StreamRecord<>(5, 12L));

        testHarness.processWatermark1(new Watermark(42));
        testHarness.processWatermark2(new Watermark(42));
        testHarness.processElement2(new StreamRecord<>("6", 13L));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new Watermark(17L));
        expectedOutput.add(new StreamRecord<>("5WM:17 TS:12", 12L));
        expectedOutput.add(new Watermark(42L));
        expectedOutput.add(new StreamRecord<>("6WM:42 TS:13", 13L));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testTimestampAndProcessingTimeQuerying() throws Exception {

        CoProcessOperator<Integer, String, String> operator =
                new CoProcessOperator<>(new ProcessingTimeQueryingProcessFunction());

        TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
                new TwoInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.open();

        testHarness.setProcessingTime(17);
        testHarness.processElement1(new StreamRecord<>(5));

        testHarness.setProcessingTime(42);
        testHarness.processElement2(new StreamRecord<>("6"));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>("5PT:17 TS:null"));
        expectedOutput.add(new StreamRecord<>("6PT:42 TS:null"));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    private static class WatermarkQueryingProcessFunction
            extends CoProcessFunction<Integer, String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement1(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect(
                    value
                            + "WM:"
                            + ctx.timerService().currentWatermark()
                            + " TS:"
                            + ctx.timestamp());
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect(
                    value
                            + "WM:"
                            + ctx.timerService().currentWatermark()
                            + " TS:"
                            + ctx.timestamp());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {}
    }

    private static class ProcessingTimeQueryingProcessFunction
            extends CoProcessFunction<Integer, String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement1(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect(
                    value
                            + "PT:"
                            + ctx.timerService().currentProcessingTime()
                            + " TS:"
                            + ctx.timestamp());
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect(
                    value
                            + "PT:"
                            + ctx.timerService().currentProcessingTime()
                            + " TS:"
                            + ctx.timestamp());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {}
    }
}
