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

package org.apache.flink.datastream.impl.extension.window.operators;

import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.window.context.OneInputWindowContext;
import org.apache.flink.datastream.api.extension.window.function.OneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.datastream.impl.extension.window.function.InternalOneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.utils.WindowUtils;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.asyncprocessing.AsyncKeyedOneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link OneInputWindowProcessOperator}. */
public class OneInputWindowProcessOperatorTest {

    @Test
    @SuppressWarnings("unchecked")
    void testMergingSessionWindowOnRecordReceivesActualMergedWindowBounds() throws Exception {
        // Capture pre call : [startTime, endTime] pairs observed inside onRecords
        final List<long[]> observedBoundsInOnRecord = new ArrayList<>();

        // Capture the single start-time reported by OnTrigger after the window fires
        final List<Long> startTimesSeenOnTrigger = new ArrayList<>();

        // Session gap= 10 s , event-time (MergingWindowAssigner path)

        WindowStrategy sessionStrategy =
                WindowStrategy.session(Duration.ofMillis(10_000), WindowStrategy.EVENT_TIME);

        WindowAssigner<Integer, TimeWindow> assigner =
                (WindowAssigner<Integer, TimeWindow>)
                        WindowUtils.createWindowAssigner(sessionStrategy);

        InternalOneInputWindowStreamProcessFunction<Integer, String, TimeWindow> internalFn =
                new InternalOneInputWindowStreamProcessFunction<>(
                        new OneInputWindowStreamProcessFunction<Integer, String>() {
                            @Override
                            public void onRecord(
                                    Integer record,
                                    Collector<String> output,
                                    PartitionedContext<String> ctx,
                                    OneInputWindowContext<Integer> windowContext)
                                    throws Exception {

                                // Record what the context reports so that the test can assert
                                // later.
                                observedBoundsInOnRecord.add(
                                        new long[] {
                                            windowContext.getStartTime(), windowContext.getEndTime()
                                        });
                                windowContext.putRecord(record);
                            }

                            public void onTrigger(
                                    Collector<String> output,
                                    PartitionedContext<String> ctx,
                                    OneInputWindowContext<Integer> windowContext)
                                    throws Exception {
                                startTimesSeenOnTrigger.add(windowContext.getStartTime());
                                output.collect(
                                        "start="
                                                + windowContext.getStartTime()
                                                + ",end="
                                                + windowContext.getEndTime());
                            }
                        },
                        assigner,
                        assigner.getDefaultTrigger(),
                        0L,
                        sessionStrategy);

        ListStateDescriptor<Integer> stateDesc =
                new ListStateDescriptor<>("window-state", Types.INT);

        OneInputWindowProcessOperator<Integer, Integer, String, TimeWindow> operator =
                new OneInputWindowProcessOperator<>(
                        internalFn,
                        assigner,
                        internalFn.getTrigger(),
                        assigner.getWindowSerializer(
                                new org.apache.flink.api.common.ExecutionConfig()),
                        stateDesc,
                        0L);

        try (AsyncKeyedOneInputStreamOperatorTestHarness<Integer, Integer, String> testHarness =
                AsyncKeyedOneInputStreamOperatorTestHarness.create(
                        operator, value -> value, Types.INT)) {
            testHarness.open();
            //
            // Record 1 (ts = 0 ) creates new session new window [0, 10_000).
            // No Merge , window = actual window
            testHarness.processElements(Collections.singleton(new StreamRecord<Integer>(1, 0)));

            assertThat(observedBoundsInOnRecord).as("one record call for record 1").hasSize(1);
            assertThat(observedBoundsInOnRecord.get(0)[0])
                    .as("record 1: getStartTime() should be window start 0 ")
                    .isEqualTo(0L);

            assertThat(observedBoundsInOnRecord.get(0)[1])
                    .as("record 1: getEndTime() should be window end 10000 ")
                    .isEqualTo(10000L);

            //
            // Record 2 (ts = 5 000) candidate window[5 000, 15 000).
            // merges with window 1 [0, 10000)
            // actual window [0, 15000)
            testHarness.processElements(Collections.singleton(new StreamRecord<Integer>(1, 5_000)));
            assertThat(observedBoundsInOnRecord).as("two onRecordsCalls in total").hasSize(2);
            assertThat(observedBoundsInOnRecord.get(1)[0])
                    .as("record 2 merged window start time ")
                    .isEqualTo(0L);
            assertThat(observedBoundsInOnRecord.get(1)[1])
                    .as("record 2 merged window endTime")
                    .isEqualTo(15_000L);
            testHarness.processWatermark(new Watermark(20000L));
            Collection<StreamRecord<String>> outputs = testHarness.getRecordOutput();
            assertThat(outputs).as("exactly one window should fire").hasSize(1);
            assertThat(outputs.iterator().next().getValue())
                    .as("onTrigger Should see correct merged window [0,15_000)")
                    .isEqualTo("start=0,end=15000");
        }
    }
}
