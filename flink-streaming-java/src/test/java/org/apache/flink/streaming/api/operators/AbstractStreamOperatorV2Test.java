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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

/** Tests for the facilities provided by {@link AbstractStreamOperatorV2}. */
public class AbstractStreamOperatorV2Test extends AbstractStreamOperatorTest {
    @Override
    protected KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
            createTestHarness(int maxParalelism, int numSubtasks, int subtaskIndex)
                    throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new TestOperatorFactory(),
                new TestKeySelector(),
                BasicTypeInfo.INT_TYPE_INFO,
                maxParalelism,
                numSubtasks,
                subtaskIndex);
    }

    private static class TestOperatorFactory extends AbstractStreamOperatorFactory<String> {
        @Override
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            return (T) new SingleInputTestOperator(parameters);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return SingleInputTestOperator.class;
        }
    }

    @Test
    public void testIdleWatermarkHandling() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        try (KeyedMultiInputStreamOperatorTestHarness<Integer, Long> testHarness =
                new KeyedMultiInputStreamOperatorTestHarness<>(
                        new WatermarkTestingOperatorFactory(), BasicTypeInfo.INT_TYPE_INFO)) {
            testHarness.setKeySelector(0, dummyKeySelector);
            testHarness.setKeySelector(1, dummyKeySelector);
            testHarness.setKeySelector(2, dummyKeySelector);
            testHarness.setup();
            testHarness.open();
            testHarness.processElement(0, new StreamRecord<>(1L, 1L));
            testHarness.processElement(0, new StreamRecord<>(3L, 3L));
            testHarness.processElement(0, new StreamRecord<>(4L, 4L));
            testHarness.processWatermark(0, new Watermark(1L));
            assertThat(testHarness.getOutput(), empty());

            testHarness.processWatermarkStatus(1, WatermarkStatus.IDLE);
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
            testHarness.processWatermarkStatus(2, WatermarkStatus.IDLE);
            expectedOutput.add(new StreamRecord<>(1L));
            expectedOutput.add(new Watermark(1L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());

            testHarness.processWatermark(0, new Watermark(3L));
            expectedOutput.add(new StreamRecord<>(3L));
            expectedOutput.add(new Watermark(3L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());

            testHarness.processWatermarkStatus(1, WatermarkStatus.ACTIVE);
            // the other input is active now, we should not emit the watermark
            testHarness.processWatermark(0, new Watermark(4L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    public void testIdlenessForwarding() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        try (KeyedMultiInputStreamOperatorTestHarness<Integer, Long> testHarness =
                new KeyedMultiInputStreamOperatorTestHarness<>(
                        new WatermarkTestingOperatorFactory(), BasicTypeInfo.INT_TYPE_INFO)) {
            testHarness.setup();
            testHarness.open();

            testHarness.processWatermarkStatus(0, WatermarkStatus.IDLE);
            testHarness.processWatermarkStatus(1, WatermarkStatus.IDLE);
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
            testHarness.processWatermarkStatus(2, WatermarkStatus.IDLE);
            expectedOutput.add(WatermarkStatus.IDLE);
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    private static class WatermarkTestingOperatorFactory
            extends AbstractStreamOperatorFactory<Long> {
        @Override
        public <T extends StreamOperator<Long>> T createStreamOperator(
                StreamOperatorParameters<Long> parameters) {
            return (T) new WatermarkTestingOperator(parameters);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return WatermarkTestingOperator.class;
        }
    }

    private static class WatermarkTestingOperator extends AbstractStreamOperatorV2<Long>
            implements MultipleInputStreamOperator<Long>, Triggerable<Integer, VoidNamespace> {

        private transient InternalTimerService<VoidNamespace> timerService;

        public WatermarkTestingOperator(StreamOperatorParameters<Long> parameters) {
            super(parameters, 3);
        }

        @Override
        public void open() throws Exception {
            super.open();

            this.timerService =
                    getInternalTimerService("test-timers", VoidNamespaceSerializer.INSTANCE, this);
        }

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            output.collect(new StreamRecord<>(timer.getTimestamp()));
        }

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer)
                throws Exception {}

        private Input<Long> createInput(int idx) {
            return new AbstractInput<Long, Long>(this, idx) {
                @Override
                public void processElement(StreamRecord<Long> element) throws Exception {
                    timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, element.getValue());
                }
            };
        }

        @Override
        public List<Input> getInputs() {
            return Arrays.asList(createInput(1), createInput(2), createInput(3));
        }
    }

    /**
     * Testing operator that can respond to commands by either setting/deleting state, emitting
     * state or setting timers.
     */
    private static class SingleInputTestOperator extends AbstractStreamOperatorV2<String>
            implements MultipleInputStreamOperator<String>, Triggerable<Integer, VoidNamespace> {

        private static final long serialVersionUID = 1L;

        private transient InternalTimerService<VoidNamespace> timerService;

        private final ValueStateDescriptor<String> stateDescriptor =
                new ValueStateDescriptor<>("state", StringSerializer.INSTANCE);

        public SingleInputTestOperator(StreamOperatorParameters<String> parameters) {
            super(parameters, 1);
        }

        @Override
        public void open() throws Exception {
            super.open();

            this.timerService =
                    getInternalTimerService("test-timers", VoidNamespaceSerializer.INSTANCE, this);
        }

        @Override
        public List<Input> getInputs() {
            return Collections.singletonList(
                    new AbstractInput<Tuple2<Integer, String>, String>(this, 1) {
                        @Override
                        public void processElement(StreamRecord<Tuple2<Integer, String>> element)
                                throws Exception {
                            String[] command = element.getValue().f1.split(":");
                            switch (command[0]) {
                                case "SET_STATE":
                                    getPartitionedState(stateDescriptor).update(command[1]);
                                    break;
                                case "DELETE_STATE":
                                    getPartitionedState(stateDescriptor).clear();
                                    break;
                                case "SET_EVENT_TIME_TIMER":
                                    timerService.registerEventTimeTimer(
                                            VoidNamespace.INSTANCE, Long.parseLong(command[1]));
                                    break;
                                case "SET_PROC_TIME_TIMER":
                                    timerService.registerProcessingTimeTimer(
                                            VoidNamespace.INSTANCE, Long.parseLong(command[1]));
                                    break;
                                case "EMIT_STATE":
                                    String stateValue =
                                            getPartitionedState(stateDescriptor).value();
                                    output.collect(
                                            new StreamRecord<>(
                                                    "ON_ELEMENT:"
                                                            + element.getValue().f0
                                                            + ":"
                                                            + stateValue));
                                    break;
                                default:
                                    throw new IllegalArgumentException();
                            }
                        }
                    });
        }

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            String stateValue = getPartitionedState(stateDescriptor).value();
            output.collect(new StreamRecord<>("ON_EVENT_TIME:" + stateValue));
        }

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            String stateValue = getPartitionedState(stateDescriptor).value();
            output.collect(new StreamRecord<>("ON_PROC_TIME:" + stateValue));
        }
    }
}
