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

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/** Tests for the {@link CoBroadcastWithNonKeyedOperator}. */
public class CoBroadcastWithNonKeyedOperatorTest {

    private static final MapStateDescriptor<String, Integer> STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "broadcast-state", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

    private static final MapStateDescriptor<Integer, String> STATE_DESCRIPTOR_A =
            new MapStateDescriptor<>(
                    "broadcast-state-A",
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

    @Test
    public void testMultiStateSupport() throws Exception {
        try (TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness =
                getInitializedTestHarness(
                        new FunctionWithMultipleStates(), STATE_DESCRIPTOR, STATE_DESCRIPTOR_A)) {
            testHarness.processElement2(new StreamRecord<>(5, 12L));
            testHarness.processElement2(new StreamRecord<>(6, 13L));

            testHarness.processElement1(new StreamRecord<>("9", 15L));

            Queue<Object> expectedBr = new ConcurrentLinkedQueue<>();
            expectedBr.add(new StreamRecord<>("9:key.6->6", 15L));
            expectedBr.add(new StreamRecord<>("9:key.5->5", 15L));
            expectedBr.add(new StreamRecord<>("9:5->value.5", 15L));
            expectedBr.add(new StreamRecord<>("9:6->value.6", 15L));

            TestHarnessUtil.assertOutputEquals(
                    "Wrong Side Output", expectedBr, testHarness.getOutput());
        }
    }

    /** {@link BroadcastProcessFunction} that puts elements on multiple broadcast states. */
    private static class FunctionWithMultipleStates
            extends BroadcastProcessFunction<String, Integer, String> {

        private static final long serialVersionUID = 7496674620398203933L;

        @Override
        public void processBroadcastElement(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            ctx.getBroadcastState(STATE_DESCRIPTOR).put("key." + value, value);
            ctx.getBroadcastState(STATE_DESCRIPTOR_A).put(value, "value." + value);
        }

        @Override
        public void processElement(String value, ReadOnlyContext ctx, Collector<String> out)
                throws Exception {
            for (Map.Entry<String, Integer> entry :
                    ctx.getBroadcastState(STATE_DESCRIPTOR).immutableEntries()) {
                out.collect(value + ":" + entry.getKey() + "->" + entry.getValue());
            }

            for (Map.Entry<Integer, String> entry :
                    ctx.getBroadcastState(STATE_DESCRIPTOR_A).immutableEntries()) {
                out.collect(value + ":" + entry.getKey() + "->" + entry.getValue());
            }
        }
    }

    @Test
    public void testBroadcastState() throws Exception {

        final Set<String> keysToRegister = new HashSet<>();
        keysToRegister.add("test1");
        keysToRegister.add("test2");
        keysToRegister.add("test3");

        try (TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness =
                getInitializedTestHarness(new TestFunction(keysToRegister), STATE_DESCRIPTOR)) {
            testHarness.processWatermark1(new Watermark(10L));
            testHarness.processWatermark2(new Watermark(10L));
            testHarness.processElement2(new StreamRecord<>(5, 12L));

            testHarness.processWatermark1(new Watermark(40L));
            testHarness.processWatermark2(new Watermark(40L));
            testHarness.processElement1(new StreamRecord<>("6", 13L));
            testHarness.processElement1(new StreamRecord<>("6", 15L));

            testHarness.processWatermark1(new Watermark(50L));
            testHarness.processWatermark2(new Watermark(50L));

            Queue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

            expectedOutput.add(new Watermark(10L));
            expectedOutput.add(new StreamRecord<>("5WM:10 TS:12", 12L));
            expectedOutput.add(new Watermark(40L));
            expectedOutput.add(new StreamRecord<>("6WM:40 TS:13", 13L));
            expectedOutput.add(new StreamRecord<>("6WM:40 TS:15", 15L));
            expectedOutput.add(new Watermark(50L));

            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct.", expectedOutput, testHarness.getOutput());
        }
    }

    private static class TestFunction extends BroadcastProcessFunction<String, Integer, String> {

        private static final long serialVersionUID = 7496674620398203933L;

        private final Set<String> keysToRegister;

        TestFunction(Set<String> keysToRegister) {
            this.keysToRegister = Preconditions.checkNotNull(keysToRegister);
        }

        @Override
        public void processBroadcastElement(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            // put an element in the broadcast state
            for (String k : keysToRegister) {
                ctx.getBroadcastState(STATE_DESCRIPTOR).put(k, value);
            }
            out.collect(value + "WM:" + ctx.currentWatermark() + " TS:" + ctx.timestamp());
        }

        @Override
        public void processElement(String value, ReadOnlyContext ctx, Collector<String> out)
                throws Exception {
            Set<String> retrievedKeySet = new HashSet<>();
            for (Map.Entry<String, Integer> entry :
                    ctx.getBroadcastState(STATE_DESCRIPTOR).immutableEntries()) {
                retrievedKeySet.add(entry.getKey());
            }

            Assert.assertEquals(keysToRegister, retrievedKeySet);

            out.collect(value + "WM:" + ctx.currentWatermark() + " TS:" + ctx.timestamp());
        }
    }

    @Test
    public void testSideOutput() throws Exception {
        try (TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness =
                getInitializedTestHarness(new FunctionWithSideOutput(), STATE_DESCRIPTOR)) {

            testHarness.processWatermark1(new Watermark(10L));
            testHarness.processWatermark2(new Watermark(10L));
            testHarness.processElement2(new StreamRecord<>(5, 12L));

            testHarness.processWatermark1(new Watermark(40L));
            testHarness.processWatermark2(new Watermark(40L));
            testHarness.processElement1(new StreamRecord<>("6", 13L));
            testHarness.processElement1(new StreamRecord<>("6", 15L));

            testHarness.processWatermark1(new Watermark(50L));
            testHarness.processWatermark2(new Watermark(50L));

            ConcurrentLinkedQueue<StreamRecord<String>> expectedBr = new ConcurrentLinkedQueue<>();
            expectedBr.add(new StreamRecord<>("BR:5 WM:10 TS:12", 12L));

            ConcurrentLinkedQueue<StreamRecord<String>> expectedNonBr =
                    new ConcurrentLinkedQueue<>();
            expectedNonBr.add(new StreamRecord<>("NON-BR:6 WM:40 TS:13", 13L));
            expectedNonBr.add(new StreamRecord<>("NON-BR:6 WM:40 TS:15", 15L));

            ConcurrentLinkedQueue<StreamRecord<String>> brSideOutput =
                    testHarness.getSideOutput(FunctionWithSideOutput.BROADCAST_TAG);
            ConcurrentLinkedQueue<StreamRecord<String>> nonBrSideOutput =
                    testHarness.getSideOutput(FunctionWithSideOutput.NON_BROADCAST_TAG);

            TestHarnessUtil.assertOutputEquals("Wrong Side Output", expectedBr, brSideOutput);
            TestHarnessUtil.assertOutputEquals("Wrong Side Output", expectedNonBr, nonBrSideOutput);
        }
    }

    /** {@link BroadcastProcessFunction} that emits elements on side outputs. */
    private static class FunctionWithSideOutput
            extends BroadcastProcessFunction<String, Integer, String> {

        private static final long serialVersionUID = 7496674620398203933L;

        static final OutputTag<String> BROADCAST_TAG =
                new OutputTag<String>("br-out") {
                    private static final long serialVersionUID = 8037335313997479800L;
                };

        static final OutputTag<String> NON_BROADCAST_TAG =
                new OutputTag<String>("non-br-out") {
                    private static final long serialVersionUID = -1092362442658548175L;
                };

        @Override
        public void processBroadcastElement(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            ctx.output(
                    BROADCAST_TAG,
                    "BR:" + value + " WM:" + ctx.currentWatermark() + " TS:" + ctx.timestamp());
        }

        @Override
        public void processElement(String value, ReadOnlyContext ctx, Collector<String> out)
                throws Exception {
            ctx.output(
                    NON_BROADCAST_TAG,
                    "NON-BR:" + value + " WM:" + ctx.currentWatermark() + " TS:" + ctx.timestamp());
        }
    }

    @Test
    public void testScaleUp() throws Exception {
        final Set<String> keysToRegister = new HashSet<>();
        keysToRegister.add("test1");
        keysToRegister.add("test2");
        keysToRegister.add("test3");

        final OperatorSubtaskState mergedSnapshot;

        try (TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness1 =
                        getInitializedTestHarness(
                                new TestFunctionWithOutput(keysToRegister),
                                10,
                                2,
                                0,
                                STATE_DESCRIPTOR);
                TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness2 =
                        getInitializedTestHarness(
                                new TestFunctionWithOutput(keysToRegister),
                                10,
                                2,
                                1,
                                STATE_DESCRIPTOR)) {
            // make sure all operators have the same state
            testHarness1.processElement2(new StreamRecord<>(3));
            testHarness2.processElement2(new StreamRecord<>(3));

            mergedSnapshot =
                    AbstractStreamOperatorTestHarness.repackageState(
                            testHarness1.snapshot(0L, 0L), testHarness2.snapshot(0L, 0L));
        }

        final Set<String> expected = new HashSet<>(3);
        expected.add("test1=3");
        expected.add("test2=3");
        expected.add("test3=3");

        final OperatorSubtaskState initState1 = repartitionInitState(mergedSnapshot, 10, 2, 3, 0);
        final OperatorSubtaskState initState2 = repartitionInitState(mergedSnapshot, 10, 2, 3, 1);
        final OperatorSubtaskState initState3 = repartitionInitState(mergedSnapshot, 10, 2, 3, 2);

        try (TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness1 =
                        getInitializedTestHarness(
                                new TestFunctionWithOutput(keysToRegister),
                                10,
                                3,
                                0,
                                initState1,
                                STATE_DESCRIPTOR);
                TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness2 =
                        getInitializedTestHarness(
                                new TestFunctionWithOutput(keysToRegister),
                                10,
                                3,
                                1,
                                initState2,
                                STATE_DESCRIPTOR);
                TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness3 =
                        getInitializedTestHarness(
                                new TestFunctionWithOutput(keysToRegister),
                                10,
                                3,
                                2,
                                initState3,
                                STATE_DESCRIPTOR)) {
            testHarness1.processElement1(new StreamRecord<>("trigger"));
            testHarness2.processElement1(new StreamRecord<>("trigger"));
            testHarness3.processElement1(new StreamRecord<>("trigger"));

            Queue<?> output1 = testHarness1.getOutput();
            Queue<?> output2 = testHarness2.getOutput();
            Queue<?> output3 = testHarness3.getOutput();

            Assert.assertEquals(expected.size(), output1.size());
            for (Object o : output1) {
                StreamRecord<String> rec = (StreamRecord<String>) o;
                Assert.assertTrue(expected.contains(rec.getValue()));
            }

            Assert.assertEquals(expected.size(), output2.size());
            for (Object o : output2) {
                StreamRecord<String> rec = (StreamRecord<String>) o;
                Assert.assertTrue(expected.contains(rec.getValue()));
            }

            Assert.assertEquals(expected.size(), output3.size());
            for (Object o : output3) {
                StreamRecord<String> rec = (StreamRecord<String>) o;
                Assert.assertTrue(expected.contains(rec.getValue()));
            }
        }
    }

    @Test
    public void testScaleDown() throws Exception {
        final Set<String> keysToRegister = new HashSet<>();
        keysToRegister.add("test1");
        keysToRegister.add("test2");
        keysToRegister.add("test3");

        final OperatorSubtaskState mergedSnapshot;

        try (TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness1 =
                        getInitializedTestHarness(
                                new TestFunctionWithOutput(keysToRegister),
                                10,
                                3,
                                0,
                                STATE_DESCRIPTOR);
                TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness2 =
                        getInitializedTestHarness(
                                new TestFunctionWithOutput(keysToRegister),
                                10,
                                3,
                                1,
                                STATE_DESCRIPTOR);
                TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness3 =
                        getInitializedTestHarness(
                                new TestFunctionWithOutput(keysToRegister),
                                10,
                                3,
                                2,
                                STATE_DESCRIPTOR)) {

            // make sure all operators have the same state
            testHarness1.processElement2(new StreamRecord<>(3));
            testHarness2.processElement2(new StreamRecord<>(3));
            testHarness3.processElement2(new StreamRecord<>(3));

            mergedSnapshot =
                    AbstractStreamOperatorTestHarness.repackageState(
                            testHarness1.snapshot(0L, 0L),
                            testHarness2.snapshot(0L, 0L),
                            testHarness3.snapshot(0L, 0L));
        }

        final Set<String> expected = new HashSet<>(3);
        expected.add("test1=3");
        expected.add("test2=3");
        expected.add("test3=3");

        final OperatorSubtaskState initState1 = repartitionInitState(mergedSnapshot, 10, 3, 2, 0);
        final OperatorSubtaskState initState2 = repartitionInitState(mergedSnapshot, 10, 3, 2, 1);

        try (TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness1 =
                        getInitializedTestHarness(
                                new TestFunctionWithOutput(keysToRegister),
                                10,
                                2,
                                0,
                                initState1,
                                STATE_DESCRIPTOR);
                TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness2 =
                        getInitializedTestHarness(
                                new TestFunctionWithOutput(keysToRegister),
                                10,
                                2,
                                1,
                                initState2,
                                STATE_DESCRIPTOR)) {
            testHarness1.processElement1(new StreamRecord<>("trigger"));
            testHarness2.processElement1(new StreamRecord<>("trigger"));

            Queue<?> output1 = testHarness1.getOutput();
            Queue<?> output2 = testHarness2.getOutput();

            Assert.assertEquals(expected.size(), output1.size());
            for (Object o : output1) {
                StreamRecord<String> rec = (StreamRecord<String>) o;
                Assert.assertTrue(expected.contains(rec.getValue()));
            }

            Assert.assertEquals(expected.size(), output2.size());
            for (Object o : output2) {
                StreamRecord<String> rec = (StreamRecord<String>) o;
                Assert.assertTrue(expected.contains(rec.getValue()));
            }
        }
    }

    private static class TestFunctionWithOutput
            extends BroadcastProcessFunction<String, Integer, String> {

        private static final long serialVersionUID = 7496674620398203933L;

        private final Set<String> keysToRegister;

        TestFunctionWithOutput(Set<String> keysToRegister) {
            this.keysToRegister = Preconditions.checkNotNull(keysToRegister);
        }

        @Override
        public void processBroadcastElement(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            // put an element in the broadcast state
            for (String k : keysToRegister) {
                ctx.getBroadcastState(STATE_DESCRIPTOR).put(k, value);
            }
        }

        @Override
        public void processElement(String value, ReadOnlyContext ctx, Collector<String> out)
                throws Exception {
            for (Map.Entry<String, Integer> entry :
                    ctx.getBroadcastState(STATE_DESCRIPTOR).immutableEntries()) {
                out.collect(entry.toString());
            }
        }
    }

    @Test
    public void testNoKeyedStateOnBroadcastSide() throws Exception {

        boolean exceptionThrown = false;

        try (TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness =
                getInitializedTestHarness(
                        new BroadcastProcessFunction<String, Integer, String>() {
                            private static final long serialVersionUID = -1725365436500098384L;

                            private final ValueStateDescriptor<String> valueState =
                                    new ValueStateDescriptor<>(
                                            "any", BasicTypeInfo.STRING_TYPE_INFO);

                            @Override
                            public void processBroadcastElement(
                                    Integer value, Context ctx, Collector<String> out)
                                    throws Exception {
                                getRuntimeContext()
                                        .getState(valueState)
                                        .value(); // this should fail
                            }

                            @Override
                            public void processElement(
                                    String value, ReadOnlyContext ctx, Collector<String> out)
                                    throws Exception {
                                // do nothing
                            }
                        })) {
            testHarness.processWatermark1(new Watermark(10L));
            testHarness.processWatermark2(new Watermark(10L));
            testHarness.processElement2(new StreamRecord<>(5, 12L));
        } catch (NullPointerException e) {
            Assert.assertEquals(
                    "Keyed state can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.",
                    e.getMessage());
            exceptionThrown = true;
        }

        if (!exceptionThrown) {
            Assert.fail("No exception thrown");
        }
    }

    @Test
    public void testNoKeyedStateOnNonBroadcastSide() throws Exception {

        boolean exceptionThrown = false;

        try (TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness =
                getInitializedTestHarness(
                        new BroadcastProcessFunction<String, Integer, String>() {
                            private static final long serialVersionUID = -1725365436500098384L;

                            private final ValueStateDescriptor<String> valueState =
                                    new ValueStateDescriptor<>(
                                            "any", BasicTypeInfo.STRING_TYPE_INFO);

                            @Override
                            public void processBroadcastElement(
                                    Integer value, Context ctx, Collector<String> out)
                                    throws Exception {
                                // do nothing
                            }

                            @Override
                            public void processElement(
                                    String value, ReadOnlyContext ctx, Collector<String> out)
                                    throws Exception {
                                getRuntimeContext()
                                        .getState(valueState)
                                        .value(); // this should fail
                            }
                        })) {
            testHarness.processWatermark1(new Watermark(10L));
            testHarness.processWatermark2(new Watermark(10L));
            testHarness.processElement1(new StreamRecord<>("5", 12L));
        } catch (NullPointerException e) {
            Assert.assertEquals(
                    "Keyed state can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.",
                    e.getMessage());
            exceptionThrown = true;
        }

        if (!exceptionThrown) {
            Assert.fail("No exception thrown");
        }
    }

    private static <IN1, IN2, OUT>
            TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> getInitializedTestHarness(
                    final BroadcastProcessFunction<IN1, IN2, OUT> function,
                    final MapStateDescriptor<?, ?>... descriptors)
                    throws Exception {

        return getInitializedTestHarness(function, 1, 1, 0, descriptors);
    }

    private static <IN1, IN2, OUT>
            TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> getInitializedTestHarness(
                    final BroadcastProcessFunction<IN1, IN2, OUT> function,
                    final int maxParallelism,
                    final int numTasks,
                    final int taskIdx,
                    final MapStateDescriptor<?, ?>... descriptors)
                    throws Exception {

        return getInitializedTestHarness(
                function, maxParallelism, numTasks, taskIdx, null, descriptors);
    }

    private static OperatorSubtaskState repartitionInitState(
            final OperatorSubtaskState initState,
            final int numKeyGroups,
            final int oldParallelism,
            final int newParallelism,
            final int subtaskIndex) {
        return AbstractStreamOperatorTestHarness.repartitionOperatorState(
                initState, numKeyGroups, oldParallelism, newParallelism, subtaskIndex);
    }

    private static <IN1, IN2, OUT>
            TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> getInitializedTestHarness(
                    final BroadcastProcessFunction<IN1, IN2, OUT> function,
                    final int maxParallelism,
                    final int numTasks,
                    final int taskIdx,
                    final OperatorSubtaskState initState,
                    final MapStateDescriptor<?, ?>... descriptors)
                    throws Exception {

        TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> testHarness =
                new TwoInputStreamOperatorTestHarness<>(
                        new CoBroadcastWithNonKeyedOperator<>(
                                Preconditions.checkNotNull(function), Arrays.asList(descriptors)),
                        maxParallelism,
                        numTasks,
                        taskIdx);
        testHarness.setup();
        testHarness.initializeState(initState);
        testHarness.open();

        return testHarness;
    }
}
