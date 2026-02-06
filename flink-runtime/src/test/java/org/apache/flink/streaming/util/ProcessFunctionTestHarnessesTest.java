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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProcessFunctionTestHarnesses}. */
class ProcessFunctionTestHarnessesTest {

    @Test
    void testHarnessForProcessFunction() throws Exception {
        ProcessFunction<Integer, Integer> function =
                new ProcessFunction<Integer, Integer>() {

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        out.collect(value);
                    }
                };
        OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(1, 10);

        assertThat(harness.extractOutputValues()).containsExactly(1);
    }

    @Test
    void testHarnessForKeyedProcessFunction() throws Exception {
        KeyedProcessFunction<Integer, Integer, Integer> function =
                new KeyedProcessFunction<Integer, Integer, Integer>() {
                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        out.collect(value);
                    }
                };
        OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                        function, x -> x, BasicTypeInfo.INT_TYPE_INFO);

        harness.processElement(1, 10);

        assertThat(harness.extractOutputValues()).containsExactly(1);
    }

    @Test
    void testHarnessForAsyncKeyedProcessFunction() throws Exception {
        KeyedProcessFunction<Integer, Integer, Integer> function =
                new KeyedProcessFunction<>() {

                    org.apache.flink.api.common.state.v2.ValueState<Integer> state;
                    org.apache.flink.api.common.state.v2.ValueStateDescriptor<Integer>
                            asyncStateDescriptor;

                    @Override
                    public void open(OpenContext openContext) {
                        asyncStateDescriptor =
                                new org.apache.flink.api.common.state.v2.ValueStateDescriptor<>(
                                        "state", Types.INT);
                        state =
                                ((StreamingRuntimeContext) getRuntimeContext())
                                        .getValueState(asyncStateDescriptor);
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        state.asyncUpdate(value + 1);
                        state.asyncValue().thenAccept(out::collect);
                    }
                };
        OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                ProcessFunctionTestHarnesses.forKeyedProcessFunctionWithStateV2(
                        function, x -> x, BasicTypeInfo.INT_TYPE_INFO);

        harness.processElement(1, 10);

        assertThat(harness.extractOutputValues()).containsExactly(2);
    }

    @Test
    void testHarnessForCoProcessFunction() throws Exception {
        CoProcessFunction<Integer, String, Integer> function =
                new CoProcessFunction<Integer, String, Integer>() {

                    @Override
                    public void processElement1(Integer value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        out.collect(value);
                    }

                    @Override
                    public void processElement2(String value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        out.collect(Integer.parseInt(value));
                    }
                };
        TwoInputStreamOperatorTestHarness<Integer, String, Integer> harness =
                ProcessFunctionTestHarnesses.forCoProcessFunction(function);

        harness.processElement2("0", 1);
        harness.processElement1(1, 10);

        assertThat(harness.extractOutputValues()).containsExactly(0, 1);
    }

    @Test
    void testHarnessForKeyedCoProcessFunction() throws Exception {
        KeyedCoProcessFunction<Integer, Integer, Integer, Integer> function =
                new KeyedCoProcessFunction<Integer, Integer, Integer, Integer>() {

                    @Override
                    public void processElement1(Integer value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        out.collect(value);
                    }

                    @Override
                    public void processElement2(Integer value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        out.collect(value);
                    }
                };

        KeyedTwoInputStreamOperatorTestHarness<Integer, Integer, Integer, Integer> harness =
                ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                        function, x -> x, x -> x, TypeInformation.of(Integer.class));

        harness.processElement1(0, 1);
        harness.processElement2(1, 10);

        assertThat(harness.extractOutputValues()).containsExactly(0, 1);
    }

    @Test
    void testHarnessForKeyedAsyncCoProcessFunction() throws Exception {
        KeyedCoProcessFunction<Integer, Integer, Integer, Integer> function =
                new KeyedCoProcessFunction<>() {

                    org.apache.flink.api.common.state.v2.ListState<Integer> state;
                    org.apache.flink.api.common.state.v2.ListStateDescriptor<Integer>
                            asyncStateDescriptor;

                    @Override
                    public void open(OpenContext openContext) {
                        asyncStateDescriptor =
                                new org.apache.flink.api.common.state.v2.ListStateDescriptor<>(
                                        "state", Types.INT);
                        state =
                                ((StreamingRuntimeContext) getRuntimeContext())
                                        .getListState(asyncStateDescriptor);
                    }

                    @Override
                    public void processElement2(Integer value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        state.asyncAdd(value);
                    }

                    @Override
                    public void processElement1(Integer value, Context ctx, Collector<Integer> out)
                            throws Exception {
                        state.asyncAdd(value);
                        state.asyncGet()
                                .thenAccept(
                                        itr -> {
                                            List<Integer> iterated = new ArrayList<>();
                                            itr.onNext(
                                                    value1 -> {
                                                        iterated.add(value1);
                                                    });
                                            out.collect(iterated.size());
                                        });
                    }
                };
        KeyedTwoInputStreamOperatorTestHarness<Integer, Integer, Integer, Integer> harness =
                ProcessFunctionTestHarnesses.forKeyedCoProcessFunctionWithStateV2(
                        function, x -> x, x -> x, TypeInformation.of(Integer.class));

        harness.processElement2(1, 1);
        harness.processElement1(1, 10);

        assertThat(harness.extractOutputValues()).containsExactly(2);
    }

    @Test
    void testHarnessForBroadcastProcessFunction() throws Exception {
        BroadcastProcessFunction<Integer, String, Integer> function =
                new BroadcastProcessFunction<Integer, String, Integer>() {

                    @Override
                    public void processElement(
                            Integer value, ReadOnlyContext ctx, Collector<Integer> out)
                            throws Exception {
                        out.collect(value);
                    }

                    @Override
                    public void processBroadcastElement(
                            String value, Context ctx, Collector<Integer> out) throws Exception {
                        out.collect(Integer.parseInt(value));
                    }
                };
        BroadcastOperatorTestHarness<Integer, String, Integer> harness =
                ProcessFunctionTestHarnesses.forBroadcastProcessFunction(function);

        harness.processBroadcastElement("0", 1);
        harness.processElement(1, 10);

        assertThat(harness.extractOutputValues()).containsExactly(0, 1);
    }

    @Test
    void testHarnessForKeyedBroadcastProcessFunction() throws Exception {
        KeyedBroadcastProcessFunction<Integer, Integer, String, Integer> function =
                new KeyedBroadcastProcessFunction<Integer, Integer, String, Integer>() {

                    @Override
                    public void processElement(
                            Integer value, ReadOnlyContext ctx, Collector<Integer> out)
                            throws Exception {
                        out.collect(value);
                    }

                    @Override
                    public void processBroadcastElement(
                            String value, Context ctx, Collector<Integer> out) throws Exception {
                        out.collect(Integer.parseInt(value));
                    }
                };

        final MapStateDescriptor<Integer, String> stateDescriptor =
                new MapStateDescriptor<>(
                        "keys", BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        KeyedBroadcastOperatorTestHarness<Integer, Integer, String, Integer> harness =
                ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(
                        function, x -> x, TypeInformation.of(Integer.class), stateDescriptor);

        harness.processBroadcastElement("0", 1);
        harness.processElement(1, 10);

        assertThat(harness.extractOutputValues()).containsExactly(0, 1);
    }
}
