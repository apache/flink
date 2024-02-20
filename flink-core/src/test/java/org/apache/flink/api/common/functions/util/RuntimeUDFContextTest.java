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

package org.apache.flink.api.common.functions.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the {@link RuntimeUDFContext}. */
class RuntimeUDFContextTest {

    private final TaskInfo taskInfo = new TaskInfoImpl("test name", 3, 1, 3, 0);

    @Test
    void testBroadcastVariableNotFound() {
        RuntimeUDFContext ctx =
                new RuntimeUDFContext(
                        taskInfo,
                        getClass().getClassLoader(),
                        new ExecutionConfig(),
                        new HashMap<>(),
                        new HashMap<>(),
                        UnregisteredMetricsGroup.createOperatorMetricGroup());

        assertThat(ctx.hasBroadcastVariable("some name")).isFalse();

        assertThatThrownBy(() -> ctx.getBroadcastVariable("some name"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("some name");

        assertThatThrownBy(() -> ctx.getBroadcastVariableWithInitializer("some name", data -> null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("some name");
    }

    @Test
    void testBroadcastVariableSimple() {
        RuntimeUDFContext ctx =
                new RuntimeUDFContext(
                        taskInfo,
                        getClass().getClassLoader(),
                        new ExecutionConfig(),
                        new HashMap<>(),
                        new HashMap<>(),
                        UnregisteredMetricsGroup.createOperatorMetricGroup());

        ctx.setBroadcastVariable("name1", Arrays.asList(1, 2, 3, 4));
        ctx.setBroadcastVariable("name2", Arrays.asList(1.0, 2.0, 3.0, 4.0));

        assertThat(ctx.hasBroadcastVariable("name1")).isTrue();
        assertThat(ctx.hasBroadcastVariable("name2")).isTrue();

        List<Integer> list1 = ctx.getBroadcastVariable("name1");
        List<Double> list2 = ctx.getBroadcastVariable("name2");

        assertThat(list1).isEqualTo(Arrays.asList(1, 2, 3, 4));
        assertThat(list2).isEqualTo(Arrays.asList(1.0, 2.0, 3.0, 4.0));

        // access again
        List<Integer> list3 = ctx.getBroadcastVariable("name1");
        List<Double> list4 = ctx.getBroadcastVariable("name2");

        assertThat(list3).isEqualTo(Arrays.asList(1, 2, 3, 4));
        assertThat(list4).isEqualTo(Arrays.asList(1.0, 2.0, 3.0, 4.0));

        // and again ;-)
        List<Integer> list5 = ctx.getBroadcastVariable("name1");
        List<Double> list6 = ctx.getBroadcastVariable("name2");

        assertThat(list5).isEqualTo(Arrays.asList(1, 2, 3, 4));
        assertThat(list6).isEqualTo(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    }

    @Test
    void testBroadcastVariableWithInitializer() {
        RuntimeUDFContext ctx =
                new RuntimeUDFContext(
                        taskInfo,
                        getClass().getClassLoader(),
                        new ExecutionConfig(),
                        new HashMap<>(),
                        new HashMap<>(),
                        UnregisteredMetricsGroup.createOperatorMetricGroup());

        ctx.setBroadcastVariable("name", Arrays.asList(1, 2, 3, 4));

        // access it the first time with an initializer
        List<Double> list =
                ctx.getBroadcastVariableWithInitializer("name", new ConvertingInitializer());
        assertThat(list).isEqualTo(Arrays.asList(1.0, 2.0, 3.0, 4.0));

        // access it the second time with an initializer (which might not get executed)
        List<Double> list2 =
                ctx.getBroadcastVariableWithInitializer("name", new ConvertingInitializer());
        assertThat(list2).isEqualTo(Arrays.asList(1.0, 2.0, 3.0, 4.0));

        // access it the third time without an initializer (should work by "chance", because the
        // result is a list)
        List<Double> list3 = ctx.getBroadcastVariable("name");
        assertThat(list3).isEqualTo(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    }

    @Test
    void testResetBroadcastVariableWithInitializer() {
        RuntimeUDFContext ctx =
                new RuntimeUDFContext(
                        taskInfo,
                        getClass().getClassLoader(),
                        new ExecutionConfig(),
                        new HashMap<>(),
                        new HashMap<>(),
                        UnregisteredMetricsGroup.createOperatorMetricGroup());

        ctx.setBroadcastVariable("name", Arrays.asList(1, 2, 3, 4));

        // access it the first time with an initializer
        List<Double> list =
                ctx.getBroadcastVariableWithInitializer("name", new ConvertingInitializer());
        assertThat(list).isEqualTo(Arrays.asList(1.0, 2.0, 3.0, 4.0));

        // set it again to something different
        ctx.setBroadcastVariable("name", Arrays.asList(2, 3, 4, 5));

        List<Double> list2 =
                ctx.getBroadcastVariableWithInitializer("name", new ConvertingInitializer());
        assertThat(list2).isEqualTo(Arrays.asList(2.0, 3.0, 4.0, 5.0));
    }

    @Test
    void testBroadcastVariableWithInitializerAndMismatch() {
        RuntimeUDFContext ctx =
                new RuntimeUDFContext(
                        taskInfo,
                        getClass().getClassLoader(),
                        new ExecutionConfig(),
                        new HashMap<>(),
                        new HashMap<>(),
                        UnregisteredMetricsGroup.createOperatorMetricGroup());

        ctx.setBroadcastVariable("name", Arrays.asList(1, 2, 3, 4));

        // access it the first time with an initializer
        int sum = ctx.getBroadcastVariableWithInitializer("name", new SumInitializer());
        assertThat(sum).isEqualTo(10);

        // access it the second time with no initializer -> should fail due to type mismatch
        assertThatThrownBy(() -> ctx.getBroadcastVariable("name"))
                .isInstanceOf(IllegalStateException.class);
    }

    // --------------------------------------------------------------------------------------------

    private static final class ConvertingInitializer
            implements BroadcastVariableInitializer<Integer, List<Double>> {
        @Override
        public List<Double> initializeBroadcastVariable(Iterable<Integer> data) {
            List<Double> list = new ArrayList<>();

            for (Integer i : data) {
                list.add(i.doubleValue());
            }
            return list;
        }
    }

    private static final class SumInitializer
            implements BroadcastVariableInitializer<Integer, Integer> {
        @Override
        public Integer initializeBroadcastVariable(Iterable<Integer> data) {
            int sum = 0;

            for (Integer i : data) {
                sum += i;
            }
            return sum;
        }
    }
}
