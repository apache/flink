/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.async;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test cases for {@link RichAsyncFunction}. */
class RichAsyncFunctionTest {

    /**
     * Test the set of iteration runtime context methods in the context of a {@link
     * RichAsyncFunction}.
     */
    @Test
    void testIterationRuntimeContext() {
        RichAsyncFunction<Integer, Integer> function =
                new RichAsyncFunction<Integer, Integer>() {
                    private static final long serialVersionUID = -2023923961609455894L;

                    @Override
                    public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture)
                            throws Exception {
                        // no op
                    }
                };

        int superstepNumber = 42;

        IterationRuntimeContext mockedIterationRuntimeContext = mock(IterationRuntimeContext.class);
        when(mockedIterationRuntimeContext.getSuperstepNumber()).thenReturn(superstepNumber);
        function.setRuntimeContext(mockedIterationRuntimeContext);

        IterationRuntimeContext iterationRuntimeContext = function.getIterationRuntimeContext();

        assertThat(iterationRuntimeContext.getSuperstepNumber()).isEqualTo(superstepNumber);

        assertThatThrownBy(() -> iterationRuntimeContext.getIterationAggregator("foobar"))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> iterationRuntimeContext.getPreviousIterationAggregate("foobar"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** Test the set of runtime context methods in the context of a {@link RichAsyncFunction}. */
    @Test
    void testRuntimeContext() {
        RichAsyncFunction<Integer, Integer> function =
                new RichAsyncFunction<Integer, Integer>() {
                    private static final long serialVersionUID = 1707630162838967972L;

                    @Override
                    public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture)
                            throws Exception {
                        // no op
                    }
                };

        final String taskName = "foobarTask";
        final OperatorMetricGroup metricGroup =
                UnregisteredMetricsGroup.createOperatorMetricGroup();
        final int numberOfParallelSubtasks = 43;
        final int indexOfSubtask = 42;
        final int attemptNumber = 1337;
        final String taskNameWithSubtask = "foobarTask (43/43)#1337";
        final Map<String, String> globalJobParameters = new HashMap<>();
        globalJobParameters.put("k1", "v1");
        final ClassLoader userCodeClassLoader = mock(ClassLoader.class);
        final boolean isObjectReused = true;

        RuntimeContext mockedRuntimeContext = mock(RuntimeContext.class);
        TaskInfo mockedTaskInfo = mock(TaskInfo.class);
        when(mockedTaskInfo.getTaskName()).thenReturn(taskName);
        when(mockedTaskInfo.getNumberOfParallelSubtasks()).thenReturn(numberOfParallelSubtasks);
        when(mockedTaskInfo.getIndexOfThisSubtask()).thenReturn(indexOfSubtask);
        when(mockedTaskInfo.getAttemptNumber()).thenReturn(attemptNumber);
        when(mockedTaskInfo.getTaskNameWithSubtasks()).thenReturn(taskNameWithSubtask);
        when(mockedRuntimeContext.getTaskInfo()).thenReturn(mockedTaskInfo);
        when(mockedRuntimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(mockedRuntimeContext.getGlobalJobParameters()).thenReturn(globalJobParameters);
        when(mockedRuntimeContext.isObjectReuseEnabled()).thenReturn(isObjectReused);
        when(mockedRuntimeContext.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);

        function.setRuntimeContext(mockedRuntimeContext);

        RuntimeContext runtimeContext = function.getRuntimeContext();

        assertThat(runtimeContext.getTaskInfo().getTaskName()).isEqualTo(taskName);
        assertThat(runtimeContext.getMetricGroup()).isEqualTo(metricGroup);
        assertThat(runtimeContext.getTaskInfo().getNumberOfParallelSubtasks())
                .isEqualTo(numberOfParallelSubtasks);
        assertThat(runtimeContext.getTaskInfo().getIndexOfThisSubtask()).isEqualTo(indexOfSubtask);
        assertThat(runtimeContext.getTaskInfo().getAttemptNumber()).isEqualTo(attemptNumber);
        assertThat(runtimeContext.getTaskInfo().getTaskNameWithSubtasks())
                .isEqualTo(taskNameWithSubtask);
        assertThat(runtimeContext.getGlobalJobParameters()).isEqualTo(globalJobParameters);
        assertThat(runtimeContext.isObjectReuseEnabled()).isEqualTo(isObjectReused);
        assertThat(runtimeContext.getUserCodeClassLoader()).isEqualTo(userCodeClassLoader);

        assertThatThrownBy(runtimeContext::getDistributedCache)
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(
                        () ->
                                runtimeContext.getState(
                                        new ValueStateDescriptor<>("foobar", Integer.class, 42)))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(
                        () ->
                                runtimeContext.getListState(
                                        new ListStateDescriptor<>("foobar", Integer.class)))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(
                        () ->
                                runtimeContext.getReducingState(
                                        new ReducingStateDescriptor<>(
                                                "foobar",
                                                new ReduceFunction<Integer>() {
                                                    private static final long serialVersionUID =
                                                            2136425961884441050L;

                                                    @Override
                                                    public Integer reduce(
                                                            Integer value1, Integer value2) {
                                                        return value1;
                                                    }
                                                },
                                                Integer.class)))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(
                        () ->
                                runtimeContext.getAggregatingState(
                                        new AggregatingStateDescriptor<>(
                                                "foobar",
                                                new AggregateFunction<Integer, Integer, Integer>() {

                                                    @Override
                                                    public Integer createAccumulator() {
                                                        return null;
                                                    }

                                                    @Override
                                                    public Integer add(
                                                            Integer value, Integer accumulator) {
                                                        return null;
                                                    }

                                                    @Override
                                                    public Integer getResult(Integer accumulator) {
                                                        return null;
                                                    }

                                                    @Override
                                                    public Integer merge(Integer a, Integer b) {
                                                        return null;
                                                    }
                                                },
                                                Integer.class)))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(
                        () ->
                                runtimeContext.getMapState(
                                        new MapStateDescriptor<>(
                                                "foobar", Integer.class, String.class)))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(
                        () ->
                                runtimeContext.addAccumulator(
                                        "foobar",
                                        new Accumulator<Integer, Integer>() {
                                            private static final long serialVersionUID =
                                                    -4673320336846482358L;

                                            @Override
                                            public void add(Integer value) {
                                                // no op
                                            }

                                            @Override
                                            public Integer getLocalValue() {
                                                return null;
                                            }

                                            @Override
                                            public void resetLocal() {}

                                            @Override
                                            public void merge(
                                                    Accumulator<Integer, Integer> other) {}

                                            @Override
                                            public Accumulator<Integer, Integer> clone() {
                                                return null;
                                            }
                                        }))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> runtimeContext.getAccumulator("foobar"))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> runtimeContext.getIntCounter("foobar"))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> runtimeContext.getLongCounter("foobar"))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> runtimeContext.getDoubleCounter("foobar"))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> runtimeContext.getHistogram("foobar"))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> runtimeContext.getBroadcastVariable("foobar"))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> runtimeContext.hasBroadcastVariable("foobar"))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> runtimeContext.getBroadcastVariable("foobar"))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(
                        () ->
                                runtimeContext.getBroadcastVariableWithInitializer(
                                        "foobar", data -> null))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
