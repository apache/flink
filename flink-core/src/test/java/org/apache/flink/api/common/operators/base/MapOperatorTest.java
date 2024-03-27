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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/** The test for map operator. */
public class MapOperatorTest implements java.io.Serializable {

    @Test
    void testMapPlain() throws Exception {
        final MapFunction<String, Integer> parser = Integer::parseInt;

        MapOperatorBase<String, Integer, MapFunction<String, Integer>> op =
                new MapOperatorBase<>(
                        parser,
                        new UnaryOperatorInformation<>(
                                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
                        "TestMapper");

        List<String> input = new ArrayList<>(asList("1", "2", "3", "4", "5", "6"));

        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.disableObjectReuse();
        List<Integer> resultMutableSafe = op.executeOnCollections(input, null, executionConfig);
        executionConfig.enableObjectReuse();
        List<Integer> resultRegular = op.executeOnCollections(input, null, executionConfig);

        assertThat(resultMutableSafe).isEqualTo(asList(1, 2, 3, 4, 5, 6));
        assertThat(resultRegular).isEqualTo(asList(1, 2, 3, 4, 5, 6));
    }

    @Test
    void testMapWithRuntimeContext() throws Exception {
        final String taskName = "Test Task";
        final AtomicBoolean opened = new AtomicBoolean();
        final AtomicBoolean closed = new AtomicBoolean();

        final MapFunction<String, Integer> parser =
                new RichMapFunction<String, Integer>() {

                    @Override
                    public void open(OpenContext openContext) {
                        opened.set(true);
                        RuntimeContext ctx = getRuntimeContext();
                        assertThat(ctx.getTaskInfo().getIndexOfThisSubtask()).isZero();
                        assertThat(ctx.getTaskInfo().getNumberOfParallelSubtasks()).isOne();
                        assertThat(ctx.getTaskInfo().getTaskName()).isEqualTo(taskName);
                    }

                    @Override
                    public Integer map(String value) {
                        return Integer.parseInt(value);
                    }

                    @Override
                    public void close() {
                        closed.set(true);
                    }
                };

        MapOperatorBase<String, Integer, MapFunction<String, Integer>> op =
                new MapOperatorBase<>(
                        parser,
                        new UnaryOperatorInformation<>(
                                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
                        taskName);

        List<String> input = new ArrayList<>(asList("1", "2", "3", "4", "5", "6"));
        final HashMap<String, Accumulator<?, ?>> accumulatorMap = new HashMap<>();
        final HashMap<String, Future<Path>> cpTasks = new HashMap<>();
        final TaskInfo taskInfo = new TaskInfoImpl(taskName, 1, 0, 1, 0);
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.disableObjectReuse();

        List<Integer> resultMutableSafe =
                op.executeOnCollections(
                        input,
                        new RuntimeUDFContext(
                                taskInfo,
                                null,
                                executionConfig,
                                cpTasks,
                                accumulatorMap,
                                UnregisteredMetricsGroup.createOperatorMetricGroup()),
                        executionConfig);

        executionConfig.enableObjectReuse();
        List<Integer> resultRegular =
                op.executeOnCollections(
                        input,
                        new RuntimeUDFContext(
                                taskInfo,
                                null,
                                executionConfig,
                                cpTasks,
                                accumulatorMap,
                                UnregisteredMetricsGroup.createOperatorMetricGroup()),
                        executionConfig);

        assertThat(resultMutableSafe).isEqualTo(asList(1, 2, 3, 4, 5, 6));
        assertThat(resultRegular).isEqualTo(asList(1, 2, 3, 4, 5, 6));

        assertThat(opened).isTrue();
        assertThat(closed).isTrue();
    }
}
