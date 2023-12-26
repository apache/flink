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
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link GroupReduceFunction}. */
@SuppressWarnings({"serial", "unchecked"})
class GroupReduceOperatorTest implements Serializable {

    private static final TypeInformation<Tuple2<String, Integer>> STRING_INT_TUPLE =
            TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {});

    @Test
    void testGroupReduceCollection() {
        try {
            final GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> reducer =
                    (values, out) -> {
                        Iterator<Tuple2<String, Integer>> input = values.iterator();

                        Tuple2<String, Integer> result = input.next();
                        int sum = result.f1;
                        while (input.hasNext()) {
                            Tuple2<String, Integer> next = input.next();
                            sum += next.f1;
                        }
                        result.f1 = sum;
                        out.collect(result);
                    };

            GroupReduceOperatorBase<
                            Tuple2<String, Integer>,
                            Tuple2<String, Integer>,
                            GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>>
                    op =
                            new GroupReduceOperatorBase<>(
                                    reducer,
                                    new UnaryOperatorInformation<>(
                                            STRING_INT_TUPLE, STRING_INT_TUPLE),
                                    new int[] {0},
                                    "TestReducer");

            List<Tuple2<String, Integer>> input =
                    new ArrayList<>(
                            asList(
                                    new Tuple2<>("foo", 1),
                                    new Tuple2<>("foo", 3),
                                    new Tuple2<>("bar", 2),
                                    new Tuple2<>("bar", 4)));

            ExecutionConfig executionConfig = new ExecutionConfig();
            executionConfig.disableObjectReuse();
            List<Tuple2<String, Integer>> resultMutableSafe =
                    op.executeOnCollections(input, null, executionConfig);
            executionConfig.enableObjectReuse();
            List<Tuple2<String, Integer>> resultRegular =
                    op.executeOnCollections(input, null, executionConfig);

            Set<Tuple2<String, Integer>> resultSetMutableSafe = new HashSet<>(resultMutableSafe);
            Set<Tuple2<String, Integer>> resultSetRegular = new HashSet<>(resultRegular);

            Set<Tuple2<String, Integer>> expectedResult =
                    new HashSet<>(asList(new Tuple2<>("foo", 4), new Tuple2<>("bar", 6)));

            assertThat(resultSetMutableSafe).isEqualTo(expectedResult);
            assertThat(resultSetRegular).isEqualTo(expectedResult);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testGroupReduceCollectionWithRuntimeContext() {
        try {
            final String taskName = "Test Task";
            final AtomicBoolean opened = new AtomicBoolean();
            final AtomicBoolean closed = new AtomicBoolean();

            final RichGroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>
                    reducer =
                            new RichGroupReduceFunction<
                                    Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                                @Override
                                public void reduce(
                                        Iterable<Tuple2<String, Integer>> values,
                                        Collector<Tuple2<String, Integer>> out)
                                        throws Exception {
                                    Iterator<Tuple2<String, Integer>> input = values.iterator();

                                    Tuple2<String, Integer> result = input.next();
                                    int sum = result.f1;
                                    while (input.hasNext()) {
                                        Tuple2<String, Integer> next = input.next();
                                        sum += next.f1;
                                    }
                                    result.f1 = sum;
                                    out.collect(result);
                                }

                                @Override
                                public void open(OpenContext openContext) throws Exception {
                                    opened.set(true);
                                    RuntimeContext ctx = getRuntimeContext();
                                    assertThat(ctx.getIndexOfThisSubtask()).isZero();
                                    assertThat(ctx.getNumberOfParallelSubtasks()).isOne();
                                    assertThat(ctx.getTaskName()).isEqualTo(taskName);
                                }

                                @Override
                                public void close() throws Exception {
                                    closed.set(true);
                                }
                            };

            GroupReduceOperatorBase<
                            Tuple2<String, Integer>,
                            Tuple2<String, Integer>,
                            GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>>
                    op =
                            new GroupReduceOperatorBase<>(
                                    reducer,
                                    new UnaryOperatorInformation<>(
                                            STRING_INT_TUPLE, STRING_INT_TUPLE),
                                    new int[] {0},
                                    "TestReducer");

            List<Tuple2<String, Integer>> input =
                    new ArrayList<>(
                            asList(
                                    new Tuple2<>("foo", 1),
                                    new Tuple2<>("foo", 3),
                                    new Tuple2<>("bar", 2),
                                    new Tuple2<>("bar", 4)));

            final TaskInfo taskInfo = new TaskInfo(taskName, 1, 0, 1, 0);

            ExecutionConfig executionConfig = new ExecutionConfig();
            executionConfig.disableObjectReuse();
            List<Tuple2<String, Integer>> resultMutableSafe =
                    op.executeOnCollections(
                            input,
                            new RuntimeUDFContext(
                                    taskInfo,
                                    null,
                                    executionConfig,
                                    new HashMap<>(),
                                    new HashMap<>(),
                                    UnregisteredMetricsGroup.createOperatorMetricGroup()),
                            executionConfig);

            executionConfig.enableObjectReuse();
            List<Tuple2<String, Integer>> resultRegular =
                    op.executeOnCollections(
                            input,
                            new RuntimeUDFContext(
                                    taskInfo,
                                    null,
                                    executionConfig,
                                    new HashMap<>(),
                                    new HashMap<>(),
                                    UnregisteredMetricsGroup.createOperatorMetricGroup()),
                            executionConfig);

            Set<Tuple2<String, Integer>> resultSetMutableSafe = new HashSet<>(resultMutableSafe);
            Set<Tuple2<String, Integer>> resultSetRegular = new HashSet<>(resultRegular);

            Set<Tuple2<String, Integer>> expectedResult =
                    new HashSet<>(asList(new Tuple2<>("foo", 4), new Tuple2<>("bar", 6)));

            assertThat(resultSetMutableSafe).isEqualTo(expectedResult);
            assertThat(resultSetRegular).isEqualTo(expectedResult);

            assertThat(opened.get()).isTrue();
            assertThat(closed.get()).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
