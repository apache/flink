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
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link CoGroupOperatorBase} on collections. */
@SuppressWarnings("serial")
class CoGroupOperatorCollectionTest implements Serializable {

    @Test
    void testExecuteOnCollection() {
        try {
            List<Tuple2<String, Integer>> input1 =
                    Arrays.asList(
                            new Tuple2Builder<String, Integer>()
                                    .add("foo", 1)
                                    .add("foobar", 1)
                                    .add("foo", 1)
                                    .add("bar", 1)
                                    .add("foo", 1)
                                    .add("foo", 1)
                                    .build());

            List<Tuple2<String, Integer>> input2 =
                    Arrays.asList(
                            new Tuple2Builder<String, Integer>()
                                    .add("foo", 1)
                                    .add("foo", 1)
                                    .add("bar", 1)
                                    .add("foo", 1)
                                    .add("barfoo", 1)
                                    .add("foo", 1)
                                    .build());

            ExecutionConfig executionConfig = new ExecutionConfig();
            final HashMap<String, Accumulator<?, ?>> accumulators = new HashMap<>();
            final HashMap<String, Future<Path>> cpTasks = new HashMap<>();
            final TaskInfo taskInfo = new TaskInfo("Test UDF", 4, 0, 4, 0);
            final RuntimeContext ctx =
                    new RuntimeUDFContext(
                            taskInfo,
                            null,
                            executionConfig,
                            cpTasks,
                            accumulators,
                            UnregisteredMetricsGroup.createOperatorMetricGroup());

            {
                SumCoGroup udf1 = new SumCoGroup();
                SumCoGroup udf2 = new SumCoGroup();

                executionConfig.disableObjectReuse();
                List<Tuple2<String, Integer>> resultSafe =
                        getCoGroupOperator(udf1)
                                .executeOnCollections(input1, input2, ctx, executionConfig);
                executionConfig.enableObjectReuse();
                List<Tuple2<String, Integer>> resultRegular =
                        getCoGroupOperator(udf2)
                                .executeOnCollections(input1, input2, ctx, executionConfig);

                assertThat(udf1.isClosed).isTrue();
                assertThat(udf2.isClosed).isTrue();

                Set<Tuple2<String, Integer>> expected =
                        new HashSet<>(
                                Arrays.asList(
                                        new Tuple2Builder<String, Integer>()
                                                .add("foo", 8)
                                                .add("bar", 2)
                                                .add("foobar", 1)
                                                .add("barfoo", 1)
                                                .build()));

                assertThat(new HashSet<>(resultSafe)).containsExactlyInAnyOrderElementsOf(expected);
                assertThat(new HashSet<>(resultRegular))
                        .containsExactlyInAnyOrderElementsOf(expected);
            }

            {
                executionConfig.disableObjectReuse();
                List<Tuple2<String, Integer>> resultSafe =
                        getCoGroupOperator(new SumCoGroup())
                                .executeOnCollections(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        ctx,
                                        executionConfig);

                executionConfig.enableObjectReuse();
                List<Tuple2<String, Integer>> resultRegular =
                        getCoGroupOperator(new SumCoGroup())
                                .executeOnCollections(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        ctx,
                                        executionConfig);

                assertThat(resultSafe).isEmpty();
                assertThat(resultRegular).isEmpty();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            fail(t.getMessage());
        }
    }

    private static class SumCoGroup
            extends RichCoGroupFunction<
                    Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private boolean isOpened = false;
        private boolean isClosed = false;

        @Override
        public void open(OpenContext openContext) throws Exception {
            isOpened = true;

            RuntimeContext ctx = getRuntimeContext();
            assertThat(ctx.getTaskName()).isEqualTo("Test UDF");
            assertThat(ctx.getNumberOfParallelSubtasks()).isEqualTo(4);
            assertThat(ctx.getIndexOfThisSubtask()).isZero();
        }

        @Override
        public void coGroup(
                Iterable<Tuple2<String, Integer>> first,
                Iterable<Tuple2<String, Integer>> second,
                Collector<Tuple2<String, Integer>> out)
                throws Exception {

            assertThat(isOpened).isTrue();
            assertThat(isClosed).isFalse();

            String f0 = null;
            int sumF1 = 0;

            for (Tuple2<String, Integer> input : first) {
                f0 = (f0 == null) ? input.f0 : f0;
                sumF1 += input.f1;
            }

            for (Tuple2<String, Integer> input : second) {
                f0 = (f0 == null) ? input.f0 : f0;
                sumF1 += input.f1;
            }

            out.collect(Tuple2.of(f0, sumF1));
        }

        @Override
        public void close() throws Exception {
            isClosed = true;
        }
    }

    private CoGroupOperatorBase<
                    Tuple2<String, Integer>,
                    Tuple2<String, Integer>,
                    Tuple2<String, Integer>,
                    CoGroupFunction<
                            Tuple2<String, Integer>,
                            Tuple2<String, Integer>,
                            Tuple2<String, Integer>>>
            getCoGroupOperator(
                    RichCoGroupFunction<
                                    Tuple2<String, Integer>,
                                    Tuple2<String, Integer>,
                                    Tuple2<String, Integer>>
                            udf) {

        TypeInformation<Tuple2<String, Integer>> tuple2Info =
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {});

        return new CoGroupOperatorBase<>(
                udf,
                new BinaryOperatorInformation<>(tuple2Info, tuple2Info, tuple2Info),
                new int[] {0},
                new int[] {0},
                "coGroup on Collections");
    }
}
