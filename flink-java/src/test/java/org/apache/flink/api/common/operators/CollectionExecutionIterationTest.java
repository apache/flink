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

package org.apache.flink.api.common.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link CollectionExecutor} with iterations. */
@SuppressWarnings("serial")
class CollectionExecutionIterationTest implements Serializable {

    @Test
    void testBulkIteration() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

            IterativeDataSet<Integer> iteration = env.fromElements(1).iterate(10);

            DataSet<Integer> result =
                    iteration.closeWith(iteration.map(new AddSuperstepNumberMapper()));

            List<Integer> collected = new ArrayList<>();
            result.output(new LocalCollectionOutputFormat<>(collected));

            env.execute();

            assertThat(collected).containsExactly(56);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testBulkIterationWithTerminationCriterion() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

            IterativeDataSet<Integer> iteration = env.fromElements(1).iterate(100);

            DataSet<Integer> iterationResult = iteration.map(new AddSuperstepNumberMapper());

            DataSet<Integer> terminationCriterion =
                    iterationResult.filter((FilterFunction<Integer>) value -> value < 50);

            List<Integer> collected = new ArrayList<>();

            iteration
                    .closeWith(iterationResult, terminationCriterion)
                    .output(new LocalCollectionOutputFormat<>(collected));

            env.execute();

            assertThat(collected).containsExactly(56);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testDeltaIteration() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

            @SuppressWarnings("unchecked")
            DataSet<Tuple2<Integer, Integer>> solInput =
                    env.fromElements(
                            new Tuple2<>(1, 0),
                            new Tuple2<>(2, 0),
                            new Tuple2<>(3, 0),
                            new Tuple2<>(4, 0));

            @SuppressWarnings("unchecked")
            DataSet<Tuple1<Integer>> workInput =
                    env.fromElements(
                            new Tuple1<>(1), new Tuple1<>(2), new Tuple1<>(3), new Tuple1<>(4));

            // Perform a delta iteration where we add those values to the workset where
            // the second tuple field is smaller than the first tuple field.
            // At the end both tuple fields must be the same.

            DeltaIteration<Tuple2<Integer, Integer>, Tuple1<Integer>> iteration =
                    solInput.iterateDelta(workInput, 10, 0);

            DataSet<Tuple2<Integer, Integer>> solDelta =
                    iteration
                            .getSolutionSet()
                            .join(iteration.getWorkset())
                            .where(0)
                            .equalTo(0)
                            .with(
                                    new JoinFunction<
                                            Tuple2<Integer, Integer>,
                                            Tuple1<Integer>,
                                            Tuple2<Integer, Integer>>() {
                                        @Override
                                        public Tuple2<Integer, Integer> join(
                                                Tuple2<Integer, Integer> first,
                                                Tuple1<Integer> second)
                                                throws Exception {
                                            return new Tuple2<Integer, Integer>(
                                                    first.f0, first.f1 + 1);
                                        }
                                    });

            DataSet<Tuple1<Integer>> nextWorkset =
                    solDelta.flatMap(
                            new FlatMapFunction<Tuple2<Integer, Integer>, Tuple1<Integer>>() {
                                @Override
                                public void flatMap(
                                        Tuple2<Integer, Integer> in, Collector<Tuple1<Integer>> out)
                                        throws Exception {
                                    if (in.f1 < in.f0) {
                                        out.collect(new Tuple1<>(in.f0));
                                    }
                                }
                            });

            List<Tuple2<Integer, Integer>> collected = new ArrayList<>();

            iteration
                    .closeWith(solDelta, nextWorkset)
                    .output(new LocalCollectionOutputFormat<>(collected));

            env.execute();

            // verify that both tuple fields are now the same
            for (Tuple2<Integer, Integer> t : collected) {
                assertThat(t.f1).isEqualTo(t.f0);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private static class AddSuperstepNumberMapper extends RichMapFunction<Integer, Integer> {

        @Override
        public Integer map(Integer value) {
            int superstep = getIterationRuntimeContext().getSuperstepNumber();
            return value + superstep;
        }
    }
}
