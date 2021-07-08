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

package org.apache.flink.test.operators;

import org.apache.flink.api.common.functions.CombineFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/** Integration tests for {@link GroupCombineFunction}. */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class ReduceWithCombinerITCase extends MultipleProgramsTestBase {

    public ReduceWithCombinerITCase(TestExecutionMode mode) {
        super(TestExecutionMode.CLUSTER);
    }

    @Test
    public void testReduceOnNonKeyedDataset() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // creates the input data and distributes them evenly among the available downstream tasks
        DataSet<Tuple2<Integer, Boolean>> input = createNonKeyedInput(env);
        List<Tuple2<Integer, Boolean>> actual =
                input.reduceGroup(new NonKeyedCombReducer()).collect();
        String expected = "10,true\n";

        compareResultAsTuples(actual, expected);
    }

    @Test
    public void testForkingReduceOnNonKeyedDataset() throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // creates the input data and distributes them evenly among the available downstream tasks
        DataSet<Tuple2<Integer, Boolean>> input = createNonKeyedInput(env);

        DataSet<Tuple2<Integer, Boolean>> r1 = input.reduceGroup(new NonKeyedCombReducer());
        DataSet<Tuple2<Integer, Boolean>> r2 = input.reduceGroup(new NonKeyedGroupCombReducer());

        List<Tuple2<Integer, Boolean>> actual = r1.union(r2).collect();
        String expected = "10,true\n10,true\n";
        compareResultAsTuples(actual, expected);
    }

    @Test
    public void testReduceOnKeyedDataset() throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // creates the input data and distributes them evenly among the available downstream tasks
        DataSet<Tuple3<String, Integer, Boolean>> input = createKeyedInput(env);
        List<Tuple3<String, Integer, Boolean>> actual =
                input.groupBy(0).reduceGroup(new KeyedCombReducer()).collect();
        String expected = "k1,6,true\nk2,4,true\n";

        compareResultAsTuples(actual, expected);
    }

    @Test
    public void testReduceOnKeyedDatasetWithSelector() throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // creates the input data and distributes them evenly among the available downstream tasks
        DataSet<Tuple3<String, Integer, Boolean>> input = createKeyedInput(env);

        List<Tuple3<String, Integer, Boolean>> actual =
                input.groupBy(new KeySelectorX()).reduceGroup(new KeyedCombReducer()).collect();
        String expected = "k1,6,true\nk2,4,true\n";

        compareResultAsTuples(actual, expected);
    }

    @Test
    public void testForkingReduceOnKeyedDataset() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // creates the input data and distributes them evenly among the available downstream tasks
        DataSet<Tuple3<String, Integer, Boolean>> input = createKeyedInput(env);

        UnsortedGrouping<Tuple3<String, Integer, Boolean>> counts = input.groupBy(0);

        DataSet<Tuple3<String, Integer, Boolean>> r1 = counts.reduceGroup(new KeyedCombReducer());
        DataSet<Tuple3<String, Integer, Boolean>> r2 =
                counts.reduceGroup(new KeyedGroupCombReducer());

        List<Tuple3<String, Integer, Boolean>> actual = r1.union(r2).collect();
        String expected = "k1,6,true\n" + "k2,4,true\n" + "k1,6,true\n" + "k2,4,true\n";
        compareResultAsTuples(actual, expected);
    }

    @Test
    public void testForkingReduceOnKeyedDatasetWithSelection() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // creates the input data and distributes them evenly among the available downstream tasks
        DataSet<Tuple3<String, Integer, Boolean>> input = createKeyedInput(env);

        UnsortedGrouping<Tuple3<String, Integer, Boolean>> counts =
                input.groupBy(new KeySelectorX());

        DataSet<Tuple3<String, Integer, Boolean>> r1 = counts.reduceGroup(new KeyedCombReducer());
        DataSet<Tuple3<String, Integer, Boolean>> r2 =
                counts.reduceGroup(new KeyedGroupCombReducer());

        List<Tuple3<String, Integer, Boolean>> actual = r1.union(r2).collect();
        String expected = "k1,6,true\n" + "k2,4,true\n" + "k1,6,true\n" + "k2,4,true\n";

        compareResultAsTuples(actual, expected);
    }

    private DataSet<Tuple2<Integer, Boolean>> createNonKeyedInput(ExecutionEnvironment env) {
        return env.fromCollection(
                        Arrays.asList(
                                new Tuple2<>(1, false),
                                new Tuple2<>(1, false),
                                new Tuple2<>(1, false),
                                new Tuple2<>(1, false),
                                new Tuple2<>(1, false),
                                new Tuple2<>(1, false),
                                new Tuple2<>(1, false),
                                new Tuple2<>(1, false),
                                new Tuple2<>(1, false),
                                new Tuple2<>(1, false)))
                .rebalance();
    }

    private static class NonKeyedCombReducer
            implements CombineFunction<Tuple2<Integer, Boolean>, Tuple2<Integer, Boolean>>,
                    GroupReduceFunction<Tuple2<Integer, Boolean>, Tuple2<Integer, Boolean>> {

        @Override
        public Tuple2<Integer, Boolean> combine(Iterable<Tuple2<Integer, Boolean>> values)
                throws Exception {
            int sum = 0;
            boolean flag = true;

            for (Tuple2<Integer, Boolean> tuple : values) {
                sum += tuple.f0;
                flag &= !tuple.f1;
            }
            return new Tuple2<>(sum, flag);
        }

        @Override
        public void reduce(
                Iterable<Tuple2<Integer, Boolean>> values, Collector<Tuple2<Integer, Boolean>> out)
                throws Exception {
            int sum = 0;
            boolean flag = true;
            for (Tuple2<Integer, Boolean> tuple : values) {
                sum += tuple.f0;
                flag &= tuple.f1;
            }
            out.collect(new Tuple2<>(sum, flag));
        }
    }

    private static class NonKeyedGroupCombReducer
            implements GroupCombineFunction<Tuple2<Integer, Boolean>, Tuple2<Integer, Boolean>>,
                    GroupReduceFunction<Tuple2<Integer, Boolean>, Tuple2<Integer, Boolean>> {

        @Override
        public void reduce(
                Iterable<Tuple2<Integer, Boolean>> values, Collector<Tuple2<Integer, Boolean>> out)
                throws Exception {
            int sum = 0;
            boolean flag = true;
            for (Tuple2<Integer, Boolean> tuple : values) {
                sum += tuple.f0;
                flag &= tuple.f1;
            }
            out.collect(new Tuple2<>(sum, flag));
        }

        @Override
        public void combine(
                Iterable<Tuple2<Integer, Boolean>> values, Collector<Tuple2<Integer, Boolean>> out)
                throws Exception {
            int sum = 0;
            boolean flag = true;
            for (Tuple2<Integer, Boolean> tuple : values) {
                sum += tuple.f0;
                flag &= !tuple.f1;
            }
            out.collect(new Tuple2<>(sum, flag));
        }
    }

    private DataSet<Tuple3<String, Integer, Boolean>> createKeyedInput(ExecutionEnvironment env) {
        return env.fromCollection(
                        Arrays.asList(
                                new Tuple3<>("k1", 1, false),
                                new Tuple3<>("k1", 1, false),
                                new Tuple3<>("k1", 1, false),
                                new Tuple3<>("k2", 1, false),
                                new Tuple3<>("k1", 1, false),
                                new Tuple3<>("k1", 1, false),
                                new Tuple3<>("k2", 1, false),
                                new Tuple3<>("k2", 1, false),
                                new Tuple3<>("k1", 1, false),
                                new Tuple3<>("k2", 1, false)))
                .rebalance();
    }

    private static class KeySelectorX
            implements KeySelector<Tuple3<String, Integer, Boolean>, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(Tuple3<String, Integer, Boolean> in) {
            return in.f0;
        }
    }

    private class KeyedCombReducer
            implements CombineFunction<
                            Tuple3<String, Integer, Boolean>, Tuple3<String, Integer, Boolean>>,
                    GroupReduceFunction<
                            Tuple3<String, Integer, Boolean>, Tuple3<String, Integer, Boolean>> {

        @Override
        public Tuple3<String, Integer, Boolean> combine(
                Iterable<Tuple3<String, Integer, Boolean>> values) throws Exception {
            String key = null;
            int sum = 0;
            boolean flag = true;

            for (Tuple3<String, Integer, Boolean> tuple : values) {
                key = (key == null) ? tuple.f0 : key;
                sum += tuple.f1;
                flag &= !tuple.f2;
            }
            return new Tuple3<>(key, sum, flag);
        }

        @Override
        public void reduce(
                Iterable<Tuple3<String, Integer, Boolean>> values,
                Collector<Tuple3<String, Integer, Boolean>> out)
                throws Exception {
            String key = null;
            int sum = 0;
            boolean flag = true;

            for (Tuple3<String, Integer, Boolean> tuple : values) {
                key = (key == null) ? tuple.f0 : key;
                sum += tuple.f1;
                flag &= tuple.f2;
            }
            out.collect(new Tuple3<>(key, sum, flag));
        }
    }

    private class KeyedGroupCombReducer
            implements GroupCombineFunction<
                            Tuple3<String, Integer, Boolean>, Tuple3<String, Integer, Boolean>>,
                    GroupReduceFunction<
                            Tuple3<String, Integer, Boolean>, Tuple3<String, Integer, Boolean>> {

        @Override
        public void combine(
                Iterable<Tuple3<String, Integer, Boolean>> values,
                Collector<Tuple3<String, Integer, Boolean>> out)
                throws Exception {
            String key = null;
            int sum = 0;
            boolean flag = true;

            for (Tuple3<String, Integer, Boolean> tuple : values) {
                key = (key == null) ? tuple.f0 : key;
                sum += tuple.f1;
                flag &= !tuple.f2;
            }
            out.collect(new Tuple3<>(key, sum, flag));
        }

        @Override
        public void reduce(
                Iterable<Tuple3<String, Integer, Boolean>> values,
                Collector<Tuple3<String, Integer, Boolean>> out)
                throws Exception {
            String key = null;
            int sum = 0;
            boolean flag = true;

            for (Tuple3<String, Integer, Boolean> tuple : values) {
                key = (key == null) ? tuple.f0 : key;
                sum += tuple.f1;
                flag &= tuple.f2;
            }
            out.collect(new Tuple3<>(key, sum, flag));
        }
    }
}
