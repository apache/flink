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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.operators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.operators.util.CollectionDataSets.POJO;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/** Integration tests for {@link DataSet#distinct}. */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class DistinctITCase extends MultipleProgramsTestBase {

    public DistinctITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testCorrectnessOfDistinctOnTuplesWithKeyFieldSelector() throws Exception {
        /*
         * check correctness of distinct on tuples with key field selector
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> distinctDs = ds.union(ds).distinct(0, 1, 2);

        List<Tuple3<Integer, Long, String>> result = distinctDs.collect();

        String expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfDistinctOnTuplesWithKeyFieldSelectorWithNotAllFieldsSelected()
            throws Exception {
        /*
         * check correctness of distinct on tuples with key field selector with not all fields selected
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.getSmall5TupleDataSet(env);
        DataSet<Tuple1<Integer>> distinctDs = ds.union(ds).distinct(0).project(0);

        List<Tuple1<Integer>> result = distinctDs.collect();

        String expected = "1\n" + "2\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfDistinctOnTuplesWithKeyExtractorFunction() throws Exception {
        /*
         * check correctness of distinct on tuples with key extractor function
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.getSmall5TupleDataSet(env);
        DataSet<Tuple1<Integer>> reduceDs = ds.union(ds).distinct(new KeySelector1()).project(0);

        List<Tuple1<Integer>> result = reduceDs.collect();

        String expected = "1\n" + "2\n";

        compareResultAsTuples(result, expected);
    }

    private static class KeySelector1
            implements KeySelector<Tuple5<Integer, Long, Integer, String, Long>, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(Tuple5<Integer, Long, Integer, String, Long> in) {
            return in.f0;
        }
    }

    @Test
    public void testCorrectnessOfDistinctOnCustomTypeWithTypeExtractor() throws Exception {
        /*
         * check correctness of distinct on custom type with type extractor
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<Tuple1<Integer>> reduceDs = ds.distinct(new KeySelector3()).map(new Mapper3());

        List<Tuple1<Integer>> result = reduceDs.collect();

        String expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n";

        compareResultAsTuples(result, expected);
    }

    private static class Mapper3 extends RichMapFunction<CustomType, Tuple1<Integer>> {
        @Override
        public Tuple1<Integer> map(CustomType value) throws Exception {
            return new Tuple1<Integer>(value.myInt);
        }
    }

    private static class KeySelector3 implements KeySelector<CustomType, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(CustomType in) {
            return in.myInt;
        }
    }

    @Test
    public void testCorrectnessOfDistinctOnTuples() throws Exception {
        /*
         * check correctness of distinct on tuples
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> distinctDs = ds.union(ds).distinct();

        List<Tuple3<Integer, Long, String>> result = distinctDs.collect();

        String expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfDistinctOnCustomTypeWithTupleReturningTypeExtractor()
            throws Exception {
        /*
         * check correctness of distinct on custom type with tuple-returning type extractor
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple2<Integer, Long>> reduceDs = ds.distinct(new KeySelector2()).project(0, 4);

        List<Tuple2<Integer, Long>> result = reduceDs.collect();

        String expected =
                "1,1\n" + "2,1\n" + "2,2\n" + "3,2\n" + "3,3\n" + "4,1\n" + "4,2\n" + "5,1\n"
                        + "5,2\n" + "5,3\n";

        compareResultAsTuples(result, expected);
    }

    private static class KeySelector2
            implements KeySelector<
                    Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Integer, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, Long> getKey(Tuple5<Integer, Long, Integer, String, Long> t) {
            return new Tuple2<Integer, Long>(t.f0, t.f4);
        }
    }

    @Test
    public void testCorrectnessOfDistinctOnTuplesWithFieldExpressions() throws Exception {
        /*
         * check correctness of distinct on tuples with field expressions
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.getSmall5TupleDataSet(env);
        DataSet<Tuple1<Integer>> reduceDs = ds.union(ds).distinct("f0").project(0);

        List<Tuple1<Integer>> result = reduceDs.collect();

        String expected = "1\n" + "2\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfDistinctOnPojos() throws Exception {
        /*
         * check correctness of distinct on Pojos
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<POJO> ds = CollectionDataSets.getDuplicatePojoDataSet(env);
        DataSet<Integer> reduceDs = ds.distinct("nestedPojo.longNumber").map(new Mapper2());

        List<Integer> result = reduceDs.collect();

        String expected = "10000\n20000\n30000\n";

        compareResultAsText(result, expected);
    }

    private static class Mapper2 implements MapFunction<CollectionDataSets.POJO, Integer> {
        @Override
        public Integer map(POJO value) throws Exception {
            return (int) value.nestedPojo.longNumber;
        }
    }

    @Test
    public void testDistinctOnFullPojo() throws Exception {
        /*
         * distinct on full Pojo
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<POJO> ds = CollectionDataSets.getDuplicatePojoDataSet(env);
        DataSet<Integer> reduceDs = ds.distinct().map(new Mapper1());

        List<Integer> result = reduceDs.collect();

        String expected = "10000\n20000\n30000\n";

        compareResultAsText(result, expected);
    }

    private static class Mapper1 implements MapFunction<CollectionDataSets.POJO, Integer> {
        @Override
        public Integer map(POJO value) throws Exception {
            return (int) value.nestedPojo.longNumber;
        }
    }

    @Test
    public void testCorrectnessOfDistinctOnAtomic() throws Exception {
        /*
         * check correctness of distinct on Integers
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds = CollectionDataSets.getIntegerDataSet(env);
        DataSet<Integer> reduceDs = ds.distinct();

        List<Integer> result = reduceDs.collect();

        String expected = "1\n2\n3\n4\n5";

        compareResultAsText(result, expected);
    }

    @Test
    public void testCorrectnessOfDistinctOnAtomicWithSelectAllChar() throws Exception {
        /*
         * check correctness of distinct on Strings, using Keys.ExpressionKeys.SELECT_ALL_CHAR
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> ds = CollectionDataSets.getStringDataSet(env);
        DataSet<String> reduceDs = ds.union(ds).distinct("*");

        List<String> result = reduceDs.collect();

        String expected =
                "I am fine.\n"
                        + "Luke Skywalker\n"
                        + "LOL\n"
                        + "Hello world, how are you?\n"
                        + "Hi\n"
                        + "Hello world\n"
                        + "Hello\n"
                        + "Random comment\n";

        compareResultAsText(result, expected);
    }
}
