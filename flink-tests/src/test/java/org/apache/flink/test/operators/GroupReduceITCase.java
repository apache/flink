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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.operators.util.CollectionDataSets.CrazyNested;
import org.apache.flink.test.operators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.operators.util.CollectionDataSets.FromTupleWithCTor;
import org.apache.flink.test.operators.util.CollectionDataSets.POJO;
import org.apache.flink.test.operators.util.CollectionDataSets.PojoContainingTupleAndWritable;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import scala.math.BigInt;

import static org.apache.flink.test.util.TestBaseUtils.compareResultAsText;
import static org.apache.flink.test.util.TestBaseUtils.compareResultAsTuples;

/**
 * Integration tests for {@link GroupReduceFunction}, {@link RichGroupReduceFunction}, and {@link
 * GroupCombineFunction}.
 */
@SuppressWarnings({"serial", "unchecked", "UnusedDeclaration"})
@RunWith(Parameterized.class)
public class GroupReduceITCase extends MultipleProgramsTestBase {

    public GroupReduceITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void
            testCorrectnessofGroupReduceOnTupleContainingPrimitiveByteArrayWithKeyFieldSelectors()
                    throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<byte[], Integer>> ds = CollectionDataSets.getTuple2WithByteArrayDataSet(env);
        DataSet<Integer> reduceDs = ds.groupBy(0).reduceGroup(new ByteArrayGroupReduce());

        List<Integer> result = reduceDs.collect();

        String expected = "0\n" + "1\n" + "2\n" + "3\n" + "4\n";

        compareResultAsText(result, expected);
    }

    private static class ByteArrayGroupReduce
            implements GroupReduceFunction<Tuple2<byte[], Integer>, Integer> {
        @Override
        public void reduce(Iterable<Tuple2<byte[], Integer>> values, Collector<Integer> out)
                throws Exception {
            int sum = 0;
            for (Tuple2<byte[], Integer> value : values) {
                sum += value.f1;
            }
            out.collect(sum);
        }
    }

    @Test
    public void testCorrectnessOfGroupReduceOnTuplesWithKeyFieldSelector() throws Exception {
        /*
         * check correctness of groupReduce on tuples with key field selector
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple2<Integer, Long>> reduceDs =
                ds.groupBy(1).reduceGroup(new Tuple3GroupReduce());

        List<Tuple2<Integer, Long>> result = reduceDs.collect();

        String expected = "1,1\n" + "5,2\n" + "15,3\n" + "34,4\n" + "65,5\n" + "111,6\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfGroupReduceOnTuplesWithMultipleKeyFieldSelectors()
            throws Exception {
        /*
         * check correctness of groupReduce on tuples with multiple key field selector
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> reduceDs =
                ds.groupBy(4, 0).reduceGroup(new Tuple5GroupReduce());

        List<Tuple5<Integer, Long, Integer, String, Long>> result = reduceDs.collect();

        String expected =
                "1,1,0,P-),1\n"
                        + "2,3,0,P-),1\n"
                        + "2,2,0,P-),2\n"
                        + "3,9,0,P-),2\n"
                        + "3,6,0,P-),3\n"
                        + "4,17,0,P-),1\n"
                        + "4,17,0,P-),2\n"
                        + "5,11,0,P-),1\n"
                        + "5,29,0,P-),2\n"
                        + "5,25,0,P-),3\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfGroupReduceOnTuplesWithKeyFieldSelectorAndGroupSorting()
            throws Exception {
        /*
         * check correctness of groupReduce on tuples with key field selector and group sorting
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> reduceDs =
                ds.groupBy(1)
                        .sortGroup(2, Order.ASCENDING)
                        .reduceGroup(new Tuple3SortedGroupReduce());

        List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

        String expected =
                "1,1,Hi\n"
                        + "5,2,Hello-Hello world\n"
                        + "15,3,Hello world, how are you?-I am fine.-Luke Skywalker\n"
                        + "34,4,Comment#1-Comment#2-Comment#3-Comment#4\n"
                        + "65,5,Comment#5-Comment#6-Comment#7-Comment#8-Comment#9\n"
                        + "111,6,Comment#10-Comment#11-Comment#12-Comment#13-Comment#14-Comment#15\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfGroupReduceOnTuplesWithKeyExtractor() throws Exception {
        /*
         * check correctness of groupReduce on tuples with key extractor
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple2<Integer, Long>> reduceDs =
                ds.groupBy(new KeySelector1()).reduceGroup(new Tuple3GroupReduce());

        List<Tuple2<Integer, Long>> result = reduceDs.collect();

        String expected = "1,1\n" + "5,2\n" + "15,3\n" + "34,4\n" + "65,5\n" + "111,6\n";

        compareResultAsTuples(result, expected);
    }

    private static class KeySelector1 implements KeySelector<Tuple3<Integer, Long, String>, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long getKey(Tuple3<Integer, Long, String> in) {
            return in.f1;
        }
    }

    @Test
    public void testCorrectnessOfGroupReduceOnCustomTypeWithTypeExtractor() throws Exception {
        /*
         * check correctness of groupReduce on custom type with type extractor
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> reduceDs =
                ds.groupBy(new KeySelector2()).reduceGroup(new CustomTypeGroupReduce());

        List<CustomType> result = reduceDs.collect();

        String expected =
                "1,0,Hello!\n"
                        + "2,3,Hello!\n"
                        + "3,12,Hello!\n"
                        + "4,30,Hello!\n"
                        + "5,60,Hello!\n"
                        + "6,105,Hello!\n";

        compareResultAsText(result, expected);
    }

    private static class KeySelector2 implements KeySelector<CustomType, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(CustomType in) {
            return in.myInt;
        }
    }

    @Test
    public void testCorrectnessOfAllGroupReduceForTuples() throws Exception {
        /*
         * check correctness of all-groupreduce for tuples
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> reduceDs =
                ds.reduceGroup(new AllAddingTuple3GroupReduce());

        List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

        String expected = "231,91,Hello World\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfAllGroupReduceForCustomTypes() throws Exception {
        /*
         * check correctness of all-groupreduce for custom types
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> reduceDs = ds.reduceGroup(new AllAddingCustomTypeGroupReduce());

        List<CustomType> result = reduceDs.collect();

        String expected = "91,210,Hello!";

        compareResultAsText(result, expected);
    }

    @Test
    public void testCorrectnessOfGroupReduceWithBroadcastSet() throws Exception {
        /*
         * check correctness of groupReduce with broadcast set
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> reduceDs =
                ds.groupBy(1)
                        .reduceGroup(new BCTuple3GroupReduce())
                        .withBroadcastSet(intDs, "ints");

        List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

        String expected =
                "1,1,55\n" + "5,2,55\n" + "15,3,55\n" + "34,4,55\n" + "65,5,55\n" + "111,6,55\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfGroupReduceIfUDFReturnsInputObjectsMultipleTimesWhileChangingThem()
            throws Exception {
        /*
         * check correctness of groupReduce if UDF returns input objects multiple times and changes it in between
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> reduceDs =
                ds.groupBy(1).reduceGroup(new InputReturningTuple3GroupReduce());

        List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

        String expected =
                "11,1,Hi!\n"
                        + "21,1,Hi again!\n"
                        + "12,2,Hi!\n"
                        + "22,2,Hi again!\n"
                        + "13,2,Hi!\n"
                        + "23,2,Hi again!\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfGroupReduceOnCustomTypeWithKeyExtractorAndCombine()
            throws Exception {
        /*
         * check correctness of groupReduce on custom type with key extractor and combine
         */
        org.junit.Assume.assumeTrue(mode != TestExecutionMode.COLLECTION);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> reduceDs =
                ds.groupBy(new KeySelector3()).reduceGroup(new CustomTypeGroupReduceWithCombine());

        List<CustomType> result = reduceDs.collect();

        String expected =
                "1,0,test1\n"
                        + "2,3,test2\n"
                        + "3,12,test3\n"
                        + "4,30,test4\n"
                        + "5,60,test5\n"
                        + "6,105,test6\n";

        compareResultAsText(result, expected);
    }

    private static class KeySelector3 implements KeySelector<CustomType, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(CustomType in) {
            return in.myInt;
        }
    }

    @Test
    public void testCorrectnessOfGroupReduceOnTuplesWithCombine() throws Exception {
        /*
         * check correctness of groupReduce on tuples with combine
         */
        org.junit.Assume.assumeTrue(mode != TestExecutionMode.COLLECTION);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // important because it determines how often the combiner is called

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple2<Integer, String>> reduceDs =
                ds.groupBy(1).reduceGroup(new Tuple3GroupReduceWithCombine());

        List<Tuple2<Integer, String>> result = reduceDs.collect();

        String expected =
                "1,test1\n"
                        + "5,test2\n"
                        + "15,test3\n"
                        + "34,test4\n"
                        + "65,test5\n"
                        + "111,test6\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfAllGroupReduceForTuplesWithCombine() throws Exception {
        /*
         * check correctness of all-groupreduce for tuples with combine
         */
        org.junit.Assume.assumeTrue(mode != TestExecutionMode.COLLECTION);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds =
                CollectionDataSets.get3TupleDataSet(env)
                        .map(new IdentityMapper<Tuple3<Integer, Long, String>>())
                        .setParallelism(4);

        Configuration cfg = new Configuration();
        cfg.setString(Optimizer.HINT_SHIP_STRATEGY, Optimizer.HINT_SHIP_STRATEGY_REPARTITION);
        DataSet<Tuple2<Integer, String>> reduceDs =
                ds.reduceGroup(new Tuple3AllGroupReduceWithCombine()).withParameters(cfg);

        List<Tuple2<Integer, String>> result = reduceDs.collect();

        String expected =
                "322,"
                        + "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfGroupreduceWithDescendingGroupSort() throws Exception {
        /*
         * check correctness of groupReduce with descending group sort
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> reduceDs =
                ds.groupBy(1)
                        .sortGroup(2, Order.DESCENDING)
                        .reduceGroup(new Tuple3SortedGroupReduce());

        List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

        String expected =
                "1,1,Hi\n"
                        + "5,2,Hello world-Hello\n"
                        + "15,3,Luke Skywalker-I am fine.-Hello world, how are you?\n"
                        + "34,4,Comment#4-Comment#3-Comment#2-Comment#1\n"
                        + "65,5,Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n"
                        + "111,6,Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfGroupReduceOnTuplesWithTupleReturningKeySelector()
            throws Exception {
        /*
         * check correctness of groupReduce on tuples with tuple-returning key selector
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> reduceDs =
                ds.groupBy(new KeySelector4()).reduceGroup(new Tuple5GroupReduce());

        List<Tuple5<Integer, Long, Integer, String, Long>> result = reduceDs.collect();

        String expected =
                "1,1,0,P-),1\n"
                        + "2,3,0,P-),1\n"
                        + "2,2,0,P-),2\n"
                        + "3,9,0,P-),2\n"
                        + "3,6,0,P-),3\n"
                        + "4,17,0,P-),1\n"
                        + "4,17,0,P-),2\n"
                        + "5,11,0,P-),1\n"
                        + "5,29,0,P-),2\n"
                        + "5,25,0,P-),3\n";

        compareResultAsTuples(result, expected);
    }

    private static class KeySelector4
            implements KeySelector<
                    Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Integer, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, Long> getKey(Tuple5<Integer, Long, Integer, String, Long> t) {
            return new Tuple2<>(t.f0, t.f4);
        }
    }

    @Test
    public void testInputOfCombinerIsSortedForCombinableGroupReduceWithGroupSorting()
            throws Exception {
        /*
         * check that input of combiner is also sorted for combinable groupReduce with group sorting
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> reduceDs =
                ds.groupBy(1)
                        .sortGroup(0, Order.ASCENDING)
                        .reduceGroup(new OrderCheckingCombinableReduce());

        List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

        String expected =
                "1,1,Hi\n"
                        + "2,2,Hello\n"
                        + "4,3,Hello world, how are you?\n"
                        + "7,4,Comment#1\n"
                        + "11,5,Comment#5\n"
                        + "16,6,Comment#10\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testDeepNesting() throws Exception {
        /*
         * Deep nesting test
         * + null value in pojo
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CrazyNested> ds = CollectionDataSets.getCrazyNestedDataSet(env);
        DataSet<Tuple2<String, Integer>> reduceDs =
                ds.groupBy("nestLvl1.nestLvl2.nestLvl3.nestLvl4.f1nal")
                        .reduceGroup(new GroupReducer1());

        List<Tuple2<String, Integer>> result = reduceDs.collect();

        String expected = "aa,1\nbb,2\ncc,3\n";

        compareResultAsTuples(result, expected);
    }

    private static class GroupReducer1
            implements GroupReduceFunction<
                    CollectionDataSets.CrazyNested, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(Iterable<CrazyNested> values, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            int c = 0;
            String n = null;
            for (CrazyNested v : values) {
                c++; // haha
                n = v.nestLvl1.nestLvl2.nestLvl3.nestLvl4.f1nal;
            }
            out.collect(new Tuple2<>(n, c));
        }
    }

    @Test
    public void testPojoExtendingFromTupleWithCustomFields() throws Exception {
        /*
         * Test Pojo extending from tuple WITH custom fields
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<FromTupleWithCTor> ds = CollectionDataSets.getPojoExtendingFromTuple(env);
        DataSet<Integer> reduceDs = ds.groupBy("special", "f2").reduceGroup(new GroupReducer2());

        List<Integer> result = reduceDs.collect();

        String expected = "3\n2\n";

        compareResultAsText(result, expected);
    }

    private static class GroupReducer2 implements GroupReduceFunction<FromTupleWithCTor, Integer> {

        @Override
        public void reduce(Iterable<FromTupleWithCTor> values, Collector<Integer> out) {
            out.collect(countElements(values));
        }
    }

    @Test
    public void testPojoContainigWritableAndTuples() throws Exception {
        /*
         * Test Pojo containing a Writable and Tuples
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<PojoContainingTupleAndWritable> ds =
                CollectionDataSets.getPojoContainingTupleAndWritable(env);
        DataSet<Integer> reduceDs =
                ds.groupBy("hadoopFan", "theTuple.*") // full tuple selection
                        .reduceGroup(new GroupReducer3());

        List<Integer> result = reduceDs.collect();

        String expected = "1\n5\n";

        compareResultAsText(result, expected);
    }

    private static class GroupReducer3
            implements GroupReduceFunction<PojoContainingTupleAndWritable, Integer> {

        @Override
        public void reduce(
                Iterable<PojoContainingTupleAndWritable> values, Collector<Integer> out) {
            out.collect(countElements(values));
        }
    }

    @Test
    public void testTupleContainingPojosAndRegularFields() throws Exception {
        /*
         * Test Tuple containing pojos and regular fields
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, CrazyNested, POJO>> ds =
                CollectionDataSets.getTupleContainingPojos(env);

        DataSet<Integer> reduceDs =
                ds.groupBy("f0", "f1.*") // nested full tuple selection
                        .reduceGroup(new GroupReducer4());

        List<Integer> result = reduceDs.collect();

        String expected = "3\n1\n";

        compareResultAsText(result, expected);
    }

    private static class GroupReducer4
            implements GroupReduceFunction<Tuple3<Integer, CrazyNested, POJO>, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(
                Iterable<Tuple3<Integer, CrazyNested, POJO>> values, Collector<Integer> out) {
            out.collect(countElements(values));
        }
    }

    @Test
    public void testStringBasedDefinitionOnGroupSort() throws Exception {
        /*
         * Test string-based definition on group sort, based on test:
         * check correctness of groupReduce with descending group sort
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> reduceDs =
                ds.groupBy(1)
                        .sortGroup("f2", Order.DESCENDING)
                        .reduceGroup(new Tuple3SortedGroupReduce());

        List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

        String expected =
                "1,1,Hi\n"
                        + "5,2,Hello world-Hello\n"
                        + "15,3,Luke Skywalker-I am fine.-Hello world, how are you?\n"
                        + "34,4,Comment#4-Comment#3-Comment#2-Comment#1\n"
                        + "65,5,Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n"
                        + "111,6,Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testIntBasedDefinitionOnGroupSortForFullNestedTuple() throws Exception {
        /*
         * Test int-based definition on group sort, for (full) nested Tuple
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds =
                CollectionDataSets.getGroupSortedNestedTupleDataSet(env);
        DataSet<String> reduceDs =
                ds.groupBy("f1")
                        .sortGroup(0, Order.DESCENDING)
                        .reduceGroup(new NestedTupleReducer());
        List<String> result = reduceDs.collect();

        String expected = "a--(2,1)-(1,3)-(1,2)-\n" + "b--(2,2)-\n" + "c--(4,9)-(3,6)-(3,3)-\n";

        compareResultAsText(result, expected);
    }

    @Test
    public void testIntBasedDefinitionOnGroupSortForPartialNestedTuple() throws Exception {
        /*
         * Test int-based definition on group sort, for (partial) nested Tuple ASC
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds =
                CollectionDataSets.getGroupSortedNestedTupleDataSet(env);
        // f0.f0 is first integer
        DataSet<String> reduceDs =
                ds.groupBy("f1")
                        .sortGroup("f0.f0", Order.ASCENDING)
                        .sortGroup("f0.f1", Order.ASCENDING)
                        .reduceGroup(new NestedTupleReducer());
        List<String> result = reduceDs.collect();

        String expected = "a--(1,2)-(1,3)-(2,1)-\n" + "b--(2,2)-\n" + "c--(3,3)-(3,6)-(4,9)-\n";

        compareResultAsText(result, expected);
    }

    @Test
    public void testStringBasedDefinitionOnGroupSortForPartialNestedTuple() throws Exception {
        /*
         * Test string-based definition on group sort, for (partial) nested Tuple DESC
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds =
                CollectionDataSets.getGroupSortedNestedTupleDataSet(env);
        // f0.f0 is first integer
        DataSet<String> reduceDs =
                ds.groupBy("f1")
                        .sortGroup("f0.f0", Order.DESCENDING)
                        .reduceGroup(new NestedTupleReducer());
        List<String> result = reduceDs.collect();

        String expected = "a--(2,1)-(1,3)-(1,2)-\n" + "b--(2,2)-\n" + "c--(4,9)-(3,3)-(3,6)-\n";

        compareResultAsText(result, expected);
    }

    @Test
    public void testStringBasedDefinitionOnGroupSortForTwoGroupingKeys() throws Exception {
        /*
         * Test string-based definition on group sort, for two grouping keys
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds =
                CollectionDataSets.getGroupSortedNestedTupleDataSet(env);
        // f0.f0 is first integer
        DataSet<String> reduceDs =
                ds.groupBy("f1")
                        .sortGroup("f0.f0", Order.DESCENDING)
                        .sortGroup("f0.f1", Order.DESCENDING)
                        .reduceGroup(new NestedTupleReducer());
        List<String> result = reduceDs.collect();

        String expected = "a--(2,1)-(1,3)-(1,2)-\n" + "b--(2,2)-\n" + "c--(4,9)-(3,6)-(3,3)-\n";

        compareResultAsText(result, expected);
    }

    @Test
    public void testStringBasedDefinitionOnGroupSortForTwoGroupingKeysWithPojos() throws Exception {
        /*
         * Test string-based definition on group sort, for two grouping keys with Pojos
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<PojoContainingTupleAndWritable> ds =
                CollectionDataSets.getGroupSortedPojoContainingTupleAndWritable(env);
        // f0.f0 is first integer
        DataSet<String> reduceDs =
                ds.groupBy("hadoopFan")
                        .sortGroup("theTuple.f0", Order.DESCENDING)
                        .sortGroup("theTuple.f1", Order.DESCENDING)
                        .reduceGroup(new GroupReducer5());
        List<String> result = reduceDs.collect();

        String expected = "1---(10,100)-\n" + "2---(30,600)-(30,400)-(30,200)-(20,201)-(20,200)-\n";

        compareResultAsText(result, expected);
    }

    @Test
    public void testTupleKeySelectorGroupSort() throws Exception {
        /*
         * check correctness of sorted groupReduce on tuples with keyselector sorting
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> reduceDs =
                ds.groupBy(new LongFieldExtractor<Tuple3<Integer, Long, String>>(1))
                        .sortGroup(
                                new StringFieldExtractor<Tuple3<Integer, Long, String>>(2),
                                Order.DESCENDING)
                        .reduceGroup(new Tuple3SortedGroupReduce());

        List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

        String expected =
                "1,1,Hi\n"
                        + "5,2,Hello world-Hello\n"
                        + "15,3,Luke Skywalker-I am fine.-Hello world, how are you?\n"
                        + "34,4,Comment#4-Comment#3-Comment#2-Comment#1\n"
                        + "65,5,Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n"
                        + "111,6,Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n";

        compareResultAsTuples(result, expected);
    }

    private static class TwoTuplePojoExtractor
            implements KeySelector<CustomType, Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, Integer> getKey(CustomType value) throws Exception {
            return new Tuple2<>(value.myInt, value.myInt);
        }
    }

    private static class StringPojoExtractor implements KeySelector<CustomType, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(CustomType value) throws Exception {
            return value.myString;
        }
    }

    @Test
    public void testPojoKeySelectorGroupSort() throws Exception {
        /*
         * check correctness of sorted groupReduce on custom type with keyselector sorting
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> reduceDs =
                ds.groupBy(new TwoTuplePojoExtractor())
                        .sortGroup(new StringPojoExtractor(), Order.DESCENDING)
                        .reduceGroup(new CustomTypeSortedGroupReduce());

        List<CustomType> result = reduceDs.collect();

        String expected =
                "1,0,Hi\n"
                        + "2,3,Hello world-Hello\n"
                        + "3,12,Luke Skywalker-I am fine.-Hello world, how are you?\n"
                        + "4,30,Comment#4-Comment#3-Comment#2-Comment#1\n"
                        + "5,60,Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n"
                        + "6,105,Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n";

        compareResultAsText(result, expected);
    }

    private static class LongFieldExtractor<T extends Tuple> implements KeySelector<T, Long> {
        private static final long serialVersionUID = 1L;
        private int field;

        public LongFieldExtractor() {}

        public LongFieldExtractor(int field) {
            this.field = field;
        }

        @Override
        public Long getKey(T t) throws Exception {
            return ((Tuple) t).getField(field);
        }
    }

    private static class IntFieldExtractor<T extends Tuple> implements KeySelector<T, Integer> {
        private static final long serialVersionUID = 1L;
        private int field;

        public IntFieldExtractor() {}

        public IntFieldExtractor(int field) {
            this.field = field;
        }

        @Override
        public Integer getKey(T t) throws Exception {
            return ((Tuple) t).getField(field);
        }
    }

    private static class StringFieldExtractor<T extends Tuple> implements KeySelector<T, String> {
        private static final long serialVersionUID = 1L;
        private int field;

        public StringFieldExtractor() {}

        public StringFieldExtractor(int field) {
            this.field = field;
        }

        @Override
        public String getKey(T t) throws Exception {
            return t.getField(field);
        }
    }

    @Test
    public void testTupleKeySelectorSortWithCombine() throws Exception {
        /*
         * check correctness of sorted groupReduce with combine on tuples with keyselector sorting
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple2<Integer, String>> reduceDs =
                ds.groupBy(new LongFieldExtractor<Tuple3<Integer, Long, String>>(1))
                        .sortGroup(
                                new StringFieldExtractor<Tuple3<Integer, Long, String>>(2),
                                Order.DESCENDING)
                        .reduceGroup(new Tuple3SortedGroupReduceWithCombine());

        List<Tuple2<Integer, String>> result = reduceDs.collect();

        if (super.mode != TestExecutionMode.COLLECTION) {
            String expected =
                    "1,Hi\n"
                            + "5,Hello world-Hello\n"
                            + "15,Luke Skywalker-I am fine.-Hello world, how are you?\n"
                            + "34,Comment#4-Comment#3-Comment#2-Comment#1\n"
                            + "65,Comment#9-Comment#8-Comment#7-Comment#6-Comment#5\n"
                            + "111,Comment#15-Comment#14-Comment#13-Comment#12-Comment#11-Comment#10\n";

            compareResultAsTuples(result, expected);
        }
    }

    private static class FiveToTwoTupleExtractor
            implements KeySelector<
                    Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Long, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Long, Integer> getKey(Tuple5<Integer, Long, Integer, String, Long> in) {
            return new Tuple2<>(in.f4, in.f2);
        }
    }

    @Test
    public void testTupleKeySelectorSortCombineOnTuple() throws Exception {
        /*
         * check correctness of sorted groupReduceon with Tuple2 keyselector sorting
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> reduceDs =
                ds.groupBy(new IntFieldExtractor<Tuple5<Integer, Long, Integer, String, Long>>(0))
                        .sortGroup(new FiveToTwoTupleExtractor(), Order.DESCENDING)
                        .reduceGroup(new Tuple5SortedGroupReduce());

        List<Tuple5<Integer, Long, Integer, String, Long>> result = reduceDs.collect();

        String expected =
                "1,1,0,Hallo,1\n"
                        + "2,5,0,Hallo Welt-Hallo Welt wie,1\n"
                        + "3,15,0,BCD-ABC-Hallo Welt wie gehts?,2\n"
                        + "4,34,0,FGH-CDE-EFG-DEF,1\n"
                        + "5,65,0,IJK-HIJ-KLM-JKL-GHI,1\n";

        compareResultAsTuples(result, expected);
    }

    private static class GroupReducer5
            implements GroupReduceFunction<
                    CollectionDataSets.PojoContainingTupleAndWritable, String> {
        @Override
        public void reduce(Iterable<PojoContainingTupleAndWritable> values, Collector<String> out)
                throws Exception {
            boolean once = false;
            StringBuilder concat = new StringBuilder();
            for (PojoContainingTupleAndWritable value : values) {
                if (!once) {
                    concat.append(value.hadoopFan.get());
                    concat.append("---");
                    once = true;
                }
                concat.append(value.theTuple);
                concat.append("-");
            }
            out.collect(concat.toString());
        }
    }

    @Test
    public void testGroupingWithPojoContainingMultiplePojos() throws Exception {
        /*
         * Test grouping with pojo containing multiple pojos (was a bug)
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<CollectionDataSets.PojoWithMultiplePojos> ds =
                CollectionDataSets.getPojoWithMultiplePojos(env);

        // f0.f0 is first integer
        DataSet<String> reduceDs = ds.groupBy("p2.a2").reduceGroup(new GroupReducer6());
        List<String> result = reduceDs.collect();

        String expected = "b\nccc\nee\n";

        compareResultAsText(result, expected);
    }

    private static class GroupReducer6
            implements GroupReduceFunction<CollectionDataSets.PojoWithMultiplePojos, String> {
        @Override
        public void reduce(
                Iterable<CollectionDataSets.PojoWithMultiplePojos> values, Collector<String> out)
                throws Exception {
            StringBuilder concat = new StringBuilder();
            for (CollectionDataSets.PojoWithMultiplePojos value : values) {
                concat.append(value.p2.a2);
            }
            out.collect(concat.toString());
        }
    }

    @Test
    public void testJavaCollectionsWithinPojos() throws Exception {
        /*
         * Test Java collections within pojos ( == test kryo)
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<CollectionDataSets.PojoWithCollection> ds =
                CollectionDataSets.getPojoWithCollection(env);
        // f0.f0 is first integer
        DataSet<String> reduceDs = ds.groupBy("key").reduceGroup(new GroupReducer7());
        List<String> result = reduceDs.collect();

        String expected =
                "callFor key 0 we got: pojo.a=apojo.a=bFor key 0 we got: pojo.a=a2pojo.a=b2\n";

        compareResultAsText(result, expected);
    }

    private static class GroupReducer7
            implements GroupReduceFunction<CollectionDataSets.PojoWithCollection, String> {

        @Override
        public void reduce(
                Iterable<CollectionDataSets.PojoWithCollection> values, Collector<String> out) {
            StringBuilder concat = new StringBuilder();
            concat.append("call");
            for (CollectionDataSets.PojoWithCollection value : values) {
                concat.append("For key ").append(value.key).append(" we got: ");

                for (CollectionDataSets.Pojo1 p : value.pojos) {
                    concat.append("pojo.a=").append(p.a);
                }
            }
            out.collect(concat.toString());
        }
    }

    @Test
    public void testGroupByGenericType() throws Exception {
        /*
         * Group by generic type
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<CollectionDataSets.PojoWithCollection> ds =
                CollectionDataSets.getPojoWithCollection(env);

        // f0.f0 is first integer
        DataSet<String> reduceDs = ds.groupBy("bigInt").reduceGroup(new GroupReducer8());
        List<String> result = reduceDs.collect();
        ExecutionConfig ec = env.getConfig();

        // check if automatic type registration with Kryo worked
        Assert.assertTrue(ec.getRegisteredKryoTypes().contains(BigInt.class));
        Assert.assertFalse(ec.getRegisteredKryoTypes().contains(java.sql.Date.class));

        String expected = null;

        String localExpected =
                "[call\n"
                        + "For key 92233720368547758070 we got:\n"
                        + "PojoWithCollection{pojos.size()=2, key=0, sqlDate=2033-05-18, bigInt=92233720368547758070, bigDecimalKeepItNull=null, scalaBigInt=10, mixed=[{someKey=1}, /this/is/wrong, uhlala]}\n"
                        + "For key 92233720368547758070 we got:\n"
                        + "PojoWithCollection{pojos.size()=2, key=0, sqlDate=1976-05-03, bigInt=92233720368547758070, bigDecimalKeepItNull=null, scalaBigInt=31104000, mixed=null}]";

        Assert.assertEquals(localExpected, result.toString());
    }

    @Test
    public void testGroupReduceSelectorKeysWithSemProps() throws Exception {

        /*
         * Test that semantic properties are correctly adapted when using Selector Keys
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple2<Integer, Long>> reduceDs =
                ds
                        // group by selector key
                        .groupBy(
                                new KeySelector<
                                        Tuple5<Integer, Long, Integer, String, Long>, Long>() {
                                    @Override
                                    public Long getKey(
                                            Tuple5<Integer, Long, Integer, String, Long> v)
                                            throws Exception {
                                        return (v.f0 * v.f1) - (v.f2 * v.f4);
                                    }
                                })
                        .reduceGroup(
                                new GroupReduceFunction<
                                        Tuple5<Integer, Long, Integer, String, Long>,
                                        Tuple5<Integer, Long, Integer, String, Long>>() {
                                    @Override
                                    public void reduce(
                                            Iterable<Tuple5<Integer, Long, Integer, String, Long>>
                                                    values,
                                            Collector<Tuple5<Integer, Long, Integer, String, Long>>
                                                    out)
                                            throws Exception {
                                        for (Tuple5<Integer, Long, Integer, String, Long> v :
                                                values) {
                                            out.collect(v);
                                        }
                                    }
                                })
                        // add forward field information
                        .withForwardedFields("0")
                        // group again and reduce
                        .groupBy(0)
                        .reduceGroup(
                                new GroupReduceFunction<
                                        Tuple5<Integer, Long, Integer, String, Long>,
                                        Tuple2<Integer, Long>>() {
                                    @Override
                                    public void reduce(
                                            Iterable<Tuple5<Integer, Long, Integer, String, Long>>
                                                    values,
                                            Collector<Tuple2<Integer, Long>> out)
                                            throws Exception {
                                        int k = 0;
                                        long s = 0;
                                        for (Tuple5<Integer, Long, Integer, String, Long> v :
                                                values) {
                                            k = v.f0;
                                            s += v.f1;
                                        }
                                        out.collect(new Tuple2<>(k, s));
                                    }
                                });

        List<Tuple2<Integer, Long>> result = reduceDs.collect();

        String expected = "1,1\n" + "2,5\n" + "3,15\n" + "4,34\n" + "5,65\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testGroupReduceWithAtomicValue() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds = env.fromElements(1, 1, 2, 3, 4);
        DataSet<Integer> reduceDs =
                ds.groupBy("*")
                        .reduceGroup(
                                new GroupReduceFunction<Integer, Integer>() {
                                    @Override
                                    public void reduce(
                                            Iterable<Integer> values, Collector<Integer> out)
                                            throws Exception {
                                        out.collect(values.iterator().next());
                                    }
                                });

        List<Integer> result = reduceDs.collect();

        String expected = "1\n" + "2\n" + "3\n" + "4";

        compareResultAsText(result, expected);
    }

    /**
     * Fix for FLINK-2019.
     *
     * @throws Exception
     */
    @Test
    public void testJodatimeDateTimeWithKryo() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, DateTime>> ds = env.fromElements(new Tuple2<>(1, DateTime.now()));
        DataSet<Tuple2<Integer, DateTime>> reduceDs = ds.groupBy("f1").sum(0).project(0);

        List<Tuple2<Integer, DateTime>> result = reduceDs.collect();

        String expected = "1\n";

        compareResultAsTuples(result, expected);
    }

    /**
     * Fix for FLINK-2158.
     *
     * @throws Exception
     */
    @Test
    public void testDateNullException() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Date>> in =
                env.fromElements(
                        new Tuple2<>(0, new Date(1230000000)),
                        new Tuple2<Integer, Date>(1, null),
                        new Tuple2<>(2, new Date(1230000000)));

        DataSet<String> r =
                in.groupBy(0)
                        .reduceGroup(
                                new GroupReduceFunction<Tuple2<Integer, Date>, String>() {
                                    @Override
                                    public void reduce(
                                            Iterable<Tuple2<Integer, Date>> values,
                                            Collector<String> out)
                                            throws Exception {
                                        for (Tuple2<Integer, Date> e : values) {
                                            out.collect(Integer.toString(e.f0));
                                        }
                                    }
                                });

        List<String> result = r.collect();

        String expected = "0\n1\n2\n";
        compareResultAsText(result, expected);
    }

    private static class GroupReducer8
            implements GroupReduceFunction<CollectionDataSets.PojoWithCollection, String> {
        @Override
        public void reduce(
                Iterable<CollectionDataSets.PojoWithCollection> values, Collector<String> out) {
            StringBuilder concat = new StringBuilder();
            concat.append("call");
            for (CollectionDataSets.PojoWithCollection value : values) {
                concat.append("\nFor key ").append(value.bigInt).append(" we got:\n").append(value);
            }
            out.collect(concat.toString());
        }
    }

    private static class NestedTupleReducer
            implements GroupReduceFunction<Tuple2<Tuple2<Integer, Integer>, String>, String> {
        @Override
        public void reduce(
                Iterable<Tuple2<Tuple2<Integer, Integer>, String>> values, Collector<String> out) {
            boolean once = false;
            StringBuilder concat = new StringBuilder();
            for (Tuple2<Tuple2<Integer, Integer>, String> value : values) {
                if (!once) {
                    concat.append(value.f1).append("--");
                    once = true;
                }
                concat.append(value.f0); // the tuple with the sorted groups
                concat.append("-");
            }
            out.collect(concat.toString());
        }
    }

    private static class Tuple3GroupReduce
            implements GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple2<Integer, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple2<Integer, Long>> out) {
            int i = 0;
            long l = 0L;

            for (Tuple3<Integer, Long, String> t : values) {
                i += t.f0;
                l = t.f1;
            }

            out.collect(new Tuple2<>(i, l));
        }
    }

    private static class Tuple3SortedGroupReduce
            implements GroupReduceFunction<
                    Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple3<Integer, Long, String>> out) {
            int sum = 0;
            long key = 0;
            StringBuilder concat = new StringBuilder();

            for (Tuple3<Integer, Long, String> next : values) {
                sum += next.f0;
                key = next.f1;
                concat.append(next.f2).append("-");
            }

            if (concat.length() > 0) {
                concat.setLength(concat.length() - 1);
            }

            out.collect(new Tuple3<>(sum, key, concat.toString()));
        }
    }

    private static class Tuple5GroupReduce
            implements GroupReduceFunction<
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple5<Integer, Long, Integer, String, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(
                Iterable<Tuple5<Integer, Long, Integer, String, Long>> values,
                Collector<Tuple5<Integer, Long, Integer, String, Long>> out) {
            int i = 0;
            long l = 0L;
            long l2 = 0L;

            for (Tuple5<Integer, Long, Integer, String, Long> t : values) {
                i = t.f0;
                l += t.f1;
                l2 = t.f4;
            }

            out.collect(new Tuple5<>(i, l, 0, "P-)", l2));
        }
    }

    private static class Tuple5SortedGroupReduce
            implements GroupReduceFunction<
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple5<Integer, Long, Integer, String, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(
                Iterable<Tuple5<Integer, Long, Integer, String, Long>> values,
                Collector<Tuple5<Integer, Long, Integer, String, Long>> out) {
            int i = 0;
            long l = 0L;
            long l2 = 0L;
            StringBuilder concat = new StringBuilder();

            for (Tuple5<Integer, Long, Integer, String, Long> t : values) {
                i = t.f0;
                l += t.f1;
                concat.append(t.f3).append("-");
                l2 = t.f4;
            }
            if (concat.length() > 0) {
                concat.setLength(concat.length() - 1);
            }

            out.collect(new Tuple5<>(i, l, 0, concat.toString(), l2));
        }
    }

    private static class CustomTypeGroupReduce
            implements GroupReduceFunction<CustomType, CustomType> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(Iterable<CustomType> values, Collector<CustomType> out) {
            final Iterator<CustomType> iter = values.iterator();

            CustomType o = new CustomType();
            CustomType c = iter.next();

            o.myString = "Hello!";
            o.myInt = c.myInt;
            o.myLong = c.myLong;

            while (iter.hasNext()) {
                CustomType next = iter.next();
                o.myLong += next.myLong;
            }

            out.collect(o);
        }
    }

    private static class CustomTypeSortedGroupReduce
            implements GroupReduceFunction<CustomType, CustomType> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(Iterable<CustomType> values, Collector<CustomType> out) {
            final Iterator<CustomType> iter = values.iterator();

            CustomType o = new CustomType();
            CustomType c = iter.next();

            StringBuilder concat = new StringBuilder(c.myString);
            o.myInt = c.myInt;
            o.myLong = c.myLong;

            while (iter.hasNext()) {
                CustomType next = iter.next();
                concat.append("-").append(next.myString);
                o.myLong += next.myLong;
            }

            o.myString = concat.toString();
            out.collect(o);
        }
    }

    private static class InputReturningTuple3GroupReduce
            implements GroupReduceFunction<
                    Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple3<Integer, Long, String>> out) {

            for (Tuple3<Integer, Long, String> t : values) {

                if (t.f0 < 4) {
                    t.f2 = "Hi!";
                    t.f0 += 10;
                    out.collect(t);
                    t.f0 += 10;
                    t.f2 = "Hi again!";
                    out.collect(t);
                }
            }
        }
    }

    private static class AllAddingTuple3GroupReduce
            implements GroupReduceFunction<
                    Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple3<Integer, Long, String>> out) {

            int i = 0;
            long l = 0L;

            for (Tuple3<Integer, Long, String> t : values) {
                i += t.f0;
                l += t.f1;
            }

            out.collect(new Tuple3<>(i, l, "Hello World"));
        }
    }

    private static class AllAddingCustomTypeGroupReduce
            implements GroupReduceFunction<CustomType, CustomType> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(Iterable<CustomType> values, Collector<CustomType> out) {

            CustomType o = new CustomType(0, 0, "Hello!");

            for (CustomType next : values) {
                o.myInt += next.myInt;
                o.myLong += next.myLong;
            }

            out.collect(o);
        }
    }

    private static class BCTuple3GroupReduce
            extends RichGroupReduceFunction<
                    Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;
        private String f2Replace = "";

        @Override
        public void open(OpenContext openContext) {

            Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
            int sum = 0;
            for (Integer i : ints) {
                sum += i;
            }
            f2Replace = sum + "";
        }

        @Override
        public void reduce(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple3<Integer, Long, String>> out) {

            int i = 0;
            long l = 0L;

            for (Tuple3<Integer, Long, String> t : values) {
                i += t.f0;
                l = t.f1;
            }

            out.collect(new Tuple3<>(i, l, this.f2Replace));
        }
    }

    private static class Tuple3GroupReduceWithCombine
            implements GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple2<Integer, String>>,
                    GroupCombineFunction<
                            Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void combine(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple3<Integer, Long, String>> out) {

            Tuple3<Integer, Long, String> o = new Tuple3<>(0, 0L, "");

            for (Tuple3<Integer, Long, String> t : values) {
                o.f0 += t.f0;
                o.f1 = t.f1;
                o.f2 = "test" + o.f1;
            }

            out.collect(o);
        }

        @Override
        public void reduce(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple2<Integer, String>> out) {

            int i = 0;
            String s = "";

            for (Tuple3<Integer, Long, String> t : values) {
                i += t.f0;
                s = t.f2;
            }

            out.collect(new Tuple2<>(i, s));
        }
    }

    private static class Tuple3SortedGroupReduceWithCombine
            implements GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple2<Integer, String>>,
                    GroupCombineFunction<
                            Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void combine(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple3<Integer, Long, String>> out) {
            int sum = 0;
            long key = 0;
            StringBuilder concat = new StringBuilder();

            for (Tuple3<Integer, Long, String> next : values) {
                sum += next.f0;
                key = next.f1;
                concat.append(next.f2).append("-");
            }

            if (concat.length() > 0) {
                concat.setLength(concat.length() - 1);
            }

            out.collect(new Tuple3<>(sum, key, concat.toString()));
        }

        @Override
        public void reduce(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple2<Integer, String>> out) {
            int i = 0;
            String s = "";

            for (Tuple3<Integer, Long, String> t : values) {
                i += t.f0;
                s = t.f2;
            }

            out.collect(new Tuple2<>(i, s));
        }
    }

    private static class Tuple3AllGroupReduceWithCombine
            implements GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple2<Integer, String>>,
                    GroupCombineFunction<
                            Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void combine(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple3<Integer, Long, String>> out) {

            Tuple3<Integer, Long, String> o = new Tuple3<>(0, 0L, "");

            for (Tuple3<Integer, Long, String> t : values) {
                o.f0 += t.f0;
                o.f1 += t.f1;
                o.f2 += "test";
            }

            out.collect(o);
        }

        @Override
        public void reduce(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple2<Integer, String>> out) {

            int i = 0;
            String s = "";

            for (Tuple3<Integer, Long, String> t : values) {
                i += t.f0 + t.f1;
                s += t.f2;
            }

            out.collect(new Tuple2<>(i, s));
        }
    }

    private static class CustomTypeGroupReduceWithCombine
            implements GroupReduceFunction<CustomType, CustomType>,
                    GroupCombineFunction<CustomType, CustomType> {
        private static final long serialVersionUID = 1L;

        @Override
        public void combine(Iterable<CustomType> values, Collector<CustomType> out)
                throws Exception {

            CustomType o = new CustomType();

            for (CustomType c : values) {
                o.myInt = c.myInt;
                o.myLong += c.myLong;
                o.myString = "test" + c.myInt;
            }

            out.collect(o);
        }

        @Override
        public void reduce(Iterable<CustomType> values, Collector<CustomType> out) {

            CustomType o = new CustomType(0, 0, "");

            for (CustomType c : values) {
                o.myInt = c.myInt;
                o.myLong += c.myLong;
                o.myString = c.myString;
            }

            out.collect(o);
        }
    }

    private static class OrderCheckingCombinableReduce
            implements GroupReduceFunction<
                            Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>>,
                    GroupCombineFunction<
                            Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple3<Integer, Long, String>> out)
                throws Exception {
            Iterator<Tuple3<Integer, Long, String>> it = values.iterator();
            Tuple3<Integer, Long, String> t = it.next();

            int i = t.f0;
            out.collect(t);

            while (it.hasNext()) {
                t = it.next();
                if (i > t.f0 || t.f2.equals("INVALID-ORDER!")) {
                    t.f2 = "INVALID-ORDER!";
                    out.collect(t);
                }
            }
        }

        @Override
        public void combine(
                Iterable<Tuple3<Integer, Long, String>> values,
                Collector<Tuple3<Integer, Long, String>> out) {

            Iterator<Tuple3<Integer, Long, String>> it = values.iterator();
            Tuple3<Integer, Long, String> t = it.next();

            int i = t.f0;
            out.collect(t);

            while (it.hasNext()) {
                t = it.next();
                if (i > t.f0) {
                    t.f2 = "INVALID-ORDER!";
                    out.collect(t);
                }
            }
        }
    }

    private static final class IdentityMapper<T> extends RichMapFunction<T, T> {
        @Override
        public T map(T value) {
            return value;
        }
    }

    private static int countElements(Iterable<?> iterable) {
        int c = 0;
        for (@SuppressWarnings("unused") Object o : iterable) {
            c++;
        }
        return c;
    }
}
