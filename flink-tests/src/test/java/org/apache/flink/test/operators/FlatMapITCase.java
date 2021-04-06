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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.operators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

/** Integration tests for {@link FlatMapFunction} and {@link RichFlatMapFunction}. */
@RunWith(Parameterized.class)
public class FlatMapITCase extends MultipleProgramsTestBase {
    public FlatMapITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testNonPassingFlatMap() throws Exception {
        /*
         * Test non-passing flatmap
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> ds = CollectionDataSets.getStringDataSet(env);
        DataSet<String> nonPassingFlatMapDs = ds.flatMap(new FlatMapper1());

        List<String> result = nonPassingFlatMapDs.collect();

        String expected = "\n";

        compareResultAsText(result, expected);
    }

    private static class FlatMapper1 implements FlatMapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (value.contains("bananas")) {
                out.collect(value);
            }
        }
    }

    @Test
    public void testDataDuplicatingFlatMap() throws Exception {
        /*
         * Test data duplicating flatmap
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> ds = CollectionDataSets.getStringDataSet(env);
        DataSet<String> duplicatingFlatMapDs = ds.flatMap(new FlatMapper2());

        List<String> result = duplicatingFlatMapDs.collect();

        String expected =
                "Hi\n"
                        + "HI\n"
                        + "Hello\n"
                        + "HELLO\n"
                        + "Hello world\n"
                        + "HELLO WORLD\n"
                        + "Hello world, how are you?\n"
                        + "HELLO WORLD, HOW ARE YOU?\n"
                        + "I am fine.\n"
                        + "I AM FINE.\n"
                        + "Luke Skywalker\n"
                        + "LUKE SKYWALKER\n"
                        + "Random comment\n"
                        + "RANDOM COMMENT\n"
                        + "LOL\n"
                        + "LOL\n";

        compareResultAsText(result, expected);
    }

    private static class FlatMapper2 implements FlatMapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            out.collect(value);
            out.collect(value.toUpperCase());
        }
    }

    @Test
    public void testFlatMapWithVaryingNumberOfEmittedTuples() throws Exception {
        /*
         * Test flatmap with varying number of emitted tuples
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> varyingTuplesMapDs = ds.flatMap(new FlatMapper3());

        List<Tuple3<Integer, Long, String>> result = varyingTuplesMapDs.collect();

        String expected =
                "1,1,Hi\n"
                        + "2,2,Hello\n"
                        + "2,2,Hello\n"
                        + "4,3,Hello world, how are you?\n"
                        + "5,3,I am fine.\n"
                        + "5,3,I am fine.\n"
                        + "7,4,Comment#1\n"
                        + "8,4,Comment#2\n"
                        + "8,4,Comment#2\n"
                        + "10,4,Comment#4\n"
                        + "11,5,Comment#5\n"
                        + "11,5,Comment#5\n"
                        + "13,5,Comment#7\n"
                        + "14,5,Comment#8\n"
                        + "14,5,Comment#8\n"
                        + "16,6,Comment#10\n"
                        + "17,6,Comment#11\n"
                        + "17,6,Comment#11\n"
                        + "19,6,Comment#13\n"
                        + "20,6,Comment#14\n"
                        + "20,6,Comment#14\n";

        compareResultAsTuples(result, expected);
    }

    private static class FlatMapper3
            implements FlatMapFunction<
                    Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(
                Tuple3<Integer, Long, String> value, Collector<Tuple3<Integer, Long, String>> out)
                throws Exception {
            final int numTuples = value.f0 % 3;
            for (int i = 0; i < numTuples; i++) {
                out.collect(value);
            }
        }
    }

    @Test
    public void testTypeConversionFlatMapperCustomToTuple() throws Exception {
        /*
         * Test type conversion flatmapper (Custom -> Tuple)
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> typeConversionFlatMapDs =
                ds.flatMap(new FlatMapper4());

        List<Tuple3<Integer, Long, String>> result = typeConversionFlatMapDs.collect();

        String expected =
                "1,0,Hi\n"
                        + "2,1,Hello\n"
                        + "2,2,Hello world\n"
                        + "3,3,Hello world, how are you?\n"
                        + "3,4,I am fine.\n"
                        + "3,5,Luke Skywalker\n"
                        + "4,6,Comment#1\n"
                        + "4,7,Comment#2\n"
                        + "4,8,Comment#3\n"
                        + "4,9,Comment#4\n"
                        + "5,10,Comment#5\n"
                        + "5,11,Comment#6\n"
                        + "5,12,Comment#7\n"
                        + "5,13,Comment#8\n"
                        + "5,14,Comment#9\n"
                        + "6,15,Comment#10\n"
                        + "6,16,Comment#11\n"
                        + "6,17,Comment#12\n"
                        + "6,18,Comment#13\n"
                        + "6,19,Comment#14\n"
                        + "6,20,Comment#15\n";

        compareResultAsTuples(result, expected);
    }

    private static class FlatMapper4
            implements FlatMapFunction<CustomType, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;
        private final Tuple3<Integer, Long, String> outTuple = new Tuple3<Integer, Long, String>();

        @Override
        public void flatMap(CustomType value, Collector<Tuple3<Integer, Long, String>> out)
                throws Exception {
            outTuple.setField(value.myInt, 0);
            outTuple.setField(value.myLong, 1);
            outTuple.setField(value.myString, 2);
            out.collect(outTuple);
        }
    }

    @Test
    public void testTypeConversionFlatMapperTupleToBasic() throws Exception {
        /*
         * Test type conversion flatmapper (Tuple -> Basic)
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<String> typeConversionFlatMapDs = ds.flatMap(new FlatMapper5());

        List<String> result = typeConversionFlatMapDs.collect();

        String expected =
                "Hi\n"
                        + "Hello\n"
                        + "Hello world\n"
                        + "Hello world, how are you?\n"
                        + "I am fine.\n"
                        + "Luke Skywalker\n"
                        + "Comment#1\n"
                        + "Comment#2\n"
                        + "Comment#3\n"
                        + "Comment#4\n"
                        + "Comment#5\n"
                        + "Comment#6\n"
                        + "Comment#7\n"
                        + "Comment#8\n"
                        + "Comment#9\n"
                        + "Comment#10\n"
                        + "Comment#11\n"
                        + "Comment#12\n"
                        + "Comment#13\n"
                        + "Comment#14\n"
                        + "Comment#15\n";

        compareResultAsText(result, expected);
    }

    private static class FlatMapper5
            implements FlatMapFunction<Tuple3<Integer, Long, String>, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(Tuple3<Integer, Long, String> value, Collector<String> out)
                throws Exception {
            out.collect(value.f2);
        }
    }

    @Test
    public void testFlatMapperIfUDFReturnsInputObjectMultipleTimesWhileChangingIt()
            throws Exception {
        /*
         * Test flatmapper if UDF returns input object
         * multiple times and changes it in between
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> inputObjFlatMapDs = ds.flatMap(new FlatMapper6());

        List<Tuple3<Integer, Long, String>> result = inputObjFlatMapDs.collect();

        String expected =
                "0,1,Hi\n"
                        + "0,2,Hello\n"
                        + "1,2,Hello\n"
                        + "0,2,Hello world\n"
                        + "1,2,Hello world\n"
                        + "2,2,Hello world\n"
                        + "0,3,I am fine.\n"
                        + "0,3,Luke Skywalker\n"
                        + "1,3,Luke Skywalker\n"
                        + "0,4,Comment#1\n"
                        + "1,4,Comment#1\n"
                        + "2,4,Comment#1\n"
                        + "0,4,Comment#3\n"
                        + "0,4,Comment#4\n"
                        + "1,4,Comment#4\n"
                        + "0,5,Comment#5\n"
                        + "1,5,Comment#5\n"
                        + "2,5,Comment#5\n"
                        + "0,5,Comment#7\n"
                        + "0,5,Comment#8\n"
                        + "1,5,Comment#8\n"
                        + "0,5,Comment#9\n"
                        + "1,5,Comment#9\n"
                        + "2,5,Comment#9\n"
                        + "0,6,Comment#11\n"
                        + "0,6,Comment#12\n"
                        + "1,6,Comment#12\n"
                        + "0,6,Comment#13\n"
                        + "1,6,Comment#13\n"
                        + "2,6,Comment#13\n"
                        + "0,6,Comment#15\n";

        compareResultAsTuples(result, expected);
    }

    private static class FlatMapper6
            implements FlatMapFunction<
                    Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(
                Tuple3<Integer, Long, String> value, Collector<Tuple3<Integer, Long, String>> out)
                throws Exception {
            final int numTuples = value.f0 % 4;
            for (int i = 0; i < numTuples; i++) {
                value.setField(i, 0);
                out.collect(value);
            }
        }
    }

    @Test
    public void testFlatMapWithBroadcastSet() throws Exception {
        /*
         * Test flatmap with broadcast set
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> ints = CollectionDataSets.getIntegerDataSet(env);

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> bcFlatMapDs =
                ds.flatMap(new RichFlatMapper1()).withBroadcastSet(ints, "ints");
        List<Tuple3<Integer, Long, String>> result = bcFlatMapDs.collect();

        String expected =
                "55,1,Hi\n"
                        + "55,2,Hello\n"
                        + "55,2,Hello world\n"
                        + "55,3,Hello world, how are you?\n"
                        + "55,3,I am fine.\n"
                        + "55,3,Luke Skywalker\n"
                        + "55,4,Comment#1\n"
                        + "55,4,Comment#2\n"
                        + "55,4,Comment#3\n"
                        + "55,4,Comment#4\n"
                        + "55,5,Comment#5\n"
                        + "55,5,Comment#6\n"
                        + "55,5,Comment#7\n"
                        + "55,5,Comment#8\n"
                        + "55,5,Comment#9\n"
                        + "55,6,Comment#10\n"
                        + "55,6,Comment#11\n"
                        + "55,6,Comment#12\n"
                        + "55,6,Comment#13\n"
                        + "55,6,Comment#14\n"
                        + "55,6,Comment#15\n";

        compareResultAsTuples(result, expected);
    }

    private static class RichFlatMapper1
            extends RichFlatMapFunction<
                    Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;
        private final Tuple3<Integer, Long, String> outTuple = new Tuple3<Integer, Long, String>();
        private Integer f2Replace = 0;

        @Override
        public void open(Configuration config) {
            Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
            int sum = 0;
            for (Integer i : ints) {
                sum += i;
            }
            f2Replace = sum;
        }

        @Override
        public void flatMap(
                Tuple3<Integer, Long, String> value, Collector<Tuple3<Integer, Long, String>> out)
                throws Exception {
            outTuple.setFields(f2Replace, value.f1, value.f2);
            out.collect(outTuple);
        }
    }
}
