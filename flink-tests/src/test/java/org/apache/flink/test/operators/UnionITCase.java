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

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/** Integration tests for {@link DataSet#union}. */
@RunWith(Parameterized.class)
public class UnionITCase extends MultipleProgramsTestBase {

    private static final String FULL_TUPLE_3_STRING =
            "1,1,Hi\n"
                    + "2,2,Hello\n"
                    + "3,2,Hello world\n"
                    + "4,3,Hello world, how are you?\n"
                    + "5,3,I am fine.\n"
                    + "6,3,Luke Skywalker\n"
                    + "7,4,Comment#1\n"
                    + "8,4,Comment#2\n"
                    + "9,4,Comment#3\n"
                    + "10,4,Comment#4\n"
                    + "11,5,Comment#5\n"
                    + "12,5,Comment#6\n"
                    + "13,5,Comment#7\n"
                    + "14,5,Comment#8\n"
                    + "15,5,Comment#9\n"
                    + "16,6,Comment#10\n"
                    + "17,6,Comment#11\n"
                    + "18,6,Comment#12\n"
                    + "19,6,Comment#13\n"
                    + "20,6,Comment#14\n"
                    + "21,6,Comment#15\n";

    public UnionITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testUnion2IdenticalDataSets() throws Exception {
        /*
         * Union of 2 Same Data Sets
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> unionDs =
                ds.union(CollectionDataSets.get3TupleDataSet(env));

        List<Tuple3<Integer, Long, String>> result = unionDs.collect();

        String expected = FULL_TUPLE_3_STRING + FULL_TUPLE_3_STRING;

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testUnion5IdenticalDataSets() throws Exception {
        /*
         * Union of 5 same Data Sets, with multiple unions
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> unionDs =
                ds.union(CollectionDataSets.get3TupleDataSet(env))
                        .union(CollectionDataSets.get3TupleDataSet(env))
                        .union(CollectionDataSets.get3TupleDataSet(env))
                        .union(CollectionDataSets.get3TupleDataSet(env));

        List<Tuple3<Integer, Long, String>> result = unionDs.collect();

        String expected =
                FULL_TUPLE_3_STRING
                        + FULL_TUPLE_3_STRING
                        + FULL_TUPLE_3_STRING
                        + FULL_TUPLE_3_STRING
                        + FULL_TUPLE_3_STRING;

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testUnionWithEmptyDataSet() throws Exception {
        /*
         * Test on union with empty dataset
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Don't know how to make an empty result in an other way than filtering it
        DataSet<Tuple3<Integer, Long, String>> empty =
                CollectionDataSets.get3TupleDataSet(env).filter(new RichFilter1());

        DataSet<Tuple3<Integer, Long, String>> unionDs =
                CollectionDataSets.get3TupleDataSet(env).union(empty);

        List<Tuple3<Integer, Long, String>> result = unionDs.collect();

        String expected = FULL_TUPLE_3_STRING;

        compareResultAsTuples(result, expected);
    }

    private static class RichFilter1 extends RichFilterFunction<Tuple3<Integer, Long, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
            return false;
        }
    }
}
