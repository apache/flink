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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Integration tests for {@link org.apache.flink.api.scala.GroupedDataSet#min} and {@link
 * org.apache.flink.api.scala.GroupedDataSet#max}.
 */
@RunWith(Parameterized.class)
public class SumMinMaxITCase extends MultipleProgramsTestBase {

    public SumMinMaxITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testSumMaxAndProject() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple2<Integer, Long>> sumDs = ds.sum(0).andMax(1).project(0, 1);

        List<Tuple2<Integer, Long>> result = sumDs.collect();

        String expected = "231,6\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testGroupedAggregate() throws Exception {
        /*
         * Grouped Aggregate
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple2<Long, Integer>> aggregateDs = ds.groupBy(1).sum(0).project(1, 0);

        List<Tuple2<Long, Integer>> result = aggregateDs.collect();

        String expected = "1,1\n" + "2,5\n" + "3,15\n" + "4,34\n" + "5,65\n" + "6,111\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testNestedAggregate() throws Exception {
        /*
         * Nested Aggregate
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple1<Integer>> aggregateDs = ds.groupBy(1).min(0).min(0).project(0);

        List<Tuple1<Integer>> result = aggregateDs.collect();

        String expected = "1\n";

        compareResultAsTuples(result, expected);
    }
}
