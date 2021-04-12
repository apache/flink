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

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/** Integration tests for {@link MapPartitionFunction}. */
@SuppressWarnings("serial")
public class MapPartitionITCase extends JavaProgramTestBase {

    private static final String IN =
            "1 1\n2 2\n2 8\n4 4\n4 4\n6 6\n7 7\n8 8\n"
                    + "1 1\n2 2\n2 2\n4 4\n4 4\n6 3\n5 9\n8 8\n1 1\n2 2\n2 2\n3 0\n4 4\n"
                    + "5 9\n7 7\n8 8\n1 1\n9 1\n5 9\n4 4\n4 4\n6 6\n7 7\n8 8\n";

    private static final String RESULT =
            "1 11\n2 12\n4 14\n4 14\n1 11\n2 12\n2 12\n4 14\n4 14\n3 16\n1 11\n2 12\n2 12\n0 13\n4 14\n1 11\n4 14\n4 14\n";

    private List<Tuple2<String, String>> input = new ArrayList<>();

    private List<Tuple2<String, Integer>> expected = new ArrayList<>();

    private List<Tuple2<String, Integer>> result = new ArrayList<>();

    @Override
    protected void preSubmit() throws Exception {

        // create input
        for (String s : IN.split("\n")) {
            String[] fields = s.split(" ");
            input.add(new Tuple2<String, String>(fields[0], fields[1]));
        }

        // create expected
        for (String s : RESULT.split("\n")) {
            String[] fields = s.split(" ");
            expected.add(new Tuple2<String, Integer>(fields[0], Integer.parseInt(fields[1])));
        }
    }

    @Override
    protected void postSubmit() {
        compareResultCollections(
                expected, result, new TestBaseUtils.TupleComparator<Tuple2<String, Integer>>());
    }

    @Override
    protected void testProgram() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, String>> data = env.fromCollection(input);

        data.mapPartition(new TestMapPartition())
                .output(new LocalCollectionOutputFormat<Tuple2<String, Integer>>(result));

        env.execute();
    }

    private static class TestMapPartition
            implements MapPartitionFunction<Tuple2<String, String>, Tuple2<String, Integer>> {

        @Override
        public void mapPartition(
                Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, Integer>> out) {
            for (Tuple2<String, String> value : values) {
                String keyString = value.f0;
                String valueString = value.f1;

                int keyInt = Integer.parseInt(keyString);
                int valueInt = Integer.parseInt(valueString);

                if (keyInt + valueInt < 10) {
                    out.collect(new Tuple2<String, Integer>(valueString, keyInt + 10));
                }
            }
        }
    }
}
