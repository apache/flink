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

package org.apache.flink.test.iterative;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;

import java.util.List;

/** Test iteration with termination criterion consuming the iteration tail. */
public class IterationTerminationWithTwoTails extends JavaProgramTestBase {
    private static final String EXPECTED = "22\n";

    @Override
    protected void testProgram() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataSet<String> initialInput = env.fromElements("1", "2", "3", "4", "5").name("input");

        IterativeDataSet<String> iteration = initialInput.iterate(5).name("Loop");

        DataSet<String> sumReduce =
                iteration.reduceGroup(new SumReducer()).name("Compute sum (GroupReduce");

        DataSet<String> terminationFilter =
                iteration
                        .filter(new TerminationFilter())
                        .name("Compute termination criterion (Map)");

        List<String> result = iteration.closeWith(sumReduce, terminationFilter).collect();

        containsResultAsText(result, EXPECTED);
    }

    private static final class SumReducer implements GroupReduceFunction<String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(Iterable<String> values, Collector<String> out) throws Exception {
            int sum = 0;
            for (String value : values) {
                sum += Integer.parseInt(value) + 1;
            }
            out.collect("" + sum);
        }
    }

    private static class TerminationFilter implements FilterFunction<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(String value) throws Exception {
            return Integer.parseInt(value) < 21;
        }
    }
}
