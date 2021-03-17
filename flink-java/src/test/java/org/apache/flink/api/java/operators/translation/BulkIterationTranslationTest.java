/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.base.BulkIterationBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Simple test for validating the parallelism of a bulk iteration. This test is not as comprehensive
 * as {@link DeltaIterationTranslationTest}.
 */
@SuppressWarnings("serial")
public class BulkIterationTranslationTest implements java.io.Serializable {

    @Test
    public void testCorrectTranslation() {
        final String jobName = "Test JobName";

        final int numIterations = 13;

        final int defaultParallelism = 133;
        final int iterationParallelism = 77;

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // ------------ construct the test program ------------------

        {
            env.setParallelism(defaultParallelism);

            @SuppressWarnings("unchecked")
            DataSet<Tuple3<Double, Long, String>> initialDataSet =
                    env.fromElements(new Tuple3<>(3.44, 5L, "abc"));

            IterativeDataSet<Tuple3<Double, Long, String>> bulkIteration =
                    initialDataSet.iterate(numIterations);
            bulkIteration.setParallelism(iterationParallelism);

            // test that multiple iteration consumers are supported
            DataSet<Tuple3<Double, Long, String>> identity =
                    bulkIteration.map(new IdentityMapper<Tuple3<Double, Long, String>>());

            DataSet<Tuple3<Double, Long, String>> result = bulkIteration.closeWith(identity);

            result.output(new DiscardingOutputFormat<Tuple3<Double, Long, String>>());
            result.writeAsText("/dev/null");
        }

        Plan p = env.createProgramPlan(jobName);

        // ------------- validate the plan ----------------

        BulkIterationBase<?> iteration =
                (BulkIterationBase<?>) p.getDataSinks().iterator().next().getInput();

        assertEquals(jobName, p.getJobName());
        assertEquals(defaultParallelism, p.getDefaultParallelism());
        assertEquals(iterationParallelism, iteration.getParallelism());
    }

    // --------------------------------------------------------------------------------------------

    private static class IdentityMapper<T> extends RichMapFunction<T, T> {
        @Override
        public T map(T value) throws Exception {
            return value;
        }
    }
}
