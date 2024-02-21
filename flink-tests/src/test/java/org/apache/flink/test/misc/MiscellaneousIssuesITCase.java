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

package org.apache.flink.test.misc;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the system behavior in multiple corner cases - when null records are passed through the
 * system. - when disjoint dataflows are executed - when accumulators are used chained after a
 * non-udf operator.
 *
 * <p>The tests are bundled into one class to reuse the same test cluster. This speeds up test
 * execution, as the majority of the test time goes usually into starting/stopping the test cluster.
 */
@SuppressWarnings("serial")
public class MiscellaneousIssuesITCase extends TestLogger {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(3)
                            .build());

    @Test
    public void testNullValues() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            DataSet<String> data =
                    env.fromElements("hallo")
                            .map(
                                    new MapFunction<String, String>() {
                                        @Override
                                        public String map(String value) throws Exception {
                                            return null;
                                        }
                                    });
            data.writeAsText("/tmp/myTest", FileSystem.WriteMode.OVERWRITE);

            try {
                env.execute();
                fail("this should fail due to null values.");
            } catch (JobExecutionException e) {
                assertTrue(findThrowable(e, NullPointerException.class).isPresent());
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testDisjointDataflows() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(5);

            // generate two different flows
            env.generateSequence(1, 10).output(new DiscardingOutputFormat<Long>());
            env.generateSequence(1, 10).output(new DiscardingOutputFormat<Long>());

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testAccumulatorsAfterNoOp() {

        final String accName = "test_accumulator";

        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(6);

            env.generateSequence(1, 1000000)
                    .rebalance()
                    .flatMap(
                            new RichFlatMapFunction<Long, Long>() {

                                private LongCounter counter;

                                @Override
                                public void open(OpenContext openContext) {
                                    counter = getRuntimeContext().getLongCounter(accName);
                                }

                                @Override
                                public void flatMap(Long value, Collector<Long> out) {
                                    counter.add(1L);
                                }
                            })
                    .output(new DiscardingOutputFormat<Long>());

            JobExecutionResult result = env.execute();

            assertEquals(1000000L, result.getAllAccumulatorResults().get(accName));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
