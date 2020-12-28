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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.runtime.client.JobExecutionException;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A series of tests (reusing one MiniCluster) where tasks fail (one or more time) and the recovery
 * should restart them to verify job completion.
 */
@SuppressWarnings("serial")
public abstract class SimpleRecoveryITCaseBase {

    @Test
    public void testFailedRunThenSuccessfulRun() throws Exception {

        try {
            List<Long> resultCollection = new ArrayList<>();

            // attempt 1
            {
                ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                env.setParallelism(4);
                env.setRestartStrategy(RestartStrategies.noRestart());

                env.generateSequence(1, 10)
                        .rebalance()
                        .map(new FailingMapper1<>())
                        .reduce(Long::sum)
                        .output(new LocalCollectionOutputFormat<>(resultCollection));

                try {
                    JobExecutionResult res = env.execute();
                    String msg =
                            res == null
                                    ? "null result"
                                    : "result in " + res.getNetRuntime() + " ms";
                    fail("The program should have failed, but returned " + msg);
                } catch (JobExecutionException e) {
                    // expected
                }
            }

            // attempt 2
            {
                ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                env.setParallelism(4);
                env.setRestartStrategy(RestartStrategies.noRestart());

                env.generateSequence(1, 10)
                        .rebalance()
                        .map(new FailingMapper1<>())
                        .reduce((ReduceFunction<Long>) Long::sum)
                        .output(new LocalCollectionOutputFormat<>(resultCollection));

                executeAndRunAssertions(env);

                long sum = 0;
                for (long l : resultCollection) {
                    sum += l;
                }
                assertEquals(55, sum);
            }

        } finally {
            FailingMapper1.failuresBeforeSuccess = 1;
        }
    }

    private void executeAndRunAssertions(ExecutionEnvironment env) throws Exception {
        try {
            JobExecutionResult result = env.execute();
            assertTrue(result.getNetRuntime() >= 0);
            assertNotNull(result.getAllAccumulatorResults());
            assertTrue(result.getAllAccumulatorResults().isEmpty());
        } catch (JobExecutionException e) {
            throw new AssertionError("The program should have succeeded on the second run", e);
        }
    }

    @Test
    public void testRestart() throws Exception {
        try {
            List<Long> resultCollection = new ArrayList<>();

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            env.setParallelism(4);
            // the default restart strategy should be taken

            env.generateSequence(1, 10)
                    .rebalance()
                    .map(new FailingMapper2<>())
                    .reduce(Long::sum)
                    .output(new LocalCollectionOutputFormat<>(resultCollection));

            executeAndRunAssertions(env);

            long sum = 0;
            for (long l : resultCollection) {
                sum += l;
            }
            assertEquals(55, sum);
        } finally {
            FailingMapper2.failuresBeforeSuccess = 1;
        }
    }

    @Test
    public void testRestartMultipleTimes() throws Exception {
        try {
            List<Long> resultCollection = new ArrayList<>();

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            env.setParallelism(4);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 100));

            env.generateSequence(1, 10)
                    .rebalance()
                    .map(new FailingMapper3<>())
                    .reduce(Long::sum)
                    .output(new LocalCollectionOutputFormat<>(resultCollection));

            executeAndRunAssertions(env);

            long sum = 0;
            for (long l : resultCollection) {
                sum += l;
            }
            assertEquals(55, sum);
        } finally {
            FailingMapper3.failuresBeforeSuccess = 3;
        }
    }

    // ------------------------------------------------------------------------------------

    private static class FailingMapper1<T> extends RichMapFunction<T, T> {

        private static volatile int failuresBeforeSuccess = 1;

        @Override
        public T map(T value) throws Exception {
            if (failuresBeforeSuccess > 0 && getRuntimeContext().getIndexOfThisSubtask() == 1) {
                failuresBeforeSuccess--;
                throw new Exception("Test Failure");
            }

            return value;
        }
    }

    private static class FailingMapper2<T> extends RichMapFunction<T, T> {

        private static volatile int failuresBeforeSuccess = 1;

        @Override
        public T map(T value) throws Exception {
            if (failuresBeforeSuccess > 0 && getRuntimeContext().getIndexOfThisSubtask() == 1) {
                failuresBeforeSuccess--;
                throw new Exception("Test Failure");
            }

            return value;
        }
    }

    private static class FailingMapper3<T> extends RichMapFunction<T, T> {

        private static volatile int failuresBeforeSuccess = 3;

        @Override
        public T map(T value) throws Exception {
            if (failuresBeforeSuccess > 0 && getRuntimeContext().getIndexOfThisSubtask() == 1) {
                failuresBeforeSuccess--;
                throw new Exception("Test Failure");
            }

            return value;
        }
    }
}
