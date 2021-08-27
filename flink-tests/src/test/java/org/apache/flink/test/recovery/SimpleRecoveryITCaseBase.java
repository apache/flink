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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * A series of tests (reusing one MiniCluster) where tasks fail (one or more time) and the recovery
 * should restart them to verify job completion.
 */
@SuppressWarnings("serial")
public abstract class SimpleRecoveryITCaseBase extends TestLogger {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_WITH_CLIENT_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(4)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Test
    public void testFailedRunThenSuccessfulRun() throws Exception {

        try {
            // attempt 1
            {
                ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                env.setParallelism(4);
                env.setRestartStrategy(RestartStrategies.noRestart());

                try {
                    env.generateSequence(1, 10)
                            .rebalance()
                            .map(new FailingMapper1<>())
                            .reduce(Long::sum)
                            .collect();
                    fail("The program should have failed, but run successfully");
                } catch (JobExecutionException e) {
                    // expected
                }
            }

            // attempt 2
            {
                ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                env.setParallelism(4);
                env.setRestartStrategy(RestartStrategies.noRestart());

                List<Long> resultCollection =
                        env.generateSequence(1, 10)
                                .rebalance()
                                .map(new FailingMapper1<>())
                                .reduce((ReduceFunction<Long>) Long::sum)
                                .collect();

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

    @Test
    public void testRestart() throws Exception {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            env.setParallelism(4);
            // the default restart strategy should be taken

            List<Long> resultCollection =
                    env.generateSequence(1, 10)
                            .rebalance()
                            .map(new FailingMapper2<>())
                            .reduce(Long::sum)
                            .collect();

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
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            env.setParallelism(4);

            List<Long> resultCollection =
                    env.generateSequence(1, 10)
                            .rebalance()
                            .map(new FailingMapper3<>())
                            .reduce(Long::sum)
                            .collect();

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
