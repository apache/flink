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

package org.apache.flink.test.example.failing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.JavaProgramTestBaseJUnit4;
import org.apache.flink.util.CollectionUtil;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.test.util.TestBaseUtils.compareResultAsText;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.junit.Assert.assertTrue;

/**
 * Tests that both jobs, the failing and the working one, are handled correctly. The first (failing)
 * job must be canceled and the client must report the failure. The second (working) job must finish
 * successfully and compute the correct result.
 */
public class TaskFailureITCase extends JavaProgramTestBaseJUnit4 {

    private static final String EXCEPTION_STRING = "This is an expected Test Exception";

    @Override
    protected void testProgram() throws Exception {
        // test failing version
        try {
            executeTask(new FailingTestMapper(), 1);
        } catch (RuntimeException e) { // expected for cluster execution
            // for cluster execution, one restart. So, exception should be appended with 1.
            assertTrue(findThrowableWithMessage(e, EXCEPTION_STRING + ":1").isPresent());
        }
        // test correct version
        executeTask(new TestMapper(), 0);
    }

    private void executeTask(MapFunction<Long, Long> mapper, int retries) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, retries);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(0));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        List<Long> result =
                CollectionUtil.iteratorToList(
                        env.fromSequence(1, 9).map(mapper).executeAndCollect());
        compareResultAsText(result, "1\n2\n3\n4\n5\n6\n7\n8\n9");
    }

    /** Working map function. */
    public static class TestMapper implements MapFunction<Long, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long map(Long value) throws Exception {
            return value;
        }
    }

    /** Failing map function. */
    public static class FailingTestMapper extends RichMapFunction<Long, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long map(Long value) throws Exception {
            throw new RuntimeException(
                    EXCEPTION_STRING + ":" + getRuntimeContext().getTaskInfo().getAttemptNumber());
        }
    }
}
