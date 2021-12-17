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

package org.apache.flink.test.accumulators;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests cases where accumulators: a) throw errors during runtime b) are not compatible with
 * existing accumulator.
 */
public class AccumulatorErrorITCase extends TestLogger {
    private static final String FAULTY_CLONE_ACCUMULATOR = "faulty-clone";
    private static final String FAULTY_MERGE_ACCUMULATOR = "faulty-merge";
    private static final String INCOMPATIBLE_ACCUMULATORS_NAME = "incompatible-accumulators";

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(3)
                            .build());

    public static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("12m"));
        return config;
    }

    @Test
    public void testFaultyAccumulator() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Test Exception forwarding with faulty Accumulator implementation
        env.generateSequence(0, 10000)
                .map(new FaultyAccumulatorUsingMapper())
                .output(new DiscardingOutputFormat<>());

        assertAccumulatorsShouldFail(env.execute());
    }

    @Test
    public void testInvalidTypeAccumulator() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Test Exception forwarding with faulty Accumulator implementation
        env.generateSequence(0, 10000)
                .map(new IncompatibleAccumulatorTypesMapper())
                .map(new IncompatibleAccumulatorTypesMapper2())
                .output(new DiscardingOutputFormat<>());

        try {
            env.execute();
            fail("Should have failed.");
        } catch (JobExecutionException e) {
            assertTrue(findThrowable(e, UnsupportedOperationException.class).isPresent());
        }
    }

    @Test
    public void testFaultyMergeAccumulator() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Test Exception forwarding with faulty Accumulator implementation
        env.generateSequence(0, 10000)
                .map(new FaultyMergeAccumulatorUsingMapper())
                .output(new DiscardingOutputFormat<>());

        assertAccumulatorsShouldFail(env.execute());
    }

    /* testFaultyAccumulator */

    private static class FaultyAccumulatorUsingMapper extends RichMapFunction<Long, Long> {
        private static final long serialVersionUID = 42;

        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext()
                    .addAccumulator(FAULTY_CLONE_ACCUMULATOR, new FaultyCloneAccumulator());
        }

        @Override
        public Long map(Long value) throws Exception {
            return -1L;
        }
    }

    private static class FaultyCloneAccumulator extends LongCounter {
        private static final long serialVersionUID = 42;

        @Override
        public LongCounter clone() {
            throw new CustomException();
        }
    }

    /* testInvalidTypeAccumulator */

    private static class IncompatibleAccumulatorTypesMapper extends RichMapFunction<Long, Long> {
        private static final long serialVersionUID = 42;

        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext().addAccumulator(INCOMPATIBLE_ACCUMULATORS_NAME, new LongCounter());
        }

        @Override
        public Long map(Long value) throws Exception {
            return -1L;
        }
    }

    private static class IncompatibleAccumulatorTypesMapper2 extends RichMapFunction<Long, Long> {
        private static final long serialVersionUID = 42;

        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext().addAccumulator(INCOMPATIBLE_ACCUMULATORS_NAME, new DoubleCounter());
        }

        @Override
        public Long map(Long value) throws Exception {
            return -1L;
        }
    }

    /** */
    private static class FaultyMergeAccumulatorUsingMapper extends RichMapFunction<Long, Long> {
        private static final long serialVersionUID = 42;

        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext()
                    .addAccumulator(FAULTY_MERGE_ACCUMULATOR, new FaultyMergeAccumulator());
        }

        @Override
        public Long map(Long value) throws Exception {
            return -1L;
        }
    }

    private static class FaultyMergeAccumulator extends LongCounter {
        private static final long serialVersionUID = 42;

        @Override
        public void merge(Accumulator<Long, Long> other) {
            throw new CustomException();
        }

        @Override
        public LongCounter clone() {
            return new FaultyMergeAccumulator();
        }
    }

    private static class CustomException extends RuntimeException {
        private static final long serialVersionUID = 42;
    }

    private static void assertAccumulatorsShouldFail(JobExecutionResult result) {
        try {
            result.getAllAccumulatorResults();
            fail("Should have failed");
        } catch (Exception ex) {
            assertTrue(findThrowable(ex, CustomException.class).isPresent());
        }
    }
}
