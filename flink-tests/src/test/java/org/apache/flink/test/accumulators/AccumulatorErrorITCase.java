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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests cases where accumulators: a) throw errors during runtime b) are not compatible with
 * existing accumulator.
 */
@ExtendWith(TestLoggerExtension.class)
class AccumulatorErrorITCase {
    private static final String FAULTY_CLONE_ACCUMULATOR = "faulty-clone";
    private static final String FAULTY_MERGE_ACCUMULATOR = "faulty-merge";
    private static final String INCOMPATIBLE_ACCUMULATORS_NAME = "incompatible-accumulators";

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(3)
                            .build());

    @Test
    void testFaultyAccumulator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Test Exception forwarding with faulty Accumulator implementation
        env.fromSequence(0, 10000)
                .map(new FaultyAccumulatorUsingMapper())
                .sinkTo(new DiscardingSink<>());

        assertAccumulatorsShouldFail(env.execute(), FaultyCloneAccumulator.class.getName());
    }

    @Test
    void testInvalidTypeAccumulator() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Test Exception forwarding with faulty Accumulator implementation
        env.fromSequence(0, 10000)
                .map(new IncompatibleAccumulatorTypesMapper())
                .map(new IncompatibleAccumulatorTypesMapper2())
                .sinkTo(new DiscardingSink<>());

        assertThatThrownBy(env::execute)
                .cause()
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testFaultyMergeAccumulator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Test Exception forwarding with faulty Accumulator implementation
        env.fromSequence(0, 10000)
                .map(new FaultyMergeAccumulatorUsingMapper())
                .sinkTo(new DiscardingSink<>());

        assertAccumulatorsShouldFail(env.execute(), FaultyMergeAccumulator.class.getName());
    }

    /* testFaultyAccumulator */

    private static class FaultyAccumulatorUsingMapper extends RichMapFunction<Long, Long> {
        private static final long serialVersionUID = 42;

        @Override
        public void open(OpenContext openContext) {
            getRuntimeContext()
                    .addAccumulator(FAULTY_CLONE_ACCUMULATOR, new FaultyCloneAccumulator());
        }

        @Override
        public Long map(Long value) {
            return -1L;
        }
    }

    private static class FaultyCloneAccumulator extends LongCounter {
        private static final long serialVersionUID = 42;

        @Override
        public LongCounter clone() {
            throw new CustomException(FaultyCloneAccumulator.class.getName());
        }
    }

    /* testInvalidTypeAccumulator */

    private static class IncompatibleAccumulatorTypesMapper extends RichMapFunction<Long, Long> {
        private static final long serialVersionUID = 42;

        @Override
        public void open(OpenContext openContext) {
            getRuntimeContext().addAccumulator(INCOMPATIBLE_ACCUMULATORS_NAME, new LongCounter());
        }

        @Override
        public Long map(Long value) {
            return -1L;
        }
    }

    private static class IncompatibleAccumulatorTypesMapper2 extends RichMapFunction<Long, Long> {
        private static final long serialVersionUID = 42;

        @Override
        public void open(OpenContext openContext) {
            getRuntimeContext().addAccumulator(INCOMPATIBLE_ACCUMULATORS_NAME, new DoubleCounter());
        }

        @Override
        public Long map(Long value) {
            return -1L;
        }
    }

    /** */
    private static class FaultyMergeAccumulatorUsingMapper extends RichMapFunction<Long, Long> {
        private static final long serialVersionUID = 42;

        @Override
        public void open(OpenContext openContext) {
            getRuntimeContext()
                    .addAccumulator(FAULTY_MERGE_ACCUMULATOR, new FaultyMergeAccumulator());
        }

        @Override
        public Long map(Long value) {
            return -1L;
        }
    }

    private static class FaultyMergeAccumulator extends LongCounter {
        private static final long serialVersionUID = 42;

        @Override
        public void merge(Accumulator<Long, Long> other) {
            throw new CustomException(FaultyMergeAccumulator.class.getName());
        }

        @Override
        public LongCounter clone() {
            return new FaultyMergeAccumulator();
        }
    }

    private static class CustomException extends RuntimeException {
        private static final long serialVersionUID = 42;

        private CustomException(String message) {
            super(message);
        }
    }

    private static void assertAccumulatorsShouldFail(
            JobExecutionResult result, String expectedMessage) {
        assertThatThrownBy(result::getAllAccumulatorResults)
                .cause()
                .hasCauseInstanceOf(CustomException.class)
                .cause()
                .hasMessage(expectedMessage);
    }
}
