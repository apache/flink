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

package org.apache.flink.api.connector.source.lib;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An integration test for the sources based on iterators.
 *
 * <p>This test uses the {@link NumberSequenceSource} as a concrete iterator source implementation,
 * but covers all runtime-related aspects for all the iterator-based sources together.
 */
@ExtendWith(TestLoggerExtension.class)
class NumberSequenceSourceITCase {

    private static final int PARALLELISM = 4;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    () ->
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(PARALLELISM)
                                    .build());

    // ------------------------------------------------------------------------

    @Test
    void testParallelSourceExecution() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        final DataStream<Long> stream =
                env.fromSource(
                        new NumberSequenceSource(1L, 1_000L),
                        WatermarkStrategy.noWatermarks(),
                        "iterator source");

        final List<Long> result = stream.executeAndCollect(10000);
        assertThat(result)
                .containsExactlyInAnyOrderElementsOf(
                        LongStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList()));
    }

    @Test
    void testCheckpointingWithDelayedAssignment() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
        final SingleOutputStreamOperator<Long> stream =
                env.fromSequence(0, 100)
                        .map(
                                x -> {
                                    if (x == 0) {
                                        Thread.sleep(50);
                                    }
                                    return x;
                                });
        List<Long> result = stream.executeAndCollect(1000);
        assertThat(result)
                .containsExactlyInAnyOrderElementsOf(
                        LongStream.rangeClosed(0, 100).boxed().collect(Collectors.toList()));
    }

    @Test
    void testLessSplitsThanParallelism() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        int n = PARALLELISM - 2;
        DataStream<Long> stream = env.fromSequence(0, n).map(l -> l);
        List<Long> result = stream.executeAndCollect(100);
        assertThat(result)
                .containsExactlyInAnyOrderElementsOf(
                        LongStream.rangeClosed(0, n).boxed().collect(Collectors.toList()));
    }
}
