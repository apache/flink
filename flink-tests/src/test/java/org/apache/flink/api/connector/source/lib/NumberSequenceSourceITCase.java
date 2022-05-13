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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * An integration test for the sources based on iterators.
 *
 * <p>This test uses the {@link NumberSequenceSource} as a concrete iterator source implementation,
 * but covers all runtime-related aspects for all the iterator-based sources together.
 */
public class NumberSequenceSourceITCase extends TestLogger {

    private static final int PARALLELISM = 4;

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    // ------------------------------------------------------------------------

    @Test
    public void testParallelSourceExecution() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        final DataStream<Long> stream =
                env.fromSource(
                        new NumberSequenceSource(1L, 1_000L),
                        WatermarkStrategy.noWatermarks(),
                        "iterator source");

        final List<Long> result = stream.executeAndCollect(10000);
        assertThat(result, containsInAnyOrder(LongStream.rangeClosed(1, 1000).boxed().toArray()));
    }

    @Test
    public void testCheckpointingWithDelayedAssignment() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
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
        assertThat(result, contains(LongStream.rangeClosed(0, 100).boxed().toArray()));
    }

    @Test
    public void testLessSplitsThanParallelism() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        int n = PARALLELISM - 2;
        DataStream<Long> stream = env.fromSequence(0, n).map(l -> l);
        List<Long> result = stream.executeAndCollect(100);
        assertThat(result, containsInAnyOrder(LongStream.rangeClosed(0, n).boxed().toArray()));
    }
}
