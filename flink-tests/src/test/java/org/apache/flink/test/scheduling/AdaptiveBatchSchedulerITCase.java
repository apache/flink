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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchScheduler;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for {@link AdaptiveBatchScheduler}. */
class AdaptiveBatchSchedulerITCase {

    private static final int DEFAULT_MAX_PARALLELISM = 4;
    private static final int SOURCE_PARALLELISM_1 = 2;
    private static final int SOURCE_PARALLELISM_2 = 8;
    private static final int NUMBERS_TO_PRODUCE = 10000;

    private static ConcurrentLinkedQueue<Map<Long, Long>> numberCountResults;

    private Map<Long, Long> expectedResult;

    @BeforeEach
    void setUp() {
        expectedResult =
                LongStream.range(0, NUMBERS_TO_PRODUCE)
                        .boxed()
                        .collect(Collectors.toMap(Function.identity(), i -> 2L));

        numberCountResults = new ConcurrentLinkedQueue<>();
    }

    @Test
    void testSchedulingWithUnknownResource() throws Exception {
        testScheduling(false);
    }

    @Test
    void testSchedulingWithFineGrainedResource() throws Exception {
        testScheduling(true);
    }

    @Test
    void testParallelismOfForwardGroupLargerThanGlobalMaxParallelism() throws Exception {
        final Configuration configuration = createConfiguration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(8);

        final DataStream<Long> source =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(8)
                        .name("source")
                        .slotSharingGroup("group1");

        source.forward().map(new NumberCounter()).name("map").slotSharingGroup("group2");
        env.execute();
    }

    @Test
    void testDifferentConsumerParallelism() throws Exception {
        final Configuration configuration = createConfiguration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(8);

        final DataStream<Long> source2 =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(8)
                        .name("source2")
                        .slotSharingGroup("group2");

        final DataStream<Long> source1 =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(8)
                        .name("source1")
                        .slotSharingGroup("group1");

        source1.forward()
                .union(source2)
                .map(new NumberCounter())
                .name("map1")
                .slotSharingGroup("group3");

        source2.map(new NumberCounter()).name("map2").slotSharingGroup("group4");

        env.execute();
    }

    private void testScheduling(Boolean isFineGrained) throws Exception {
        executeJob(isFineGrained);

        Map<Long, Long> numberCountResultMap =
                numberCountResults.stream()
                        .flatMap(map -> map.entrySet().stream())
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (v1, v2) -> v1 + v2));

        for (int i = 0; i < NUMBERS_TO_PRODUCE; i++) {
            if (numberCountResultMap.get(i) != expectedResult.get(i)) {
                System.out.println(i + ": " + numberCountResultMap.get(i));
            }
        }
        assertThat(numberCountResultMap).isEqualTo(expectedResult);
    }

    private void executeJob(Boolean isFineGrained) throws Exception {
        final Configuration configuration = createConfiguration();
        if (isFineGrained) {
            configuration.set(ClusterOptions.ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT, true);
            configuration.set(ClusterOptions.FINE_GRAINED_SHUFFLE_MODE_ALL_BLOCKING, true);
        }

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        List<SlotSharingGroup> slotSharingGroups = new ArrayList<>();

        for (int i = 0; i < 3; ++i) {
            SlotSharingGroup group;
            if (isFineGrained) {
                group =
                        SlotSharingGroup.newBuilder("group" + i)
                                .setCpuCores(1.0)
                                .setTaskHeapMemory(MemorySize.parse("100m"))
                                .build();
            } else {
                group = SlotSharingGroup.newBuilder("group" + i).build();
            }
            slotSharingGroups.add(group);
        }

        final DataStream<Long> source1 =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(SOURCE_PARALLELISM_1)
                        .name("source1")
                        .slotSharingGroup(slotSharingGroups.get(0));

        final DataStream<Long> source2 =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(SOURCE_PARALLELISM_2)
                        .name("source2")
                        .slotSharingGroup(slotSharingGroups.get(1));

        source1.union(source2)
                .rescale()
                .map(new NumberCounter())
                .name("map")
                .slotSharingGroup(slotSharingGroups.get(2));

        env.execute();
    }

    private static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT, "0");
        configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 5000L);
        configuration.setInteger(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM,
                DEFAULT_MAX_PARALLELISM);
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK,
                MemorySize.parse("150kb"));
        configuration.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4kb"));
        configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);

        return configuration;
    }

    private static class NumberCounter extends RichMapFunction<Long, Long> {

        private final Map<Long, Long> numberCountResult = new HashMap<>();

        @Override
        public Long map(Long value) throws Exception {
            numberCountResult.put(value, numberCountResult.getOrDefault(value, 0L) + 1L);

            return value;
        }

        @Override
        public void close() throws Exception {
            numberCountResults.add(numberCountResult);
        }
    }
}
