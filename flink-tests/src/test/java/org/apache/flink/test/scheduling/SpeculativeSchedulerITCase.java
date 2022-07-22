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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.SlowTaskDetectorOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.scheduler.adaptivebatch.SpeculativeScheduler;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link SpeculativeScheduler}. */
class SpeculativeSchedulerITCase {

    @TempDir private Path temporaryFolder;
    private static final int MAX_PARALLELISM = 4;
    private static final int NUMBERS_TO_PRODUCE = 10000;
    private static final int FAILURE_COUNT = 20;

    private int parallelism;

    // the key is the subtask index so that different attempts will not add duplicated results
    private static ConcurrentMap<Integer, Map<Long, Long>> numberCountResults;

    private Map<Long, Long> expectedResult;

    @BeforeEach
    void setUp() {
        parallelism = 4;

        expectedResult =
                LongStream.range(0, NUMBERS_TO_PRODUCE)
                        .boxed()
                        .collect(Collectors.toMap(Function.identity(), i -> 1L));

        NumberCounterMap.toFailCounter.set(0);

        numberCountResults = new ConcurrentHashMap<>();
    }

    @Test
    void testSpeculativeExecution() throws Exception {
        executeJob();
        waitUntilJobArchived();
        checkResults();
    }

    @Test
    void testSpeculativeExecutionWithFailover() throws Exception {
        NumberCounterMap.toFailCounter.set(FAILURE_COUNT);
        executeJob();
        waitUntilJobArchived();
        checkResults();
    }

    @Test
    void testSpeculativeExecutionWithAdaptiveParallelism() throws Exception {
        parallelism = -1;
        executeJob();
        waitUntilJobArchived();
        checkResults();
    }

    @Test
    void testBlockSlowNodeInSpeculativeExecution() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.BLOCK_SLOW_NODE_DURATION, Duration.ofMinutes(1));
        JobClient client = executeJobAsync(configuration);

        assertThatThrownBy(
                        () -> client.getJobExecutionResult().get(30, TimeUnit.SECONDS),
                        "The local node is expected to be blocked but it is not.")
                .isInstanceOf(TimeoutException.class);
    }

    private void checkResults() {
        final Map<Long, Long> numberCountResultMap =
                numberCountResults.values().stream()
                        .flatMap(map -> map.entrySet().stream())
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, Map.Entry::getValue, Long::sum));

        assertThat(numberCountResultMap).isEqualTo(expectedResult);
    }

    private void executeJob() throws Exception {
        JobClient client = executeJobAsync(new Configuration());
        client.getJobExecutionResult().get();
    }

    private JobClient executeJobAsync(Configuration configuration) throws Exception {
        configuration.setString(RestOptions.BIND_PORT, "0");
        configuration.set(JobManagerOptions.ARCHIVE_DIR, temporaryFolder.getRoot().toString());
        configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 5000L);
        configuration.set(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE, 1);
        configuration.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4kb"));
        configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, MAX_PARALLELISM);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);

        // for speculative execution
        configuration.set(
                JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.AdaptiveBatch);
        configuration.set(JobManagerOptions.SPECULATIVE_ENABLED, true);
        // for testing, does not block node by default
        if (!configuration.contains(JobManagerOptions.BLOCK_SLOW_NODE_DURATION)) {
            configuration.set(JobManagerOptions.BLOCK_SLOW_NODE_DURATION, Duration.ZERO);
        }
        configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_MULTIPLIER, 1.0);
        configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_RATIO, 0.2);
        configuration.set(
                SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_LOWER_BOUND, Duration.ofMillis(0));

        // for adaptive parallelism
        configuration.set(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_DEFAULT_SOURCE_PARALLELISM,
                MAX_PARALLELISM);
        configuration.set(JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_MIN_PARALLELISM, 1);
        configuration.set(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_MAX_PARALLELISM, MAX_PARALLELISM);
        configuration.set(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_AVG_DATA_VOLUME_PER_TASK,
                MemorySize.parse("150kb"));

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(-1);

        final DataStream<Long> source =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(parallelism)
                        .name("source")
                        .slotSharingGroup("group1");

        final DataStream<Long> map =
                source.rebalance()
                        .map(new NumberCounterMap())
                        .setParallelism(parallelism)
                        .name("map")
                        .slotSharingGroup("group2");

        map.rebalance()
                .addSink(new NumberCounterSink())
                .setParallelism(parallelism)
                .name("sink")
                .slotSharingGroup("group3");

        return env.executeAsync();
    }

    private void waitUntilJobArchived() throws InterruptedException {
        while (temporaryFolder.getRoot().toFile().listFiles().length < 1) {
            Thread.sleep(1000);
        }
    }

    private static class NumberCounterMap extends RichMapFunction<Long, Long> {
        private static final AtomicInteger toFailCounter = new AtomicInteger(0);

        @Override
        public Long map(Long value) throws Exception {
            if (toFailCounter.decrementAndGet() >= 0) {
                throw new Exception("Forced failure for testing");
            }

            if (isSlowTask()) {
                Thread.sleep(1_000_000);
            }
            return value;
        }

        private boolean isSlowTask() {
            return (getRuntimeContext().getAttemptNumber()
                                    + getRuntimeContext().getIndexOfThisSubtask())
                            % 2
                    == 1;
        }
    }

    private static class NumberCounterSink extends RichSinkFunction<Long> {

        private final Map<Long, Long> numberCountResult = new HashMap<>();

        @Override
        public void invoke(Long value, Context context) throws Exception {
            numberCountResult.merge(value, 1L, Long::sum);
        }

        @Override
        public void finish() {
            numberCountResults.put(getRuntimeContext().getIndexOfThisSubtask(), numberCountResult);
        }
    }
}
