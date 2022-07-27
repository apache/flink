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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.SlowTaskDetectorOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.scheduler.adaptivebatch.SpeculativeScheduler;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
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

    private static final AtomicInteger slowTaskCounter = new AtomicInteger(1);

    private int parallelism;

    // the key is the subtask index so that different attempts will not add duplicated results
    private static ConcurrentMap<Integer, Map<Long, Long>> numberCountResults;

    private Map<Long, Long> expectedResult;

    @BeforeEach
    void setUp() {
        parallelism = 4;
        slowTaskCounter.set(1);

        expectedResult =
                LongStream.range(0, NUMBERS_TO_PRODUCE)
                        .boxed()
                        .collect(Collectors.toMap(Function.identity(), i -> 1L));

        NumberCounterMap.toFailCounter.set(0);

        numberCountResults = new ConcurrentHashMap<>();
    }

    @Test
    void testSpeculativeExecution() throws Exception {
        executeJob(this::setupJobWithSlowMap);
        waitUntilJobArchived();
        checkResults();
    }

    @Test
    void testSpeculativeExecutionWithFailover() throws Exception {
        NumberCounterMap.toFailCounter.set(FAILURE_COUNT);
        executeJob(this::setupJobWithSlowMap);
        waitUntilJobArchived();
        checkResults();
    }

    @Test
    void testSpeculativeExecutionWithAdaptiveParallelism() throws Exception {
        parallelism = -1;
        executeJob(this::setupJobWithSlowMap);
        waitUntilJobArchived();
        checkResults();
    }

    @Test
    void testBlockSlowNodeInSpeculativeExecution() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.BLOCK_SLOW_NODE_DURATION, Duration.ofMinutes(1));
        JobClient client = executeJobAsync(configuration, this::setupJobWithSlowMap);

        assertThatThrownBy(
                        () -> client.getJobExecutionResult().get(30, TimeUnit.SECONDS),
                        "The local node is expected to be blocked but it is not.")
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    void testSpeculativeExecutionOfSourceFunction() throws Exception {
        executeJob(this::setupJobWithSlowSourceFunction);
        waitUntilJobArchived();
        checkResults();
    }

    @Test
    void testSpeculativeExecutionOfInputFormatSource() throws Exception {
        executeJob(this::setupJobWithSlowInputFormatSource);
        waitUntilJobArchived();
        checkResults();
    }

    @Test
    void testSpeculativeExecutionOfNewSource() throws Exception {
        executeJob(this::setupJobWithSlowNewSource);
        waitUntilJobArchived();
        checkResults();
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

    private void executeJob(Consumer<StreamExecutionEnvironment> jobSetupFunc) throws Exception {
        JobClient client = executeJobAsync(new Configuration(), jobSetupFunc);
        client.getJobExecutionResult().get();
    }

    private JobClient executeJobAsync(
            Configuration configuration, Consumer<StreamExecutionEnvironment> jobSetupFunc)
            throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configure(configuration));
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(-1);

        jobSetupFunc.accept(env);

        return env.executeAsync();
    }

    private Configuration configure(final Configuration configuration) {
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

        return configuration;
    }

    private void waitUntilJobArchived() throws InterruptedException {
        while (temporaryFolder.getRoot().toFile().listFiles().length < 1) {
            Thread.sleep(1000);
        }
    }

    private void setupJobWithSlowMap(StreamExecutionEnvironment env) {
        final DataStream<Long> source =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(parallelism)
                        .name("source")
                        .slotSharingGroup("sourceGroup");

        final DataStream<Long> map =
                source.rebalance()
                        .map(new NumberCounterMap())
                        .setParallelism(parallelism)
                        .name("map")
                        .slotSharingGroup("mapGroup");

        addSink(map);
    }

    private void setupJobWithSlowSourceFunction(StreamExecutionEnvironment env) {
        final DataStream<Long> source =
                new DataStreamSource<>(
                                env,
                                BasicTypeInfo.LONG_TYPE_INFO,
                                new StreamSource<>(new TestingSourceFunc()),
                                true,
                                "source",
                                Boundedness.BOUNDED)
                        .setParallelism(parallelism)
                        .slotSharingGroup("sourceGroup");
        addSink(source);
    }

    private void setupJobWithSlowInputFormatSource(StreamExecutionEnvironment env) {
        final DataStream<Long> source =
                new DataStreamSource<>(
                                env,
                                BasicTypeInfo.LONG_TYPE_INFO,
                                new StreamSource<>(
                                        new InputFormatSourceFunction<>(
                                                new TestingInputFormat(),
                                                TypeInformation.of(Long.class))),
                                true,
                                "source",
                                Boundedness.BOUNDED)
                        .setParallelism(parallelism)
                        .slotSharingGroup("sourceGroup");
        addSink(source);
    }

    private void setupJobWithSlowNewSource(StreamExecutionEnvironment env) {
        final DataStream<Long> source =
                env.fromSource(
                        new TestingNumberSequenceSource(),
                        WatermarkStrategy.noWatermarks(),
                        "source");
        addSink(source);
    }

    private void addSink(DataStream<Long> dataStream) {
        dataStream
                .rebalance()
                .addSink(new NumberCounterSink())
                .setParallelism(parallelism)
                .name("sink")
                .slotSharingGroup("sinkGroup");
    }

    private static class NumberCounterMap extends RichMapFunction<Long, Long> {
        private static final AtomicInteger toFailCounter = new AtomicInteger(0);

        @Override
        public Long map(Long value) throws Exception {
            if (toFailCounter.decrementAndGet() >= 0) {
                throw new Exception("Forced failure for testing");
            }

            maybeSleep();

            return value;
        }

        private static void reset() {
            toFailCounter.set(0);
        }
    }

    private static class TestingSourceFunc extends RichParallelSourceFunction<Long> {
        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            maybeSleep();

            final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            final int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

            final long start = subtaskIndex * NUMBERS_TO_PRODUCE / numSubtasks;
            final long end = (subtaskIndex + 1) * NUMBERS_TO_PRODUCE / numSubtasks;
            for (long i = start; i < end; i++) {
                ctx.collect(i);
            }
        }

        @Override
        public void cancel() {}
    }

    private static class TestingInputFormat extends GenericInputFormat<Long> {
        private static final int NUM_SPLITS = 100;

        private long nextNumberToEmit;
        private long maxNumberToEmit;
        private boolean end;

        public GenericInputSplit[] createInputSplits(int minNumSplits) {
            final GenericInputSplit[] splits = new GenericInputSplit[NUM_SPLITS];
            for (int i = 0; i < NUM_SPLITS; ++i) {
                splits[i] = new GenericInputSplit(i, NUM_SPLITS);
            }
            return splits;
        }

        @Override
        public boolean reachedEnd() {
            return end;
        }

        @Override
        public Long nextRecord(Long reuse) {
            maybeSleep();

            if (nextNumberToEmit <= maxNumberToEmit) {
                return nextNumberToEmit++;
            }
            end = true;
            return null;
        }

        @Override
        public void open(GenericInputSplit split) throws IOException {
            super.open(split);

            nextNumberToEmit = partitionNumber * NUMBERS_TO_PRODUCE / NUM_SPLITS;
            maxNumberToEmit = (partitionNumber + 1) * NUMBERS_TO_PRODUCE / NUM_SPLITS - 1;
            end = false;
        }
    }

    private static class TestingNumberSequenceSource extends NumberSequenceSource {
        private TestingNumberSequenceSource() {
            super(0, NUMBERS_TO_PRODUCE - 1);
        }

        @Override
        public SourceReader<Long, NumberSequenceSplit> createReader(
                SourceReaderContext readerContext) {
            return new TestingIteratorSourceReader(readerContext);
        }
    }

    private static class TestingIteratorSourceReader<
                    E, IT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IT>>
            extends IteratorSourceReader<E, IT, SplitT> {

        private TestingIteratorSourceReader(SourceReaderContext context) {
            super(context);
        }

        @Override
        public InputStatus pollNext(ReaderOutput<E> output) {
            maybeSleep();
            return super.pollNext(output);
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

    private static void maybeSleep() {
        if (slowTaskCounter.getAndDecrement() > 0) {
            try {
                Thread.sleep(Integer.MAX_VALUE);
            } catch (Exception e) {
                throw new RuntimeException();
            }
        }
    }
}
