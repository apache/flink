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
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.BatchExecutionOptions;
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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.scheduler.adaptivebatch.SpeculativeScheduler;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
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
        configuration.set(BatchExecutionOptions.BLOCK_SLOW_NODE_DURATION, Duration.ofMinutes(1));
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

    @Test
    public void testSpeculativeSlowSink() throws Exception {
        executeJob(this::setupSpeculativeSlowSink);
        waitUntilJobArchived();

        checkResults();

        // no speculative executions for committer
        assertThat(DummyCommitter.attempts.get()).isEqualTo(parallelism);
        // there is a speculative execution for writer
        assertThat(DummyCommitter.foundSpeculativeWriter).isTrue();
    }

    @Test
    public void testNonSpeculativeSlowSinkFunction() throws Exception {
        executeJob(this::setupNonSpeculativeSlowSinkFunction);
        waitUntilJobArchived();

        checkResults();
    }

    @Test
    public void testSpeculativeSlowSinkFunction() throws Exception {
        executeJob(this::setupSpeculativeSlowSinkFunction);
        waitUntilJobArchived();

        checkResults();
    }

    @Test
    public void testSpeculativeOutputFormatSink() throws Exception {
        executeJob(this::setupSlowOutputFormatSink);
        waitUntilJobArchived();

        checkResults();

        assertThat(DummySpeculativeOutputFormat.foundSpeculativeAttempt).isTrue();
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
        configuration.set(BatchExecutionOptions.SPECULATIVE_ENABLED, true);
        // for testing, does not block node by default
        if (!configuration.contains(BatchExecutionOptions.BLOCK_SLOW_NODE_DURATION)) {
            configuration.set(BatchExecutionOptions.BLOCK_SLOW_NODE_DURATION, Duration.ZERO);
        }
        configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_MULTIPLIER, 1.0);
        configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_RATIO, 0.2);
        configuration.set(
                SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_LOWER_BOUND, Duration.ofMillis(0));

        // for adaptive parallelism
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM,
                MAX_PARALLELISM);
        configuration.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM, 1);
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM, MAX_PARALLELISM);
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK,
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

    private void setupSpeculativeSlowSink(StreamExecutionEnvironment env) {
        final DataStream<Long> source =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(parallelism)
                        .name("source")
                        .slotSharingGroup("sourceGroup");
        source.sinkTo(new SpeculativeSink())
                .setParallelism(parallelism)
                .name("sink")
                .slotSharingGroup("sinkGroup");
    }

    private void setupNonSpeculativeSlowSinkFunction(StreamExecutionEnvironment env) {
        final DataStream<Long> source =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(parallelism)
                        .name("source")
                        .slotSharingGroup("sourceGroup");
        source.addSink(new NonSpeculativeSinkFunction())
                .setParallelism(parallelism)
                .name("sink")
                .slotSharingGroup("sinkGroup");
    }

    private void setupSpeculativeSlowSinkFunction(StreamExecutionEnvironment env) {
        final DataStream<Long> source =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(parallelism)
                        .name("source")
                        .slotSharingGroup("sourceGroup");
        source.addSink(new SpeculativeSinkFunction())
                .setParallelism(parallelism)
                .name("sink")
                .slotSharingGroup("sinkGroup");
    }

    private void setupSlowOutputFormatSink(StreamExecutionEnvironment env) {
        final DataStream<Long> source =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(parallelism)
                        .name("source")
                        .slotSharingGroup("group1");

        source.rebalance()
                .writeUsingOutputFormat(new DummySpeculativeOutputFormat())
                .setParallelism(parallelism)
                .name("sink")
                .slotSharingGroup("group3");
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

    private static class SpeculativeSink
            implements TwoPhaseCommittingSink<Long, Tuple3<Integer, Integer, Map<Long, Long>>>,
                    SupportsConcurrentExecutionAttempts {

        @Override
        public PrecommittingSinkWriter<Long, Tuple3<Integer, Integer, Map<Long, Long>>>
                createWriter(InitContext context) {
            return new DummyPrecommittingSinkWriter(
                    context.getSubtaskId(), context.getAttemptNumber());
        }

        @Override
        public Committer<Tuple3<Integer, Integer, Map<Long, Long>>> createCommitter() {
            return new DummyCommitter();
        }

        @Override
        public SimpleVersionedSerializer<Tuple3<Integer, Integer, Map<Long, Long>>>
                getCommittableSerializer() {
            return new SimpleVersionedSerializer<Tuple3<Integer, Integer, Map<Long, Long>>>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(Tuple3<Integer, Integer, Map<Long, Long>> obj)
                        throws IOException {
                    return InstantiationUtil.serializeObject(obj);
                }

                @Override
                public Tuple3<Integer, Integer, Map<Long, Long>> deserialize(
                        int version, byte[] serialized) throws IOException {
                    try {
                        return InstantiationUtil.deserializeObject(
                                serialized, Thread.currentThread().getContextClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }
    }

    private static class DummyPrecommittingSinkWriter
            implements PrecommittingSinkWriter<Long, Tuple3<Integer, Integer, Map<Long, Long>>> {

        private final int subTaskIndex;

        private final int attemptNumber;

        public DummyPrecommittingSinkWriter(int subTaskIndex, int attemptNumber) {
            this.subTaskIndex = subTaskIndex;
            this.attemptNumber = attemptNumber;
        }

        private final Map<Long, Long> numberCountResult = new HashMap<>();

        @Override
        public void write(Long value, Context context) throws IOException, InterruptedException {
            numberCountResult.merge(value, 1L, Long::sum);
            maybeSleep();
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public Collection<Tuple3<Integer, Integer, Map<Long, Long>>> prepareCommit() {
            return Collections.singleton(Tuple3.of(subTaskIndex, attemptNumber, numberCountResult));
        }

        @Override
        public void close() throws Exception {}
    }

    private static class DummyCommitter
            implements Committer<Tuple3<Integer, Integer, Map<Long, Long>>> {

        private static AtomicBoolean blocked = new AtomicBoolean(false);
        private static AtomicInteger attempts = new AtomicInteger(0);

        private static volatile boolean foundSpeculativeWriter;

        public DummyCommitter() {
            attempts.incrementAndGet();
        }

        @Override
        public void commit(
                Collection<CommitRequest<Tuple3<Integer, Integer, Map<Long, Long>>>> committables)
                throws InterruptedException {

            for (CommitRequest<Tuple3<Integer, Integer, Map<Long, Long>>> request : committables) {
                Tuple3<Integer, Integer, Map<Long, Long>> committable = request.getCommittable();
                numberCountResults.put(committable.f0, committable.f2);
                // attempt number larger than 0
                if (committable.f1 > 0) {
                    foundSpeculativeWriter = true;
                }
            }

            if (!blocked.getAndSet(true)) {
                Thread.sleep(5000);
            }
        }

        @Override
        public void close() throws Exception {}
    }

    private static class NonSpeculativeSinkFunction extends RichSinkFunction<Long> {

        private final Map<Long, Long> numberCountResult = new HashMap<>();

        @Override
        public void invoke(Long value, Context context) throws Exception {
            if (slowTaskCounter.getAndDecrement() > 0) {
                Thread.sleep(5000);
            }
            numberCountResult.merge(value, 1L, Long::sum);
        }

        @Override
        public void finish() {
            if (getRuntimeContext().getAttemptNumber() == 0) {
                numberCountResults.put(
                        getRuntimeContext().getIndexOfThisSubtask(), numberCountResult);
            }
        }
    }

    private static class SpeculativeSinkFunction extends RichSinkFunction<Long>
            implements SupportsConcurrentExecutionAttempts {

        private final Map<Long, Long> numberCountResult = new HashMap<>();

        @Override
        public void invoke(Long value, Context context) throws Exception {
            numberCountResult.merge(value, 1L, Long::sum);
            maybeSleep();
        }

        @Override
        public void finish() {
            numberCountResults.put(getRuntimeContext().getIndexOfThisSubtask(), numberCountResult);
        }
    }

    /** Outputs format which waits for the previous mapper. */
    private static class DummySpeculativeOutputFormat
            implements OutputFormat<Long>, FinalizeOnMaster, SupportsConcurrentExecutionAttempts {

        private static final long serialVersionUID = 1L;

        private static volatile boolean foundSpeculativeAttempt;

        private int taskNumber;

        private boolean taskFailed;

        private final Map<Long, Long> numberCountResult = new HashMap<>();

        @Override
        public void configure(Configuration parameters) {}

        @Override
        public void open(InitializationContext context) throws IOException {
            taskNumber = context.getTaskNumber();
        }

        @Override
        public void writeRecord(Long value) throws IOException {
            try {
                numberCountResult.merge(value, 1L, Long::sum);
                if (taskNumber == 0) {
                    maybeSleep();
                }
            } catch (Throwable t) {
                taskFailed = true;
            }
        }

        @Override
        public void close() throws IOException {
            if (!taskFailed) {
                numberCountResults.put(taskNumber, numberCountResult);
            }
        }

        @Override
        public void finalizeGlobal(FinalizationContext context) throws IOException {
            for (int i = 0; i < context.getParallelism(); i++) {
                if (context.getFinishedAttempt(i) != 0) {
                    foundSpeculativeAttempt = true;
                }
            }
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
