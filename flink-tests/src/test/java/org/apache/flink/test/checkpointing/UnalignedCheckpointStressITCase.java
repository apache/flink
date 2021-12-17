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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.LogLevelRule;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.event.Level;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.MAX_RETAINED_CHECKPOINTS;
import static org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.base.Preconditions.checkState;
import static org.apache.flink.shaded.guava30.com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;

/**
 * A stress test that runs for a pre-defined amount of time, verifying data correctness and every
 * couple of checkpoints is triggering fail over to stress test unaligned checkpoints.
 */
public class UnalignedCheckpointStressITCase extends TestLogger {

    private static final int CHECKPOINT_INTERVAL = 20;
    private static final int MINIMUM_COMPLETED_CHECKPOINTS_BETWEEN_FAILURES = 2;
    private static final int MAXIMUM_COMPLETED_CHECKPOINTS_BETWEEN_FAILURES = 10;
    private static final long TEST_DURATION = Time.seconds(20).toMilliseconds();
    private static final int NUM_TASK_MANAGERS = 3;
    private static final int NUM_TASK_SLOTS = 2;
    private static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;
    private static final int BUFFER_SIZE = 1024 * 4;
    private static final int BUFFER_TIME = 4;
    private static final int NORMAL_RECORD_SLEEP = 1;
    private static final int SMALL_RECORD_SIZE = (BUFFER_SIZE / BUFFER_TIME) * NORMAL_RECORD_SLEEP;

    @ClassRule
    public static final LogLevelRule NETWORK_LOGGER =
            new LogLevelRule().set(NetworkActionsLogger.class, Level.TRACE);

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    // a separate folder is used because temporaryFolder is cleaned up
    // after each checkpoint
    @Rule public TemporaryFolder changelogFolder = new TemporaryFolder();

    private MiniClusterWithClientResource cluster;

    @Before
    public void setup() throws Exception {
        Configuration configuration = new Configuration();
        File folder = temporaryFolder.getRoot();
        configuration.set(CHECKPOINTS_DIRECTORY, folder.toURI().toString());
        configuration.set(MAX_RETAINED_CHECKPOINTS, 1);

        // Configure DFS DSTL for this test as it might produce too much GC pressure if
        // ChangelogStateBackend is used.
        // Doing it on cluster level unconditionally as randomization currently happens on the job
        // level (environment); while this factory can only be set on the cluster level.
        FsStateChangelogStorageFactory.configure(configuration, changelogFolder.newFolder());

        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberTaskManagers(NUM_TASK_MANAGERS)
                                .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS)
                                .build());
        cluster.before();
    }

    @After
    public void shutDownExistingCluster() {
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
    }

    @Test
    public void runStressTest() throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofMillis(TEST_DURATION));
        Optional<File> externalizedCheckpoint = Optional.empty();
        while (deadline.hasTimeLeft()) {
            externalizedCheckpoint =
                    Optional.of(runAndTakeExternalCheckpoint(externalizedCheckpoint));
            cleanDirectoryExcept(externalizedCheckpoint.get());
        }
    }

    /**
     * Test program starts with 4 separate sources. Sources 1 and 2 are union-ed together, and
     * sources 3 and 4 are also union-ed together. Those two union streams are then connected.
     * Result is a two input task with unions on both sides of the input. After that we are using
     * processing time window, however to not blow up state, we are throttling throughput a bit just
     * before the window operator, in the same operator chain. At the end of the job, there is the
     * main throttling/back-pressure inducing mapper and another mapper inducing failures.
     */
    private void testProgram(StreamExecutionEnvironment env) {
        int numberOfSources1 = PARALLELISM;
        int numberOfSources2 = PARALLELISM / 2;
        int numberOfSources3 = PARALLELISM / 3;
        int numberOfSources4 = PARALLELISM / 4;
        int totalNumberOfSources =
                numberOfSources1 + numberOfSources2 + numberOfSources3 + numberOfSources4;
        DataStreamSource<Record> source1 =
                env.addSource(new LegacySourceFunction(0)).setParallelism(numberOfSources1);

        DataStreamSource<Record> source2 =
                env.addSource(new LegacySourceFunction(numberOfSources1))
                        .setParallelism(numberOfSources2);
        DataStreamSource<Record> source3 =
                env.addSource(new LegacySourceFunction(numberOfSources1 + numberOfSources2))
                        .setParallelism(numberOfSources3);
        DataStreamSource<Record> source4 =
                env.addSource(
                                new LegacySourceFunction(
                                        numberOfSources1 + numberOfSources2 + numberOfSources3))
                        .setParallelism(numberOfSources4);

        DataStream<Record> source12 = source1.union(source2);
        DataStream<Record> source34 = source3.union(source4);

        SingleOutputStreamOperator<Record> sources =
                source12.keyBy(Record::getSourceId)
                        .connect(source34.keyBy(Record::getSourceId))
                        .process(
                                new KeyedCoProcessFunction<Integer, Record, Record, Record>() {
                                    @Override
                                    public void processElement1(
                                            Record value, Context ctx, Collector<Record> out) {
                                        out.collect(value.validate());
                                    }

                                    @Override
                                    public void processElement2(
                                            Record value, Context ctx, Collector<Record> out) {
                                        out.collect(value.validate());
                                    }
                                });

        DataStream<Record> stream =
                sources.rebalance()
                        .map((MapFunction<Record, Record>) value -> value.validate())
                        .keyBy(Record::getSourceId)
                        // add small throttling to prevent WindowOperator from blowing up
                        .map(new ThrottlingMap(100));
        DataStreamUtils.reinterpretAsKeyedStream(stream, Record::getSourceId)
                .window(
                        TumblingProcessingTimeWindows.of(
                                Time.milliseconds(NORMAL_RECORD_SLEEP * 5)))
                .process(new ReEmitAll())
                // main throttling
                .map(new ThrottlingMap(Math.max(1, totalNumberOfSources - 2)))
                .setParallelism(1)
                .map(new FailingMapper())
                .setParallelism(1);
    }

    private void cleanDirectoryExcept(File externalizedCheckpoint) throws IOException {
        File directoryToKeep = externalizedCheckpoint.getParentFile();
        for (File directory : temporaryFolder.getRoot().listFiles()) {
            if (!directory.equals(directoryToKeep)) {
                FileUtils.deleteDirectory(directory);
            }
        }
    }

    private File runAndTakeExternalCheckpoint(Optional<File> startingCheckpoint) throws Exception {

        StreamExecutionEnvironment env = defineEnvironment();
        testProgram(env);

        StreamGraph streamGraph = env.getStreamGraph();
        startingCheckpoint
                .map(File::toString)
                .map(SavepointRestoreSettings::forPath)
                .ifPresent(streamGraph::setSavepointRestoreSettings);
        JobGraph jobGraph = streamGraph.getJobGraph();

        try {
            submitJobAndWaitForResult(
                    cluster.getClusterClient(), jobGraph, getClass().getClassLoader());
        } catch (Exception e) {
            if (!ExceptionUtils.findThrowable(e, ExpectedTestException.class).isPresent()) {
                throw e;
            }
        }

        return discoverRetainedCheckpoint();
    }

    private static final Pattern LAST_INT_PATTERN = Pattern.compile("[^0-9]+([0-9]+)$");

    private static int getCheckpointNumberFromPath(Path checkpointDir) {
        Matcher matcher = LAST_INT_PATTERN.matcher(checkpointDir.toString());
        checkState(matcher.find());
        return Integer.parseInt(matcher.group(1));
    }

    private File discoverRetainedCheckpoint() throws Exception {
        // structure: root/attempt/checkpoint/_metadata
        File rootDir = temporaryFolder.getRoot();
        Path checkpointDir = null;

        for (int i = 0; i <= 1000 && checkpointDir == null; i++) {
            Thread.sleep(5);
            try (Stream<Path> files = Files.walk(Paths.get(rootDir.getPath()))) {
                checkpointDir =
                        files.filter(Files::isRegularFile)
                                .filter(path -> path.endsWith("_metadata"))
                                .map(path -> path.getParent())
                                .sorted(
                                        Comparator.comparingInt(
                                                UnalignedCheckpointStressITCase
                                                        ::getCheckpointNumberFromPath))
                                .reduce((first, second) -> second)
                                .orElse(null);
            }
        }
        if (checkpointDir == null) {
            List<Path> files =
                    Files.walk(Paths.get(rootDir.getPath())).collect(Collectors.toList());
            throw new IllegalStateException("Failed to find _metadata file among " + files);
        }
        return checkpointDir.toFile();
    }

    private StreamExecutionEnvironment defineEnvironment() {
        Configuration configuration = new Configuration();

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(CHECKPOINT_INTERVAL);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        return env;
    }

    private static class RecordGenerator {
        private final int sourceId;

        private final SizeMode sizeMode;
        private final SpeedMode speedMode;

        public RecordGenerator(int sourceId) {
            this.sourceId = sourceId;
            sizeMode = SizeMode.valueOf(sourceId);
            speedMode = SpeedMode.valueOf(sourceId);
        }

        public Record next(long value) throws InterruptedException {
            int sleep = speedMode.getSleep();
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
            return new Record(sourceId, value, sizeMode.getSize());
        }
    }

    private enum SizeMode {
        SMALL {
            @Override
            public int getSize() {
                return SMALL_RECORD_SIZE;
            }
        },
        LARGE {
            @Override
            public int getSize() {
                return SMALL_RECORD_SIZE * 4;
            }
        },
        RANDOM {
            @Override
            public int getSize() {
                return ThreadLocalRandom.current().nextInt(4) * SMALL_RECORD_SIZE
                        + SMALL_RECORD_SIZE;
            }
        };

        public static SizeMode valueOf(int n) {
            checkState(n >= 0);
            return SizeMode.values()[n % SizeMode.values().length];
        }

        public abstract int getSize();
    }

    /** Average sleep should be {@link #NORMAL_RECORD_SLEEP}. */
    private enum SpeedMode {
        SLOW {
            @Override
            public int getSleep() {
                return ThreadLocalRandom.current().nextInt(NORMAL_RECORD_SLEEP * 10);
            }
        },
        NORMAL {
            @Override
            public int getSleep() {
                return ThreadLocalRandom.current().nextInt(NORMAL_RECORD_SLEEP + 1);
            }
        },
        FAST {
            @Override
            public int getSleep() {
                return ThreadLocalRandom.current().nextInt(10) == 0 ? 1 : 0;
            }
        },
        BURST {
            @Override
            public int getSleep() {
                int burstChance = 1000;
                return ThreadLocalRandom.current().nextInt(burstChance) == 0
                        ? burstChance * NORMAL_RECORD_SLEEP
                        : 0;
            }
        };

        /** @return sleep time in milliseconds */
        public abstract int getSleep();

        public static SpeedMode valueOf(int n) {
            checkState(n >= 0);
            return SpeedMode.values()[n % SpeedMode.values().length];
        }
    }

    private static class LegacySourceFunction extends AbstractRichFunction
            implements ParallelSourceFunction<Record>, CheckpointedFunction {
        private final int sourceIdOffset;

        private long nextValue;
        private ListState<Long> nextState;

        private volatile boolean running = true;

        public LegacySourceFunction(int sourceIdOffset) {
            this.sourceIdOffset = sourceIdOffset;
        }

        @Override
        public void run(SourceContext<Record> ctx) throws Exception {
            RecordGenerator generator =
                    new RecordGenerator(
                            getRuntimeContext().getIndexOfThisSubtask() + sourceIdOffset);
            while (running) {
                Record next = generator.next(nextValue);
                synchronized (ctx.getCheckpointLock()) {
                    nextValue++;
                    ctx.collect(next);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            nextState.clear();
            nextState.add(nextValue);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            nextState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("state", Long.class));
            // We are not supporting rescaling
            nextValue = getOnlyElement(nextState.get(), 0L);
        }
    }

    private static class ReEmitAll
            extends ProcessWindowFunction<Record, Record, Integer, TimeWindow> {
        @Override
        public void process(
                Integer integer,
                Context context,
                Iterable<Record> elements,
                Collector<Record> out) {
            for (Record element : elements) {
                out.collect(element);
            }
        }
    }

    private static class FailingMapper implements MapFunction<Record, Record>, CheckpointListener {
        @Nullable private Long firstCompletedCheckpoint;
        @Nullable private Record lastProcessedRecord;

        private final int completedCheckpointsBeforeFailure =
                ThreadLocalRandom.current()
                        .nextInt(
                                MINIMUM_COMPLETED_CHECKPOINTS_BETWEEN_FAILURES,
                                MAXIMUM_COMPLETED_CHECKPOINTS_BETWEEN_FAILURES + 1);

        @Override
        public Record map(Record value) throws Exception {
            lastProcessedRecord = value;
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (firstCompletedCheckpoint == null) {
                firstCompletedCheckpoint = checkpointId;
            }
            if (completedCheckpointsBeforeFailure <= checkpointId - firstCompletedCheckpoint) {
                throw new ExpectedTestException(
                        lastProcessedRecord == null ? "no record" : lastProcessedRecord.toString());
            }
        }
    }

    /** Payload record. */
    public static class Record implements Serializable {
        public int sourceId;
        public long value;
        public byte[] payload;

        public Record() {
            this(0, 0, SMALL_RECORD_SIZE);
        }

        public Record(int sourceId, long value, int payloadSize) {
            this.sourceId = sourceId;
            this.payload = new byte[payloadSize];
            this.value = value;
            for (int i = 0; i < payload.length; i++) {
                payload[i] = payloadAt(i);
            }
        }

        public int getSourceId() {
            return sourceId;
        }

        public long getValue() {
            return value;
        }

        public Record validate() {
            for (int i = 0; i < payload.length; i++) {
                checkState(
                        payload[i] == payloadAt(i),
                        "Expected %s at position %s, but found %s in %s",
                        payloadAt(i),
                        i,
                        payload[i],
                        this);
            }
            return this;
        }

        private byte payloadAt(int index) {
            return (byte) ((value + index) % 128);
        }

        @Override
        public String toString() {
            return String.format(
                    "Record(sourceId=%d, payload.length=%d, value=%d)",
                    sourceId, payload.length, value);
        }
    }

    private static class ThrottlingMap implements MapFunction<Record, Record> {
        private final int chance;

        public ThrottlingMap(int chance) {
            this.chance = chance;
        }

        @Override
        public Record map(Record value) throws Exception {
            if (ThreadLocalRandom.current().nextInt(chance) == 0) {
                Thread.sleep(NORMAL_RECORD_SLEEP);
            }
            return value.validate();
        }
    }
}
