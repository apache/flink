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

package org.apache.flink.test.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.Serializable;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PREEMPTIVE_PERSIST_THRESHOLD;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINT_STORAGE;
import static org.apache.flink.configuration.CheckpointingOptions.FS_SMALL_FILE_THRESHOLD;
import static org.apache.flink.configuration.CheckpointingOptions.LOCAL_RECOVERY;
import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG;
import static org.apache.flink.configuration.StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_ENABLED;
import static org.apache.flink.runtime.jobgraph.SavepointRestoreSettings.forPath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForCheckpoint;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_MODE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ENABLE_UNALIGNED;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Tests rescaling with Changelog enabled and with timers in state. It uses an adaptation of a
 * ChangelogTestProgram that aims to generate the highest load possible while still allowing
 * checkpointing. For that, it uses rate-limited FLIP-27 source and Unaligned checkpoints.
 */
@RunWith(Parameterized.class)
public class ChangelogRescalingITCase extends TestLogger {
    /** The rate at which events will be generated by the source. */
    private static final int EVENTS_PER_SECOND_PER_READER = 100;
    /** Payload size of each event generated randomly. */
    private static final int PAYLOAD_SIZE = 1000;
    /** Size of (ProcessingTime) windows. */
    private static final Time WINDOW_SIZE = Time.milliseconds(100);
    /** Slide size. */
    private static final Time WINDOW_SLIDE = Time.milliseconds(10);
    /** Time to Accumulate some timer delete operations. */
    private static final int ACCUMULATE_TIME_MILLIS = 5_000;

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Parameters(name = "Rescale {0} -> {1}")
    public static Object[] parameters() {
        return new Object[][] {new Object[] {6, 4}, new Object[] {4, 6}};
    }

    private final int parallelism1;
    private final int parallelism2;

    private MiniClusterWithClientResource cluster;

    public ChangelogRescalingITCase(int parallelism1, int parallelism2) {
        this.parallelism1 = parallelism1;
        this.parallelism2 = parallelism2;
    }

    @Before
    public void before() throws Exception {
        Configuration configuration = new Configuration();
        FsStateChangelogStorageFactory.configure(
                configuration, temporaryFolder.newFolder(), Duration.ofMinutes(1), 10);
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberSlotsPerTaskManager(Math.max(parallelism1, parallelism2))
                                .build());
        cluster.before();
    }

    @After
    public void after() {
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
    }

    @Test
    public void test() throws Exception {
        // before rescale
        JobID jobID1 = submit(configureJob(parallelism1, temporaryFolder.newFolder()), graph -> {});

        Thread.sleep(ACCUMULATE_TIME_MILLIS);
        String cpLocation = checkpointAndCancel(jobID1);

        // rescale and checkpoint to verify
        JobID jobID2 =
                submit(
                        configureJob(parallelism2, temporaryFolder.newFolder()),
                        graph -> graph.setSavepointRestoreSettings(forPath(cpLocation)));
        waitForAllTaskRunning(cluster.getMiniCluster(), jobID2, true);
        cluster.getClusterClient().cancel(jobID2).get();
    }

    private JobID submit(Configuration conf, Consumer<JobGraph> updateGraph)
            throws InterruptedException, ExecutionException {
        JobGraph jobGraph = createJobGraph(conf);
        updateGraph.accept(jobGraph);
        return cluster.getClusterClient().submitJob(jobGraph).get();
    }

    private JobGraph createJobGraph(Configuration conf) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        SingleOutputStreamOperator<TestEvent> map =
                env.fromSource(
                                new ThrottlingNumberSequenceSource(
                                        0, Long.MAX_VALUE, EVENTS_PER_SECOND_PER_READER),
                                WatermarkStrategy.noWatermarks(),
                                "Sequence Source")
                        .keyBy(ChangelogRescalingITCase::key)
                        .map(
                                el -> {
                                    // Thread.sleep(100); // don't block barriers
                                    byte[] bytes = new byte[PAYLOAD_SIZE];
                                    ThreadLocalRandom.current().nextBytes(bytes);
                                    return new TestEvent(el, bytes);
                                });
        DataStreamUtils.reinterpretAsKeyedStream(map, e -> key(e.id))
                .window(SlidingProcessingTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))
                .process(
                        new ProcessWindowFunction<TestEvent, String, Long, TimeWindow>() {
                            @Override
                            public void process(
                                    Long key,
                                    ProcessWindowFunction<TestEvent, String, Long, TimeWindow>
                                                    .Context
                                            context,
                                    Iterable<TestEvent> elements,
                                    Collector<String> out) {}
                        })
                .sinkTo(new DiscardingSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private static long key(Long num) {
        return num % 1000;
    }

    private Configuration configureJob(int parallelism, File cpDir) {
        Configuration conf = new Configuration();

        conf.set(EXTERNALIZED_CHECKPOINT, RETAIN_ON_CANCELLATION);
        conf.set(DEFAULT_PARALLELISM, parallelism);
        conf.set(ENABLE_STATE_CHANGE_LOG, true);
        conf.set(CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        conf.set(CHECKPOINTING_INTERVAL, Duration.ofMillis(10));
        conf.set(CHECKPOINT_STORAGE, "filesystem");
        conf.set(CHECKPOINTS_DIRECTORY, cpDir.toURI().toString());
        conf.set(STATE_BACKEND, "hashmap");
        conf.set(LOCAL_RECOVERY, false); // not supported by changelog
        // tune changelog
        conf.set(PREEMPTIVE_PERSIST_THRESHOLD, MemorySize.ofMebiBytes(10));
        conf.set(PERIODIC_MATERIALIZATION_INTERVAL, Duration.ofMinutes(3));
        // tune flink
        conf.set(FS_SMALL_FILE_THRESHOLD, MemorySize.ofMebiBytes(1));
        conf.set(OBJECT_REUSE, true);

        conf.set(ENABLE_UNALIGNED, true); // speedup
        conf.set(ALIGNED_CHECKPOINT_TIMEOUT, Duration.ZERO); // prevent randomization
        conf.set(BUFFER_DEBLOAT_ENABLED, false); // prevent randomization
        conf.set(RESTART_STRATEGY, "none"); // not expecting any failures

        return conf;
    }

    private static final class TestEvent implements Serializable {
        private final long id;

        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final byte[] payload;

        private TestEvent(long id, byte[] payload) {
            this.id = id;
            this.payload = payload;
        }
    }

    private static class ThrottlingNumberSequenceSource extends NumberSequenceSource {
        private final int numbersPerSecond;

        public ThrottlingNumberSequenceSource(long from, long to, int numbersPerSecondPerReader) {
            super(from, to);
            this.numbersPerSecond = numbersPerSecondPerReader;
        }

        @Override
        public SourceReader<Long, NumberSequenceSplit> createReader(
                SourceReaderContext readerContext) {
            return new ThrottlingIteratorSourceReader<>(
                    readerContext, new SourceRateLimiter(numbersPerSecond));
        }
    }

    private static class ThrottlingIteratorSourceReader<
                    E, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>>
            extends IteratorSourceReader<E, IterT, SplitT> {
        private final SourceRateLimiter rateLimiter;

        public ThrottlingIteratorSourceReader(
                SourceReaderContext context, SourceRateLimiter rateLimiter) {
            super(context);
            this.rateLimiter = rateLimiter;
        }

        @Override
        public InputStatus pollNext(ReaderOutput<E> output) {
            if (rateLimiter.request()) {
                return super.pollNext(output);
            } else {
                return InputStatus.NOTHING_AVAILABLE;
            }
        }
    }

    private static final class SourceRateLimiter {
        private final AtomicBoolean newTokensAdded = new AtomicBoolean(false);
        private final int tokensToAdd;
        private int tokensAvailable;

        public SourceRateLimiter(int tokensPerSecond) {
            this(
                    tokensPerSecond < 10 ? 1000 : 100,
                    tokensPerSecond < 10 ? tokensPerSecond : tokensPerSecond / 10);
        }

        public SourceRateLimiter(int intervalMs, int tokensToAdd) {
            checkArgument(intervalMs > 0);
            checkArgument(tokensToAdd > 0);
            this.tokensToAdd = tokensToAdd;
            this.tokensAvailable = tokensToAdd;
            new Timer("source-limiter", true)
                    .scheduleAtFixedRate(
                            new TimerTask() {
                                @Override
                                public void run() {
                                    newTokensAdded.set(true); // "catch up" is ok
                                }
                            },
                            intervalMs,
                            intervalMs);
        }

        public boolean request() {
            if (tokensAvailable == 0 && newTokensAdded.compareAndSet(true, false)) {
                tokensAvailable = tokensToAdd;
            }
            if (tokensAvailable > 0) {
                tokensAvailable--;
                return true;
            } else {
                return false;
            }
        }
    }

    private String checkpointAndCancel(JobID jobID) throws Exception {
        waitForCheckpoint(jobID, cluster.getMiniCluster(), 1);
        cluster.getClusterClient().cancel(jobID).get();
        waitForSuccessfulTermination(jobID);
        return CommonTestUtils.getLatestCompletedCheckpointPath(jobID, cluster.getMiniCluster())
                .<NoSuchElementException>orElseThrow(
                        () -> {
                            throw new NoSuchElementException("No checkpoint was created yet");
                        });
    }

    private void waitForSuccessfulTermination(JobID jobID) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () ->
                        cluster.getClusterClient()
                                .getJobStatus(jobID)
                                .get()
                                .isGloballyTerminalState());
        if (cluster.getClusterClient().getJobStatus(jobID).get().isGloballyTerminalState()) {
            cluster.getClusterClient()
                    .requestJobResult(jobID)
                    .get()
                    .getSerializedThrowable()
                    .ifPresent(
                            serializedThrowable -> {
                                throw new RuntimeException(serializedThrowable);
                            });
        }
    }
}
