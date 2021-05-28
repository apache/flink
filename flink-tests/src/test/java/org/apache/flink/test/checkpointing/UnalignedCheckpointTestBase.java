/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.FailsWithAdaptiveScheduler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.shaded.netty4.io.netty.util.internal.PlatformDependent;

import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.CHECKPOINT_DIR_PREFIX;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME;
import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT;
import static org.apache.flink.util.Preconditions.checkState;

/** Base class for tests related to unaligned checkpoints. */
@Category(FailsWithAdaptiveScheduler.class) // FLINK-21689
public abstract class UnalignedCheckpointTestBase extends TestLogger {
    protected static final Logger LOG = LoggerFactory.getLogger(UnalignedCheckpointTestBase.class);
    protected static final String NUM_INPUTS = "inputs_";
    protected static final String NUM_OUTPUTS = "outputs";
    protected static final String NUM_OUT_OF_ORDER = "outOfOrder";
    protected static final String NUM_FAILURES = "failures";
    protected static final String NUM_DUPLICATES = "duplicates";
    protected static final String NUM_LOST = "lost";
    protected static final int BUFFER_PER_CHANNEL = 1;
    /** For multi-gate tests. */
    protected static final int NUM_SOURCES = 3;

    private static final long HEADER = 0xABCDEAFCL << 32;
    private static final long HEADER_MASK = 0xFFFFFFFFL << 32;

    @Rule public final TemporaryFolder temp = new TemporaryFolder();

    @Rule public ErrorCollector collector = new ErrorCollector();

    @Nullable
    protected File execute(UnalignedSettings settings) throws Exception {
        final File checkpointDir = temp.newFolder();
        Configuration conf = settings.getConfiguration(checkpointDir);

        final StreamGraph streamGraph = getStreamGraph(settings, conf);
        final int requiredSlots =
                streamGraph.getStreamNodes().stream()
                        .mapToInt(node -> node.getParallelism())
                        .reduce(0, settings.channelType.slotSharing ? Integer::max : Integer::sum);
        int numberTaskmanagers = settings.channelType.slotsToTaskManagers.apply(requiredSlots);
        final int slotsPerTM = (requiredSlots + numberTaskmanagers - 1) / numberTaskmanagers;
        final MiniClusterWithClientResource miniCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(conf)
                                .setNumberTaskManagers(numberTaskmanagers)
                                .setNumberSlotsPerTaskManager(slotsPerTM)
                                .build());
        miniCluster.before();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);
        settings.configure(env);
        try {
            waitForCleanShutdown();
            final CompletableFuture<JobSubmissionResult> result =
                    miniCluster.getMiniCluster().submitJob(streamGraph.getJobGraph());

            checkCounters(
                    miniCluster
                            .getMiniCluster()
                            .requestJobResult(result.get().getJobID())
                            .get()
                            .toJobExecutionResult(getClass().getClassLoader()));
        } catch (Exception e) {
            if (!ExceptionUtils.findThrowable(e, TestException.class).isPresent()) {
                throw e;
            }
        } finally {
            miniCluster.after();
        }
        if (settings.generateCheckpoint) {
            return Files.find(checkpointDir.toPath(), 2, this::isCompletedCheckpoint)
                    .max(Comparator.comparing(Path::toString))
                    .map(Path::toFile)
                    .orElseThrow(() -> new IllegalStateException("Cannot generate checkpoint"));
        }
        return null;
    }

    private boolean isCompletedCheckpoint(Path path, BasicFileAttributes attr) {
        return attr.isDirectory()
                && path.getFileName().toString().startsWith(CHECKPOINT_DIR_PREFIX)
                && hasMetadata(path);
    }

    private boolean hasMetadata(Path file) {
        try {
            return Files.find(
                            file.toAbsolutePath(),
                            1,
                            (path, attrs) ->
                                    path.getFileName().toString().equals(METADATA_FILE_NAME))
                    .findAny()
                    .isPresent();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
            return false; // should never happen
        }
    }

    private StreamGraph getStreamGraph(UnalignedSettings settings, Configuration conf) {
        // a dummy environment used to retrieve the DAG, mini cluster will be used later
        final StreamExecutionEnvironment setupEnv =
                StreamExecutionEnvironment.createLocalEnvironment(conf);
        settings.configure(setupEnv);

        settings.dagCreator.create(
                setupEnv,
                settings.minCheckpoints,
                settings.channelType.slotSharing,
                settings.expectedFailures - settings.failuresAfterSourceFinishes);

        return setupEnv.getStreamGraph();
    }

    private void waitForCleanShutdown() throws InterruptedException {
        // direct memory in netty will be freed through gc/finalization
        // too many successive executions will lead to OOM by netty
        // slow down when half the memory is taken and wait for gc
        if (PlatformDependent.usedDirectMemory() > PlatformDependent.maxDirectMemory() / 2) {
            final Duration waitTime = Duration.ofSeconds(10);
            Deadline deadline = Deadline.fromNow(waitTime);
            while (PlatformDependent.usedDirectMemory() > 0 && deadline.hasTimeLeft()) {
                System.gc();
                Thread.sleep(100);
            }
            final Duration timeLeft = deadline.timeLeft();
            if (timeLeft.isNegative()) {
                LOG.warn(
                        "Waited 10s for clean shutdown of previous runs but there is still direct memory in use: "
                                + PlatformDependent.usedDirectMemory());
            } else {
                LOG.info(
                        "Needed to wait {} ms for full cleanup of previous runs.",
                        waitTime.minus(timeLeft).toMillis());
            }
        }
    }

    protected abstract void checkCounters(JobExecutionResult result);

    /** A source that generates longs in a fixed number of splits. */
    protected static class LongSource
            implements Source<Long, LongSource.LongSplit, LongSource.EnumeratorState> {
        private final int minCheckpoints;
        private final int numSplits;
        private final int expectedRestarts;
        private final long checkpointingInterval;

        protected LongSource(
                int minCheckpoints,
                int numSplits,
                int expectedRestarts,
                long checkpointingInterval) {
            this.minCheckpoints = minCheckpoints;
            this.numSplits = numSplits;
            this.expectedRestarts = expectedRestarts;
            this.checkpointingInterval = checkpointingInterval;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<Long, LongSplit> createReader(SourceReaderContext readerContext) {
            return new LongSourceReader(
                    readerContext.getIndexOfSubtask(),
                    minCheckpoints,
                    expectedRestarts,
                    checkpointingInterval);
        }

        @Override
        public SplitEnumerator<LongSplit, EnumeratorState> createEnumerator(
                SplitEnumeratorContext<LongSplit> enumContext) {
            List<LongSplit> splits =
                    IntStream.range(0, numSplits)
                            .mapToObj(i -> new LongSplit(i, numSplits))
                            .collect(Collectors.toList());
            return new LongSplitSplitEnumerator(enumContext, new EnumeratorState(splits, 0, 0));
        }

        @Override
        public SplitEnumerator<LongSplit, EnumeratorState> restoreEnumerator(
                SplitEnumeratorContext<LongSplit> enumContext, EnumeratorState state) {
            return new LongSplitSplitEnumerator(enumContext, state);
        }

        @Override
        public SimpleVersionedSerializer<LongSplit> getSplitSerializer() {
            return new SplitVersionedSerializer();
        }

        @Override
        public SimpleVersionedSerializer<EnumeratorState> getEnumeratorCheckpointSerializer() {
            return new EnumeratorVersionedSerializer();
        }

        private static class LongSourceReader implements SourceReader<Long, LongSplit> {
            private final int subtaskIndex;
            private final int minCheckpoints;
            private final int expectedRestarts;
            private final LongCounter numInputsCounter = new LongCounter();
            private final List<LongSplit> splits = new ArrayList<>();
            private final Duration pumpInterval;
            private int numAbortedCheckpoints;
            private int numRestarts;
            private int numCompletedCheckpoints;
            private boolean finishing;
            private boolean recovered;
            @Nullable private Deadline pumpingUntil = null;

            public LongSourceReader(
                    int subtaskIndex,
                    int minCheckpoints,
                    int expectedRestarts,
                    long checkpointingInterval) {
                this.subtaskIndex = subtaskIndex;
                this.minCheckpoints = minCheckpoints;
                this.expectedRestarts = expectedRestarts;
                pumpInterval = Duration.ofMillis(checkpointingInterval);
            }

            @Override
            public void start() {}

            @Override
            public InputStatus pollNext(ReaderOutput<Long> output) throws InterruptedException {
                for (LongSplit split : splits) {
                    output.collect(withHeader(split.nextNumber), split.nextNumber);
                    split.nextNumber += split.increment;
                }

                if (finishing) {
                    return InputStatus.END_OF_INPUT;
                }

                if (pumpingUntil != null && pumpingUntil.isOverdue()) {
                    pumpingUntil = null;
                }
                if (pumpingUntil == null) {
                    Thread.sleep(1);
                }
                return InputStatus.MORE_AVAILABLE;
            }

            @Override
            public List<LongSplit> snapshotState(long checkpointId) {
                LOG.info(
                        "Snapshotted {} @ {} subtask ({} attempt)",
                        splits,
                        subtaskIndex,
                        numRestarts);
                // barrier passed, so no need to add more data for this test
                pumpingUntil = null;
                return splits;
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) {
                LOG.info(
                        "notifyCheckpointComplete {} @ {} subtask ({} attempt)",
                        numCompletedCheckpoints,
                        subtaskIndex,
                        numRestarts);
                // Update polling state before final checkpoint such that if there is an issue
                // during finishing, after recovery the source immediately starts finishing
                // again. In this way, we avoid a deadlock where some tasks need another
                // checkpoint completed, while some tasks are finishing (and thus there are no
                // new checkpoint).
                updatePollingState();
                numCompletedCheckpoints++;
                recovered = true;
                numAbortedCheckpoints = 0;
            }

            @Override
            public void notifyCheckpointAborted(long checkpointId) {
                if (numAbortedCheckpoints++ > 100) {
                    // aborted too many checkpoints in a row, which usually indicates that part of
                    // the pipeline is already completed
                    // here simply also advance completed checkpoints to avoid running into a live
                    // lock
                    numCompletedCheckpoints = minCheckpoints + 1;
                }
                updatePollingState();
            }

            @Override
            public CompletableFuture<Void> isAvailable() {
                return FutureUtils.completedVoidFuture();
            }

            @Override
            public void addSplits(List<LongSplit> splits) {
                this.splits.addAll(splits);
                updatePollingState();
                LOG.info(
                        "Added splits {}, finishing={}, pumping until {} @ {} subtask ({} attempt)",
                        splits,
                        finishing,
                        pumpingUntil,
                        subtaskIndex,
                        numRestarts);
            }

            @Override
            public void notifyNoMoreSplits() {
                updatePollingState();
            }

            private void updatePollingState() {
                if (numCompletedCheckpoints >= minCheckpoints && numRestarts >= expectedRestarts) {
                    finishing = true;
                    LOG.info("Finishing @ {} subtask ({} attempt)", subtaskIndex, numRestarts);
                } else if (recovered) {
                    // a successful checkpoint as a proxy for a finished recovery
                    // cause backpressure until next checkpoint is added
                    pumpingUntil = Deadline.fromNow(pumpInterval);
                    LOG.info(
                            "Pumping until {} @ {} subtask ({} attempt)",
                            pumpingUntil,
                            subtaskIndex,
                            numRestarts);
                }
            }

            @Override
            public void handleSourceEvents(SourceEvent sourceEvent) {
                if (sourceEvent instanceof SyncEvent) {
                    numRestarts = ((SyncEvent) sourceEvent).numRestarts;
                    numCompletedCheckpoints = ((SyncEvent) sourceEvent).numCheckpoints;
                    LOG.info(
                            "Set restarts={}, numCompletedCheckpoints={} @ {} subtask ({} attempt)",
                            numRestarts,
                            numCompletedCheckpoints,
                            subtaskIndex,
                            numRestarts);
                    updatePollingState();
                }
            }

            @Override
            public void close() throws Exception {
                for (LongSplit split : splits) {
                    numInputsCounter.add(split.nextNumber / split.increment);
                }
            }
        }

        private static class SyncEvent implements SourceEvent {
            final int numRestarts;
            final int numCheckpoints;

            SyncEvent(int numRestarts, int numCheckpoints) {
                this.numRestarts = numRestarts;
                this.numCheckpoints = numCheckpoints;
            }
        }

        private static class LongSplit implements SourceSplit {
            private final int increment;
            private long nextNumber;

            public LongSplit(long nextNumber, int increment) {
                this.nextNumber = nextNumber;
                this.increment = increment;
            }

            public int getBaseNumber() {
                return (int) (nextNumber % increment);
            }

            @Override
            public String splitId() {
                return String.valueOf(increment);
            }

            @Override
            public String toString() {
                return "LongSplit{" + "increment=" + increment + ", nextNumber=" + nextNumber + '}';
            }
        }

        private static class LongSplitSplitEnumerator
                implements SplitEnumerator<LongSplit, EnumeratorState> {
            private final SplitEnumeratorContext<LongSplit> context;
            private final EnumeratorState state;
            private final Map<Integer, Integer> subtaskRestarts = new HashMap<>();

            private LongSplitSplitEnumerator(
                    SplitEnumeratorContext<LongSplit> context, EnumeratorState state) {
                this.context = context;
                this.state = state;
            }

            @Override
            public void start() {}

            @Override
            public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

            @Override
            public void addSplitsBack(List<LongSplit> splits, int subtaskId) {
                LOG.info("addSplitsBack {}", splits);
                // Called on recovery
                subtaskRestarts.compute(
                        subtaskId,
                        (id, oldCount) -> oldCount == null ? state.numRestarts + 1 : oldCount + 1);
                state.unassignedSplits.addAll(splits);
            }

            @Override
            public void addReader(int subtaskId) {
                if (context.registeredReaders().size() == context.currentParallelism()) {
                    if (!state.unassignedSplits.isEmpty()) {
                        Map<Integer, List<LongSplit>> assignment =
                                state.unassignedSplits.stream()
                                        .collect(Collectors.groupingBy(LongSplit::getBaseNumber));
                        LOG.info("Assigning splits {}", assignment);
                        context.assignSplits(new SplitsAssignment<>(assignment));
                        state.unassignedSplits.clear();
                    }
                    context.registeredReaders().keySet().forEach(context::signalNoMoreSplits);
                    Optional<Integer> restarts =
                            subtaskRestarts.values().stream().max(Comparator.naturalOrder());
                    if (restarts.isPresent() && restarts.get() > state.numRestarts) {
                        state.numRestarts = restarts.get();
                        // Implicitly sync the restart count of all subtasks with state.numRestarts
                        subtaskRestarts.clear();
                        final SyncEvent event =
                                new SyncEvent(state.numRestarts, state.numCompletedCheckpoints);
                        context.registeredReaders()
                                .keySet()
                                .forEach(index -> context.sendEventToSourceReader(index, event));
                    }
                }
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) {
                state.numCompletedCheckpoints++;
            }

            @Override
            public EnumeratorState snapshotState(long checkpointId) throws Exception {
                LOG.info("snapshotState {}", state);
                return state;
            }

            @Override
            public void close() throws IOException {}
        }

        private static class EnumeratorState {
            final List<LongSplit> unassignedSplits;
            int numRestarts;
            int numCompletedCheckpoints;

            public EnumeratorState(
                    List<LongSplit> unassignedSplits,
                    int numRestarts,
                    int numCompletedCheckpoints) {
                this.unassignedSplits = unassignedSplits;
                this.numRestarts = numRestarts;
                this.numCompletedCheckpoints = numCompletedCheckpoints;
            }

            @Override
            public String toString() {
                return "EnumeratorState{"
                        + "unassignedSplits="
                        + unassignedSplits
                        + ", numRestarts="
                        + numRestarts
                        + ", numCompletedCheckpoints="
                        + numCompletedCheckpoints
                        + '}';
            }
        }

        private static class EnumeratorVersionedSerializer
                implements SimpleVersionedSerializer<EnumeratorState> {
            private final SplitVersionedSerializer splitVersionedSerializer =
                    new SplitVersionedSerializer();

            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(EnumeratorState state) {
                final ByteBuffer byteBuffer =
                        ByteBuffer.allocate(
                                state.unassignedSplits.size() * SplitVersionedSerializer.LENGTH
                                        + 8);
                byteBuffer.putInt(state.numRestarts);
                byteBuffer.putInt(state.numCompletedCheckpoints);
                for (final LongSplit unassignedSplit : state.unassignedSplits) {
                    byteBuffer.put(splitVersionedSerializer.serialize(unassignedSplit));
                }
                return byteBuffer.array();
            }

            @Override
            public EnumeratorState deserialize(int version, byte[] serialized) {
                final ByteBuffer byteBuffer = ByteBuffer.wrap(serialized);
                final int numRestarts = byteBuffer.getInt();
                final int numCompletedCheckpoints = byteBuffer.getInt();

                final List<LongSplit> splits =
                        new ArrayList<>(serialized.length / SplitVersionedSerializer.LENGTH);

                final byte[] serializedSplit = new byte[SplitVersionedSerializer.LENGTH];
                while (byteBuffer.hasRemaining()) {
                    byteBuffer.get(serializedSplit);
                    splits.add(splitVersionedSerializer.deserialize(version, serializedSplit));
                }
                return new EnumeratorState(splits, numRestarts, numCompletedCheckpoints);
            }
        }

        private static class SplitVersionedSerializer
                implements SimpleVersionedSerializer<LongSplit> {
            static final int LENGTH = 16;

            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(LongSplit split) {
                final byte[] bytes = new byte[LENGTH];
                ByteBuffer.wrap(bytes).putLong(split.nextNumber).putInt(split.increment);
                return bytes;
            }

            @Override
            public LongSplit deserialize(int version, byte[] serialized) {
                final ByteBuffer byteBuffer = ByteBuffer.wrap(serialized);
                return new LongSplit(byteBuffer.getLong(), byteBuffer.getInt());
            }
        }
    }

    interface DagCreator {
        void create(
                StreamExecutionEnvironment environment,
                int minCheckpoints,
                boolean slotSharing,
                int expectedFailuresUntilSourceFinishes);
    }

    /** Which channels are used to connect the tasks. */
    protected enum ChannelType {
        LOCAL(true, n -> 1),
        REMOTE(false, n -> n),
        MIXED(true, n -> Math.min(n, 3));

        final boolean slotSharing;
        final Function<Integer, Integer> slotsToTaskManagers;

        ChannelType(boolean slotSharing, Function<Integer, Integer> slotsToTaskManagers) {
            this.slotSharing = slotSharing;
            this.slotsToTaskManagers = slotsToTaskManagers;
        }

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    /** Builder-like interface for all relevant unaligned settings. */
    protected static class UnalignedSettings {
        private int parallelism;
        private final int minCheckpoints = 10;
        @Nullable private File restoreCheckpoint;
        private boolean generateCheckpoint = false;
        int expectedFailures = 0;
        int tolerableCheckpointFailures = 0;
        private final DagCreator dagCreator;
        private int alignmentTimeout = 0;
        private Duration checkpointTimeout = CHECKPOINTING_TIMEOUT.defaultValue();
        private int failuresAfterSourceFinishes = 0;
        private ChannelType channelType = ChannelType.MIXED;

        public UnalignedSettings(DagCreator dagCreator) {
            this.dagCreator = dagCreator;
        }

        public UnalignedSettings setParallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public UnalignedSettings setRestoreCheckpoint(File restoreCheckpoint) {
            this.restoreCheckpoint = restoreCheckpoint;
            return this;
        }

        public UnalignedSettings setGenerateCheckpoint(boolean generateCheckpoint) {
            this.generateCheckpoint = generateCheckpoint;
            return this;
        }

        public UnalignedSettings setExpectedFailures(int expectedFailures) {
            this.expectedFailures = expectedFailures;
            return this;
        }

        public UnalignedSettings setCheckpointTimeout(Duration checkpointTimeout) {
            this.checkpointTimeout = checkpointTimeout;
            return this;
        }

        public UnalignedSettings setAlignmentTimeout(int alignmentTimeout) {
            this.alignmentTimeout = alignmentTimeout;
            return this;
        }

        public UnalignedSettings setFailuresAfterSourceFinishes(int failuresAfterSourceFinishes) {
            this.failuresAfterSourceFinishes = failuresAfterSourceFinishes;
            return this;
        }

        public UnalignedSettings setChannelTypes(ChannelType channelType) {
            this.channelType = channelType;
            return this;
        }

        public UnalignedSettings setTolerableCheckpointFailures(int tolerableCheckpointFailures) {
            this.tolerableCheckpointFailures = tolerableCheckpointFailures;
            return this;
        }

        public void configure(StreamExecutionEnvironment env) {
            env.enableCheckpointing(Math.max(100L, parallelism * 50L));
            env.getCheckpointConfig().setAlignmentTimeout(Duration.ofMillis(alignmentTimeout));
            env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout.toMillis());
            env.getCheckpointConfig()
                    .setTolerableCheckpointFailureNumber(tolerableCheckpointFailures);
            env.setParallelism(parallelism);
            env.setRestartStrategy(
                    RestartStrategies.fixedDelayRestart(
                            generateCheckpoint ? expectedFailures / 2 : expectedFailures,
                            Time.milliseconds(100)));
            env.getCheckpointConfig().enableUnalignedCheckpoints(true);
            // for custom partitioner
            env.getCheckpointConfig().setForceUnalignedCheckpoints(true);
            if (generateCheckpoint) {
                env.getCheckpointConfig()
                        .enableExternalizedCheckpoints(
                                CheckpointConfig.ExternalizedCheckpointCleanup
                                        .RETAIN_ON_CANCELLATION);
            }
        }

        public Configuration getConfiguration(File checkpointDir) {
            Configuration conf = new Configuration();

            conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, 0.9f);
            conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4kb"));
            conf.setString(StateBackendOptions.STATE_BACKEND, "filesystem");
            conf.setString(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
            if (restoreCheckpoint != null) {
                conf.set(
                        SavepointConfigOptions.SAVEPOINT_PATH,
                        restoreCheckpoint.toURI().toString());
            }

            conf.set(
                    NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL, BUFFER_PER_CHANNEL);
            conf.set(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 60000);
            conf.setString(AkkaOptions.ASK_TIMEOUT, "1 min");
            return conf;
        }

        @Override
        public String toString() {
            return "UnalignedSettings{"
                    + "parallelism="
                    + parallelism
                    + ", minCheckpoints="
                    + minCheckpoints
                    + ", restoreCheckpoint="
                    + restoreCheckpoint
                    + ", generateCheckpoint="
                    + generateCheckpoint
                    + ", expectedFailures="
                    + expectedFailures
                    + ", dagCreator="
                    + dagCreator
                    + ", alignmentTimeout="
                    + alignmentTimeout
                    + ", failuresAfterSourceFinishes="
                    + failuresAfterSourceFinishes
                    + ", channelType="
                    + channelType
                    + '}';
        }
    }

    /** Shifts the partitions one up. */
    protected static class ShiftingPartitioner implements Partitioner<Long> {
        @Override
        public int partition(Long key, int numPartitions) {
            return (int) ((withoutHeader(key) + 1) % numPartitions);
        }
    }

    /** Distributes chunks of the size of numPartitions in a round robin fashion. */
    protected static class ChunkDistributingPartitioner implements Partitioner<Long> {
        @Override
        public int partition(Long key, int numPartitions) {
            return (int) ((withoutHeader(key) / numPartitions) % numPartitions);
        }
    }

    /** A mapper that fails in particular situations/attempts. */
    protected static class FailingMapper extends RichMapFunction<Long, Long>
            implements CheckpointedFunction, CheckpointListener {
        private static final ListStateDescriptor<FailingMapperState>
                FAILING_MAPPER_STATE_DESCRIPTOR =
                        new ListStateDescriptor<>("state", FailingMapperState.class);
        private ListState<FailingMapperState> listState;
        @Nullable private transient FailingMapperState state;
        private final FilterFunction<FailingMapperState> failDuringMap;
        private final FilterFunction<FailingMapperState> failDuringSnapshot;
        private final FilterFunction<FailingMapperState> failDuringRecovery;
        private final FilterFunction<FailingMapperState> failDuringClose;
        private long lastValue;

        protected FailingMapper(
                FilterFunction<FailingMapperState> failDuringMap,
                FilterFunction<FailingMapperState> failDuringSnapshot,
                FilterFunction<FailingMapperState> failDuringRecovery,
                FilterFunction<FailingMapperState> failDuringClose) {
            this.failDuringMap = failDuringMap;
            this.failDuringSnapshot = failDuringSnapshot;
            this.failDuringRecovery = failDuringRecovery;
            this.failDuringClose = failDuringClose;
        }

        @Override
        public Long map(Long value) throws Exception {
            lastValue = withoutHeader(value);
            checkFail(failDuringMap, "map");
            return value;
        }

        public void checkFail(FilterFunction<FailingMapperState> failFunction, String description)
                throws Exception {
            if (state != null && failFunction.filter(state)) {
                failMapper(description);
            }
        }

        private void failMapper(String description) throws Exception {
            throw new TestException(
                    "Failing "
                            + description
                            + " @ "
                            + state.completedCheckpoints
                            + " ("
                            + state.runNumber
                            + " attempt); last value "
                            + lastValue);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (state != null) {
                state.completedCheckpoints++;
            }
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkFail(failDuringSnapshot, "snapshotState");
            listState.clear();
            if (state != null) {
                listState.add(state);
            }
        }

        @Override
        public void close() throws Exception {
            checkFail(failDuringClose, "close");
            super.close();
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState =
                    context.getOperatorStateStore().getListState(FAILING_MAPPER_STATE_DESCRIPTOR);
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                state = Iterables.get(listState.get(), 0, new FailingMapperState(0, 0));
                state.runNumber = getRuntimeContext().getAttemptNumber();
            }
            checkFail(failDuringRecovery, "initializeState");
        }

        /** State for {@link FailingMapper}. */
        protected static class FailingMapperState {
            protected long completedCheckpoints;
            protected long runNumber;

            protected FailingMapperState(long completedCheckpoints, long runNumber) {
                this.completedCheckpoints = completedCheckpoints;
                this.runNumber = runNumber;
            }
        }
    }

    /** Base for state of the a specific {@link VerifyingSinkBase}. */
    public static class VerifyingSinkStateBase {
        protected long numOutOfOrderness;
        protected long numLostValues;
        protected long numDuplicates;
        protected long numOutput = 0;
        protected long completedCheckpoints;

        @Override
        public String toString() {
            return "StateBase{"
                    + "numOutOfOrderness="
                    + numOutOfOrderness
                    + ", numLostValues="
                    + numLostValues
                    + ", numDuplicates="
                    + numDuplicates
                    + ", numOutput="
                    + numOutput
                    + ", completedCheckpoints="
                    + completedCheckpoints
                    + '}';
        }
    }

    /**
     * A sink that checks if the members arrive in the expected order without any missing values.
     */
    protected abstract static class VerifyingSinkBase<State extends VerifyingSinkStateBase>
            extends RichSinkFunction<Long> implements CheckpointedFunction, CheckpointListener {
        private final LongCounter numOutputCounter = new LongCounter();
        private final LongCounter outOfOrderCounter = new LongCounter();
        private final LongCounter lostCounter = new LongCounter();
        private final LongCounter duplicatesCounter = new LongCounter();
        private final IntCounter numFailures = new IntCounter();
        private final Duration backpressureInterval;
        private ListState<State> stateList;
        protected transient State state;
        protected final long minCheckpoints;
        private boolean recovered;
        @Nullable private Deadline backpressureUntil;

        protected VerifyingSinkBase(long minCheckpoints, long checkpointingInterval) {
            this.minCheckpoints = minCheckpoints;
            this.backpressureInterval = Duration.ofMillis(checkpointingInterval);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator(NUM_OUTPUTS, numOutputCounter);
            getRuntimeContext().addAccumulator(NUM_OUT_OF_ORDER, outOfOrderCounter);
            getRuntimeContext().addAccumulator(NUM_DUPLICATES, duplicatesCounter);
            getRuntimeContext().addAccumulator(NUM_LOST, lostCounter);
            getRuntimeContext().addAccumulator(NUM_FAILURES, numFailures);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            final State state = createState();
            stateList =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "state", (Class<State>) state.getClass()));
            this.state = getOnlyElement(stateList.get(), state);
            LOG.info(
                    "Inducing no backpressure @ {} subtask ({} attempt)",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getAttemptNumber());
        }

        protected abstract State createState();

        protected void induceBackpressure() throws InterruptedException {
            if (backpressureUntil != null) {
                // induce heavy backpressure until enough checkpoints have been written
                Thread.sleep(1);
                if (backpressureUntil.isOverdue()) {
                    backpressureUntil = null;
                }
            }
            // after all checkpoints have been completed, the remaining data should be flushed out
            // fairly quickly
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateList.clear();
            stateList.add(state);
            if (recovered) {
                backpressureUntil = Deadline.fromNow(backpressureInterval);
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            recovered = true;
            state.completedCheckpoints++;
            if (state.completedCheckpoints < minCheckpoints) {
                this.backpressureUntil = Deadline.fromNow(backpressureInterval);
                LOG.info(
                        "Inducing backpressure until {} @ {} subtask ({} attempt)",
                        backpressureUntil,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber());
            } else {
                this.backpressureUntil = null;
                LOG.info(
                        "Inducing no backpressure @ {} subtask ({} attempt)",
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber());
            }
        }

        @Override
        public void close() throws Exception {
            numOutputCounter.add(state.numOutput);
            outOfOrderCounter.add(state.numOutOfOrderness);
            duplicatesCounter.add(state.numDuplicates);
            lostCounter.add(state.numLostValues);
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                numFailures.add(getRuntimeContext().getAttemptNumber());
            }
            LOG.info(
                    "Last state {} @ {} subtask ({} attempt)",
                    state,
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getAttemptNumber());
            super.close();
        }
    }

    static class MinEmittingFunction extends RichCoFlatMapFunction<Long, Long, Long>
            implements CheckpointedFunction {
        private ListState<State> stateList;
        private State state;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateList.clear();
            stateList.add(state);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateList =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("state", State.class));
            state = getOnlyElement(stateList.get(), new State());
        }

        @Override
        public void flatMap1(Long value, Collector<Long> out) {
            long baseValue = withoutHeader(value);
            state.lastLeft = baseValue;
            if (state.lastRight >= baseValue) {
                out.collect(value);
            }
        }

        @Override
        public void flatMap2(Long value, Collector<Long> out) {
            long baseValue = withoutHeader(value);
            state.lastRight = baseValue;
            if (state.lastLeft >= baseValue) {
                out.collect(value);
            }
        }

        private static class State {
            private long lastLeft = Long.MIN_VALUE;
            private long lastRight = Long.MIN_VALUE;
        }
    }

    protected static long withHeader(long value) {
        checkState(
                value <= Integer.MAX_VALUE,
                "Value too large for header, this indicates that the test is running too long.");
        return value ^ HEADER;
    }

    protected static long withoutHeader(long value) {
        checkHeader(value);
        return value ^ HEADER;
    }

    protected static long checkHeader(long value) {
        if ((value & HEADER_MASK) != HEADER) {
            throw new IllegalArgumentException(
                    "Stream corrupted. Cannot find the header "
                            + Long.toHexString(HEADER)
                            + " in the value "
                            + Long.toHexString(value));
        }
        return value;
    }

    private static class TestException extends Exception {
        public TestException(String s) {
            super(s);
        }
    }
}
