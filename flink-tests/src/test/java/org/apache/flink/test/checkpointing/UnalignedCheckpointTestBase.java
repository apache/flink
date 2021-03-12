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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.testutils.junit.FailsWithAdaptiveScheduler;
import org.apache.flink.util.Collector;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.fail;

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
        StreamExecutionEnvironment env = settings.createEnvironment(checkpointDir);

        settings.dagCreator.create(
                env,
                settings.minCheckpoints,
                settings.slotSharing,
                settings.expectedFailures - settings.failuresAfterSourceFinishes);
        try {
            waitForCleanShutdown();
            final JobExecutionResult result = env.execute();

            checkCounters(result);
        } catch (Exception e) {
            if (settings.generateCheckpoint) {
                return Files.find(
                                checkpointDir.toPath(),
                                2,
                                (file, attr) ->
                                        attr.isDirectory()
                                                && file.getFileName().toString().startsWith("chk"))
                        .min(Comparator.comparing(Path::toString))
                        .map(Path::toFile)
                        .orElseThrow(
                                () -> new IllegalStateException("Cannot generate checkpoint", e));
            }
            throw e;
        }
        if (settings.generateCheckpoint) {
            fail("Could not generate checkpoint");
        }
        return null;
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

        protected LongSource(int minCheckpoints, int numSplits, int expectedRestarts) {
            this.minCheckpoints = minCheckpoints;
            this.numSplits = numSplits;
            this.expectedRestarts = expectedRestarts;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<Long, LongSplit> createReader(SourceReaderContext readerContext) {
            return new LongSourceReader(
                    readerContext.getIndexOfSubtask(), minCheckpoints, expectedRestarts);
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
            private int numAbortedCheckpoints;
            private int numRestarts;
            private int numCompletedCheckpoints;
            private int numCheckpointsInThisAttempt;
            private PollingState pollingState = PollingState.THROTTLING;

            enum PollingState {
                THROTTLING,
                PUMPING,
                FINISHING
            }

            public LongSourceReader(int subtaskIndex, int minCheckpoints, int expectedRestarts) {
                this.subtaskIndex = subtaskIndex;
                this.minCheckpoints = minCheckpoints;
                this.expectedRestarts = expectedRestarts;
            }

            @Override
            public void start() {}

            @Override
            public InputStatus pollNext(ReaderOutput<Long> output) throws InterruptedException {
                for (LongSplit split : splits) {
                    output.collect(withHeader(split.nextNumber), split.nextNumber);
                    split.nextNumber += split.increment;
                }

                switch (pollingState) {
                    case FINISHING:
                        return InputStatus.END_OF_INPUT;
                    case THROTTLING:
                        // throttle source as long as sink is not backpressuring (which it does only
                        // after full recovery)
                        Thread.sleep(1);
                        return InputStatus.MORE_AVAILABLE;
                    case PUMPING:
                        return InputStatus.MORE_AVAILABLE;
                    default:
                        throw new IllegalStateException("Unexpected state: " + pollingState);
                }
            }

            @Override
            public List<LongSplit> snapshotState(long checkpointId) {
                LOG.info(
                        "Snapshotted {} @ {} subtask ({} attempt)",
                        splits,
                        subtaskIndex,
                        numRestarts);
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
                numCheckpointsInThisAttempt++;
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
                    updatePollingState();
                }
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
                        "Added splits {}, pollingState={} @ {} subtask ({} attempt)",
                        splits,
                        pollingState,
                        subtaskIndex,
                        numRestarts);
            }

            @Override
            public void notifyNoMoreSplits() {
                updatePollingState();
            }

            private void updatePollingState() {
                PollingState oldState = pollingState;
                if (numCompletedCheckpoints >= minCheckpoints && numRestarts >= expectedRestarts) {
                    pollingState = PollingState.FINISHING;
                } else if (numCheckpointsInThisAttempt == 0) {
                    // speed up recovery by throttling - use a successful checkpoint as a proxy
                    // for a finished recovery
                    pollingState = PollingState.THROTTLING;
                } else {
                    // cause backpressure
                    pollingState = PollingState.PUMPING;
                }
                if (oldState != pollingState) {
                    LOG.info(
                            "Switched from {} to {} @ {} subtask ({} attempt)",
                            oldState,
                            pollingState,
                            subtaskIndex,
                            numRestarts);
                }
            }

            @Override
            public void handleSourceEvents(SourceEvent sourceEvent) {
                if (sourceEvent instanceof SyncEvent) {
                    numRestarts = ((SyncEvent) sourceEvent).numRestarts;
                    numCompletedCheckpoints = ((SyncEvent) sourceEvent).numCheckpoints;
                    updatePollingState();
                    LOG.info(
                            "Set restarts={}, numCompletedCheckpoints={}, pollingState={} @ {} subtask ({} attempt)",
                            numRestarts,
                            numCompletedCheckpoints,
                            pollingState,
                            subtaskIndex,
                            numRestarts);
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
            public EnumeratorState snapshotState() throws Exception {
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

    /** Builder-like interface for all relevant unaligned settings. */
    protected static class UnalignedSettings {
        private int parallelism;
        private int slotsPerTaskManager = 1;
        private final int minCheckpoints = 10;
        private boolean slotSharing = true;
        @Nullable private File restoreCheckpoint;
        private boolean generateCheckpoint = false;
        private int numSlots;
        int expectedFailures = 0;
        private final DagCreator dagCreator;
        private int alignmentTimeout = 0;
        private int failuresAfterSourceFinishes = 0;

        public UnalignedSettings(DagCreator dagCreator) {
            this.dagCreator = dagCreator;
        }

        public UnalignedSettings setParallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public UnalignedSettings setSlotsPerTaskManager(int slotsPerTaskManager) {
            this.slotsPerTaskManager = slotsPerTaskManager;
            return this;
        }

        public UnalignedSettings setSlotSharing(boolean slotSharing) {
            this.slotSharing = slotSharing;
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

        public UnalignedSettings setNumSlots(int numSlots) {
            this.numSlots = numSlots;
            return this;
        }

        public UnalignedSettings setExpectedFailures(int expectedFailures) {
            this.expectedFailures = expectedFailures;
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

        public StreamExecutionEnvironment createEnvironment(File checkpointDir) {
            Configuration conf = new Configuration();

            conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);
            conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, .9f);
            final int taskManagers = (numSlots + slotsPerTaskManager - 1) / slotsPerTaskManager;
            conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, taskManagers);
            conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4kb"));
            conf.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
            conf.setString(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
            if (restoreCheckpoint != null) {
                conf.set(
                        SavepointConfigOptions.SAVEPOINT_PATH,
                        restoreCheckpoint.toURI().toString());
            }

            conf.set(
                    NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL, BUFFER_PER_CHANNEL);
            conf.set(
                    NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE,
                    slotsPerTaskManager);
            conf.set(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 60000);
            conf.setString(AkkaOptions.ASK_TIMEOUT, "1 min");

            final LocalStreamEnvironment env =
                    StreamExecutionEnvironment.createLocalEnvironment(parallelism, conf);
            env.enableCheckpointing(100);
            env.getCheckpointConfig().setAlignmentTimeout(alignmentTimeout);
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
            return env;
        }

        @Override
        public String toString() {
            return "UnalignedSettings{"
                    + "parallelism="
                    + parallelism
                    + ", slotsPerTaskManager="
                    + slotsPerTaskManager
                    + ", slotSharing="
                    + slotSharing
                    + ", restoreCheckpoint="
                    + restoreCheckpoint
                    + ", generateCheckpoint="
                    + generateCheckpoint
                    + ", numSlots="
                    + numSlots
                    + ", expectedFailures="
                    + expectedFailures
                    + ", dagCreator="
                    + dagCreator
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
            throw new Exception(
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
        private ListState<State> stateList;
        protected transient State state;
        protected final long minCheckpoints;
        protected boolean backpressure;

        protected VerifyingSinkBase(long minCheckpoints) {
            this.minCheckpoints = minCheckpoints;
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
            backpressure = false;
            LOG.info(
                    "Inducing backpressure=false @ {} subtask ({} attempt)",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getAttemptNumber());
        }

        protected abstract State createState();

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateList.clear();
            stateList.add(state);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            state.completedCheckpoints++;
            boolean backpressure = this.backpressure;
            this.backpressure = state.completedCheckpoints < minCheckpoints;
            if (backpressure != this.backpressure) {
                LOG.info(
                        "Inducing backpressure={} @ {} subtask ({} attempt)",
                        this.backpressure,
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
}
