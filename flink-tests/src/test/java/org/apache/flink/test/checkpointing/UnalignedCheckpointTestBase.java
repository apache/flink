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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.Rule;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;

/** Base class for tests related to unaligned checkpoints. */
public abstract class UnalignedCheckpointTestBase extends TestLogger {
    protected static final Logger LOG = LoggerFactory.getLogger(UnalignedCheckpointTestBase.class);
    protected static final String NUM_OUTPUTS = "outputs";
    protected static final String NUM_OUT_OF_ORDER = "outOfOrder";
    protected static final String NUM_FAILURES = "failures";
    protected static final String NUM_DUPLICATES = "duplicates";
    protected static final String NUM_LOST = "lost";
    public static final int BUFFER_PER_CHANNEL = 1;

    @Rule public final TemporaryFolder temp = new TemporaryFolder();

    @Rule public ErrorCollector collector = new ErrorCollector();

    @Nullable
    protected File execute(UnalignedSettings settings) throws Exception {
        final File checkpointDir = temp.newFolder();
        StreamExecutionEnvironment env = settings.createEnvironment(checkpointDir);

        settings.dagCreator.create(
                env, settings.minCheckpoints, settings.slotSharing, settings.expectedFailures - 1);
        try {
            final JobExecutionResult result = env.execute();

            collector.checkThat(
                    "NUM_OUT_OF_ORDER",
                    result.<Long>getAccumulatorResult(NUM_OUT_OF_ORDER),
                    equalTo(0L));
            collector.checkThat(
                    "NUM_DUPLICATES",
                    result.<Long>getAccumulatorResult(NUM_DUPLICATES),
                    equalTo(0L));
            collector.checkThat(
                    "NUM_LOST", result.<Long>getAccumulatorResult(NUM_LOST), equalTo(0L));
            collector.checkThat(
                    "NUM_FAILURES",
                    result.<Integer>getAccumulatorResult(NUM_FAILURES),
                    equalTo(settings.expectedFailures));
        } catch (Exception e) {
            if (settings.generateCheckpoint) {
                return Files.find(
                                checkpointDir.toPath(),
                                2,
                                (file, attr) ->
                                        attr.isDirectory()
                                                && file.getFileName().toString().startsWith("chk"))
                        .findFirst()
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

    /** A source that generates longs in a fixed number of splits. */
    protected static class LongSource
            implements Source<Long, LongSource.LongSplit, LongSource.EnumeratorState> {
        private final long minCheckpoints;
        private final int numSplits;
        private final int expectedRestarts;

        protected LongSource(long minCheckpoints, int numSplits, int expectedRestarts) {
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
            return new LongSourceReader(minCheckpoints, expectedRestarts);
        }

        @Override
        public SplitEnumerator<LongSplit, EnumeratorState> createEnumerator(
                SplitEnumeratorContext<LongSplit> enumContext) {
            List<LongSplit> splits =
                    IntStream.range(0, numSplits)
                            .mapToObj(i -> new LongSplit(i, numSplits, 0))
                            .collect(Collectors.toList());
            return new LongSplitSplitEnumerator(enumContext, new EnumeratorState(splits, 0));
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

            private final long minCheckpoints;
            private final int expectedRestarts;
            private final LongCounter numInputsCounter = new LongCounter();
            private LongSplit split;
            private int numAbortedCheckpoints;
            private boolean throttle = true;
            private int numRestarts;

            public LongSourceReader(final long minCheckpoints, int expectedRestarts) {
                this.minCheckpoints = minCheckpoints;
                this.expectedRestarts = expectedRestarts;
            }

            @Override
            public void start() {}

            @Override
            public InputStatus pollNext(ReaderOutput<Long> output) throws InterruptedException {
                if (split == null) {
                    return InputStatus.NOTHING_AVAILABLE;
                }

                output.collect(split.nextNumber, split.nextNumber);
                split.nextNumber += split.increment;

                if (throttle) {
                    // throttle source as long as sink is not backpressuring (which it does only
                    // after full recovery)
                    Thread.sleep(1);
                }
                return split.numCompletedCheckpoints >= minCheckpoints
                                && numRestarts >= expectedRestarts
                        ? InputStatus.END_OF_INPUT
                        : InputStatus.MORE_AVAILABLE;
            }

            @Override
            public List<LongSplit> snapshotState(long checkpointId) {
                if (split == null) {
                    return Collections.emptyList();
                }
                LOG.info(
                        "Snapshotted {} @ {} subtask ({} attempt)",
                        split,
                        split.nextNumber % split.increment,
                        numRestarts);
                return singletonList(split);
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) {
                if (split != null) {
                    LOG.info(
                            "notifyCheckpointComplete {} @ {} subtask ({} attempt)",
                            split.numCompletedCheckpoints,
                            split.nextNumber % split.increment,
                            numRestarts);
                    split.numCompletedCheckpoints++;
                    numAbortedCheckpoints = 0;
                    throttle = split.numCompletedCheckpoints >= minCheckpoints;
                }
            }

            @Override
            public void notifyCheckpointAborted(long checkpointId) {
                if (numAbortedCheckpoints++ > 10) {
                    // aborted too many checkpoints in a row, which usually indicates that part of
                    // the pipeline is already completed
                    // here simply also advance completed checkpoints to avoid running into a live
                    // lock
                    split.numCompletedCheckpoints++;
                }
            }

            @Override
            public CompletableFuture<Void> isAvailable() {
                return FutureUtils.completedVoidFuture();
            }

            @Override
            public void addSplits(List<LongSplit> splits) {
                if (split != null) {
                    throw new IllegalStateException(
                            "Tried to add " + splits + " but already got " + split);
                }
                split = Iterables.getOnlyElement(splits);
                LOG.info(
                        "Added split {} @ {} subtask ({} attempt)",
                        split,
                        split.nextNumber % split.increment,
                        numRestarts);
            }

            @Override
            public void notifyNoMoreSplits() {}

            @Override
            public void handleSourceEvents(SourceEvent sourceEvent) {
                if (sourceEvent instanceof RestartEvent) {
                    numRestarts = ((RestartEvent) sourceEvent).numRestarts;
                }
            }

            @Override
            public void close() throws Exception {
                if (split != null) {
                    numInputsCounter.add(split.nextNumber / split.increment);
                }
            }
        }

        private static class RestartEvent implements SourceEvent {
            final int numRestarts;

            private RestartEvent(int numRestarts) {
                this.numRestarts = numRestarts;
            }
        }

        private static class LongSplit implements SourceSplit {
            private final int increment;
            private long nextNumber;
            private int numCompletedCheckpoints;

            public LongSplit(long nextNumber, int increment, int numCompletedCheckpoints) {
                this.nextNumber = nextNumber;
                this.increment = increment;
                this.numCompletedCheckpoints = numCompletedCheckpoints;
            }

            @Override
            public String splitId() {
                return String.valueOf(increment);
            }

            @Override
            public String toString() {
                return "LongSplit{"
                        + "increment="
                        + increment
                        + ", nextNumber="
                        + nextNumber
                        + ", numCompletedCheckpoints="
                        + numCompletedCheckpoints
                        + '}';
            }
        }

        private static class LongSplitSplitEnumerator
                implements SplitEnumerator<LongSplit, EnumeratorState> {
            private final SplitEnumeratorContext<LongSplit> context;
            private final EnumeratorState state;

            private LongSplitSplitEnumerator(
                    SplitEnumeratorContext<LongSplit> context, EnumeratorState state) {
                this.context = context;
                this.state = state;
            }

            @Override
            public void start() {}

            @Override
            public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {}

            @Override
            public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

            @Override
            public void addSplitsBack(List<LongSplit> splits, int subtaskId) {
                if (!splits.isEmpty()) {
                    LOG.info("addSplitsBack {}", splits);
                    state.unassignedSplits.addAll(splits);
                }
                if (subtaskId == 0) {
                    // currently always called on failure
                    state.numRestarts++;
                }
            }

            @Override
            public void addReader(int subtaskId) {
                if (context.registeredReaders().size() == context.currentParallelism()
                        && !state.unassignedSplits.isEmpty()) {
                    int numReaders = context.registeredReaders().size();
                    Map<Integer, List<LongSplit>> assignment = new HashMap<>();
                    for (int i = 0; i < state.unassignedSplits.size(); i++) {
                        assignment
                                .computeIfAbsent(i % numReaders, t -> new ArrayList<>())
                                .add(state.unassignedSplits.get(i));
                    }
                    LOG.info("Assigning splits {}", assignment);
                    context.assignSplits(new SplitsAssignment<>(assignment));
                    state.unassignedSplits.clear();
                }
                context.sendEventToSourceReader(subtaskId, new RestartEvent(state.numRestarts));
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) {
                state.unassignedSplits.forEach(s -> s.numCompletedCheckpoints++);
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
            private final List<LongSplit> unassignedSplits;
            private int numRestarts;

            public EnumeratorState(List<LongSplit> unassignedSplits, int numRestarts) {
                this.unassignedSplits = unassignedSplits;
                this.numRestarts = numRestarts;
            }

            @Override
            public String toString() {
                return "EnumeratorState{"
                        + "unassignedSplits="
                        + unassignedSplits
                        + ", numRestarts="
                        + numRestarts
                        + '}';
            }
        }

        private static class EnumeratorVersionedSerializer
                implements SimpleVersionedSerializer<EnumeratorState> {
            private SplitVersionedSerializer splitVersionedSerializer =
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
                                        + 4);
                byteBuffer.putInt(state.numRestarts);
                for (final LongSplit unassignedSplit : state.unassignedSplits) {
                    byteBuffer.put(splitVersionedSerializer.serialize(unassignedSplit));
                }
                return byteBuffer.array();
            }

            @Override
            public EnumeratorState deserialize(int version, byte[] serialized) {
                final ByteBuffer byteBuffer = ByteBuffer.wrap(serialized);
                final int numRestarts = byteBuffer.getInt();

                final List<LongSplit> splits =
                        new ArrayList<>(serialized.length / SplitVersionedSerializer.LENGTH);

                final byte[] serializedSplit = new byte[SplitVersionedSerializer.LENGTH];
                while (byteBuffer.hasRemaining()) {
                    byteBuffer.get(serializedSplit);
                    splits.add(splitVersionedSerializer.deserialize(version, serializedSplit));
                }
                return new EnumeratorState(splits, numRestarts);
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
                ByteBuffer.wrap(bytes)
                        .putLong(split.nextNumber)
                        .putInt(split.increment)
                        .putInt(split.numCompletedCheckpoints);
                return bytes;
            }

            @Override
            public LongSplit deserialize(int version, byte[] serialized) {
                final ByteBuffer byteBuffer = ByteBuffer.wrap(serialized);
                return new LongSplit(
                        byteBuffer.getLong(), byteBuffer.getInt(), byteBuffer.getInt());
            }
        }
    }

    interface DagCreator {
        void create(
                StreamExecutionEnvironment environment,
                int minCheckpoints,
                boolean slotSharing,
                int expectedFailures);
    }

    /** Builder-like interface for all relevant unaligned settings. */
    protected static class UnalignedSettings {
        private int parallelism;
        private int slotsPerTaskManager = 1;
        private int minCheckpoints = 10;
        private boolean slotSharing = true;
        @Nullable private File restoreCheckpoint;
        private boolean generateCheckpoint = false;
        private int numSlots;
        private int numBuffers;
        private int expectedFailures = 0;
        private final DagCreator dagCreator;
        private int alignmentTimeout = 0;

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

        public UnalignedSettings setNumBuffers(int numBuffers) {
            this.numBuffers = numBuffers;
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

        public StreamExecutionEnvironment createEnvironment(File checkpointDir) {
            Configuration conf = new Configuration();

            conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);
            conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, .9f);
            final int taskManagers = (numSlots + slotsPerTaskManager - 1) / slotsPerTaskManager;
            conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, taskManagers);
            conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4kb"));
            conf.set(
                    TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse(numBuffers * 4 + "kb"));

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
            lastValue = value;
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
            LOG.info(
                    "Init state {} @ {} subtask ({} attempt)",
                    this.state,
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getAttemptNumber());
        }

        protected abstract State createState();

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            LOG.info(
                    "Snapshot state {} @ {} subtask ({} attempt)",
                    state,
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getAttemptNumber());
            stateList.clear();
            stateList.add(state);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            state.completedCheckpoints++;
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
}
