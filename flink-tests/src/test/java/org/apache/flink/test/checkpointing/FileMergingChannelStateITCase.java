/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.util.CheckpointStorageUtils;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.state.ChannelStateHelper.collectUniqueDisposableInChannelState;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests recovery of file-merged channel state after a job is restarted from a checkpoint. */
class FileMergingChannelStateITCase {

    private static final Logger LOG = LoggerFactory.getLogger(FileMergingChannelStateITCase.class);

    private static final int TASK_MANAGER_COUNT = 3;
    private static final int WORD_COUNT = 16;
    private static final long RECORD_COUNT = 160_000L;
    private static final long EXPECTED_COUNT_PER_WORD = RECORD_COUNT / WORD_COUNT;
    private static final String SLOW_MAPPER_UID = "slow-word-mapper";

    private static final List<String> WORDS =
            Arrays.asList(
                    "apple",
                    "banana",
                    "cherry",
                    "date",
                    "elderberry",
                    "fig",
                    "grape",
                    "honeydew",
                    "kiwi",
                    "lemon",
                    "mango",
                    "nectarine",
                    "orange",
                    "papaya",
                    "quince",
                    "raspberry");

    @TempDir private java.nio.file.Path checkpointDirectory;

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(clusterConfiguration())
                            .setNumberTaskManagers(TASK_MANAGER_COUNT)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private static Configuration clusterConfiguration() {
        return new Configuration()
                .set(CheckpointingOptions.FILE_MERGING_ENABLED, true)
                .set(CheckpointingOptions.FILE_MERGING_ACROSS_BOUNDARY, false)
                .set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 50);
    }

    @Test
    void testRestoreFileMergedChannelState(@InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        final SharedReference<AtomicBoolean> mapperThrottle =
                sharedObjects.add(new AtomicBoolean(true));
        final SharedReference<AtomicLongArray> result =
                sharedObjects.add(new AtomicLongArray(WORD_COUNT));
        final String checkpointPath;
        final StreamExecutionEnvironment initialEnv =
                createEnvironment(mapperThrottle, result, null);
        final JobClient initialJobClient =
                initialEnv.executeAsync("file-merging-channel-state-word-count-initial");

        try {
            CommonTestUtils.waitForAllTaskRunning(miniCluster, initialJobClient.getJobID(), true);
            checkpointPath =
                    waitForCheckpointWithSlowMapperChannelState(
                            initialJobClient.getJobID(), miniCluster);
            assertFileMergedChannelState(TestUtils.loadCheckpointMetadata(checkpointPath));
        } finally {
            try {
                CommonTestUtils.terminateJob(initialJobClient);
            } finally {
                initialEnv.close();
            }
        }

        final StreamExecutionEnvironment restoredEnv =
                createEnvironment(mapperThrottle, result, checkpointPath);
        final JobClient restoredJobClient =
                restoredEnv.executeAsync("file-merging-channel-state-word-count-restored");

        try {
            CommonTestUtils.waitForAllTaskRunning(miniCluster, restoredJobClient.getJobID(), true);
            mapperThrottle.get().set(false);

            final JobExecutionResult executionResult =
                    restoredJobClient.getJobExecutionResult().get(2, TimeUnit.MINUTES);
            assertThat(executionResult.getJobID()).isEqualTo(restoredJobClient.getJobID());
            for (int i = 0; i < WORD_COUNT; i++) {
                assertThat(result.get().get(i))
                        .as("final count for word %s", WORDS.get(i))
                        .isEqualTo(EXPECTED_COUNT_PER_WORD);
            }
        } finally {
            try {
                if (!restoredJobClient.getJobExecutionResult().isDone()) {
                    CommonTestUtils.terminateJob(restoredJobClient);
                }
            } finally {
                restoredEnv.close();
            }
        }
    }

    private StreamExecutionEnvironment createEnvironment(
            SharedReference<AtomicBoolean> mapperThrottle,
            SharedReference<AtomicLongArray> result,
            @Nullable String recoveryPath) {
        final Configuration configuration = new Configuration();
        configuration.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        if (recoveryPath != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, recoveryPath);
        }
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(1), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ZERO);
        CheckpointStorageUtils.configureFileSystemCheckpointStorage(
                env, checkpointDirectory.toUri());
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 3, 100L);

        env.fromSequence(0L, RECORD_COUNT - 1L)
                .setParallelism(1)
                .slotSharingGroup("source")
                .rebalance()
                .map(new SlowWordMapper(mapperThrottle))
                .setParallelism(1)
                .slotSharingGroup("channel")
                .uid(SLOW_MAPPER_UID)
                .keyBy(value -> value.f0)
                .sum(1)
                .setParallelism(1)
                .slotSharingGroup("state")
                .addSink(new ResultSink(result))
                .setParallelism(1)
                .slotSharingGroup("state");
        return env;
    }

    /**
     * Returns the path of a completed checkpoint that carries in-flight input channel state for the
     * slow mapper.
     *
     * <p>Whether a particular checkpoint contains in-flight data for a particular subtask depends
     * on where the barriers happen to be when the checkpoint is triggered, so waiting for a fixed
     * number of checkpoints - or for the latest checkpoint that persisted <em>any</em> in-flight
     * data anywhere in the job - also accepts checkpoints that do not exercise channel state
     * recovery for the mapper at all. Inspect the metadata of every completed checkpoint instead
     * and return the first one that really contains the state this test is about.
     */
    private static String waitForCheckpointWithSlowMapperChannelState(
            JobID jobID, MiniCluster miniCluster) throws Exception {
        final Set<Long> inspectedCheckpoints = new HashSet<>();
        final AtomicReference<String> restorePath = new AtomicReference<>();
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobID).get();
                    for (CompletedCheckpointStats checkpoint :
                            checkpointsNotInspectedYet(graph, inspectedCheckpoints)) {
                        if (carriesSlowMapperChannelState(checkpoint)) {
                            restorePath.set(checkpoint.getExternalPath());
                            return true;
                        }
                    }
                    failIfJobStoppedCheckpointing(graph, inspectedCheckpoints);
                    return false;
                });
        return restorePath.get();
    }

    /**
     * Returns the retained checkpoints that persisted in-flight data and have not been looked at by
     * an earlier call, oldest first: the earliest usable checkpoint is the one that leaves the most
     * records for the restored job to replay.
     */
    private static List<CompletedCheckpointStats> checkpointsNotInspectedYet(
            AccessExecutionGraph graph, Set<Long> inspectedCheckpoints) {
        final CheckpointStatsSnapshot snapshot = graph.getCheckpointStatsSnapshot();
        if (snapshot == null) {
            return Collections.emptyList();
        }
        // The history is ordered from the newest to the oldest checkpoint.
        final List<AbstractCheckpointStats> history =
                new ArrayList<>(snapshot.getHistory().getCheckpoints());
        Collections.reverse(history);
        return history.stream()
                .filter(CompletedCheckpointStats.class::isInstance)
                .map(CompletedCheckpointStats.class::cast)
                .filter(checkpoint -> checkpoint.getPersistedData() > 0L)
                .filter(checkpoint -> checkpoint.getExternalPath() != null)
                .filter(checkpoint -> inspectedCheckpoints.add(checkpoint.getCheckpointId()))
                .collect(Collectors.toList());
    }

    /**
     * Returns whether restoring from the given checkpoint would exercise file-merged channel state
     * recovery, i.e. whether it holds in-flight input channel state for the slow mapper.
     */
    private static boolean carriesSlowMapperChannelState(CompletedCheckpointStats checkpoint) {
        try {
            final CheckpointMetadata metadata =
                    TestUtils.loadCheckpointMetadata(checkpoint.getExternalPath());
            return !collectChannelStateDelegates(metadata).slowMapperInputChannelState.isEmpty();
        } catch (IOException e) {
            // The checkpoint was subsumed and cleaned up while it was being inspected.
            LOG.debug("Skipping checkpoint {}.", checkpoint.getExternalPath(), e);
            return false;
        }
    }

    /**
     * Stops the wait with the job's own failure cause once the job has reached a terminal state, as
     * no further checkpoint can complete from then on.
     */
    private static void failIfJobStoppedCheckpointing(
            AccessExecutionGraph graph, Set<Long> inspectedCheckpoints) {
        if (!graph.getState().isGloballyTerminalState()) {
            return;
        }
        final ErrorInfo failureInfo = graph.getFailureInfo();
        throw new IllegalStateException(
                String.format(
                        "Job reached the terminal state %s before completing a checkpoint with "
                                + "in-flight input channel state for %s. Inspected checkpoints: %s.",
                        graph.getState(), SLOW_MAPPER_UID, inspectedCheckpoints),
                failureInfo == null ? null : failureInfo.getException());
    }

    private static void assertFileMergedChannelState(CheckpointMetadata metadata) {
        final ChannelStateDelegates delegates = collectChannelStateDelegates(metadata);

        assertThat(delegates.all)
                .as("channel state delegates in the checkpoint")
                .isNotEmpty()
                .allSatisfy(
                        handle -> assertThat(handle).isInstanceOf(SegmentFileStateHandle.class));
        assertThat(delegates.all.stream().mapToLong(StreamStateHandle::getStateSize).sum())
                .isPositive();
        assertThat(delegates.slowMapperInputChannelState)
                .as("channel state delegates belonging to the stateless slow mapper")
                .isNotEmpty();
    }

    private static ChannelStateDelegates collectChannelStateDelegates(CheckpointMetadata metadata) {
        final ChannelStateDelegates delegates = new ChannelStateDelegates();
        for (OperatorState operatorState : metadata.getOperatorStates()) {
            for (OperatorSubtaskState subtaskState : operatorState.getStates()) {
                collectUniqueDisposableInChannelState(
                                Stream.of(
                                        subtaskState.getInputChannelState(),
                                        subtaskState.getUpstreamOutputBufferState(),
                                        subtaskState.getResultSubpartitionState()))
                        .forEach(delegates.all::add);
                if (operatorState.getOperatorUid().filter(SLOW_MAPPER_UID::equals).isPresent()) {
                    collectUniqueDisposableInChannelState(
                                    Stream.of(subtaskState.getInputChannelState()))
                            .forEach(delegates.slowMapperInputChannelState::add);
                }
            }
        }
        return delegates;
    }

    /** The channel state delegates found in a checkpoint, split by what the test asserts on. */
    private static final class ChannelStateDelegates {

        private final List<StreamStateHandle> all = new ArrayList<>();
        private final List<StreamStateHandle> slowMapperInputChannelState = new ArrayList<>();
    }

    private static final class SlowWordMapper extends RichMapFunction<Long, Tuple2<String, Long>> {

        private static final long serialVersionUID = 1L;

        private final SharedReference<AtomicBoolean> mapperThrottle;

        private SlowWordMapper(SharedReference<AtomicBoolean> mapperThrottle) {
            this.mapperThrottle = mapperThrottle;
        }

        @Override
        public Tuple2<String, Long> map(Long value) throws Exception {
            if (mapperThrottle.get().get()) {
                Thread.sleep(1L);
            }
            return Tuple2.of(WORDS.get((int) (value % WORD_COUNT)), 1L);
        }
    }

    private static final class ResultSink
            implements SinkFunction<Tuple2<String, Long>>, CheckpointedFunction {

        private static final long serialVersionUID = 1L;

        private final SharedReference<AtomicLongArray> result;

        private transient ListState<long[]> resultState;
        private long[] counts = new long[WORD_COUNT];

        private ResultSink(SharedReference<AtomicLongArray> result) {
            this.result = result;
        }

        @Override
        public void invoke(Tuple2<String, Long> value, Context context) {
            final int wordIndex = WORDS.indexOf(value.f0);
            counts[wordIndex] = value.f1;
            result.get().set(wordIndex, value.f1);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            resultState.update(Arrays.asList(counts.clone()));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            resultState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "word-count-results",
                                            LongPrimitiveArraySerializer.INSTANCE));
            counts = new long[WORD_COUNT];
            if (context.isRestored()) {
                for (long[] restoredCounts : resultState.get()) {
                    counts = restoredCounts.clone();
                }
            }
            for (int i = 0; i < WORD_COUNT; i++) {
                result.get().set(i, counts[i]);
            }
        }
    }
}
