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
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
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

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.state.ChannelStateHelper.collectUniqueDisposableInChannelState;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests recovery of file-merged channel state after a job is restarted from a checkpoint. */
class FileMergingChannelStateITCase {

    private static final int TASK_MANAGER_COUNT = 3;
    private static final int WORD_COUNT = 16;
    private static final int INITIAL_CHECKPOINTS_TO_WAIT = 2;
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
            // The first periodic checkpoint can start before the slow mapper has accumulated input
            // channel state.
            checkpointPath =
                    CommonTestUtils.waitForCheckpointWithInflightBuffers(
                            initialJobClient.getJobID(), miniCluster, INITIAL_CHECKPOINTS_TO_WAIT);
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

    private static void assertFileMergedChannelState(CheckpointMetadata metadata) {
        final List<StreamStateHandle> channelStateDelegates = new ArrayList<>();
        final List<StreamStateHandle> slowMapperChannelStateDelegates = new ArrayList<>();
        for (OperatorState operatorState : metadata.getOperatorStates()) {
            for (OperatorSubtaskState subtaskState : operatorState.getStates()) {
                final List<StreamStateHandle> subtaskChannelStateDelegates =
                        collectUniqueDisposableInChannelState(
                                        Stream.of(
                                                subtaskState.getInputChannelState(),
                                                subtaskState.getUpstreamOutputBufferState(),
                                                subtaskState.getResultSubpartitionState()))
                                .collect(Collectors.toList());
                channelStateDelegates.addAll(subtaskChannelStateDelegates);
                if (operatorState.getOperatorUid().filter(SLOW_MAPPER_UID::equals).isPresent()) {
                    collectUniqueDisposableInChannelState(
                                    Stream.of(subtaskState.getInputChannelState()))
                            .forEach(slowMapperChannelStateDelegates::add);
                }
            }
        }

        assertThat(channelStateDelegates)
                .as("channel state delegates in the checkpoint")
                .isNotEmpty()
                .allSatisfy(
                        handle -> assertThat(handle).isInstanceOf(SegmentFileStateHandle.class));
        assertThat(channelStateDelegates.stream().mapToLong(StreamStateHandle::getStateSize).sum())
                .isPositive();
        assertThat(slowMapperChannelStateDelegates)
                .as("channel state delegates belonging to the stateless slow mapper")
                .isNotEmpty();
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
