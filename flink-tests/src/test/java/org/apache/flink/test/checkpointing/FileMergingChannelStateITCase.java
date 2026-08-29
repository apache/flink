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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.state.ChannelStateHelper.collectUniqueDisposableInChannelState;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests recovery of file-merged channel state after the TaskManager is replaced. */
class FileMergingChannelStateITCase {

    private static final int TASK_MANAGER_COUNT = 3;
    private static final int WORD_COUNT = 16;
    private static final long RECORD_COUNT = 16_000L;
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
                .set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
    }

    @Test
    void testRestoreFileMergedChannelState(@InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        final SharedReference<AtomicLong> mappedRecords = sharedObjects.add(new AtomicLong());
        final SharedReference<AtomicLongArray> result =
                sharedObjects.add(new AtomicLongArray(WORD_COUNT));
        final StreamExecutionEnvironment env = createEnvironment(mappedRecords, result);
        final JobClient jobClient = env.executeAsync("file-merging-channel-state-word-count");

        try {
            waitUntilCondition(() -> mappedRecords.get().get() >= 100L, 100L, 300);

            final String checkpointPath =
                    miniCluster.triggerCheckpoint(jobClient.getJobID()).get(30, TimeUnit.SECONDS);
            assertFileMergedChannelState(TestUtils.loadCheckpointMetadata(checkpointPath));

            for (int i = 0; i < TASK_MANAGER_COUNT; i++) {
                miniCluster.terminateTaskManager(0).get(30, TimeUnit.SECONDS);
            }
            for (int i = 0; i < TASK_MANAGER_COUNT; i++) {
                miniCluster.startTaskManager();
            }

            final long mappedRecordsBeforeRecovery = mappedRecords.get().get();
            waitUntilCondition(
                    () -> mappedRecords.get().get() >= mappedRecordsBeforeRecovery + 1_000L,
                    100L,
                    300);
            final String postRecoveryCheckpointPath =
                    miniCluster.triggerCheckpoint(jobClient.getJobID()).get(30, TimeUnit.SECONDS);
            assertFileMergedChannelState(
                    TestUtils.loadCheckpointMetadata(postRecoveryCheckpointPath));

            final JobExecutionResult executionResult =
                    jobClient.getJobExecutionResult().get(2, TimeUnit.MINUTES);
            assertThat(executionResult.getJobID()).isEqualTo(jobClient.getJobID());
            for (int i = 0; i < WORD_COUNT; i++) {
                assertThat(result.get().get(i))
                        .as("final count for word %s", WORDS.get(i))
                        .isEqualTo(EXPECTED_COUNT_PER_WORD);
            }
        } finally {
            if (!jobClient.getJobExecutionResult().isDone()) {
                jobClient.cancel().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private StreamExecutionEnvironment createEnvironment(
            SharedReference<AtomicLong> mappedRecords, SharedReference<AtomicLongArray> result) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(TimeUnit.DAYS.toMillis(1), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ZERO);
        CheckpointStorageUtils.configureFileSystemCheckpointStorage(
                env, checkpointDirectory.toUri());
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 3, 100L);

        env.fromSequence(0L, RECORD_COUNT - 1L)
                .setParallelism(1)
                .slotSharingGroup("source")
                .rebalance()
                .map(new SlowWordMapper(mappedRecords))
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

        private final SharedReference<AtomicLong> mappedRecords;

        private SlowWordMapper(SharedReference<AtomicLong> mappedRecords) {
            this.mappedRecords = mappedRecords;
        }

        @Override
        public Tuple2<String, Long> map(Long value) throws Exception {
            Thread.sleep(1L);
            mappedRecords.get().incrementAndGet();
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
