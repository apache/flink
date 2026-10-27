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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;

/**
 * Integration test: recovery with filtering must keep at most one pre-filter source buffer in
 * flight per task, even when records are large enough to span many source buffers.
 *
 * <p>Strategy: configure the network memory segment to Flink's minimum page size and generate
 * records several times larger. Every record therefore forces the recovery loop to allocate many
 * consecutive pre-filter buffers, exercising the serialized getBuffer/recycle cycle that must reuse
 * a single heap segment. A sleeping downstream operator induces back-pressure on the keyBy shuffle
 * so that inflight buffers actually accumulate and are captured by the unaligned checkpoint;
 * otherwise the recovery path would have nothing to filter.
 *
 * <p>Filtering during recovery is gated by two config options. Both are pinned on explicitly in
 * {@link #getEnv} so the test deterministically exercises the filtering handler.
 *
 * <p>If the one-at-a-time invariant were violated during recovery, the runtime check in {@code
 * InputChannelRecoveredStateHandler.getPreFilterBuffer} would fail with {@link
 * IllegalStateException} and the job would fail. Reaching a completed post-recovery checkpoint is
 * therefore proof that the invariant held across all buffer cycles.
 */
@ExtendWith({TestLoggerExtension.class})
class RecoveredStateFilteringLargeRecordITCase {

    private static final int NUM_TASK_MANAGERS = 1;
    private static final int SLOTS_PER_TASK_MANAGER = 8;
    private static final int MAX_SLOTS = NUM_TASK_MANAGERS * SLOTS_PER_TASK_MANAGER;
    private static final Random RANDOM = new Random();

    /**
     * Network buffer size during the test. Pinned to the minimum Flink allows so that each record
     * forces as many consecutive getBuffer/recycle cycles on the recovery thread as possible.
     */
    private static final MemorySize SEGMENT_SIZE = new MemorySize(MemoryManager.MIN_PAGE_SIZE);

    /**
     * Characters per record. ASCII chars serialize to 1 byte each in UTF-8, so a record of this
     * many chars spans roughly {@code RECORD_CHARS / SEGMENT_SIZE} source buffers. Each such record
     * exercises that many consecutive getBuffer/recycle cycles through the filtering handler.
     */
    private static final int RECORD_CHARS = 8 * (int) SEGMENT_SIZE.getBytes();

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(
                                    new Configuration()
                                            .set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 10))
                            .setNumberTaskManagers(NUM_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
                            .build());

    @TempDir private File temporaryFolder;

    @Test
    void testLargeRecordRecoveryPreservesOneBufferInvariant(
            @InjectMiniCluster MiniCluster miniCluster) throws Exception {
        // Random parallelisms to cover a variety of rescale (and no-rescale) combinations.
        int parallelismPhase1 = RANDOM.nextInt(MAX_SLOTS) + 1;
        int parallelismPhase2 = RANDOM.nextInt(MAX_SLOTS) + 1;
        int parallelismPhase3 = RANDOM.nextInt(MAX_SLOTS) + 1;

        // Phase 1: run at parallelism N and take an unaligned checkpoint with inflight buffers.
        JobClient job1 = runJob(parallelismPhase1, null);
        CommonTestUtils.waitForJobStatus(job1, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, job1.getJobID(), false);
        String checkpointPath1 =
                CommonTestUtils.waitForCheckpointWithInflightBuffers(job1.getJobID(), miniCluster);
        job1.cancel().get();

        // Phase 2: restore from phase 1's checkpoint. Wait for one completed checkpoint with
        // inflight buffers — that checkpoint is the one taken during recovery, which is what
        // phase 3 will restore from to exercise the "recovery-of-a-recovered-checkpoint" path.
        JobClient job2 = runJob(parallelismPhase2, checkpointPath1);
        CommonTestUtils.waitForJobStatus(job2, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, job2.getJobID(), false);
        String checkpointPath2 =
                CommonTestUtils.waitForCheckpointWithInflightBuffers(job2.getJobID(), miniCluster);
        job2.cancel().get();

        // Phase 3: restore from phase 2's post-recovery checkpoint. Wait for several checkpoints
        // to run the restored job long enough to shake out stability issues beyond the first
        // post-recovery checkpoint.
        JobClient job3 = runJob(parallelismPhase3, checkpointPath2);
        CommonTestUtils.waitForJobStatus(job3, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, job3.getJobID(), false);
        CommonTestUtils.waitForNCheckpointsWithInflightBuffers(job3.getJobID(), miniCluster, 5);
        job3.cancel().get();
    }

    private JobClient runJob(int parallelism, @Nullable String recoveryPath) throws Exception {
        StreamExecutionEnvironment env = getEnv(recoveryPath);
        env.setParallelism(parallelism);

        DataStream<String> source =
                env.fromSource(
                                createLargeRecordSource(),
                                WatermarkStrategy.noWatermarks(),
                                "large-record-source")
                        .setParallelism(parallelism);

        // keyBy triggers a hash shuffle, and the slow downstream map induces back-pressure so the
        // shuffle's input channels accumulate inflight buffers — which is exactly what an
        // unaligned checkpoint captures and the rescaled recovery must then replay through the
        // filtering path.
        source.keyBy((KeySelector<String, String>) value -> value)
                .map(
                        x -> {
                            Thread.sleep(5);
                            return x;
                        })
                .name("slow-map")
                .setParallelism(parallelism)
                .sinkTo(new DiscardingSink<>())
                .name("discarding-sink")
                .setParallelism(parallelism);

        return env.executeAsync();
    }

    private StreamExecutionEnvironment getEnv(@Nullable String recoveryPath) {
        Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
        conf.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, Duration.ofSeconds(0));
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        conf.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.toURI().toString());
        conf.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        // Filtering during recovery requires BOTH flags: UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM
        // gates the feature, and CHECKPOINTING_DURING_RECOVERY_ENABLED turns it on.
        conf.set(CheckpointingOptions.UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM, true);
        conf.set(CheckpointingOptions.CHECKPOINTING_DURING_RECOVERY_ENABLED, true);
        conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, SEGMENT_SIZE);
        if (recoveryPath != null) {
            conf.set(StateRecoveryOptions.SAVEPOINT_PATH, recoveryPath);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.disableOperatorChaining();
        return env;
    }

    private static DataGeneratorSource<String> createLargeRecordSource() {
        return new DataGeneratorSource<>(
                index -> {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    char[] chars = new char[RECORD_CHARS];
                    for (int i = 0; i < chars.length; i++) {
                        chars[i] = (char) ('a' + rnd.nextInt(26));
                    }
                    return new String(chars);
                },
                new NumberSequenceSource(0, Long.MAX_VALUE - 1) {
                    @Override
                    protected List<NumberSequenceSplit> splitNumberRange(
                            long from, long to, int numSplitsIgnored) {
                        return super.splitNumberRange(from, to, MAX_SLOTS);
                    }
                },
                RateLimiterStrategy.perSecond(2000),
                Types.STRING) {};
    }
}
