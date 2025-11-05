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
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;

/**
 * Integration test for rescaling jobs with mixed (UC-supported and UC-unsupported) exchanges from
 * an unaligned checkpoint.
 */
@RunWith(Parameterized.class)
public class UnalignedCheckpointRescaleWithMixedExchangesITCase extends TestLogger {

    private static final int NUM_TASK_MANAGERS = 1;
    private static final int SLOTS_PER_TASK_MANAGER = 10;
    private static final int MAX_SLOTS = NUM_TASK_MANAGERS * SLOTS_PER_TASK_MANAGER;
    private static final Random RANDOM = new Random();

    private static MiniClusterWithClientResource cluster;
    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Parameterized.Parameter public ExecuteJobViaEnv executeJobViaEnv;

    @Parameterized.Parameters(name = "Test case {index}")
    public static Collection<ExecuteJobViaEnv> parameter() {
        return List.of(
                UnalignedCheckpointRescaleWithMixedExchangesITCase::createMultiOutputDAG,
                UnalignedCheckpointRescaleWithMixedExchangesITCase::createMultiInputDAG,
                UnalignedCheckpointRescaleWithMixedExchangesITCase::createRescalePartitionerDAG,
                UnalignedCheckpointRescaleWithMixedExchangesITCase::createMixedComplexityDAG,
                UnalignedCheckpointRescaleWithMixedExchangesITCase::createPartEmptyHashExchangeDAG);
    }

    @Before
    public void setup() throws Exception {
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(new Configuration())
                                .setNumberTaskManagers(NUM_TASK_MANAGERS)
                                .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
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

    /**
     * Tests rescaling from an unaligned checkpoint with different job structures that have mixed
     * (UC-supported and UC-unsupported) exchanges.
     */
    @Test
    public void testRescaleFromUnalignedCheckpoint() throws Exception {
        final MiniCluster miniCluster = cluster.getMiniCluster();

        // Step 1: Run the job with initial parallelism and take a checkpoint
        JobClient jobClient1 = executeJobViaEnv.executeJob(getUnalignedCheckpointEnv(null));

        CommonTestUtils.waitForJobStatus(jobClient1, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient1.getJobID(), false);
        String checkpointPath =
                CommonTestUtils.waitForCheckpointWithInflightBuffers(
                        jobClient1.getJobID(), miniCluster);
        jobClient1.cancel().get();

        // Step 2: Restore the job with a different parallelism
        JobClient jobClient2 =
                executeJobViaEnv.executeJob(getUnalignedCheckpointEnv(checkpointPath));

        CommonTestUtils.waitForJobStatus(jobClient2, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient2.getJobID(), false);
        CommonTestUtils.waitForCheckpointWithInflightBuffers(jobClient2.getJobID(), miniCluster);
        jobClient2.cancel().get();
    }

    private StreamExecutionEnvironment getUnalignedCheckpointEnv(@Nullable String recoveryPath)
            throws IOException {
        Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
        // Disable aligned timeout to ensure it works with unaligned checkpoint directly
        conf.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, Duration.ofSeconds(0));
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        conf.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        conf.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                temporaryFolder.newFolder().toURI().toString());
        conf.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        // Decrease the memory segment size to avoid the test is so slow for some reasons:
        // 1. When a flink job recovers from unaligned checkpoint, it has to consume all inflight
        // buffers during recovery phase. The smaller the buffer size, the fewer records are
        // snapshotted during the checkpoint, resulting in fewer records are needed to be consumed
        // during recovery.
        // 2. Forward or rescale exchange does not support unaligned checkpoint, it means Forward
        // or rescale exchanges are still using aligned checkpoint even if unaligned checkpoint is
        // enabled. All buffers(records) before barrier must be consumed for aligned checkpoint.
        // The smaller the buffer size means the fewer records are needed to be consumed during
        // aligned checkpoint.
        conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("1 kb"));
        // To prevent the picked checkpoint is deleted
        conf.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 50);
        if (recoveryPath != null) {
            conf.set(StateRecoveryOptions.SAVEPOINT_PATH, recoveryPath);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.disableOperatorChaining();
        return env;
    }

    private static JobClient createMultiOutputDAG(StreamExecutionEnvironment env) throws Exception {
        DataGeneratorSource<Long> source =
                new DataGeneratorSource<>(
                        index -> index,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(5000),
                        Types.LONG);

        int sourceParallelism = getRandomParallelism();
        DataStream<Long> sourceStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Data Generator")
                        .setParallelism(sourceParallelism);

        sourceStream
                .keyBy((KeySelector<Long, Long>) value -> value)
                .map(
                        x -> {
                            Thread.sleep(5);
                            return x;
                        })
                .name("Map after keyBy")
                .setParallelism(getRandomParallelism());

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        sourceStream
                .map(
                        x -> {
                            Thread.sleep(1);
                            return x;
                        })
                .name("Map after forward")
                .setParallelism(sourceParallelism);

        return env.executeAsync();
    }

    private static JobClient createMultiInputDAG(StreamExecutionEnvironment env) throws Exception {
        int source1Parallelism = getRandomParallelism();
        DataGeneratorSource<Long> source1 =
                new DataGeneratorSource<>(
                        index -> index,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(5000),
                        Types.LONG);
        DataStream<Long> sourceStream1 =
                env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Source 1")
                        .setParallelism(source1Parallelism);

        int source2Parallelism = getRandomParallelism();
        DataGeneratorSource<Long> source2 =
                new DataGeneratorSource<>(
                        index -> index,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(5000),
                        Types.LONG);
        DataStream<Long> sourceStream2 =
                env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Source 2")
                        .setParallelism(source2Parallelism);

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        DataStream<Long> forwardedStream =
                sourceStream2.map(x -> x).setParallelism(source2Parallelism);

        sourceStream1
                .rebalance()
                .connect(forwardedStream.rebalance())
                .map(new SleepingCoMap())
                .name("Co-Map")
                .setParallelism(getRandomParallelism());

        return env.executeAsync();
    }

    private static JobClient createRescalePartitionerDAG(StreamExecutionEnvironment env)
            throws Exception {
        DataGeneratorSource<Long> source =
                new DataGeneratorSource<>(
                        index -> index,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(5000),
                        Types.LONG);

        int sourceParallelism = getRandomParallelism();
        DataStream<Long> sourceStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Data Generator")
                        .setParallelism(sourceParallelism);

        sourceStream
                .keyBy((KeySelector<Long, Long>) value -> value)
                .map(
                        x -> {
                            Thread.sleep(5);
                            return x;
                        })
                .name("Map after keyBy")
                .setParallelism(getRandomParallelism());

        sourceStream
                .rescale()
                .map(
                        x -> {
                            Thread.sleep(1);
                            return x;
                        })
                .name("Map after rescale")
                .setParallelism(getRandomParallelism());

        return env.executeAsync();
    }

    private static JobClient createMixedComplexityDAG(StreamExecutionEnvironment env)
            throws Exception {
        // Multi-input part
        int source1Parallelism = getRandomParallelism();
        DataGeneratorSource<Long> source1 =
                new DataGeneratorSource<>(
                        index -> index,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(5000),
                        Types.LONG);
        DataStream<Long> sourceStream1 =
                env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Source 1")
                        .setParallelism(source1Parallelism);

        int source2Parallelism = getRandomParallelism();
        DataGeneratorSource<Long> source2 =
                new DataGeneratorSource<>(
                        index -> index,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(5000),
                        Types.LONG);
        DataStream<Long> sourceStream2 =
                env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Source 2")
                        .setParallelism(source2Parallelism);

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        DataStream<Long> forwardedStream =
                sourceStream2.map(x -> x).setParallelism(source2Parallelism);

        DataStream<Long> multiInputMap =
                sourceStream1
                        .rebalance()
                        .connect(forwardedStream.rebalance())
                        .map(new SleepingCoMap())
                        .name("Co-Map")
                        .setParallelism(getRandomParallelism());

        // Multi-output part
        multiInputMap
                .keyBy((KeySelector<Long, Long>) value -> value)
                .map(
                        x -> {
                            Thread.sleep(5);
                            return x;
                        })
                .name("Map after keyBy")
                .setParallelism(getRandomParallelism());

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        multiInputMap
                .map(
                        x -> {
                            Thread.sleep(1);
                            return x;
                        })
                .name("Map after forward")
                .setParallelism(multiInputMap.getParallelism());

        return env.executeAsync();
    }

    /**
     * Creates a DAG where the downstream MapAfterKeyBy task receives input from two hash exchanges:
     * one with actual data and one that is empty due to filtering. This tests unaligned checkpoint
     * rescaling with mixed empty and non-empty hash partitions.
     */
    private static JobClient createPartEmptyHashExchangeDAG(StreamExecutionEnvironment env)
            throws Exception {
        int source1Parallelism = getRandomParallelism();
        DataGeneratorSource<Long> source1 =
                new DataGeneratorSource<>(
                        index -> index,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(5000),
                        Types.LONG);
        DataStream<Long> sourceStream1 =
                env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Source 1")
                        .setParallelism(source1Parallelism);

        int source2Parallelism = getRandomParallelism();
        DataGeneratorSource<Long> source2 =
                new DataGeneratorSource<>(
                        index -> index,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(5000),
                        Types.LONG);

        // Filter all records to simulate empty state exchange
        DataStream<Long> sourceStream2 =
                env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Source 2")
                        .setParallelism(source2Parallelism)
                        .filter(value -> false)
                        .setParallelism(source2Parallelism);

        sourceStream1
                .union(sourceStream2)
                .keyBy((KeySelector<Long, Long>) value -> value)
                .map(
                        x -> {
                            Thread.sleep(5);
                            return x;
                        })
                .name("MapAfterKeyBy")
                .setParallelism(getRandomParallelism());

        return env.executeAsync();
    }

    private static int getRandomParallelism() {
        return RANDOM.nextInt(MAX_SLOTS) + 1;
    }

    /** A simple CoMapFunction that sleeps for 1ms for each element. */
    private static class SleepingCoMap implements CoMapFunction<Long, Long, Long> {
        @Override
        public Long map1(Long value) throws Exception {
            Thread.sleep(1);
            return value;
        }

        @Override
        public Long map2(Long value) throws Exception {
            Thread.sleep(1);
            return value;
        }
    }

    public interface ExecuteJobViaEnv {
        JobClient executeJob(StreamExecutionEnvironment env) throws Exception;
    }
}
