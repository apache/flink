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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.Collections;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;

/**
 * Integration test for unaligned checkpoint rescaling when one downstream task receives multiple
 * inputs derived from the same upstream stream.
 */
@ExtendWith(ParameterizedTestExtension.class)
class UnalignedCheckpointRescaleSameUpstreamITCase {

    private static final int INITIAL_PARALLELISM = 2;
    private static final int RESTORED_PARALLELISM = 4;
    private static final int SLOTS_PER_TASK_MANAGER = 8;
    private static final int CHECKPOINTS_TO_WAIT = 10;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(
                                    new Configuration()
                                            .set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 50))
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
                            .build());

    @TempDir private File temporaryFolder;

    @Parameter private boolean recoverOutputOnDownstream;

    @Parameter(1)
    private SameUpstreamDag dag;

    @Parameters(name = "recoverOutputOnDownstream={0}, dag={1}")
    private static Object[][] parameters() {
        return new Object[][] {
            new Object[] {false, SameUpstreamDag.CONNECT},
            new Object[] {true, SameUpstreamDag.CONNECT},
            new Object[] {false, SameUpstreamDag.UNION},
            new Object[] {true, SameUpstreamDag.UNION}
        };
    }

    @TestTemplate
    void testRescaleFromUnalignedCheckpointWithSameUpstream(
            @InjectMiniCluster MiniCluster miniCluster) throws Exception {
        final JobGraph initialJobGraph =
                createJobGraph(null, INITIAL_PARALLELISM, recoverOutputOnDownstream);

        final JobClient initialJobClient = submitJob(initialJobGraph, miniCluster);
        final String checkpointPath;
        try {
            waitForRunning(initialJobClient, miniCluster);
            checkpointPath =
                    CommonTestUtils.waitForCheckpointWithInflightBuffers(
                            initialJobGraph.getJobID(), miniCluster, CHECKPOINTS_TO_WAIT);
        } finally {
            CommonTestUtils.terminateJob(initialJobClient);
        }

        final JobGraph restoredJobGraph =
                createJobGraph(checkpointPath, RESTORED_PARALLELISM, recoverOutputOnDownstream);

        final JobClient restoredJobClient = submitJob(restoredJobGraph, miniCluster);
        try {
            waitForRunning(restoredJobClient, miniCluster);
            CommonTestUtils.waitForCheckpointWithInflightBuffers(
                    restoredJobGraph.getJobID(), miniCluster);
        } finally {
            cancelIfRunning(restoredJobClient);
        }
    }

    private JobGraph createJobGraph(
            @Nullable String recoveryPath, int parallelism, boolean recoverOutputOnDownstream)
            throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(
                        createConfiguration(recoveryPath, recoverOutputOnDownstream));
        env.disableOperatorChaining();
        dag.build(env, parallelism);
        return env.getStreamGraph().getJobGraph();
    }

    private Configuration createConfiguration(
            @Nullable String recoveryPath, boolean recoverOutputOnDownstream) {
        final Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
        conf.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, Duration.ofSeconds(0));
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        conf.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.toURI().toString());
        conf.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        conf.set(
                CheckpointingOptions.UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM,
                recoverOutputOnDownstream);
        conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4 kb"));
        if (recoveryPath != null) {
            conf.set(StateRecoveryOptions.SAVEPOINT_PATH, recoveryPath);
        }
        return conf;
    }

    private static JobClient submitJob(JobGraph jobGraph, MiniCluster miniCluster)
            throws Exception {
        miniCluster.submitJob(jobGraph).get();
        return new MiniClusterJobClient(
                jobGraph.getJobID(),
                miniCluster,
                Thread.currentThread().getContextClassLoader(),
                MiniClusterJobClient.JobFinalizationBehavior.NOTHING);
    }

    private static void waitForRunning(JobClient jobClient, MiniCluster miniCluster)
            throws Exception {
        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient.getJobID(), false);
    }

    private static void cancelIfRunning(JobClient jobClient) throws Exception {
        if (jobClient.getJobStatus().get() != JobStatus.FAILED) {
            CommonTestUtils.terminateJob(jobClient);
        }
    }

    private static class SleepingCoMap<T> implements CoMapFunction<T, T, T> {
        @Override
        public T map1(T value) throws Exception {
            Thread.sleep(1);
            return value;
        }

        @Override
        public T map2(T value) throws Exception {
            Thread.sleep(5);
            return value;
        }
    }

    private static class SleepingMap<T> implements MapFunction<T, T> {
        @Override
        public T map(T value) throws Exception {
            Thread.sleep(5);
            return value;
        }
    }

    private enum SameUpstreamDag {
        CONNECT {
            @Override
            void build(StreamExecutionEnvironment env, int parallelism) {
                final DataStream<Long> upstream =
                        env.fromSequence(0, Long.MAX_VALUE)
                                .name("Upstream")
                                .uid("upstream")
                                .setParallelism(parallelism);
                final DataStream<Long> leftInput = upstream.rebalance();
                final DataStream<Long> rightInput =
                        upstream.keyBy((KeySelector<Long, Long>) value -> value);

                leftInput
                        .connect(rightInput)
                        .map(new SleepingCoMap<>())
                        .name("Co-Map")
                        .uid("co-map")
                        .setParallelism(parallelism)
                        .sinkTo(new DiscardingSink<>())
                        .name("Discarding Sink")
                        .uid("sink")
                        .setParallelism(parallelism);
            }
        },
        UNION {
            @Override
            void build(StreamExecutionEnvironment env, int parallelism) {
                final DataStream<Long> upstream =
                        env.fromSequence(0, Long.MAX_VALUE)
                                .name("Upstream")
                                .uid("upstream")
                                .setParallelism(parallelism);
                final DataStream<Long> leftInput = upstream.rebalance();
                final DataStream<Long> rightInput =
                        upstream.keyBy((KeySelector<Long, Long>) value -> value);

                leftInput
                        .union(rightInput)
                        .map(new SleepingMap<>())
                        .name("Slow Map")
                        .uid("slow-map")
                        .setParallelism(parallelism)
                        .sinkTo(new DiscardingSink<>())
                        .name("Discarding Sink")
                        .uid("sink")
                        .setParallelism(parallelism);
            }
        };

        abstract void build(StreamExecutionEnvironment env, int parallelism);
    }
}
