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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.util.ratelimit.GatedRateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2.DefaultCommitter;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

/**
 * Integration test for {@link org.apache.flink.api.connector.sink.Sink} run time implementation.
 */
public class SinkV2ITCase extends AbstractTestBase {
    static final List<Integer> SOURCE_DATA =
            Arrays.asList(
                    895, 127, 148, 161, 148, 662, 822, 491, 275, 122, 850, 630, 682, 765, 434, 970,
                    714, 795, 288, 422);

    // source send data two times
    static final int STREAMING_SOURCE_SEND_ELEMENTS_NUM = SOURCE_DATA.size() * 2;

    static final List<String> EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE =
            SOURCE_DATA.stream()
                    // source send data two times
                    .flatMap(
                            x ->
                                    Collections.nCopies(
                                            2, Tuple3.of(x, null, Long.MIN_VALUE).toString())
                                            .stream())
                    .collect(Collectors.toList());

    static final List<String> EXPECTED_COMMITTED_DATA_IN_BATCH_MODE =
            SOURCE_DATA.stream()
                    .map(x -> Tuple3.of(x, null, Long.MIN_VALUE).toString())
                    .collect(Collectors.toList());

    static final Queue<Committer.CommitRequest<String>> COMMIT_QUEUE =
            new ConcurrentLinkedQueue<>();

    static final BooleanSupplier COMMIT_QUEUE_RECEIVE_ALL_DATA =
            (BooleanSupplier & Serializable)
                    () -> COMMIT_QUEUE.size() == STREAMING_SOURCE_SEND_ELEMENTS_NUM;

    @BeforeEach
    public void init() {
        COMMIT_QUEUE.clear();
    }

    @Test
    public void writerAndCommitterExecuteInStreamingMode() throws Exception {
        final StreamExecutionEnvironment env = buildStreamEnv();
        final FiniteTestSource<Integer> source =
                new FiniteTestSource<>(COMMIT_QUEUE_RECEIVE_ALL_DATA, SOURCE_DATA);

        env.addSource(source, IntegerTypeInfo.INT_TYPE_INFO)
                // Introduce the keyBy to assert unaligned checkpoint is enabled on the source ->
                // sink writer edge
                .keyBy((KeySelector<Integer, Integer>) value -> value)
                .sinkTo(
                        TestSinkV2.<Integer>newBuilder()
                                .setDefaultCommitter(
                                        (Supplier<Queue<Committer.CommitRequest<String>>>
                                                        & Serializable)
                                                () -> COMMIT_QUEUE)
                                .build());
        executeAndVerifyStreamGraph(env);
        assertThat(
                COMMIT_QUEUE.stream()
                        .map(Committer.CommitRequest::getCommittable)
                        .collect(Collectors.toList()),
                containsInAnyOrder(EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE.toArray()));
    }

    @Test
    public void writerAndPrecommitToplogyAndCommitterExecuteInStreamingMode() throws Exception {
        final StreamExecutionEnvironment env = buildStreamEnv();
        final FiniteTestSource<Integer> source =
                new FiniteTestSource<>(COMMIT_QUEUE_RECEIVE_ALL_DATA, SOURCE_DATA);

        env.addSource(source, IntegerTypeInfo.INT_TYPE_INFO)
                // Introduce the keyBy to assert unaligned checkpoint is enabled on the source ->
                // sink writer edge
                .keyBy((KeySelector<Integer, Integer>) value -> value)
                .sinkTo(
                        TestSinkV2.<Integer>newBuilder()
                                .setDefaultCommitter(
                                        (Supplier<Queue<Committer.CommitRequest<String>>>
                                                        & Serializable)
                                                () -> COMMIT_QUEUE)
                                .setWithPreCommitTopology(true)
                                .build());
        executeAndVerifyStreamGraph(env);
        assertThat(
                COMMIT_QUEUE.stream()
                        .map(Committer.CommitRequest::getCommittable)
                        .collect(Collectors.toList()),
                containsInAnyOrder(
                        EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE.stream()
                                .map(s -> s + "Transformed")
                                .toArray()));
    }

    @Test
    public void writerAndCommitterExecuteInBatchMode() throws Exception {
        final StreamExecutionEnvironment env = buildBatchEnv();

        env.fromData(SOURCE_DATA)
                // Introduce the rebalance to assert unaligned checkpoint is enabled on the source
                // -> sink writer edge
                .rebalance()
                .sinkTo(
                        TestSinkV2.<Integer>newBuilder()
                                .setDefaultCommitter(
                                        (Supplier<Queue<Committer.CommitRequest<String>>>
                                                        & Serializable)
                                                () -> COMMIT_QUEUE)
                                .build());
        executeAndVerifyStreamGraph(env);
        assertThat(
                COMMIT_QUEUE.stream()
                        .map(Committer.CommitRequest::getCommittable)
                        .collect(Collectors.toList()),
                containsInAnyOrder(EXPECTED_COMMITTED_DATA_IN_BATCH_MODE.toArray()));
    }

    @Test
    public void writerAndPrecommitToplogyAndCommitterExecuteInBatchMode() throws Exception {
        final StreamExecutionEnvironment env = buildBatchEnv();

        env.fromData(SOURCE_DATA)
                // Introduce the rebalance to assert unaligned checkpoint is enabled on the source
                // -> sink writer edge
                .rebalance()
                .sinkTo(
                        TestSinkV2.<Integer>newBuilder()
                                .setDefaultCommitter(
                                        (Supplier<Queue<Committer.CommitRequest<String>>>
                                                        & Serializable)
                                                () -> COMMIT_QUEUE)
                                .setWithPreCommitTopology(true)
                                .build());
        executeAndVerifyStreamGraph(env);
        assertThat(
                COMMIT_QUEUE.stream()
                        .map(Committer.CommitRequest::getCommittable)
                        .collect(Collectors.toList()),
                containsInAnyOrder(
                        EXPECTED_COMMITTED_DATA_IN_BATCH_MODE.stream()
                                .map(s -> s + "Transformed")
                                .toArray()));
    }

    @ParameterizedTest
    @CsvSource({"1, 2", "2, 1", "1, 1"})
    public void writerAndCommitterExecuteInStreamingModeWithScaling(
            int initialParallelism,
            int scaledParallelism,
            @TempDir File checkpointDir,
            @InjectMiniCluster MiniCluster miniCluster,
            @InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        final DefaultCommitter committer =
                new DefaultCommitter(
                        (Supplier<Queue<Committer.CommitRequest<String>>> & Serializable)
                                () -> COMMIT_QUEUE);
        final Configuration config = createConfigForScalingTest(checkpointDir, initialParallelism);

        // first run
        final JobID jobID = runStreamingWithScalingTest(config, true, committer, clusterClient);

        // second run
        config.set(StateRecoveryOptions.SAVEPOINT_PATH, getCheckpointPath(miniCluster, jobID));
        config.set(CoreOptions.DEFAULT_PARALLELISM, scaledParallelism);
        runStreamingWithScalingTest(config, false, committer, clusterClient);

        assertThat(
                COMMIT_QUEUE.stream()
                        .map(Committer.CommitRequest::getCommittable)
                        .collect(Collectors.toList()),
                containsInAnyOrder(EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE.toArray()));
    }

    private JobID runStreamingWithScalingTest(
            Configuration config,
            boolean shouldMapperFail,
            DefaultCommitter committer,
            ClusterClient<?> clusterClient)
            throws Exception {
        final StreamExecutionEnvironment env = buildStreamEnvWithCheckpointDir(config);
        final Source<Integer, ?, ?> source = createStreamingSource();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                .rebalance()
                .map(new FailingCheckpointMapper(!shouldMapperFail))
                .sinkTo(
                        TestSinkV2.<Integer>newBuilder()
                                .setCommitter(committer)
                                .setWithPostCommitTopology(true)
                                .build());

        final JobID jobId = clusterClient.submitJob(env.getStreamGraph().getJobGraph()).get();
        clusterClient.requestJobResult(jobId).get();

        return jobId;
    }

    private String getCheckpointPath(MiniCluster miniCluster, JobID secondJobId)
            throws InterruptedException, ExecutionException, FlinkJobNotFoundException {
        final Optional<String> completedCheckpoint =
                CommonTestUtils.getLatestCompletedCheckpointPath(secondJobId, miniCluster);

        assertThat(completedCheckpoint.isPresent(), is(true));
        return completedCheckpoint.get();
    }

    private StreamExecutionEnvironment buildStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100);
        return env;
    }

    private StreamExecutionEnvironment buildStreamEnvWithCheckpointDir(Configuration config) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100);

        return env;
    }

    private StreamExecutionEnvironment buildBatchEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        return env;
    }

    private Configuration createConfigForScalingTest(File checkpointDir, int parallelism) {
        final Configuration config = new Configuration();
        config.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 2000);
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");

        return config;
    }

    private void executeAndVerifyStreamGraph(StreamExecutionEnvironment env) throws Exception {
        StreamGraph streamGraph = env.getStreamGraph();
        assertNoUnalignedCheckpointInSink(streamGraph);
        assertUnalignedCheckpointInNonSink(streamGraph);
        env.execute(streamGraph);
    }

    private void assertNoUnalignedCheckpointInSink(StreamGraph streamGraph) {
        // all the out edges between sink nodes should not support unaligned checkpoints
        org.assertj.core.api.Assertions.assertThat(streamGraph.getStreamNodes())
                .filteredOn(t -> t.getOperatorName().contains("Sink"))
                .flatMap(StreamNode::getOutEdges)
                .allMatch(e -> !e.supportsUnalignedCheckpoints())
                .isNotEmpty();
    }

    private void assertUnalignedCheckpointInNonSink(StreamGraph streamGraph) {
        // All connections are rebalance between source and source, so all the out edges of nodes
        // upstream of the sink should support unaligned checkpoints
        org.assertj.core.api.Assertions.assertThat(streamGraph.getStreamNodes())
                .filteredOn(t -> !t.getOperatorName().contains("Sink"))
                .flatMap(StreamNode::getOutEdges)
                .allMatch(StreamEdge::supportsUnalignedCheckpoints)
                .isNotEmpty();
    }

    /**
     * A stream source that: 1) emits a list of elements without allowing checkpoints, 2) then waits
     * for two more checkpoints to complete, 3) then re-emits the same elements before 4) waiting
     * for another two checkpoints and 5) exiting.
     */
    private Source<Integer, ?, ?> createStreamingSource() {
        RateLimiterStrategy rateLimiterStrategy =
                parallelism -> new BurstingRateLimiter(SOURCE_DATA.size() / 4, 2);
        return new DataGeneratorSource<>(
                l -> SOURCE_DATA.get(l.intValue() % SOURCE_DATA.size()),
                SOURCE_DATA.size() * 2L,
                rateLimiterStrategy,
                IntegerTypeInfo.INT_TYPE_INFO);
    }

    private static class BurstingRateLimiter implements RateLimiter {
        private final RateLimiter rateLimiter;
        private final int numCheckpointCooldown;
        private int cooldown;

        public BurstingRateLimiter(int recordPerCycle, int numCheckpointCooldown) {
            rateLimiter = new GatedRateLimiter(recordPerCycle);
            this.numCheckpointCooldown = numCheckpointCooldown;
        }

        @Override
        public CompletionStage<Void> acquire() {
            CompletionStage<Void> stage = rateLimiter.acquire();
            cooldown = numCheckpointCooldown;
            return stage;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (cooldown-- <= 0) {
                rateLimiter.notifyCheckpointComplete(checkpointId);
            }
        }
    }

    private static class FailingCheckpointMapper
            implements MapFunction<Integer, Integer>, CheckpointListener {

        private static final AtomicBoolean failed = new AtomicBoolean(false);
        private long lastCheckpointId = 0;
        private int emittedBetweenCheckpoint = 0;

        FailingCheckpointMapper(boolean failed) {
            FailingCheckpointMapper.failed.set(failed);
        }

        @Override
        public Integer map(Integer value) {
            if (lastCheckpointId >= 1 && emittedBetweenCheckpoint > 0 && !failed.get()) {
                failed.set(true);
                throw new RuntimeException("Planned exception.");
            }
            emittedBetweenCheckpoint++;
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            lastCheckpointId = checkpointId;
            emittedBetweenCheckpoint = 0;
        }
    }
}
