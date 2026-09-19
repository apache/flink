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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that restoring a savepoint is rejected when a chaining change places operators with
 * different recorded max parallelism onto a single keyed vertex.
 *
 * <p>A keyed operator is a normal {@code keyBy} chain head with an auto-derived max parallelism
 * ({@value #CHAIN_HEAD_MAX_PARALLELISM} for parallelism 1). A downstream stateful operator carrying
 * an explicit, different max parallelism chains after it. With chaining OFF the two are separate
 * vertices and each records its own value; with chaining ON they merge into one keyed vertex that
 * now carries two different recorded values. The savepoint's key-group count for the keyed operator
 * therefore cannot be reconciled with the downstream operator's, so restore is rejected with a
 * clear error instead of remapping keyed state through an incompatible key-group layout -- the same
 * outcome whether the downstream value is below or above the chain head's.
 */
class ChainingMaxParallelismStateLossITCase {

    private static final int NUM_KEYS = 4;
    private static final long JOB1_PER_KEY = 100;
    private static final long JOB2_PER_KEY = 50;

    /** Auto-derived max parallelism of the keyed operator (chain head) at parallelism 1. */
    private static final int CHAIN_HEAD_MAX_PARALLELISM = 128;

    private static final int EXPLICIT_BELOW_HEAD = 64;
    private static final int EXPLICIT_ABOVE_HEAD = 256;

    /** Final running count observed per key (per-key counts are monotonic). */
    private static final Map<Integer, Long> COUNTS = new ConcurrentHashMap<>();

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @TempDir private Path tempDir;

    @ParameterizedTest(name = "backend={0}")
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void rejectsRestoreWhenDownstreamMaxParallelismBelowChainHead(
            String backend,
            @InjectClusterClient ClusterClient<?> client,
            @InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        assertThatThrownBy(
                        () ->
                                savepointChainedOffRestoreChainedOn(
                                        EXPLICIT_BELOW_HEAD, backend, client, miniCluster))
                .hasStackTraceContaining("recorded different maximum parallelism");
    }

    @ParameterizedTest(name = "backend={0}")
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void rejectsRestoreWhenDownstreamMaxParallelismAboveChainHead(
            String backend,
            @InjectClusterClient ClusterClient<?> client,
            @InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        assertThatThrownBy(
                        () ->
                                savepointChainedOffRestoreChainedOn(
                                        EXPLICIT_ABOVE_HEAD, backend, client, miniCluster))
                .hasStackTraceContaining("recorded different maximum parallelism");
    }

    /**
     * Runs one savepoint (chaining OFF, keyed operator and downstream operator on separate vertices
     * at their own max parallelism) then restore (chaining ON, downstream operator chained under
     * the keyed head) cycle, returning the per-key counts if the restore is not rejected.
     */
    private Map<Integer, Long> savepointChainedOffRestoreChainedOn(
            int downstreamMaxParallelism,
            String backend,
            ClusterClient<?> client,
            MiniCluster miniCluster)
            throws Exception {
        COUNTS.clear();

        // Job 1 (chaining OFF): drive each key to JOB1_PER_KEY, then savepoint and cancel.
        final JobGraph job1 =
                buildJobGraph(false, JOB1_PER_KEY, false, downstreamMaxParallelism, backend);
        final JobID jobId1 = job1.getJobID();
        client.submitJob(job1).get();
        waitForAllTaskRunning(miniCluster, jobId1, false);
        waitUntilAllKeysReach(JOB1_PER_KEY);

        final String savepoint =
                client.triggerSavepoint(
                                jobId1,
                                tempDir.resolve("savepoints").toUri().toString(),
                                SavepointFormatType.CANONICAL)
                        .get();
        client.cancel(jobId1).get();
        waitUntilNoJobRunning(client);

        // Job 2 (chaining ON): the downstream operator chains under the keyed head, whose max
        // parallelism differs from the downstream operator's explicit one, so restore is rejected.
        COUNTS.clear();
        final JobGraph job2 =
                buildJobGraph(true, JOB2_PER_KEY, true, downstreamMaxParallelism, backend);
        job2.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepoint));
        submitJobAndWaitForResult(client, job2, getClass().getClassLoader());

        return new TreeMap<>(COUNTS);
    }

    private JobGraph buildJobGraph(
            boolean chaining,
            long elementsPerKey,
            boolean terminate,
            int downstreamMaxParallelism,
            String backend) {
        // State backend and checkpoint storage are configured per job so a single shared cluster
        // can run both backends across the parameterized cases.
        final Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, backend);
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                tempDir.resolve("checkpoints").toUri().toString());
        // Default is already true; set explicitly for clarity -- this is what lets the downstream
        // operator chain under a keyed head with a different (auto-derived) max parallelism.
        config.set(
                PipelineOptions.OPERATOR_CHAINING_CHAIN_OPERATORS_WITH_DIFFERENT_MAX_PARALLELISM,
                true);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        // No env-level max parallelism, so the keyed (chain-head) operator uses an auto-derived
        // value
        // while the downstream operator carries its own explicit one.
        env.enableCheckpointing(Duration.ofMinutes(10).toMillis());
        RestartStrategyUtils.configureNoRestartStrategy(env);
        if (!chaining) {
            env.disableOperatorChaining();
        }

        final DataStream<Integer> source =
                env.addSource(new ControllableSource(NUM_KEYS, elementsPerKey, terminate))
                        .uid("src")
                        .name("src");

        // A plain keyBy makes the keyed operator a chain head (the keyBy hash edge is a chain
        // break).
        final KeyedStream<Integer, Integer> keyed = source.keyBy(value -> value % NUM_KEYS);

        final SingleOutputStreamOperator<Tuple2<Integer, Long>> counted =
                keyed.process(new PerKeyCounter()).name("keyed").uid("keyed");

        // A forward (chainable) edge to a downstream stateful operator with an explicit, different
        // max parallelism: it becomes a chained non-head under the keyed head when chaining is on.
        final SingleOutputStreamOperator<Tuple2<Integer, Long>> mapped =
                counted.map(new StatefulPassThrough())
                        .name("mapped")
                        .uid("mapped")
                        .setMaxParallelism(downstreamMaxParallelism);

        mapped.addSink(new CountsCollectingSink()).uid("sink").name("sink");

        return env.getStreamGraph().getJobGraph();
    }

    private void waitUntilAllKeysReach(long target) throws InterruptedException {
        while (true) {
            final Map<Integer, Long> current = new TreeMap<>(COUNTS);
            if (current.size() == NUM_KEYS
                    && current.values().stream().allMatch(v -> v >= target)) {
                return;
            }
            Thread.sleep(25);
        }
    }

    private void waitUntilNoJobRunning(ClusterClient<?> client) throws Exception {
        while (!client.listJobs().get().stream()
                .allMatch(s -> s.getJobState().isGloballyTerminalState())) {
            Thread.sleep(50);
        }
    }

    /**
     * Emits each of {@code numKeys} keys {@code elementsPerKey} times, then either terminates or
     * stays alive (sleeping) so a savepoint can be taken while the job runs.
     */
    private static final class ControllableSource extends RichParallelSourceFunction<Integer> {

        private static final long serialVersionUID = 1L;

        private final int numKeys;
        private final long elementsPerKey;
        private final boolean terminateAfterEmission;

        private volatile boolean running = true;

        ControllableSource(int numKeys, long elementsPerKey, boolean terminateAfterEmission) {
            this.numKeys = numKeys;
            this.elementsPerKey = elementsPerKey;
            this.terminateAfterEmission = terminateAfterEmission;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            final Object lock = ctx.getCheckpointLock();
            for (long i = 0; i < elementsPerKey && running; i++) {
                synchronized (lock) {
                    for (int key = 0; key < numKeys; key++) {
                        ctx.collect(key);
                    }
                }
            }
            if (terminateAfterEmission) {
                return;
            }
            // Stay alive (without emitting more) so the state is frozen while a savepoint is taken.
            while (running) {
                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /** Per-key monotonic counter backed by keyed {@link ValueState}. */
    private static final class PerKeyCounter
            extends KeyedProcessFunction<Integer, Integer, Tuple2<Integer, Long>> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> counter;

        @Override
        public void open(OpenContext openContext) {
            counter =
                    getRuntimeContext().getState(new ValueStateDescriptor<>("counter", Long.class));
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<Tuple2<Integer, Long>> out)
                throws Exception {
            final Long previous = counter.value();
            final long next = (previous == null ? 0L : previous) + 1L;
            counter.update(next);
            out.collect(Tuple2.of(ctx.getCurrentKey(), next));
        }
    }

    /**
     * Pass-through map that keeps operator (non-keyed) list state, so it records an operator state
     * with its vertex's max parallelism in the savepoint.
     */
    private static final class StatefulPassThrough
            extends RichMapFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>>
            implements CheckpointedFunction {

        private static final long serialVersionUID = 1L;

        private transient ListState<Long> operatorState;

        @Override
        public Tuple2<Integer, Long> map(Tuple2<Integer, Long> value) {
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            operatorState.update(Collections.singletonList(1L));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            operatorState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("op", Long.class));
        }
    }

    /**
     * Records the maximum count seen per key so the test thread can read the final per-key counts.
     */
    private static final class CountsCollectingSink implements SinkFunction<Tuple2<Integer, Long>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void invoke(Tuple2<Integer, Long> value, Context context) {
            COUNTS.merge(value.f0, value.f1, Math::max);
        }
    }
}
