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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.scheduling.ParallelismOverridesTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Restores a savepoint into a job whose stateful vertex is rescaled through {@link
 * PipelineOptions#PARALLELISM_OVERRIDES} on a {@link StreamGraph} submission. The override must
 * change the parallelism without changing the operator ids the state is keyed by, so the restore
 * succeeds without allowing non-restored state (FLINK-38770).
 */
class ParallelismOverridesSavepointITCase {

    private static final String STATEFUL_UID = "savepoint-stateful";
    private static final JobVertexID STATEFUL_VERTEX_ID =
            ParallelismOverridesTestUtils.vertexIdOfChainHead(STATEFUL_UID);
    private static final int INITIAL_PARALLELISM = 2;
    private static final int OVERRIDE = 4;
    private static final int NUM_KEYS = 100;
    private static final long WAIT_RETRY_INTERVAL_MILLIS = 100L;
    private static final int WAIT_RETRY_ATTEMPTS = 1200;

    private static final AtomicLong PROCESSED = new AtomicLong();
    private static final Set<Long> KEYS_SEEN_IN_RUN = ConcurrentHashMap.newKeySet();
    private static final AtomicBoolean RESTORED_STATE_SEEN = new AtomicBoolean();

    @TempDir Path checkpointDir;
    @TempDir Path savepointDir;

    @Test
    void testStateRestoresIntoOverriddenParallelism() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Default);
        configuration.set(RestOptions.BIND_PORT, "0");
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toUri().toString());
        configuration.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toUri().toString());

        try (MiniCluster miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(8)
                                .build())) {
            miniCluster.start();

            // first run: state at the initial parallelism
            resetProbes();
            final JobID firstJob = miniCluster.submitJob(statefulStreamGraph()).get().getJobID();
            CommonTestUtils.waitUntilCondition(
                    () -> PROCESSED.get() > 10 * NUM_KEYS,
                    WAIT_RETRY_INTERVAL_MILLIS,
                    WAIT_RETRY_ATTEMPTS);
            final String savepointPath =
                    miniCluster
                            .triggerSavepoint(
                                    firstJob,
                                    savepointDir.toUri().toString(),
                                    true,
                                    SavepointFormatType.CANONICAL)
                            .get();

            // the probes are static, so the first job's tasks must be gone before they are reset
            miniCluster.requestJobResult(firstJob).get();

            // second run: same program, stateful vertex overridden to a higher parallelism
            resetProbes();
            final StreamGraph restored = statefulStreamGraph();
            restored.getJobConfiguration()
                    .set(
                            PipelineOptions.PARALLELISM_OVERRIDES,
                            Collections.singletonMap(
                                    STATEFUL_VERTEX_ID.toHexString(), String.valueOf(OVERRIDE)));
            restored.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepointPath, false));
            final JobID secondJob = miniCluster.submitJob(restored).get().getJobID();

            CommonTestUtils.waitUntilCondition(
                    () -> {
                        final JobStatus status = miniCluster.getJobStatus(secondJob).get();
                        if (status.isGloballyTerminalState()) {
                            throw new AssertionError(
                                    "restored job terminated with "
                                            + status
                                            + ": "
                                            + miniCluster
                                                    .getArchivedExecutionGraph(secondJob)
                                                    .get()
                                                    .getFailureInfo());
                        }
                        return status == JobStatus.RUNNING && RESTORED_STATE_SEEN.get();
                    },
                    WAIT_RETRY_INTERVAL_MILLIS,
                    WAIT_RETRY_ATTEMPTS);

            final ArchivedExecutionGraph graph =
                    miniCluster.getArchivedExecutionGraph(secondJob).get();
            assertThat(graph.getAllVertices())
                    .as("vertices of the restored job, keyed by id")
                    .containsKey(STATEFUL_VERTEX_ID);
            assertThat(graph.getAllVertices().get(STATEFUL_VERTEX_ID).getParallelism())
                    .isEqualTo(OVERRIDE);

            miniCluster.cancelJob(secondJob).get();
        }
    }

    private static void resetProbes() {
        PROCESSED.set(0);
        KEYS_SEEN_IN_RUN.clear();
        RESTORED_STATE_SEEN.set(false);
    }

    private static StreamGraph statefulStreamGraph() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setParallelism(1);
        env.enableCheckpointing(500);
        env.fromSequence(0, Long.MAX_VALUE)
                .name("source")
                .keyBy(v -> v % NUM_KEYS)
                .map(new CountingMap())
                .setParallelism(INITIAL_PARALLELISM)
                .uid(STATEFUL_UID)
                .name("stateful")
                .sinkTo(new DiscardingSink<>())
                .name("sink");
        final StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobId(JobID.generate());
        return streamGraph;
    }

    /** Counts records per key; reports whether a key already had state when first seen. */
    private static final class CountingMap extends RichMapFunction<Long, Long> {

        private transient ValueState<Long> count;

        @Override
        public void open(OpenContext openContext) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public Long map(Long value) throws Exception {
            final long key = value % NUM_KEYS;
            final Long previous = count.value();
            if (KEYS_SEEN_IN_RUN.add(key) && previous != null) {
                RESTORED_STATE_SEEN.set(true);
            }
            count.update(previous == null ? 1L : previous + 1L);
            PROCESSED.incrementAndGet();
            return value;
        }
    }
}
