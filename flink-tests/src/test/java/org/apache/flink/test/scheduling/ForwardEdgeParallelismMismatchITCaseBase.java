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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptions.ForwardEdgeParallelismMismatchMode;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies which branch of {@link PipelineOptions#FORWARD_EDGE_PARALLELISM_MISMATCH_MODE} a {@code
 * source -> forward -> sink} job takes after its parallelism is changed.
 *
 * <p>The branch is decided per producer job vertex from the real producer and consumer parallelism:
 * if they are equal no branch is taken; otherwise every producer subtask either logs the configured
 * branch (REBALANCE, KEEP_FORWARD) or fails the job (FAIL). Subclasses run the jobs and use the
 * helpers of this class to build them and to assert the branch.
 */
@ExtendWith(ParameterizedTestExtension.class)
abstract class ForwardEdgeParallelismMismatchITCaseBase {

    private static final int NUM_SLOTS = 8;

    private static final String REBALANCE_LOG = "Replacing forward partitioner";
    private static final String KEEP_FORWARD_LOG = "Keeping forward partitioner";
    private static final String FAIL_MESSAGE = "Forward partitioning cannot be preserved";

    static final List<ParallelismChange> PARALLELISM_CHANGES =
            Arrays.asList(
                    new ParallelismChange(4, 4, 4),
                    new ParallelismChange(4, 5, 5),
                    new ParallelismChange(4, 3, 3),
                    new ParallelismChange(1, 1, 2),
                    new ParallelismChange(2, 2, 4),
                    new ParallelismChange(2, 2, 3),
                    new ParallelismChange(4, 4, 2),
                    new ParallelismChange(3, 3, 2),
                    new ParallelismChange(2, 2, 1),
                    new ParallelismChange(4, 2, 3),
                    new ParallelismChange(2, 4, 2));

    @Parameter(0)
    public SchedulerType scheduler;

    @Parameter(1)
    public ParallelismChange parallelismChange;

    @Parameter(2)
    public ForwardEdgeParallelismMismatchMode mode;

    @RegisterExtension
    private final LoggerAuditingExtension streamTaskLogs =
            new LoggerAuditingExtension(StreamTask.class, Level.DEBUG);

    /**
     * Runs a job with the initial parallelism and changes it to the new parallelism.
     *
     * @return the failure of the job with the new parallelism, if any.
     */
    abstract Optional<Throwable> runAndRescale(MiniCluster miniCluster) throws Exception;

    @TestTemplate
    void testForwardEdgeParallelismMismatch() throws Exception {
        final Optional<Throwable> failure;
        try (MiniCluster miniCluster = startMiniCluster()) {
            failure = runAndRescale(miniCluster);
        }
        assertExpectedBranch(failure);
    }

    /** Combines the given schedulers with every parallelism change and mode. */
    static List<Object[]> parameters(
            List<SchedulerType> schedulers, boolean excludeUnchangedParallelism) {
        final List<Object[]> parameters = new ArrayList<>();
        for (SchedulerType scheduler : schedulers) {
            for (ParallelismChange parallelismChange : PARALLELISM_CHANGES) {
                if (excludeUnchangedParallelism && parallelismChange.isUnchanged()) {
                    continue;
                }
                for (ForwardEdgeParallelismMismatchMode mode :
                        ForwardEdgeParallelismMismatchMode.values()) {
                    parameters.add(new Object[] {scheduler, parallelismChange, mode});
                }
            }
        }
        return parameters;
    }

    /**
     * Asserts the branch taken after the parallelism change, from the given failure of the job
     * running with the new parallelism and the partitioner logs of all jobs of the test.
     */
    private void assertExpectedBranch(Optional<Throwable> failure) {
        final List<String> partitionerLogs =
                streamTaskLogs.getMessages().stream()
                        .filter(m -> m.contains(REBALANCE_LOG) || m.contains(KEEP_FORWARD_LOG))
                        .collect(Collectors.toList());

        final String failureDescription =
                failure.map(ExceptionUtils::stringifyException).orElse("<none>");
        if (!isMismatch()) {
            assertThat(failure).as("job failure: %s", failureDescription).isEmpty();
            assertThat(partitionerLogs).as("partitioner logs").isEmpty();
        } else if (scheduler == SchedulerType.AdaptiveBatch) {
            // Forward groups require equal parallelism, so the job is rejected before deployment.
            assertThat(failure).as("job failure").isPresent();
            assertThat(ExceptionUtils.findThrowable(failure.get(), IllegalStateException.class))
                    .as("forward group rejection, actual failure: %s", failureDescription)
                    .isPresent();
            assertThat(partitionerLogs).as("partitioner logs").isEmpty();
        } else if (mode == ForwardEdgeParallelismMismatchMode.FAIL) {
            assertThat(failure).as("job failure").isPresent();
            assertThat(ExceptionUtils.findThrowableWithMessage(failure.get(), FAIL_MESSAGE))
                    .as("FAIL mode exception, actual failure: %s", failureDescription)
                    .isPresent();
            assertThat(
                            ExceptionUtils.findThrowableWithMessage(
                                    failure.get(), expectedParallelismMessage()))
                    .as("real parallelism in exception, actual failure: %s", failureDescription)
                    .isPresent();
            assertThat(partitionerLogs).as("partitioner logs").isEmpty();
        } else {
            assertThat(failure).as("job failure: %s", failureDescription).isEmpty();
            final String expectedLog =
                    mode == ForwardEdgeParallelismMismatchMode.REBALANCE
                            ? REBALANCE_LOG
                            : KEEP_FORWARD_LOG;
            assertThat(partitionerLogs)
                    .as("partitioner logs, one per producer subtask")
                    .hasSize(parallelismChange.newSource)
                    .allSatisfy(
                            m ->
                                    assertThat(m)
                                            .contains(expectedLog)
                                            .contains(expectedParallelismMessage()));
        }
    }

    private boolean isMismatch() {
        return parallelismChange.newSource != parallelismChange.newSink;
    }

    private String expectedParallelismMessage() {
        return String.format(
                "producer parallelism %d != consumer parallelism %d",
                parallelismChange.newSource, parallelismChange.newSink);
    }

    /**
     * Used for both the cluster and the job. The AdaptiveScheduler options are ignored by the other
     * schedulers, and checkpointing is ignored in BATCH mode.
     */
    private Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, scheduler);
        configuration.set(RestOptions.BIND_PORT, "0");
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING, Duration.ZERO);
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ZERO);
        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
        // Otherwise checkpointing enables restarts and FAIL would restart the job forever.
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        configuration.set(PipelineOptions.FORWARD_EDGE_PARALLELISM_MISMATCH_MODE, mode);
        return configuration;
    }

    /** Creates a {@code source -> forward -> sink} job with the initial parallelism. */
    JobGraph createJobGraph(Source<Long, ?, ?> source) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(createConfiguration());
        env.setRuntimeMode(
                scheduler == SchedulerType.AdaptiveBatch
                        ? RuntimeExecutionMode.BATCH
                        : RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelismChange.initial);
        env.disableOperatorChaining();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                .forward()
                .sinkTo(new DiscardingSink<>())
                .name("sink");

        return env.getStreamGraph().getJobGraph();
    }

    MiniCluster startMiniCluster() throws Exception {
        final MiniCluster miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(createConfiguration())
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(NUM_SLOTS)
                                .build());
        miniCluster.start();
        return miniCluster;
    }

    static JobVertexID sourceId(JobGraph jobGraph) {
        return jobGraph.getVerticesSortedTopologicallyFromSources().get(0).getID();
    }

    static JobVertexID sinkId(JobGraph jobGraph) {
        return jobGraph.getVerticesSortedTopologicallyFromSources().get(1).getID();
    }

    /** Initial parallelism of both vertices and their parallelism after the change. */
    static final class ParallelismChange {
        final int initial;
        final int newSource;
        final int newSink;

        ParallelismChange(int initial, int newSource, int newSink) {
            this.initial = initial;
            this.newSource = newSource;
            this.newSink = newSink;
        }

        boolean isUnchanged() {
            return initial == newSource && initial == newSink;
        }

        @Override
        public String toString() {
            return String.format("(%d,%d)->(%d,%d)", initial, initial, newSource, newSink);
        }
    }
}
