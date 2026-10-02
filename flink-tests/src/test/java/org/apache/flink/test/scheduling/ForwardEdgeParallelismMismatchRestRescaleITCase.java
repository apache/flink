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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;

/**
 * Changes the parallelism of a running job via the resource requirements API, which only the {@link
 * SchedulerType#Adaptive} scheduler supports.
 */
class ForwardEdgeParallelismMismatchRestRescaleITCase
        extends ForwardEdgeParallelismMismatchITCaseBase {

    /** Throttled so that the job keeps running until the test cancels it. */
    private static final double RECORDS_PER_SECOND = 100;

    @Parameters(name = "scheduler={0}, parallelism={1}, mode={2}")
    private static List<Object[]> parameters() {
        // An unchanged parallelism doesn't trigger a rescale.
        return parameters(Collections.singletonList(SchedulerType.Adaptive), true);
    }

    private static DataGeneratorSource<Long> createSource() {
        return new DataGeneratorSource<>(
                index -> index,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                Types.LONG);
    }

    /** Rescales the job once it has completed a checkpoint. */
    @Override
    Optional<Throwable> runAndRescale(MiniCluster miniCluster) throws Exception {
        final JobGraph jobGraph = createJobGraph(createSource());
        final JobID jobId = jobGraph.getJobID();
        final JobVertexID sourceId = sourceId(jobGraph);
        final JobVertexID sinkId = sinkId(jobGraph);
        miniCluster.submitJob(jobGraph).get();
        awaitCheckpointWithInitialParallelism(miniCluster, jobId, sourceId, sinkId);

        miniCluster
                .updateJobResourceRequirements(
                        jobId,
                        JobResourceRequirements.newBuilder()
                                .setParallelismForJobVertex(
                                        sourceId,
                                        parallelismChange.newSource,
                                        parallelismChange.newSource)
                                .setParallelismForJobVertex(
                                        sinkId,
                                        parallelismChange.newSink,
                                        parallelismChange.newSink)
                                .build())
                .get();

        if (awaitCheckpointOrTermination(
                miniCluster,
                jobId,
                sourceId,
                sinkId,
                parallelismChange.newSource,
                parallelismChange.newSink)) {
            return getFailure(miniCluster, jobId);
        }
        miniCluster.cancelJob(jobId).get();
        return Optional.empty();
    }

    /**
     * Waits until the job completes a checkpoint with the initial parallelism, which means all its
     * tasks are running. The job is not expected to terminate.
     */
    private void awaitCheckpointWithInitialParallelism(
            MiniCluster miniCluster, JobID jobId, JobVertexID sourceId, JobVertexID sinkId)
            throws Exception {
        waitUntilCondition(
                () -> {
                    final AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobId).get();
                    if (graph.getState().isGloballyTerminalState()) {
                        throw new AssertionError(
                                "Job terminated unexpectedly in state " + graph.getState());
                    }
                    return isCheckpointedWithExpectedParallelism(
                            graph,
                            sourceId,
                            sinkId,
                            parallelismChange.initial,
                            parallelismChange.initial);
                });
    }

    /**
     * Like {@link #awaitCheckpointWithInitialParallelism}, but the job may also terminate, e.g.
     * when FAIL rejects the new parallelism.
     *
     * @return whether the job terminated.
     */
    private static boolean awaitCheckpointOrTermination(
            MiniCluster miniCluster,
            JobID jobId,
            JobVertexID sourceId,
            JobVertexID sinkId,
            int expectedSourceParallelism,
            int expectedSinkParallelism)
            throws Exception {
        waitUntilCondition(
                () -> {
                    final AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobId).get();
                    return graph.getState().isGloballyTerminalState()
                            || isCheckpointedWithExpectedParallelism(
                                    graph,
                                    sourceId,
                                    sinkId,
                                    expectedSourceParallelism,
                                    expectedSinkParallelism);
                });
        return miniCluster.getJobStatus(jobId).get().isGloballyTerminalState();
    }

    private static boolean isCheckpointedWithExpectedParallelism(
            AccessExecutionGraph graph,
            JobVertexID sourceId,
            JobVertexID sinkId,
            int expectedSourceParallelism,
            int expectedSinkParallelism) {
        final CompletedCheckpointStats checkpoint =
                graph.getCheckpointStatsSnapshot().getHistory().getLatestCompletedCheckpoint();
        return checkpoint != null
                && checkpoint.getTaskStateStats(sourceId).getNumberOfSubtasks()
                        == expectedSourceParallelism
                && checkpoint.getTaskStateStats(sinkId).getNumberOfSubtasks()
                        == expectedSinkParallelism;
    }

    private Optional<Throwable> getFailure(MiniCluster miniCluster, JobID jobId) throws Exception {
        return miniCluster
                .requestJobResult(jobId)
                .get()
                .getSerializedThrowable()
                .map(t -> t.deserializeError(getClass().getClassLoader()));
    }
}
