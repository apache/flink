/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testtasks.OnceBlockingNoOpInvokable;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for rescale terminated reasons and terminal state, only meaningful when rescale history is
 * enabled. Each case starts its own cluster with the configuration it needs.
 */
class RescaleTimelineHistoryEnabledITCase extends RescaleTimelineITCaseBase {

    // Matches the default poll interval of the CommonTestUtils#waitUntilCondition it replaced.
    private static final long RETRY_INTERVAL_MILLIS = 100L;

    @BeforeEach
    void setUp() {
        OnceBlockingNoOpInvokable.reset();
    }

    @Test
    void testRescaleTerminatedByJobFinished() throws Exception {
        // With the short shared cooldown the DefaultStateTransitionManager re-enters Idling on a
        // wall-clock timer and terminates the in-progress rescale with
        // NO_RESOURCES_OR_PARALLELISMS_CHANGE before the job finishes; goToFinished then finds it
        // already terminated and its JOB_FINISHED stamp is ignored, so waiting cannot win.
        // Widen the cooldown to keep the rescale in-progress until the job finishes.
        startCluster(Duration.ofSeconds(60), Duration.ofSeconds(60));

        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);

        // The upper bound (PARALLELISM * 2) exceeds the available slots, so this rescale is only
        // recorded in the history without changing the parallelism. Wait until it is recorded
        // before unblocking, otherwise on a slow machine the task can finish first and the size-2
        // condition below times out.
        waitUntilConditionWithTimeout(
                () -> getRescaleHistory(miniCluster, jobGraph).size() == 2, 10000);

        OnceBlockingNoOpInvokable.unblock();

        // Generous budget: on a loaded CI leg the unblock-to-finish window can itself exceed 10s.
        waitUntilConditionWithTimeout(
                () -> {
                    List<Rescale> rescaleHistory = getRescaleHistory(miniCluster, jobGraph);
                    return hasRescaleHistoryMetCondition(
                            rescaleHistory, 2, TerminatedReason.JOB_FINISHED);
                },
                60000);
    }

    @Test
    void testRescaleTerminatedByJobCancelled() throws Exception {
        startCluster(createEnabledConfiguration());

        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);

        miniCluster.cancelJob(jobGraph.getJobID());

        waitUntilConditionWithTimeout(
                () -> {
                    List<Rescale> rescaleHistory = getRescaleHistory(miniCluster, jobGraph);
                    return hasRescaleHistoryMetCondition(
                            rescaleHistory, 2, TerminatedReason.JOB_CANCELED);
                },
                10000);
    }

    @Test
    void testRescaleTerminatedByJobFailed() throws Exception {
        startCluster(createEnabledConfiguration());

        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        miniCluster.terminateTaskManager(1);

        waitForVertexParallelismReachedAndJobRunning(
                jobGraph, JOB_VERTEX_ID, NUMBER_SLOTS_PER_TASK_MANAGER);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);

        miniCluster.terminateTaskManager(0);

        waitUntilConditionWithTimeout(
                () -> {
                    List<Rescale> rescaleHistory = getRescaleHistory(miniCluster, jobGraph);
                    return hasRescaleHistoryMetCondition(
                            rescaleHistory, 3, TerminatedReason.JOB_FAILED);
                },
                10000);
    }

    @Test
    void testRescaleTerminatedByNoResourcesOrNoParallelismsChange() throws Exception {
        // NO_RESOURCES_OR_PARALLELISMS_CHANGE is only stamped when the update RPC is processed
        // during Cooldown and the manager re-enters Idling. Unlike the sibling
        // testRescaleTerminatedByResourceRequirementsUpdated, this case must wait out the whole
        // cooldown, so it cannot reuse 60s: 10s outlasts the RPC yet stays within the 60s budget.
        startCluster(Duration.ofSeconds(10), Duration.ofMillis(50));

        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);
        miniCluster.submitJob(jobGraph).join();
        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);

        waitUntilConditionWithTimeout(
                () -> {
                    List<Rescale> rescaleHistory = getRescaleHistory(miniCluster, jobGraph);
                    return hasRescaleHistoryMetCondition(
                            rescaleHistory,
                            2,
                            TerminatedReason.NO_RESOURCES_OR_PARALLELISMS_CHANGE);
                },
                60000);
    }

    @Test
    void testRescaleTerminatedByResourcesNotEnoughException() throws Exception {
        startCluster(createEnabledConfiguration());

        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);
        miniCluster.submitJob(jobGraph).join();
        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        miniCluster.terminateTaskManager(1);
        miniCluster.terminateTaskManager(0);

        waitUntilConditionWithTimeout(
                () -> {
                    List<Rescale> rescaleHistory = getRescaleHistory(miniCluster, jobGraph);
                    return hasRescaleHistoryMetCondition(
                            rescaleHistory, 2, TerminatedReason.EXCEPTION_OCCURRED);
                },
                100000);
    }

    @Test
    void testRescaleTerminatedByResourceRequirementsUpdated() throws Exception {
        // The second update only terminates the first rescale with RESOURCE_REQUIREMENTS_UPDATED
        // while that rescale is still in-progress; once it is terminated, updateRescale is a no-op.
        // With the short shared cooldown the manager re-enters Idling and terminates it on a
        // wall-clock timer first, so waiting cannot win the race. Widening cooldown and
        // stabilization to 60s keeps the rescale in-progress far longer than the RPC round trip.
        startCluster(Duration.ofSeconds(60), Duration.ofSeconds(60));

        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);
        miniCluster.submitJob(jobGraph).join();
        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);
        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 3);

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        final ExecutionGraphInfo executionGraphInfo =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID()).join();
        assertThat(executionGraphInfo.getRescalesStatsSnapshot()).isNotNull();
        List<Rescale> rescaleHistory =
                executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory();
        assertThat(rescaleHistory).hasSize(3);
        Rescale rescale = rescaleHistory.get(1);
        assertTerminalRelatedFields(rescale, TerminatedReason.RESOURCE_REQUIREMENTS_UPDATED);
    }

    // Private methods.
    private static Configuration createEnabledConfiguration() {
        final Configuration configuration = createConfiguration();
        configuration.set(WebOptions.MAX_ADAPTIVE_SCHEDULER_RESCALE_HISTORY_SIZE, 3);
        return configuration;
    }

    /**
     * Starts the fixture cluster with rescale history enabled plus the given executing-phase
     * cooldown and resource-stabilization timeouts, kept wide enough to observe a rescale's
     * terminated reason before the default short cooldown races past it.
     */
    private void startCluster(Duration cooldown, Duration resourceStabilizationTimeout)
            throws Exception {
        final Configuration configuration = createEnabledConfiguration();
        configuration.set(JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING, cooldown);
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                resourceStabilizationTimeout);
        startCluster(configuration);
    }

    private void waitUntilConditionWithTimeout(
            SupplierWithException<Boolean, Exception> condition, long timeoutMillis)
            throws Exception {
        // Poll synchronously via waitUtil. The previous CompletableFuture#runAsync + get(timeout)
        // wrapper hangs on JDK 11: on a ForkJoinPool worker (JUnit's executor) timedGet
        // help-executes the unbounded poll loop inline on the waiting thread, so the timeout
        // never fires.
        org.apache.flink.core.testutils.CommonTestUtils.waitUtil(
                () -> {
                    try {
                        return condition.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                Duration.ofMillis(timeoutMillis),
                Duration.ofMillis(RETRY_INTERVAL_MILLIS),
                "Condition was not met within " + timeoutMillis + " ms.");
    }

    private boolean hasRescaleHistoryMetCondition(
            List<Rescale> rescales, int expectedSize, TerminatedReason terminatedReasonOfLatest) {
        return rescales.size() == expectedSize
                && terminatedReasonOfLatest == rescales.get(0).getTerminatedReason();
    }

    private static List<Rescale> getRescaleHistory(MiniCluster miniCluster, JobGraph jobGraph)
            throws InterruptedException, ExecutionException {
        ExecutionGraphInfo executionGraphInfo =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID()).get();
        return executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory();
    }
}
