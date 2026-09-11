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
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleDetails.VertexParallelismRescaleInfo;
import org.apache.flink.runtime.rest.messages.job.rescales.SchedulerStateSpan;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testtasks.OnceBlockingNoOpInvokable;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for recording rescale history by {@link DefaultRescaleTimeline} or {@link
 * RescaleTimeline.NoOpRescaleTimeline}. Each case runs once with rescale history disabled and once
 * with it enabled, asserting the parameter-appropriate outcome. Cases that are only meaningful with
 * rescale history enabled live in {@link RescaleTimelineHistoryEnabledITCase}.
 */
@ExtendWith(ParameterizedTestExtension.class)
class RescaleTimelineITCase extends RescaleTimelineITCaseBase {

    @Parameter private Configuration configuration;

    @Parameters
    static Collection<Object[]> getClusterConfigs() {
        Configuration confNonEnabledRescaleHistory = createConfiguration();
        Configuration confEnabledRescaleHistory = createConfiguration();
        confEnabledRescaleHistory.set(WebOptions.MAX_ADAPTIVE_SCHEDULER_RESCALE_HISTORY_SIZE, 3);
        return Arrays.asList(
                new Object[] {confNonEnabledRescaleHistory},
                new Object[] {confEnabledRescaleHistory});
    }

    @BeforeEach
    void setUp() throws Exception {
        OnceBlockingNoOpInvokable.reset();
        startCluster(configuration);
    }

    // Tests for rescale trigger causes.
    @TestTemplate
    void testRecordRescaleForInitialScheduling() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);
        OnceBlockingNoOpInvokable.unblock();

        final ExecutionGraphInfo executionGraphInfo =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID()).join();
        runAdaptedParameterizedAssertion(
                executionGraphInfo,
                () -> {
                    assertThat(executionGraphInfo.getRescalesStatsSnapshot()).isNotNull();
                    List<Rescale> rescaleHistory =
                            executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory();
                    assertThat(rescaleHistory).hasSize(1);
                    Rescale rescale = rescaleHistory.get(0);
                    assertTriggerCause(rescale, TriggerCause.INITIAL_SCHEDULE);
                    assertTerminalRelatedFields(rescale, TerminatedReason.SUCCEEDED);
                    assertThat(
                                    rescale.getSchedulerStates().stream()
                                            .map(SchedulerStateSpan::getState)
                                            .collect(Collectors.toList()))
                            .containsExactly(
                                    "Created",
                                    "WaitingForResources",
                                    "CreatingExecutionGraph",
                                    "Executing");
                });
    }

    @TestTemplate
    void testRecordRescaleForNewResourcesRequirements() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, NUMBER_SLOTS_PER_TASK_MANAGER);

        waitForVertexParallelismReachedAndJobRunning(
                jobGraph, JOB_VERTEX_ID, NUMBER_SLOTS_PER_TASK_MANAGER);

        OnceBlockingNoOpInvokable.unblock();

        final ExecutionGraphInfo executionGraphInfo =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID()).join();
        runAdaptedParameterizedAssertion(
                executionGraphInfo,
                () -> {
                    assertThat(executionGraphInfo.getRescalesStatsSnapshot()).isNotNull();
                    List<Rescale> rescaleHistory =
                            executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory();
                    assertThat(rescaleHistory).hasSize(2);
                    Rescale rescale = rescaleHistory.get(0);
                    assertTriggerCause(rescale, TriggerCause.UPDATE_REQUIREMENT);
                    assertTerminalRelatedFields(rescale, TerminatedReason.SUCCEEDED);
                });
    }

    @TestTemplate
    void testRecordRescaleForNewAvailableResource() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph =
                createBlockingJobGraph(PARALLELISM + NUMBER_SLOTS_PER_TASK_MANAGER);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        miniCluster.startTaskManager();

        waitForVertexParallelismReachedAndJobRunning(
                jobGraph,
                JOB_VERTEX_ID,
                NUMBER_SLOTS_PER_TASK_MANAGER * (NUMBER_TASK_MANAGERS + 1));

        OnceBlockingNoOpInvokable.unblock();

        final ExecutionGraphInfo executionGraphInfo =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID()).join();
        runAdaptedParameterizedAssertion(
                executionGraphInfo,
                () -> {
                    assertThat(executionGraphInfo.getRescalesStatsSnapshot()).isNotNull();
                    List<Rescale> rescaleHistory =
                            executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory();
                    assertThat(rescaleHistory).hasSize(2);
                    Rescale rescale = rescaleHistory.get(0);
                    assertTriggerCause(rescale, TriggerCause.NEW_RESOURCE_AVAILABLE);
                    assertTerminalRelatedFields(rescale, TerminatedReason.SUCCEEDED);
                });
    }

    @TestTemplate
    void testRecordRescaleForRecoverableFailover() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        miniCluster.terminateTaskManager(0);
        waitForVertexParallelismReachedAndJobRunning(
                jobGraph, JOB_VERTEX_ID, NUMBER_SLOTS_PER_TASK_MANAGER);

        OnceBlockingNoOpInvokable.unblock();

        final ExecutionGraphInfo executionGraphInfo =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID()).join();
        runAdaptedParameterizedAssertion(
                executionGraphInfo,
                () -> {
                    assertThat(executionGraphInfo.getRescalesStatsSnapshot()).isNotNull();
                    List<Rescale> rescaleHistory =
                            executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory();
                    assertThat(rescaleHistory).hasSize(2);
                    Rescale rescale = rescaleHistory.get(0);
                    assertTriggerCause(rescale, TriggerCause.RECOVERABLE_FAILOVER);
                    assertTerminalRelatedFields(rescale, TerminatedReason.SUCCEEDED);
                });
    }

    // End of tests for rescale trigger causes.

    // Tests for rescale terminated reasons and terminal state.
    /** Already tested in {@link #testRecordRescaleForInitialScheduling()}, etc. */
    @Disabled
    @TestTemplate
    void testRescaleTerminatedBySucceeded() {}

    /**
     * The test case is disabled after processing by
     * https://lists.apache.org/thread/hh7w2p6lnmbo1q6d9ngkttdyrw4lp74h. Merge the current
     * non-terminated rescale and the new rescale triggered by recoverable failover into the current
     * rescale.
     */
    @Disabled
    @TestTemplate
    void testRescaleTerminatedByJobRestarting() {}

    // End of tests for rescale terminated reasons and terminal state.

    @TestTemplate
    void testRecordRescaleWithoutPreRescaleInfo() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        OnceBlockingNoOpInvokable.unblock();

        final ExecutionGraphInfo executionGraphInfo =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID()).join();
        runAdaptedParameterizedAssertion(
                executionGraphInfo,
                () -> {
                    assertThat(executionGraphInfo.getRescalesStatsSnapshot()).isNotNull();
                    Rescale rescale =
                            executionGraphInfo
                                    .getRescalesStatsSnapshot()
                                    .getRescaleHistory()
                                    .get(0);
                    assertThat(rescale).isNotNull();
                    Map<SlotSharingGroupId, SlotSharingGroupRescale> slots = rescale.getSlots();
                    assertThat(slots.values())
                            .allSatisfy(
                                    (Consumer<SlotSharingGroupRescale>)
                                            slotSharingGroupRescale -> {
                                                assertThat(
                                                                slotSharingGroupRescale
                                                                        .getPreRescaleSlots())
                                                        .isNull();
                                                assertSlotSharingGroupRescaleNotNullBesidesPreRelatedFields(
                                                        slotSharingGroupRescale);
                                            });
                    Map<JobVertexID, VertexParallelismRescaleInfo> vertices = rescale.getVertices();
                    assertThat(vertices.values())
                            .allSatisfy(
                                    (Consumer<VertexParallelismRescaleInfo>)
                                            vpr -> {
                                                assertThat(vpr.getPreRescaleParallelism()).isNull();
                                                assertVertexParallelismRescaleNotNullBesidesPreRelatedFields(
                                                        vpr);
                                            });
                });
    }

    /** Tested in {@link #testRecordRescaleForNewAvailableResource()} already. */
    @TestTemplate
    void testRecordRescaleWithPreRescaleInfo() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM + 1);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        miniCluster.startTaskManager();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM + 1);

        OnceBlockingNoOpInvokable.unblock();

        final ExecutionGraphInfo executionGraphInfo =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID()).join();
        runAdaptedParameterizedAssertion(
                executionGraphInfo,
                () -> {
                    assertThat(executionGraphInfo.getRescalesStatsSnapshot()).isNotNull();
                    Rescale rescale =
                            executionGraphInfo
                                    .getRescalesStatsSnapshot()
                                    .getRescaleHistory()
                                    .get(0);
                    assertThat(rescale).isNotNull();
                    Map<SlotSharingGroupId, SlotSharingGroupRescale> slots = rescale.getSlots();
                    assertThat(slots.values())
                            .allSatisfy(
                                    (Consumer<SlotSharingGroupRescale>)
                                            slotSharingGroupRescale -> {
                                                assertThat(
                                                                slotSharingGroupRescale
                                                                        .getPreRescaleSlots())
                                                        .isNotNull();
                                                assertSlotSharingGroupRescaleNotNullBesidesPreRelatedFields(
                                                        slotSharingGroupRescale);
                                            });
                    Map<JobVertexID, VertexParallelismRescaleInfo> vertices = rescale.getVertices();
                    assertThat(vertices.values())
                            .allSatisfy(
                                    (Consumer<VertexParallelismRescaleInfo>)
                                            vpr -> {
                                                assertThat(vpr.getPreRescaleParallelism())
                                                        .isNotNull();
                                                assertVertexParallelismRescaleNotNullBesidesPreRelatedFields(
                                                        vpr);
                                            });
                });
    }

    /**
     * Test for 'Merge the current non-terminated rescale and the new rescale triggered by
     * recoverable failover into the current rescale' anyone case.
     */
    @TestTemplate
    void testRecordNonTerminatedRescaleMergingWithNewRecoverableFailureTriggerCause()
            throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);

        miniCluster.terminateTaskManager(0);

        // Wait for the failover to complete before snapshotting: the merge that re-stamps the
        // trigger cause to RECOVERABLE_FAILOVER runs before the job is RUNNING again at the
        // reduced parallelism (one TaskManager left).
        waitForVertexParallelismReachedAndJobRunning(
                jobGraph, JOB_VERTEX_ID, NUMBER_SLOTS_PER_TASK_MANAGER);

        final ExecutionGraphInfo executionGraphInfo =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID()).join();
        runAdaptedParameterizedAssertion(
                executionGraphInfo,
                () -> {
                    assertThat(executionGraphInfo.getRescalesStatsSnapshot()).isNotNull();
                    List<Rescale> rescaleHistory =
                            executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory();
                    assertThat(rescaleHistory).hasSize(2);
                    assertThat(rescaleHistory.get(0).getTriggerCause())
                            .isEqualTo(TriggerCause.RECOVERABLE_FAILOVER);
                });
    }

    @TestTemplate
    void testRecordInProgressRescale() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);

        final ExecutionGraphInfo executionGraphInfo =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID()).join();
        runAdaptedParameterizedAssertion(
                executionGraphInfo,
                () -> {
                    assertThat(executionGraphInfo.getRescalesStatsSnapshot()).isNotNull();
                    List<Rescale> rescaleHistory =
                            executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory();
                    assertThat(rescaleHistory).hasSize(2);
                    Rescale rescale = rescaleHistory.get(0);
                    assertThat(rescale.getTerminatedReason()).isNull();
                    assertThat(rescale.getTerminalState()).isNull();
                });
    }

    // Private methods.
    private void runAdaptedParameterizedAssertion(
            ExecutionGraphInfo executionGraphInfo,
            RunnableWithException assertionForEnabledRescaleHistory)
            throws Exception {
        if (enabledRescaleHistory(configuration)) {
            assertionForEnabledRescaleHistory.run();
        } else {
            assertThat(executionGraphInfo.getRescalesStatsSnapshot()).isNotNull();
            assertThat(executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory()).isEmpty();
        }
    }

    private static boolean enabledRescaleHistory(Configuration configuration) {
        return configuration.get(WebOptions.MAX_ADAPTIVE_SCHEDULER_RESCALE_HISTORY_SIZE) > 0;
    }

    private static void assertTriggerCause(Rescale rescale, TriggerCause expectedTriggerCause) {
        assertThat(rescale).isNotNull();
        assertThat(rescale.getTriggerCause()).isEqualTo(expectedTriggerCause);
    }

    private void assertSlotSharingGroupRescaleNotNullBesidesPreRelatedFields(
            SlotSharingGroupRescale slotSharingGroupRescale) {
        assertThat(slotSharingGroupRescale.getDesiredSlots()).isNotNull();
        assertThat(slotSharingGroupRescale.getPostRescaleSlots()).isNotNull();
        assertThat(slotSharingGroupRescale.getSlotSharingGroupName()).isNotNull();
        assertThat(slotSharingGroupRescale.getMinimalRequiredSlots()).isNotNull();
        assertThat(slotSharingGroupRescale.getAcquiredResourceProfile()).isNotNull();
        assertThat(slotSharingGroupRescale.getRequestResourceProfile()).isNotNull();
        assertThat(slotSharingGroupRescale.getSlotSharingGroupId()).isNotNull();
    }

    private void assertVertexParallelismRescaleNotNullBesidesPreRelatedFields(
            VertexParallelismRescaleInfo vpr) {
        assertThat(vpr.getDesiredParallelism()).isNotNull();
        assertThat(vpr.getPostRescaleParallelism()).isNotNull();
        assertThat(vpr.getSlotSharingGroupName()).isNotNull();
        assertThat(vpr.getSufficientParallelism()).isNotNull();
        assertThat(vpr.getJobVertexId()).isNotNull();
        assertThat(vpr.getSlotSharingGroupId()).isNotNull();
        assertThat(vpr.getSlotSharingGroupName()).isNotNull();
    }
}
