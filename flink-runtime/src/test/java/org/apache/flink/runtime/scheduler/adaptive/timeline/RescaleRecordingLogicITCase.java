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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testtasks.OnceBlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test for recording rescale history by {@link DefaultRescaleTimeline} or {@link
 * RescaleTimeline.NoOpRescaleTimeline}.
 */
@ExtendWith(ParameterizedTestExtension.class)
class RescaleRecordingLogicITCase {

    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int NUMBER_TASK_MANAGERS = 2;
    private static final int PARALLELISM = NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS;
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();

    @Parameter private Configuration configuration;
    private MiniClusterResource miniClusterResource;

    @Parameters
    static Collection<Object[]> getClusterConfigs() {
        Configuration confNonEnabledRescaleHistory = createConfiguration();
        Configuration confEnabledRescaleHistory = createConfiguration();
        confEnabledRescaleHistory.set(WebOptions.MAX_ADAPTIVE_SCHEDULER_RESCALE_HISTORY_SIZE, 3);
        return Arrays.asList(
                new Object[] {confNonEnabledRescaleHistory},
                new Object[] {confEnabledRescaleHistory});
    }

    private static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ofMillis(50));
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING,
                // Use the 0.1 seconds to trigger the long-time non-terminal rescale event after a
                // rescaling.
                Duration.ofMillis(100));
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ofMillis(50));
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                Duration.ofSeconds(2));
        return configuration;
    }

    @BeforeEach
    void setUp() throws Exception {
        OnceBlockingNoOpInvokable.reset();
        this.miniClusterResource =
                new MiniClusterResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                                .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
                                .build());
        miniClusterResource.before();
    }

    @AfterEach
    void tearDown() {
        miniClusterResource.after();
    }

    // Tests for rescale trigger causes.
    @TestTemplate
    void testRecordingRescaleTriggerredByInitialSchedule() throws Exception {
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
    void testRecordingRescaleTriggerredByUpdateRequirement() throws Exception {
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
    void testRecordingRescaleTriggerredByNewResourceAvailable() throws Exception {
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
    void testRecordingRescaleTriggerredByRecoverableFailover() throws Exception {
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
    /** Already tested in {@link #testRecordingRescaleTriggerredByInitialSchedule()}, etc. */
    @Disabled
    @TestTemplate
    void testRecordingRescaleTerminatedBySucceeded() {}

    @TestTemplate
    void testRecordingRescaleTerminatedByJobFinished() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);

        OnceBlockingNoOpInvokable.unblock();

        assumeThat(enabledRescaleHistory(configuration)).isTrue();
        waitUntilConditionWithTimeout(
                () -> {
                    List<Rescale> rescaleHistory = getRescaleHistory(miniCluster, jobGraph);
                    return hasRescaleHistoryMetCondition(
                            rescaleHistory, 2, TerminatedReason.JOB_FINISHED);
                },
                10000);
    }

    @TestTemplate
    void testRecordingRescaleTerminatedByJobCancelled() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);

        miniCluster.cancelJob(jobGraph.getJobID());

        assumeThat(enabledRescaleHistory(configuration)).isTrue();
        waitUntilConditionWithTimeout(
                () -> {
                    List<Rescale> rescaleHistory = getRescaleHistory(miniCluster, jobGraph);
                    return hasRescaleHistoryMetCondition(
                            rescaleHistory, 2, TerminatedReason.JOB_CANCELED);
                },
                10000);
    }

    @TestTemplate
    void testRecordingRescaleTerminatedByJobFailed() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        miniCluster.terminateTaskManager(1);

        waitForVertexParallelismReachedAndJobRunning(
                jobGraph, JOB_VERTEX_ID, NUMBER_SLOTS_PER_TASK_MANAGER);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);

        miniCluster.terminateTaskManager(0);

        assumeThat(enabledRescaleHistory(configuration)).isTrue();
        waitUntilConditionWithTimeout(
                () -> {
                    List<Rescale> rescaleHistory = getRescaleHistory(miniCluster, jobGraph);
                    return hasRescaleHistoryMetCondition(
                            rescaleHistory, 3, TerminatedReason.JOB_FAILED);
                },
                10000);
    }

    @TestTemplate
    void testRecordingRescaleTerminatedByNoResourcesOrParallelismsChange() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);
        miniCluster.submitJob(jobGraph).join();
        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);

        assumeThat(enabledRescaleHistory(configuration)).isTrue();
        waitUntilConditionWithTimeout(
                () -> {
                    List<Rescale> rescaleHistory = getRescaleHistory(miniCluster, jobGraph);
                    return hasRescaleHistoryMetCondition(
                            rescaleHistory,
                            2,
                            TerminatedReason.NO_RESOURCES_OR_PARALLELISMS_CHANGE);
                },
                20000);
    }

    @TestTemplate
    void testRecordingRescaleTerminatedByResourcesNotEnoughException() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);
        miniCluster.submitJob(jobGraph).join();
        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        miniCluster.terminateTaskManager(1);
        miniCluster.terminateTaskManager(0);

        assumeThat(enabledRescaleHistory(configuration)).isTrue();
        waitUntilConditionWithTimeout(
                () -> {
                    List<Rescale> rescaleHistory = getRescaleHistory(miniCluster, jobGraph);
                    return hasRescaleHistoryMetCondition(
                            rescaleHistory, 2, TerminatedReason.EXCEPTION_OCCURRED);
                },
                100000);
    }

    @TestTemplate
    void testRecordingRescaleTerminatedByResourceRequirementsUpdated() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);
        miniCluster.submitJob(jobGraph).join();
        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);
        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 3);

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        final ExecutionGraphInfo executionGraphInfo =
                miniCluster.getExecutionGraphInfo(jobGraph.getJobID()).join();
        runAdaptedParameterizedAssertion(
                executionGraphInfo,
                () -> {
                    assertThat(executionGraphInfo.getRescalesStatsSnapshot()).isNotNull();
                    List<Rescale> rescaleHistory =
                            executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory();
                    assertThat(rescaleHistory).hasSize(3);
                    Rescale rescale = rescaleHistory.get(1);
                    assertTerminalRelatedFields(
                            rescale, TerminatedReason.RESOURCE_REQUIREMENTS_UPDATED);
                });
    }

    /**
     * The test case is disabled after processing by
     * https://lists.apache.org/thread/hh7w2p6lnmbo1q6d9ngkttdyrw4lp74h. Merge the current
     * non-terminated rescale and the new rescale triggered by recoverable failover into the current
     * rescale.
     */
    @Disabled
    @TestTemplate
    void testRecordingRescaleTerminatedByJobRestarting() {}

    // End of tests for rescale terminated reasons and terminal state.

    @TestTemplate
    void testRecordingRescaleWithoutPreRescaleInfo() throws Exception {
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
                    Map<JobVertexID, VertexParallelismRescale> vertices = rescale.getVertices();
                    assertThat(vertices.values())
                            .allSatisfy(
                                    (Consumer<VertexParallelismRescale>)
                                            vpr -> {
                                                assertThat(vpr.getPreRescaleParallelism()).isNull();
                                                assertVertexParallelismRescaleNotNullBesidesPreRelatedFields(
                                                        vpr);
                                            });
                });
    }

    /** Tested in {@link #testRecordingRescaleTriggerredByNewResourceAvailable} already. */
    @TestTemplate
    void testRecordingRescaleWithPreRescaleInfo() throws Exception {
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
                    Map<JobVertexID, VertexParallelismRescale> vertices = rescale.getVertices();
                    assertThat(vertices.values())
                            .allSatisfy(
                                    (Consumer<VertexParallelismRescale>)
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
    void testUseNonTerminatedRescaleToRecordMergingWithNewRecoverableFailureTriggerCause()
            throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

        updateJobResourceRequirements(miniCluster, jobGraph, 1, PARALLELISM * 2);

        miniCluster.terminateTaskManager(0);

        waitForVertexParallelismReachedAndJobRunning(jobGraph, JOB_VERTEX_ID, PARALLELISM);

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
    void testRecordingInProgressRescale() throws Exception {
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
    private JobGraph createBlockingJobGraph(int parallelism) {
        final JobVertex blockingOperator = new JobVertex("Blocking operator", JOB_VERTEX_ID);
        SlotSharingGroup sharingGroup = new SlotSharingGroup();
        sharingGroup.setSlotSharingGroupName("slot-sharing-group-A");
        blockingOperator.setSlotSharingGroup(sharingGroup);
        blockingOperator.setInvokableClass(OnceBlockingNoOpInvokable.class);
        blockingOperator.setParallelism(parallelism);
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(blockingOperator);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(jobGraph, 1, 0L);
        return jobGraph;
    }

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

    private static void assertTerminalRelatedFields(
            Rescale rescale, TerminatedReason expectedTerminatedReason) {
        TerminatedReason terminatedReason = rescale.getTerminatedReason();
        assertThat(terminatedReason).isEqualTo(expectedTerminatedReason);
        assertThat(terminatedReason.getTerminalState())
                .isEqualTo(rescale.getTerminalState())
                .isEqualTo(expectedTerminatedReason.getTerminalState());
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
        assertThat(slotSharingGroupRescale.getRequiredResourceProfile()).isNotNull();
        assertThat(slotSharingGroupRescale.getSlotSharingGroupId()).isNotNull();
    }

    private void assertVertexParallelismRescaleNotNullBesidesPreRelatedFields(
            VertexParallelismRescale vpr) {
        assertThat(vpr.getDesiredParallelism()).isNotNull();
        assertThat(vpr.getPostRescaleParallelism()).isNotNull();
        assertThat(vpr.getSlotSharingGroupName()).isNotNull();
        assertThat(vpr.getSufficientParallelism()).isNotNull();
        assertThat(vpr.getJobVertexId()).isNotNull();
        assertThat(vpr.getSlotSharingGroupId()).isNotNull();
        assertThat(vpr.getSlotSharingGroupName()).isNotNull();
    }

    private void waitUntilConditionWithTimeout(
            SupplierWithException<Boolean, Exception> condition, long timeoutMillis)
            throws Exception {
        CompletableFuture.runAsync(
                        () -> {
                            try {
                                CommonTestUtils.waitUntilCondition(condition);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                .get(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void waitForVertexParallelismReachedAndJobRunning(
            JobGraph jobGraph, JobVertexID jobVertexId, int targetParallelism) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final ArchivedExecutionGraph archivedExecutionGraph =
                            miniClusterResource
                                    .getMiniCluster()
                                    .getArchivedExecutionGraph(jobGraph.getJobID())
                                    .get();
                    final AccessExecutionJobVertex executionJobVertex =
                            archivedExecutionGraph.getAllVertices().get(jobVertexId);
                    if (executionJobVertex == null) {
                        // parallelism was not yet determined
                        return false;
                    }
                    return executionJobVertex.getParallelism() == targetParallelism;
                });
        CommonTestUtils.waitUntilCondition(
                () ->
                        miniClusterResource.getMiniCluster().getJobStatus(jobGraph.getJobID()).get()
                                == JobStatus.RUNNING);
    }

    private void updateJobResourceRequirements(
            MiniCluster miniCluster, JobGraph jobGraph, int lowerBound, int upperBound)
            throws ExecutionException, InterruptedException {
        miniCluster
                .updateJobResourceRequirements(
                        jobGraph.getJobID(),
                        JobResourceRequirements.newBuilder()
                                .setParallelismForJobVertex(
                                        jobGraph.getVertices().iterator().next().getID(),
                                        lowerBound,
                                        upperBound)
                                .build())
                .get();
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
