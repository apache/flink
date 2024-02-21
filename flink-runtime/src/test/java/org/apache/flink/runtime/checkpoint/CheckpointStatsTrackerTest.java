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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

class CheckpointStatsTrackerTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    /** Tests that the number of remembered checkpoints configuration is respected. */
    @Test
    void testTrackerWithoutHistory() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID, 3, 256)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionJobVertex jobVertex = graph.getJobVertex(jobVertexID);

        CheckpointStatsTracker tracker =
                new CheckpointStatsTracker(0, new UnregisteredMetricsGroup(), new JobID());

        PendingCheckpointStats pending =
                tracker.reportPendingCheckpoint(
                        0,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        singletonMap(jobVertexID, jobVertex.getParallelism()));

        pending.reportSubtaskStats(jobVertexID, createSubtaskStats(0));
        pending.reportSubtaskStats(jobVertexID, createSubtaskStats(1));
        pending.reportSubtaskStats(jobVertexID, createSubtaskStats(2));

        tracker.reportCompletedCheckpoint(pending.toCompletedCheckpointStats(null));

        CheckpointStatsSnapshot snapshot = tracker.createSnapshot();
        // History should be empty
        assertThat(snapshot.getHistory().getCheckpoints().iterator()).isExhausted();

        // Counts should be available
        CheckpointStatsCounts counts = snapshot.getCounts();
        assertThat(counts.getNumberOfCompletedCheckpoints()).isOne();
        assertThat(counts.getTotalNumberOfCheckpoints()).isOne();

        // Summary should be available
        CompletedCheckpointStatsSummarySnapshot summary = snapshot.getSummaryStats();
        assertThat(summary.getStateSizeStats().getCount()).isOne();
        assertThat(summary.getEndToEndDurationStats().getCount()).isOne();

        // Latest completed checkpoint
        assertThat(snapshot.getHistory().getLatestCompletedCheckpoint()).isNotNull();
        assertThat(snapshot.getHistory().getLatestCompletedCheckpoint().getCheckpointId()).isZero();
    }

    /** Tests tracking of checkpoints. */
    @Test
    void testCheckpointTracking() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID, 3, 256)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionJobVertex jobVertex = graph.getJobVertex(jobVertexID);
        Map<JobVertexID, Integer> vertexToDop =
                singletonMap(jobVertexID, jobVertex.getParallelism());

        CheckpointStatsTracker tracker =
                new CheckpointStatsTracker(10, new UnregisteredMetricsGroup(), new JobID());

        // Completed checkpoint
        PendingCheckpointStats completed1 =
                tracker.reportPendingCheckpoint(
                        0,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        vertexToDop);

        completed1.reportSubtaskStats(jobVertexID, createSubtaskStats(0));
        completed1.reportSubtaskStats(jobVertexID, createSubtaskStats(1));
        completed1.reportSubtaskStats(jobVertexID, createSubtaskStats(2));

        tracker.reportCompletedCheckpoint(completed1.toCompletedCheckpointStats(null));

        // Failed checkpoint
        PendingCheckpointStats failed =
                tracker.reportPendingCheckpoint(
                        1,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        vertexToDop);

        tracker.reportFailedCheckpoint(failed.toFailedCheckpoint(12, null));

        // Completed savepoint
        PendingCheckpointStats savepoint =
                tracker.reportPendingCheckpoint(
                        2,
                        1,
                        CheckpointProperties.forSavepoint(true, SavepointFormatType.CANONICAL),
                        vertexToDop);

        savepoint.reportSubtaskStats(jobVertexID, createSubtaskStats(0));
        savepoint.reportSubtaskStats(jobVertexID, createSubtaskStats(1));
        savepoint.reportSubtaskStats(jobVertexID, createSubtaskStats(2));

        tracker.reportCompletedCheckpoint(savepoint.toCompletedCheckpointStats(null));

        // In Progress
        PendingCheckpointStats inProgress =
                tracker.reportPendingCheckpoint(
                        3,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        vertexToDop);

        RestoredCheckpointStats restored =
                new RestoredCheckpointStats(
                        81,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        123,
                        null,
                        42);
        tracker.reportInitializationStartTs(123);
        tracker.reportRestoredCheckpoint(restored);

        CheckpointStatsSnapshot snapshot = tracker.createSnapshot();

        // Counts
        CheckpointStatsCounts counts = snapshot.getCounts();
        assertThat(counts.getTotalNumberOfCheckpoints()).isEqualTo(4);
        assertThat(counts.getNumberOfInProgressCheckpoints()).isOne();
        assertThat(counts.getNumberOfCompletedCheckpoints()).isEqualTo(2);
        assertThat(counts.getNumberOfFailedCheckpoints()).isOne();

        tracker.reportFailedCheckpointsWithoutInProgress();

        CheckpointStatsSnapshot snapshot1 = tracker.createSnapshot();
        counts = snapshot1.getCounts();
        assertThat(counts.getTotalNumberOfCheckpoints()).isEqualTo(5);
        assertThat(counts.getNumberOfInProgressCheckpoints()).isOne();
        assertThat(counts.getNumberOfCompletedCheckpoints()).isEqualTo(2);
        assertThat(counts.getNumberOfFailedCheckpoints()).isEqualTo(2);

        // Summary stats
        CompletedCheckpointStatsSummarySnapshot summary = snapshot.getSummaryStats();
        assertThat(summary.getStateSizeStats().getCount()).isEqualTo(2);
        assertThat(summary.getEndToEndDurationStats().getCount()).isEqualTo(2);

        // History
        CheckpointStatsHistory history = snapshot.getHistory();
        Iterator<AbstractCheckpointStats> it = history.getCheckpoints().iterator();

        assertThat(it).hasNext();
        AbstractCheckpointStats stats = it.next();
        assertThat(stats.getCheckpointId()).isEqualTo(3);
        assertThat(stats.getStatus().isInProgress()).isTrue();

        assertThat(it).hasNext();
        stats = it.next();
        assertThat(stats.getCheckpointId()).isEqualTo(2);
        assertThat(stats.getStatus().isCompleted()).isTrue();

        assertThat(it).hasNext();
        stats = it.next();
        assertThat(stats.getCheckpointId()).isOne();
        assertThat(stats.getStatus().isFailed()).isTrue();

        assertThat(it).hasNext();
        stats = it.next();
        assertThat(stats.getCheckpointId()).isZero();
        assertThat(stats.getStatus().isCompleted()).isTrue();

        assertThat(it).isExhausted();

        // Latest checkpoints
        assertThat(snapshot.getHistory().getLatestCompletedCheckpoint().getCheckpointId())
                .isEqualTo(completed1.getCheckpointId());
        assertThat(snapshot.getHistory().getLatestSavepoint().getCheckpointId())
                .isEqualTo(savepoint.getCheckpointId());
        assertThat(snapshot.getHistory().getLatestFailedCheckpoint().getCheckpointId())
                .isEqualTo(failed.getCheckpointId());
        assertThat(snapshot.getLatestRestoredCheckpoint()).isEqualTo(restored);
    }

    /** Tests that snapshots are only created if a new snapshot has been reported or updated. */
    @Test
    void testCreateSnapshot() {
        JobVertexID jobVertexID = new JobVertexID();
        CheckpointStatsTracker tracker =
                new CheckpointStatsTracker(10, new UnregisteredMetricsGroup(), new JobID());

        CheckpointStatsSnapshot snapshot1 = tracker.createSnapshot();

        // Pending checkpoint => new snapshot
        PendingCheckpointStats pending =
                tracker.reportPendingCheckpoint(
                        0,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        singletonMap(jobVertexID, 1));

        pending.reportSubtaskStats(jobVertexID, createSubtaskStats(0));

        CheckpointStatsSnapshot snapshot2 = tracker.createSnapshot();
        assertThat(snapshot2).isNotEqualTo(snapshot1);

        assertThat(tracker.createSnapshot()).isEqualTo(snapshot2);

        // Complete checkpoint => new snapshot
        tracker.reportCompletedCheckpoint(pending.toCompletedCheckpointStats(null));

        CheckpointStatsSnapshot snapshot3 = tracker.createSnapshot();
        assertThat(snapshot3).isNotEqualTo(snapshot2);

        // Restore operation => new snapshot
        tracker.reportInitializationStartTs(0);
        tracker.reportRestoredCheckpoint(
                new RestoredCheckpointStats(
                        12,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        12,
                        null,
                        42));

        CheckpointStatsSnapshot snapshot4 = tracker.createSnapshot();
        assertThat(snapshot4).isNotEqualTo(snapshot3);
        assertThat(tracker.createSnapshot()).isEqualTo(snapshot4);
    }

    @Test
    public void testSpanCreation() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        final List<Span> reportedSpans = new ArrayList<>();

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public void addSpan(SpanBuilder spanBuilder) {
                        reportedSpans.add(spanBuilder.build());
                    }
                };

        CheckpointStatsTracker tracker = new CheckpointStatsTracker(10, metricGroup, new JobID());

        PendingCheckpointStats pending =
                tracker.reportPendingCheckpoint(
                        42,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        singletonMap(jobVertexID, 1));

        pending.reportSubtaskStats(jobVertexID, createSubtaskStats(0));

        // Complete checkpoint => new snapshot
        tracker.reportCompletedCheckpoint(pending.toCompletedCheckpointStats(null));

        assertThat(reportedSpans.size()).isEqualTo(1);
        assertThat(
                        reportedSpans.stream()
                                .map(span -> span.getAttributes().get("checkpointId"))
                                .collect(Collectors.toList()))
                .containsExactly(42L);
    }

    @Test
    public void testInitializationSpanCreation() throws Exception {
        final List<Span> reportedSpans = new ArrayList<>();

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public void addSpan(SpanBuilder spanBuilder) {
                        reportedSpans.add(spanBuilder.build());
                    }
                };

        CheckpointStatsTracker tracker =
                new CheckpointStatsTracker(10, metricGroup, new JobID(), 2);

        tracker.reportInitializationStartTs(100);
        tracker.reportRestoredCheckpoint(
                new RestoredCheckpointStats(
                        42,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        100,
                        null,
                        1024));

        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder3 =
                new SubTaskInitializationMetricsBuilder(110)
                        .setStatus(InitializationStatus.COMPLETED);
        subTaskInitializationMetricsBuilder3.addDurationMetric("MailboxStartDurationMs", 10);
        subTaskInitializationMetricsBuilder3.addDurationMetric("ReadOutputDataDurationMs", 20);
        subTaskInitializationMetricsBuilder3.addDurationMetric("InitializeStateDurationMs", 30);
        subTaskInitializationMetricsBuilder3.addDurationMetric("GateRestoreDurationMs", 40);
        tracker.reportInitializationMetrics(subTaskInitializationMetricsBuilder3.build(215));
        assertThat(reportedSpans).isEmpty();

        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder2 =
                new SubTaskInitializationMetricsBuilder(110)
                        .setStatus(InitializationStatus.COMPLETED);
        subTaskInitializationMetricsBuilder2.addDurationMetric("MailboxStartDurationMs", 10);
        subTaskInitializationMetricsBuilder2.addDurationMetric("ReadOutputDataDurationMs", 20);
        subTaskInitializationMetricsBuilder2.addDurationMetric("InitializeStateDurationMs", 30);
        subTaskInitializationMetricsBuilder2.addDurationMetric("GateRestoreDurationMs", 40);
        tracker.reportInitializationMetrics(subTaskInitializationMetricsBuilder2.build(215));

        assertThat(reportedSpans.size()).isEqualTo(1);
        Span reportedSpan = Iterables.getOnlyElement(reportedSpans);
        assertThat(reportedSpan.getStartTsMillis()).isEqualTo(100L);
        assertThat(reportedSpan.getEndTsMillis()).isEqualTo(215L);
        assertThat(reportedSpan.getAttributes().get("checkpointId")).isEqualTo(42L);
        assertThat(reportedSpan.getAttributes().get("fullSize")).isEqualTo(1024L);

        // simulate another failover with the same instance
        reportedSpans.clear();

        tracker.reportInitializationStartTs(100);
        tracker.reportRestoredCheckpoint(
                new RestoredCheckpointStats(
                        44,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        100,
                        null,
                        1024));

        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder1 =
                new SubTaskInitializationMetricsBuilder(110)
                        .setStatus(InitializationStatus.COMPLETED);
        subTaskInitializationMetricsBuilder1.addDurationMetric("MailboxStartDurationMs", 10);
        subTaskInitializationMetricsBuilder1.addDurationMetric("ReadOutputDataDurationMs", 20);
        subTaskInitializationMetricsBuilder1.addDurationMetric("InitializeStateDurationMs", 30);
        subTaskInitializationMetricsBuilder1.addDurationMetric("GateRestoreDurationMs", 40);
        tracker.reportInitializationMetrics(subTaskInitializationMetricsBuilder1.build(215));
        assertThat(reportedSpans).isEmpty();

        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder =
                new SubTaskInitializationMetricsBuilder(110)
                        .setStatus(InitializationStatus.COMPLETED);
        subTaskInitializationMetricsBuilder.addDurationMetric("MailboxStartDurationMs", 10);
        subTaskInitializationMetricsBuilder.addDurationMetric("ReadOutputDataDurationMs", 20);
        subTaskInitializationMetricsBuilder.addDurationMetric("InitializeStateDurationMs", 30);
        subTaskInitializationMetricsBuilder.addDurationMetric("GateRestoreDurationMs", 40);
        tracker.reportInitializationMetrics(subTaskInitializationMetricsBuilder.build(215));

        assertThat(reportedSpans.size()).isEqualTo(1);
        reportedSpan = Iterables.getOnlyElement(reportedSpans);
        assertThat(reportedSpan.getAttributes().get("checkpointId")).isEqualTo(44L);
    }

    /** Tests the registration of the checkpoint metrics. */
    @Test
    void testMetricsRegistration() {
        final Collection<String> registeredGaugeNames = new ArrayList<>();

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                        if (gauge != null) {
                            registeredGaugeNames.add(name);
                        }
                        return gauge;
                    }
                };

        new CheckpointStatsTracker(0, metricGroup, new JobID());

        // Make sure this test is adjusted when further metrics are added
        assertThat(registeredGaugeNames)
                .containsAll(
                        Arrays.asList(
                                CheckpointStatsTracker.NUMBER_OF_CHECKPOINTS_METRIC,
                                CheckpointStatsTracker.NUMBER_OF_IN_PROGRESS_CHECKPOINTS_METRIC,
                                CheckpointStatsTracker.NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC,
                                CheckpointStatsTracker.NUMBER_OF_FAILED_CHECKPOINTS_METRIC,
                                CheckpointStatsTracker.LATEST_RESTORED_CHECKPOINT_TIMESTAMP_METRIC,
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_SIZE_METRIC,
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_FULL_SIZE_METRIC,
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_DURATION_METRIC,
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_PROCESSED_DATA_METRIC,
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_PERSISTED_DATA_METRIC,
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_EXTERNAL_PATH_METRIC,
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_ID_METRIC));
        assertThat(registeredGaugeNames).hasSize(12);
    }

    /**
     * Tests that the metrics are updated properly. We had a bug that required new stats snapshots
     * in order to update the metrics.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testMetricsAreUpdated() throws Exception {
        final Map<String, Gauge<?>> registeredGauges = new HashMap<>();

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                        registeredGauges.put(name, gauge);
                        return gauge;
                    }
                };

        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionJobVertex jobVertex = graph.getJobVertex(jobVertexID);

        CheckpointStatsTracker stats = new CheckpointStatsTracker(0, metricGroup, new JobID());

        // Make sure to adjust this test if metrics are added/removed
        assertThat(registeredGauges).hasSize(12);

        // Check initial values
        Gauge<Long> numCheckpoints =
                (Gauge<Long>)
                        registeredGauges.get(CheckpointStatsTracker.NUMBER_OF_CHECKPOINTS_METRIC);
        Gauge<Integer> numInProgressCheckpoints =
                (Gauge<Integer>)
                        registeredGauges.get(
                                CheckpointStatsTracker.NUMBER_OF_IN_PROGRESS_CHECKPOINTS_METRIC);
        Gauge<Long> numCompletedCheckpoints =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC);
        Gauge<Long> numFailedCheckpoints =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.NUMBER_OF_FAILED_CHECKPOINTS_METRIC);
        Gauge<Long> latestRestoreTimestamp =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.LATEST_RESTORED_CHECKPOINT_TIMESTAMP_METRIC);
        Gauge<Long> latestCompletedSize =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_SIZE_METRIC);
        Gauge<Long> latestCompletedFullSize =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_FULL_SIZE_METRIC);
        Gauge<Long> latestCompletedDuration =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_DURATION_METRIC);
        Gauge<Long> latestProcessedData =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_PROCESSED_DATA_METRIC);
        Gauge<Long> latestPersistedData =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_PERSISTED_DATA_METRIC);
        Gauge<String> latestCompletedExternalPath =
                (Gauge<String>)
                        registeredGauges.get(
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_EXTERNAL_PATH_METRIC);
        Gauge<Long> latestCompletedId =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_ID_METRIC);

        assertThat(numCheckpoints.getValue()).isZero();
        assertThat(numInProgressCheckpoints.getValue()).isZero();
        assertThat(numCompletedCheckpoints.getValue()).isZero();
        assertThat(numFailedCheckpoints.getValue()).isZero();
        assertThat(latestRestoreTimestamp.getValue()).isEqualTo(-1);
        assertThat(latestCompletedSize.getValue()).isEqualTo(-1);
        assertThat(latestCompletedFullSize.getValue()).isEqualTo(-1);
        assertThat(latestCompletedDuration.getValue()).isEqualTo(-1);
        assertThat(latestProcessedData.getValue()).isEqualTo(-1);
        assertThat(latestPersistedData.getValue()).isEqualTo(-1);
        assertThat(latestCompletedExternalPath.getValue()).isEqualTo("n/a");
        assertThat(latestCompletedId.getValue()).isEqualTo(-1);

        PendingCheckpointStats pending =
                stats.reportPendingCheckpoint(
                        0,
                        0,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        singletonMap(jobVertexID, 1));

        // Check counts
        assertThat(numCheckpoints.getValue()).isOne();
        assertThat(numInProgressCheckpoints.getValue()).isOne();
        assertThat(numCompletedCheckpoints.getValue()).isZero();
        assertThat(numFailedCheckpoints.getValue()).isZero();

        long ackTimestamp = 11231230L;
        long checkpointedSize = 123L;
        long fullCheckpointSize = 12381238L;
        long processedData = 4242L;
        long persistedData = 4444L;
        long ignored = 0;
        String externalPath = "myexternalpath";

        SubtaskStateStats subtaskStats =
                new SubtaskStateStats(
                        0,
                        ackTimestamp,
                        checkpointedSize,
                        fullCheckpointSize,
                        ignored,
                        ignored,
                        processedData,
                        persistedData,
                        ignored,
                        ignored,
                        false,
                        true);

        assertThat(pending.reportSubtaskStats(jobVertexID, subtaskStats)).isTrue();

        stats.reportCompletedCheckpoint(pending.toCompletedCheckpointStats(externalPath));

        // Verify completed checkpoint updated
        assertThat(numCheckpoints.getValue()).isOne();
        assertThat(numInProgressCheckpoints.getValue()).isZero();
        assertThat(numCompletedCheckpoints.getValue()).isOne();
        assertThat(numFailedCheckpoints.getValue()).isZero();
        assertThat(latestRestoreTimestamp.getValue()).isEqualTo(-1);
        assertThat(latestCompletedSize.getValue()).isEqualTo(checkpointedSize);
        assertThat(latestCompletedFullSize.getValue()).isEqualTo(fullCheckpointSize);
        assertThat(latestProcessedData.getValue()).isEqualTo(processedData);
        assertThat(latestPersistedData.getValue()).isEqualTo(persistedData);
        assertThat(latestCompletedDuration.getValue()).isEqualTo(ackTimestamp);
        assertThat(latestCompletedExternalPath.getValue()).isEqualTo(externalPath);
        assertThat(latestCompletedId.getValue()).isZero();

        // Check failed
        PendingCheckpointStats nextPending =
                stats.reportPendingCheckpoint(
                        1,
                        11,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        singletonMap(jobVertexID, 1));

        long failureTimestamp = 1230123L;
        stats.reportFailedCheckpoint(nextPending.toFailedCheckpoint(failureTimestamp, null));

        // Verify updated
        assertThat(numCheckpoints.getValue()).isEqualTo(2);
        assertThat(numInProgressCheckpoints.getValue()).isZero();
        assertThat(numCompletedCheckpoints.getValue()).isOne();
        assertThat(numFailedCheckpoints.getValue()).isOne(); // one failed now
        assertThat(latestCompletedId.getValue()).isZero();

        // Check restore
        long restoreTimestamp = 183419283L;
        RestoredCheckpointStats restored =
                new RestoredCheckpointStats(
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        restoreTimestamp,
                        null,
                        42);
        stats.reportInitializationStartTs(restoreTimestamp);
        stats.reportRestoredCheckpoint(restored);

        assertThat(numCheckpoints.getValue()).isEqualTo(2);
        assertThat(numInProgressCheckpoints.getValue()).isZero();
        assertThat(numCompletedCheckpoints.getValue()).isOne();
        assertThat(numFailedCheckpoints.getValue()).isOne();
        assertThat(latestCompletedId.getValue()).isZero();

        assertThat(latestRestoreTimestamp.getValue()).isEqualTo(restoreTimestamp);

        // Check Internal Checkpoint Configuration
        PendingCheckpointStats thirdPending =
                stats.reportPendingCheckpoint(
                        2,
                        5000,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        singletonMap(jobVertexID, 1));

        thirdPending.reportSubtaskStats(jobVertexID, subtaskStats);
        stats.reportCompletedCheckpoint(thirdPending.toCompletedCheckpointStats(null));
        assertThat(latestCompletedId.getValue()).isEqualTo(2);

        // Verify external path is "n/a", because internal checkpoint won't generate external path.
        assertThat(latestCompletedExternalPath.getValue()).isEqualTo("n/a");
    }

    // ------------------------------------------------------------------------

    private SubtaskStateStats createSubtaskStats(int index) {
        return new SubtaskStateStats(index, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, true);
    }
}
