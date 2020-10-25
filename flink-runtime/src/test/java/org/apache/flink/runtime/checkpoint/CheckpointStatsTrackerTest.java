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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckpointStatsTrackerTest {

	/**
	 * Tests access to the snapshotting settings.
	 */
	@Test
	public void testGetSnapshottingSettings() throws Exception {
		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		when(jobVertex.getParallelism()).thenReturn(1);

		JobCheckpointingSettings snapshottingSettings = new JobCheckpointingSettings(
			Collections.singletonList(new JobVertexID()),
			Collections.singletonList(new JobVertexID()),
			Collections.singletonList(new JobVertexID()),
			new CheckpointCoordinatorConfiguration(
				181238123L,
				19191992L,
				191929L,
				123,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				false,
				false,
				false,
				0
			),
			null);

		CheckpointStatsTracker tracker = new CheckpointStatsTracker(
			0,
			Collections.singletonList(jobVertex),
			snapshottingSettings.getCheckpointCoordinatorConfiguration(),
			new UnregisteredMetricsGroup());

		assertEquals(snapshottingSettings.getCheckpointCoordinatorConfiguration(), tracker.getJobCheckpointingConfiguration());
	}

	/**
	 * Tests that the number of remembered checkpoints configuration
	 * is respected.
	 */
	@Test
	public void testTrackerWithoutHistory() throws Exception {
		int numberOfSubtasks = 3;

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		when(jobVertex.getParallelism()).thenReturn(numberOfSubtasks);

		CheckpointStatsTracker tracker = new CheckpointStatsTracker(
			0,
			Collections.singletonList(jobVertex),
			mock(CheckpointCoordinatorConfiguration.class),
			new UnregisteredMetricsGroup());

		PendingCheckpointStats pending = tracker.reportPendingCheckpoint(
			0,
			1,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

		pending.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(0));
		pending.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(1));
		pending.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(2));

		pending.reportCompletedCheckpoint(null);

		CheckpointStatsSnapshot snapshot = tracker.createSnapshot();
		// History should be empty
		assertFalse(snapshot.getHistory().getCheckpoints().iterator().hasNext());

		// Counts should be available
		CheckpointStatsCounts counts = snapshot.getCounts();
		assertEquals(1, counts.getNumberOfCompletedCheckpoints());
		assertEquals(1, counts.getTotalNumberOfCheckpoints());

		// Summary should be available
		CompletedCheckpointStatsSummary summary = snapshot.getSummaryStats();
		assertEquals(1, summary.getStateSizeStats().getCount());
		assertEquals(1, summary.getEndToEndDurationStats().getCount());

		// Latest completed checkpoint
		assertNotNull(snapshot.getHistory().getLatestCompletedCheckpoint());
		assertEquals(0, snapshot.getHistory().getLatestCompletedCheckpoint().getCheckpointId());
	}

	/**
	 * Tests tracking of checkpoints.
	 */
	@Test
	public void testCheckpointTracking() throws Exception {
		int numberOfSubtasks = 3;

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		when(jobVertex.getParallelism()).thenReturn(numberOfSubtasks);

		CheckpointStatsTracker tracker = new CheckpointStatsTracker(
			10,
			Collections.singletonList(jobVertex),
			mock(CheckpointCoordinatorConfiguration.class),
			new UnregisteredMetricsGroup());

		// Completed checkpoint
		PendingCheckpointStats completed1 = tracker.reportPendingCheckpoint(
			0,
			1,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

		completed1.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(0));
		completed1.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(1));
		completed1.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(2));

		completed1.reportCompletedCheckpoint(null);

		// Failed checkpoint
		PendingCheckpointStats failed = tracker.reportPendingCheckpoint(
			1,
			1,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

		failed.reportFailedCheckpoint(12, null);

		// Completed savepoint
		PendingCheckpointStats savepoint = tracker.reportPendingCheckpoint(
			2,
			1,
			CheckpointProperties.forSavepoint(true));

		savepoint.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(0));
		savepoint.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(1));
		savepoint.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(2));

		savepoint.reportCompletedCheckpoint(null);

		// In Progress
		PendingCheckpointStats inProgress = tracker.reportPendingCheckpoint(
			3,
			1,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

		RestoredCheckpointStats restored = new RestoredCheckpointStats(81, CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION), 123, null);
		tracker.reportRestoredCheckpoint(restored);

		CheckpointStatsSnapshot snapshot = tracker.createSnapshot();

		// Counts
		CheckpointStatsCounts counts = snapshot.getCounts();
		assertEquals(4, counts.getTotalNumberOfCheckpoints());
		assertEquals(1, counts.getNumberOfInProgressCheckpoints());
		assertEquals(2, counts.getNumberOfCompletedCheckpoints());
		assertEquals(1, counts.getNumberOfFailedCheckpoints());

		// Summary stats
		CompletedCheckpointStatsSummary summary = snapshot.getSummaryStats();
		assertEquals(2, summary.getStateSizeStats().getCount());
		assertEquals(2, summary.getEndToEndDurationStats().getCount());

		// History
		CheckpointStatsHistory history = snapshot.getHistory();
		Iterator<AbstractCheckpointStats> it = history.getCheckpoints().iterator();

		assertTrue(it.hasNext());
		AbstractCheckpointStats stats = it.next();
		assertEquals(3, stats.getCheckpointId());
		assertTrue(stats.getStatus().isInProgress());

		assertTrue(it.hasNext());
		stats = it.next();
		assertEquals(2, stats.getCheckpointId());
		assertTrue(stats.getStatus().isCompleted());

		assertTrue(it.hasNext());
		stats = it.next();
		assertEquals(1, stats.getCheckpointId());
		assertTrue(stats.getStatus().isFailed());

		assertTrue(it.hasNext());
		stats = it.next();
		assertEquals(0, stats.getCheckpointId());
		assertTrue(stats.getStatus().isCompleted());

		assertFalse(it.hasNext());

		// Latest checkpoints
		assertEquals(completed1.getCheckpointId(), snapshot.getHistory().getLatestCompletedCheckpoint().getCheckpointId());
		assertEquals(savepoint.getCheckpointId(), snapshot.getHistory().getLatestSavepoint().getCheckpointId());
		assertEquals(failed.getCheckpointId(), snapshot.getHistory().getLatestFailedCheckpoint().getCheckpointId());
		assertEquals(restored, snapshot.getLatestRestoredCheckpoint());
	}

	/**
	 * Tests that snapshots are only created if a new snapshot has been reported
	 * or updated.
	 */
	@Test
	public void testCreateSnapshot() throws Exception {
		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		when(jobVertex.getParallelism()).thenReturn(1);

		CheckpointStatsTracker tracker = new CheckpointStatsTracker(
			10,
			Collections.singletonList(jobVertex),
			mock(CheckpointCoordinatorConfiguration.class),
			new UnregisteredMetricsGroup());

		CheckpointStatsSnapshot snapshot1 = tracker.createSnapshot();

		// Pending checkpoint => new snapshot
		PendingCheckpointStats pending = tracker.reportPendingCheckpoint(
			0,
			1,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

		pending.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(0));

		CheckpointStatsSnapshot snapshot2 = tracker.createSnapshot();
		assertNotEquals(snapshot1, snapshot2);

		assertEquals(snapshot2, tracker.createSnapshot());

		// Complete checkpoint => new snapshot
		pending.reportCompletedCheckpoint(null);

		CheckpointStatsSnapshot snapshot3 = tracker.createSnapshot();
		assertNotEquals(snapshot2, snapshot3);

		// Restore operation => new snapshot
		tracker.reportRestoredCheckpoint(new RestoredCheckpointStats(12, CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION), 12, null));

		CheckpointStatsSnapshot snapshot4 = tracker.createSnapshot();
		assertNotEquals(snapshot3, snapshot4);
		assertEquals(snapshot4, tracker.createSnapshot());
	}

	/**
	 * Tests the registration of the checkpoint metrics.
	 */
	@Test
	public void testMetricsRegistration() throws Exception {
		final Collection<String> registeredGaugeNames = new ArrayList<>();

		MetricGroup metricGroup = new UnregisteredMetricsGroup() {
			@Override
			public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
				if (gauge != null) {
					registeredGaugeNames.add(name);
				}
				return gauge;
			}
		};

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		when(jobVertex.getParallelism()).thenReturn(1);

		new CheckpointStatsTracker(
			0,
			Collections.singletonList(jobVertex),
			mock(CheckpointCoordinatorConfiguration.class),
			metricGroup);

		// Make sure this test is adjusted when further metrics are added
		assertTrue(registeredGaugeNames.containsAll(Arrays.asList(
			CheckpointStatsTracker.NUMBER_OF_CHECKPOINTS_METRIC,
			CheckpointStatsTracker.NUMBER_OF_IN_PROGRESS_CHECKPOINTS_METRIC,
			CheckpointStatsTracker.NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC,
			CheckpointStatsTracker.NUMBER_OF_FAILED_CHECKPOINTS_METRIC,
			CheckpointStatsTracker.LATEST_RESTORED_CHECKPOINT_TIMESTAMP_METRIC,
			CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_SIZE_METRIC,
			CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_DURATION_METRIC,
			CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_PROCESSED_DATA_METRIC,
			CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_PERSISTED_DATA_METRIC,
			CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_EXTERNAL_PATH_METRIC
		)));
		assertEquals(10, registeredGaugeNames.size());
	}

	/**
	 * Tests that the metrics are updated properly. We had a bug that required new stats
	 * snapshots in order to update the metrics.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testMetricsAreUpdated() throws Exception {
		final Map<String, Gauge<?>> registeredGauges = new HashMap<>();

		MetricGroup metricGroup = new UnregisteredMetricsGroup() {
			@Override
			public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
				registeredGauges.put(name, gauge);
				return gauge;
			}
		};

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		when(jobVertex.getParallelism()).thenReturn(1);

		CheckpointStatsTracker stats = new CheckpointStatsTracker(
			0,
			Collections.singletonList(jobVertex),
			mock(CheckpointCoordinatorConfiguration.class),
			metricGroup);

		// Make sure to adjust this test if metrics are added/removed
		assertEquals(10, registeredGauges.size());

		// Check initial values
		Gauge<Long> numCheckpoints = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.NUMBER_OF_CHECKPOINTS_METRIC);
		Gauge<Integer> numInProgressCheckpoints = (Gauge<Integer>) registeredGauges.get(CheckpointStatsTracker.NUMBER_OF_IN_PROGRESS_CHECKPOINTS_METRIC);
		Gauge<Long> numCompletedCheckpoints = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC);
		Gauge<Long> numFailedCheckpoints = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.NUMBER_OF_FAILED_CHECKPOINTS_METRIC);
		Gauge<Long> latestRestoreTimestamp = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.LATEST_RESTORED_CHECKPOINT_TIMESTAMP_METRIC);
		Gauge<Long> latestCompletedSize = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_SIZE_METRIC);
		Gauge<Long> latestCompletedDuration = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_DURATION_METRIC);
		Gauge<Long> latestProcessedData = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_PROCESSED_DATA_METRIC);
		Gauge<Long> latestPersistedData = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_PERSISTED_DATA_METRIC);
		Gauge<String> latestCompletedExternalPath = (Gauge<String>) registeredGauges.get(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_EXTERNAL_PATH_METRIC);

		assertEquals(Long.valueOf(0), numCheckpoints.getValue());
		assertEquals(Integer.valueOf(0), numInProgressCheckpoints.getValue());
		assertEquals(Long.valueOf(0), numCompletedCheckpoints.getValue());
		assertEquals(Long.valueOf(0), numFailedCheckpoints.getValue());
		assertEquals(Long.valueOf(-1), latestRestoreTimestamp.getValue());
		assertEquals(Long.valueOf(-1), latestCompletedSize.getValue());
		assertEquals(Long.valueOf(-1), latestCompletedDuration.getValue());
		assertEquals(Long.valueOf(-1), latestProcessedData.getValue());
		assertEquals(Long.valueOf(-1), latestPersistedData.getValue());
		assertEquals("n/a", latestCompletedExternalPath.getValue());

		PendingCheckpointStats pending = stats.reportPendingCheckpoint(
			0,
			0,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

		// Check counts
		assertEquals(Long.valueOf(1), numCheckpoints.getValue());
		assertEquals(Integer.valueOf(1), numInProgressCheckpoints.getValue());
		assertEquals(Long.valueOf(0), numCompletedCheckpoints.getValue());
		assertEquals(Long.valueOf(0), numFailedCheckpoints.getValue());

		long ackTimestamp = 11231230L;
		long stateSize = 12381238L;
		long processedData = 4242L;
		long persistedData = 4444L;
		long ignored = 0;
		String externalPath = "myexternalpath";

		SubtaskStateStats subtaskStats = new SubtaskStateStats(
			0,
			ackTimestamp,
			stateSize,
			ignored,
			ignored,
			processedData,
			persistedData,
			ignored,
			ignored);

		assertTrue(pending.reportSubtaskStats(jobVertex.getJobVertexId(), subtaskStats));

		pending.reportCompletedCheckpoint(externalPath);

		// Verify completed checkpoint updated
		assertEquals(Long.valueOf(1), numCheckpoints.getValue());
		assertEquals(Integer.valueOf(0), numInProgressCheckpoints.getValue());
		assertEquals(Long.valueOf(1), numCompletedCheckpoints.getValue());
		assertEquals(Long.valueOf(0), numFailedCheckpoints.getValue());
		assertEquals(Long.valueOf(-1), latestRestoreTimestamp.getValue());
		assertEquals(Long.valueOf(stateSize), latestCompletedSize.getValue());
		assertEquals(Long.valueOf(processedData), latestProcessedData.getValue());
		assertEquals(Long.valueOf(persistedData), latestPersistedData.getValue());
		assertEquals(Long.valueOf(ackTimestamp), latestCompletedDuration.getValue());
		assertEquals(externalPath, latestCompletedExternalPath.getValue());

		// Check failed
		PendingCheckpointStats nextPending = stats.reportPendingCheckpoint(
			1,
			11,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

		long failureTimestamp = 1230123L;
		nextPending.reportFailedCheckpoint(failureTimestamp, null);

		// Verify updated
		assertEquals(Long.valueOf(2), numCheckpoints.getValue());
		assertEquals(Integer.valueOf(0), numInProgressCheckpoints.getValue());
		assertEquals(Long.valueOf(1), numCompletedCheckpoints.getValue());
		assertEquals(Long.valueOf(1), numFailedCheckpoints.getValue()); // one failed now

		// Check restore
		long restoreTimestamp = 183419283L;
		RestoredCheckpointStats restored = new RestoredCheckpointStats(
			1,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			restoreTimestamp,
			null);
		stats.reportRestoredCheckpoint(restored);

		assertEquals(Long.valueOf(2), numCheckpoints.getValue());
		assertEquals(Integer.valueOf(0), numInProgressCheckpoints.getValue());
		assertEquals(Long.valueOf(1), numCompletedCheckpoints.getValue());
		assertEquals(Long.valueOf(1), numFailedCheckpoints.getValue());

		assertEquals(Long.valueOf(restoreTimestamp), latestRestoreTimestamp.getValue());

		// Check Internal Checkpoint Configuration
		PendingCheckpointStats thirdPending = stats.reportPendingCheckpoint(
			2,
			5000,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

		thirdPending.reportSubtaskStats(jobVertex.getJobVertexId(), subtaskStats);
		thirdPending.reportCompletedCheckpoint(null);

		// Verify external path is "n/a", because internal checkpoint won't generate external path.
		assertEquals("n/a", latestCompletedExternalPath.getValue());
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates a "disabled" checkpoint tracker for tests.
	 */
	static CheckpointStatsTracker createTestTracker() {
		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		when(jobVertex.getParallelism()).thenReturn(1);

		return new CheckpointStatsTracker(
			0,
			Collections.singletonList(jobVertex),
			mock(CheckpointCoordinatorConfiguration.class),
			new UnregisteredMetricsGroup());
	}

	private SubtaskStateStats createSubtaskStats(int index) {
		return new SubtaskStateStats(index, 0, 0, 0, 0, 0, 0, 0, 0);
	}
}
