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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.junit.Test;

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
			181238123L,
			19191992L,
			191929L,
			123,
			ExternalizedCheckpointSettings.none(),
			null,
			false);

		CheckpointStatsTracker tracker = new CheckpointStatsTracker(
			0,
			Collections.singletonList(jobVertex),
			snapshottingSettings,
			new UnregisteredMetricsGroup());

		assertEquals(snapshottingSettings, tracker.getSnapshottingSettings());
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
			mock(JobCheckpointingSettings.class),
			new UnregisteredMetricsGroup());

		PendingCheckpointStats pending = tracker.reportPendingCheckpoint(
			0,
			1,
			CheckpointProperties.forStandardCheckpoint());

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
		assertEquals(1, summary.getAlignmentBufferedStats().getCount());

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
			mock(JobCheckpointingSettings.class),
			new UnregisteredMetricsGroup());

		// Completed checkpoint
		PendingCheckpointStats completed1 = tracker.reportPendingCheckpoint(
			0,
			1,
			CheckpointProperties.forStandardCheckpoint());

		completed1.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(0));
		completed1.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(1));
		completed1.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(2));

		completed1.reportCompletedCheckpoint(null);

		// Failed checkpoint
		PendingCheckpointStats failed = tracker.reportPendingCheckpoint(
			1,
			1,
			CheckpointProperties.forStandardCheckpoint());

		failed.reportFailedCheckpoint(12, null);

		// Completed savepoint
		PendingCheckpointStats savepoint = tracker.reportPendingCheckpoint(
			2,
			1,
			CheckpointProperties.forStandardSavepoint());

		savepoint.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(0));
		savepoint.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(1));
		savepoint.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(2));

		savepoint.reportCompletedCheckpoint(null);

		// In Progress
		PendingCheckpointStats inProgress = tracker.reportPendingCheckpoint(
			3,
			1,
			CheckpointProperties.forStandardCheckpoint());

		RestoredCheckpointStats restored = new RestoredCheckpointStats(81, CheckpointProperties.forStandardCheckpoint(), 123, null);
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
		assertEquals(2, summary.getAlignmentBufferedStats().getCount());

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
			mock(JobCheckpointingSettings.class),
			new UnregisteredMetricsGroup());

		CheckpointStatsSnapshot snapshot1 = tracker.createSnapshot();

		// Pending checkpoint => new snapshot
		PendingCheckpointStats pending = tracker.reportPendingCheckpoint(
			0,
			1,
			CheckpointProperties.forStandardCheckpoint());

		pending.reportSubtaskStats(jobVertex.getJobVertexId(), createSubtaskStats(0));

		CheckpointStatsSnapshot snapshot2 = tracker.createSnapshot();
		assertNotEquals(snapshot1, snapshot2);

		assertEquals(snapshot2, tracker.createSnapshot());

		// Complete checkpoint => new snapshot
		pending.reportCompletedCheckpoint(null);

		CheckpointStatsSnapshot snapshot3 = tracker.createSnapshot();
		assertNotEquals(snapshot2, snapshot3);

		// Restore operation => new snapshot
		tracker.reportRestoredCheckpoint(new RestoredCheckpointStats(12, CheckpointProperties.forStandardCheckpoint(), 12, null));

		CheckpointStatsSnapshot snapshot4 = tracker.createSnapshot();
		assertNotEquals(snapshot3, snapshot4);
		assertEquals(snapshot4, tracker.createSnapshot());
	}

	/**
	 * Tests the registration of the checkpoint metrics.
	 */
	@Test
	public void testMetricsRegistration() throws Exception {
		MetricGroup metricGroup = mock(MetricGroup.class);

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		when(jobVertex.getParallelism()).thenReturn(1);

		new CheckpointStatsTracker(
			0,
			Collections.singletonList(jobVertex),
			mock(JobCheckpointingSettings.class),
			metricGroup);

		verify(metricGroup, times(1)).gauge(eq(CheckpointStatsTracker.NUMBER_OF_CHECKPOINTS_METRIC), any(Gauge.class));
		verify(metricGroup, times(1)).gauge(eq(CheckpointStatsTracker.NUMBER_OF_IN_PROGRESS_CHECKPOINTS_METRIC), any(Gauge.class));
		verify(metricGroup, times(1)).gauge(eq(CheckpointStatsTracker.NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC), any(Gauge.class));
		verify(metricGroup, times(1)).gauge(eq(CheckpointStatsTracker.NUMBER_OF_FAILED_CHECKPOINTS_METRIC), any(Gauge.class));
		verify(metricGroup, times(1)).gauge(eq(CheckpointStatsTracker.LATEST_RESTORED_CHECKPOINT_TIMESTAMP_METRIC), any(Gauge.class));
		verify(metricGroup, times(1)).gauge(eq(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_SIZE_METRIC), any(Gauge.class));
		verify(metricGroup, times(1)).gauge(eq(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_DURATION_METRIC), any(Gauge.class));
		verify(metricGroup, times(1)).gauge(eq(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_ALIGNMENT_BUFFERED_METRIC), any(Gauge.class));
		verify(metricGroup, times(1)).gauge(eq(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_EXTERNAL_PATH_METRIC), any(Gauge.class));

		// Make sure this test is adjusted when further metrics are added
		verify(metricGroup, times(9)).gauge(any(String.class), any(Gauge.class));
	}

	/**
	 * Tests that the metrics are updated properly. We had a bug that required new stats
	 * snapshots in order to update the metrics.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testMetricsAreUpdated() throws Exception {
		final Map<String, Gauge<?>> registeredGauges = new HashMap<>();

		MetricGroup metricGroup = new MetricGroup() {
			@Override
			public Counter counter(int name) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public Counter counter(String name) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public <C extends Counter> C counter(int name, C counter) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public <C extends Counter> C counter(String name, C counter) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public <T, G extends Gauge<T>> G gauge(int name, G gauge) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
				registeredGauges.put(name, gauge);
				return gauge;
			}

			@Override
			public <H extends Histogram> H histogram(String name, H histogram) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public <H extends Histogram> H histogram(int name, H histogram) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public <M extends Meter> M meter(String name, M meter) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public <M extends Meter> M meter(int name, M meter) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public MetricGroup addGroup(int name) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public MetricGroup addGroup(String name) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public String[] getScopeComponents() {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public Map<String, String> getAllVariables() {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public String getMetricIdentifier(String metricName) {
				throw new UnsupportedOperationException("Not expected in this test");
			}

			@Override
			public String getMetricIdentifier(String metricName, CharacterFilter filter) {
				throw new UnsupportedOperationException("Not expected in this test");
			}
		};

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		when(jobVertex.getParallelism()).thenReturn(1);

		CheckpointStatsTracker stats = new CheckpointStatsTracker(
			0,
			Collections.singletonList(jobVertex),
			mock(JobCheckpointingSettings.class),
			metricGroup);

		// Make sure to adjust this test if metrics are added/removed
		assertEquals(9, registeredGauges.size());

		// Check initial values
		Gauge<Long> numCheckpoints = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.NUMBER_OF_CHECKPOINTS_METRIC);
		Gauge<Integer> numInProgressCheckpoints = (Gauge<Integer>) registeredGauges.get(CheckpointStatsTracker.NUMBER_OF_IN_PROGRESS_CHECKPOINTS_METRIC);
		Gauge<Long> numCompletedCheckpoints = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC);
		Gauge<Long> numFailedCheckpoints = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.NUMBER_OF_FAILED_CHECKPOINTS_METRIC);
		Gauge<Long> latestRestoreTimestamp = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.LATEST_RESTORED_CHECKPOINT_TIMESTAMP_METRIC);
		Gauge<Long> latestCompletedSize = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_SIZE_METRIC);
		Gauge<Long> latestCompletedDuration = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_DURATION_METRIC);
		Gauge<Long> latestCompletedAlignmentBuffered = (Gauge<Long>) registeredGauges.get(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_ALIGNMENT_BUFFERED_METRIC);
		Gauge<String> latestCompletedExternalPath = (Gauge<String>) registeredGauges.get(CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_EXTERNAL_PATH_METRIC);

		assertEquals(Long.valueOf(0), numCheckpoints.getValue());
		assertEquals(Integer.valueOf(0), numInProgressCheckpoints.getValue());
		assertEquals(Long.valueOf(0), numCompletedCheckpoints.getValue());
		assertEquals(Long.valueOf(0), numFailedCheckpoints.getValue());
		assertEquals(Long.valueOf(-1), latestRestoreTimestamp.getValue());
		assertEquals(Long.valueOf(-1), latestCompletedSize.getValue());
		assertEquals(Long.valueOf(-1), latestCompletedDuration.getValue());
		assertEquals(Long.valueOf(-1), latestCompletedAlignmentBuffered.getValue());
		assertEquals("n/a", latestCompletedExternalPath.getValue());

		PendingCheckpointStats pending = stats.reportPendingCheckpoint(
			0,
			0,
			CheckpointProperties.forStandardCheckpoint());

		// Check counts
		assertEquals(Long.valueOf(1), numCheckpoints.getValue());
		assertEquals(Integer.valueOf(1), numInProgressCheckpoints.getValue());
		assertEquals(Long.valueOf(0), numCompletedCheckpoints.getValue());
		assertEquals(Long.valueOf(0), numFailedCheckpoints.getValue());

		long ackTimestamp = 11231230L;
		long stateSize = 12381238L;
		long ignored = 0;
		long alignmenetBuffered = 182812L;
		String externalPath = "myexternalpath";

		SubtaskStateStats subtaskStats = new SubtaskStateStats(
			0,
			ackTimestamp,
			stateSize,
			ignored,
			ignored,
			alignmenetBuffered,
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
		assertEquals(Long.valueOf(ackTimestamp), latestCompletedDuration.getValue());
		assertEquals(Long.valueOf(alignmenetBuffered), latestCompletedAlignmentBuffered.getValue());
		assertEquals(externalPath, latestCompletedExternalPath.getValue());

		// Check failed
		PendingCheckpointStats nextPending = stats.reportPendingCheckpoint(
			1,
			11,
			CheckpointProperties.forStandardCheckpoint());

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
			CheckpointProperties.forStandardCheckpoint(),
			restoreTimestamp,
			null);
		stats.reportRestoredCheckpoint(restored);

		assertEquals(Long.valueOf(2), numCheckpoints.getValue());
		assertEquals(Integer.valueOf(0), numInProgressCheckpoints.getValue());
		assertEquals(Long.valueOf(1), numCompletedCheckpoints.getValue());
		assertEquals(Long.valueOf(1), numFailedCheckpoints.getValue());

		assertEquals(Long.valueOf(restoreTimestamp), latestRestoreTimestamp.getValue());
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
			mock(JobCheckpointingSettings.class),
			new UnregisteredMetricsGroup());
	}

	private SubtaskStateStats createSubtaskStats(int index) {
		return new SubtaskStateStats(index, 0, 0, 0, 0, 0, 0);
	}
}
