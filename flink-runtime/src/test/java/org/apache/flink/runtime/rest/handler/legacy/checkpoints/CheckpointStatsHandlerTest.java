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

package org.apache.flink.runtime.rest.handler.legacy.checkpoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStatsCounts;
import org.apache.flink.runtime.checkpoint.CheckpointStatsHistory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStatsSummary;
import org.apache.flink.runtime.checkpoint.FailedCheckpointStats;
import org.apache.flink.runtime.checkpoint.MinMaxAvgStats;
import org.apache.flink.runtime.checkpoint.PendingCheckpointStats;
import org.apache.flink.runtime.checkpoint.RestoredCheckpointStats;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the CheckpointStatsHandler.
 */
public class CheckpointStatsHandlerTest {

	@Test
	public void testArchiver() throws IOException {
		JsonArchivist archivist = new CheckpointStatsDetailsHandler.CheckpointStatsDetailsJsonArchivist();
		TestCheckpointStats testCheckpointStats = createTestCheckpointStats();
		when(testCheckpointStats.graph.getJobID()).thenReturn(new JobID());

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(testCheckpointStats.graph);
		Assert.assertEquals(3, archives.size());

		ObjectMapper mapper = new ObjectMapper();

		Iterator<ArchivedJson> iterator = archives.iterator();
		ArchivedJson archive1 = iterator.next();
		Assert.assertEquals("/jobs/" + testCheckpointStats.graph.getJobID() + "/checkpoints/details/" + testCheckpointStats.inProgress.getCheckpointId(), archive1.getPath());
		compareInProgressCheckpoint(testCheckpointStats.inProgress, mapper.readTree(archive1.getJson()));

		ArchivedJson archive2 = iterator.next();
		Assert.assertEquals("/jobs/" + testCheckpointStats.graph.getJobID() + "/checkpoints/details/" + testCheckpointStats.completedSavepoint.getCheckpointId(), archive2.getPath());
		compareCompletedSavepoint(testCheckpointStats.completedSavepoint, mapper.readTree(archive2.getJson()));

		ArchivedJson archive3 = iterator.next();
		Assert.assertEquals("/jobs/" + testCheckpointStats.graph.getJobID() + "/checkpoints/details/" + testCheckpointStats.failed.getCheckpointId(), archive3.getPath());
		compareFailedCheckpoint(testCheckpointStats.failed, mapper.readTree(archive3.getJson()));
	}

	@Test
	public void testGetPaths() {
		CheckpointStatsHandler handler = new CheckpointStatsHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/checkpoints", paths[0]);
	}

	/**
	 * Tests a complete checkpoint stats snapshot.
	 */
	@Test
	public void testCheckpointStatsRequest() throws Exception {
		TestCheckpointStats testCheckpointStats = createTestCheckpointStats();

		CheckpointStatsHandler handler = new CheckpointStatsHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
		String json = handler.handleRequest(testCheckpointStats.graph, Collections.<String, String>emptyMap()).get();

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(json);

		compareCheckpointStats(testCheckpointStats, rootNode);
	}

	private static TestCheckpointStats createTestCheckpointStats() {
		// Counts
		CheckpointStatsCounts counts = mock(CheckpointStatsCounts.class);
		when(counts.getNumberOfRestoredCheckpoints()).thenReturn(123123123L);
		when(counts.getTotalNumberOfCheckpoints()).thenReturn(12981231203L);
		when(counts.getNumberOfInProgressCheckpoints()).thenReturn(191919);
		when(counts.getNumberOfCompletedCheckpoints()).thenReturn(882828200L);
		when(counts.getNumberOfFailedCheckpoints()).thenReturn(99171510L);

		// Summary
		CompletedCheckpointStatsSummary summary = mock(CompletedCheckpointStatsSummary.class);

		MinMaxAvgStats stateSizeSummary = mock(MinMaxAvgStats.class);
		when(stateSizeSummary.getMinimum()).thenReturn(81238123L);
		when(stateSizeSummary.getMaximum()).thenReturn(19919191999L);
		when(stateSizeSummary.getAverage()).thenReturn(1133L);

		MinMaxAvgStats durationSummary = mock(MinMaxAvgStats.class);
		when(durationSummary.getMinimum()).thenReturn(1182L);
		when(durationSummary.getMaximum()).thenReturn(88654L);
		when(durationSummary.getAverage()).thenReturn(171L);

		MinMaxAvgStats alignmentBufferedSummary = mock(MinMaxAvgStats.class);
		when(alignmentBufferedSummary.getMinimum()).thenReturn(81818181899L);
		when(alignmentBufferedSummary.getMaximum()).thenReturn(89999911118654L);
		when(alignmentBufferedSummary.getAverage()).thenReturn(11203131L);

		when(summary.getStateSizeStats()).thenReturn(stateSizeSummary);
		when(summary.getEndToEndDurationStats()).thenReturn(durationSummary);
		when(summary.getAlignmentBufferedStats()).thenReturn(alignmentBufferedSummary);

		// Latest
		CompletedCheckpointStats latestCompleted = mock(CompletedCheckpointStats.class);
		when(latestCompleted.getCheckpointId()).thenReturn(1992139L);
		when(latestCompleted.getTriggerTimestamp()).thenReturn(1919191900L);
		when(latestCompleted.getLatestAckTimestamp()).thenReturn(1977791901L);
		when(latestCompleted.getStateSize()).thenReturn(111939272822L);
		when(latestCompleted.getEndToEndDuration()).thenReturn(121191L);
		when(latestCompleted.getAlignmentBuffered()).thenReturn(1L);
		when(latestCompleted.getExternalPath()).thenReturn("latest-completed-external-path");

		CompletedCheckpointStats latestSavepoint = mock(CompletedCheckpointStats.class);
		when(latestSavepoint.getCheckpointId()).thenReturn(1992140L);
		when(latestSavepoint.getTriggerTimestamp()).thenReturn(1919191900L);
		when(latestSavepoint.getLatestAckTimestamp()).thenReturn(1977791901L);
		when(latestSavepoint.getStateSize()).thenReturn(111939272822L);
		when(latestSavepoint.getEndToEndDuration()).thenReturn(121191L);
		when(latestCompleted.getAlignmentBuffered()).thenReturn(182813L);
		when(latestSavepoint.getExternalPath()).thenReturn("savepoint-external-path");

		FailedCheckpointStats latestFailed = mock(FailedCheckpointStats.class);
		when(latestFailed.getCheckpointId()).thenReturn(1112L);
		when(latestFailed.getTriggerTimestamp()).thenReturn(12828L);
		when(latestFailed.getLatestAckTimestamp()).thenReturn(1901L);
		when(latestFailed.getFailureTimestamp()).thenReturn(11999976L);
		when(latestFailed.getStateSize()).thenReturn(111L);
		when(latestFailed.getEndToEndDuration()).thenReturn(12L);
		when(latestFailed.getAlignmentBuffered()).thenReturn(2L);
		when(latestFailed.getFailureMessage()).thenReturn("expected cause");

		RestoredCheckpointStats latestRestored = mock(RestoredCheckpointStats.class);
		when(latestRestored.getCheckpointId()).thenReturn(1199L);
		when(latestRestored.getRestoreTimestamp()).thenReturn(434242L);
		when(latestRestored.getProperties()).thenReturn(CheckpointProperties.forSavepoint());
		when(latestRestored.getExternalPath()).thenReturn("restored savepoint path");

		// History
		CheckpointStatsHistory history = mock(CheckpointStatsHistory.class);
		List<AbstractCheckpointStats> checkpoints = new ArrayList<>();

		PendingCheckpointStats inProgress = mock(PendingCheckpointStats.class);
		when(inProgress.getCheckpointId()).thenReturn(1992141L);
		when(inProgress.getStatus()).thenReturn(CheckpointStatsStatus.IN_PROGRESS);
		when(inProgress.getProperties()).thenReturn(
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
		when(inProgress.getTriggerTimestamp()).thenReturn(1919191900L);
		when(inProgress.getLatestAckTimestamp()).thenReturn(1977791901L);
		when(inProgress.getStateSize()).thenReturn(111939272822L);
		when(inProgress.getEndToEndDuration()).thenReturn(121191L);
		when(inProgress.getAlignmentBuffered()).thenReturn(1L);
		when(inProgress.getNumberOfSubtasks()).thenReturn(501);
		when(inProgress.getNumberOfAcknowledgedSubtasks()).thenReturn(101);

		CompletedCheckpointStats completedSavepoint = mock(CompletedCheckpointStats.class);
		when(completedSavepoint.getCheckpointId()).thenReturn(1322139L);
		when(completedSavepoint.getStatus()).thenReturn(CheckpointStatsStatus.COMPLETED);
		when(completedSavepoint.getProperties()).thenReturn(CheckpointProperties.forSavepoint());
		when(completedSavepoint.getTriggerTimestamp()).thenReturn(191900L);
		when(completedSavepoint.getLatestAckTimestamp()).thenReturn(197791901L);
		when(completedSavepoint.getStateSize()).thenReturn(1119822L);
		when(completedSavepoint.getEndToEndDuration()).thenReturn(12191L);
		when(completedSavepoint.getAlignmentBuffered()).thenReturn(111L);
		when(completedSavepoint.getNumberOfSubtasks()).thenReturn(33501);
		when(completedSavepoint.getNumberOfAcknowledgedSubtasks()).thenReturn(211);
		when(completedSavepoint.isDiscarded()).thenReturn(true);
		when(completedSavepoint.getExternalPath()).thenReturn("completed-external-path");

		FailedCheckpointStats failed = mock(FailedCheckpointStats.class);
		when(failed.getCheckpointId()).thenReturn(110719L);
		when(failed.getStatus()).thenReturn(CheckpointStatsStatus.FAILED);
		when(failed.getProperties()).thenReturn(
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
		when(failed.getTriggerTimestamp()).thenReturn(191900L);
		when(failed.getLatestAckTimestamp()).thenReturn(197791901L);
		when(failed.getStateSize()).thenReturn(1119822L);
		when(failed.getEndToEndDuration()).thenReturn(12191L);
		when(failed.getAlignmentBuffered()).thenReturn(111L);
		when(failed.getNumberOfSubtasks()).thenReturn(33501);
		when(failed.getNumberOfAcknowledgedSubtasks()).thenReturn(1);
		when(failed.getFailureTimestamp()).thenReturn(119230L);
		when(failed.getFailureMessage()).thenReturn("failure message");

		checkpoints.add(inProgress);
		checkpoints.add(completedSavepoint);
		checkpoints.add(failed);
		when(history.getCheckpoints()).thenReturn(checkpoints);
		when(history.getLatestCompletedCheckpoint()).thenReturn(latestCompleted);
		when(history.getLatestSavepoint()).thenReturn(latestSavepoint);
		when(history.getLatestFailedCheckpoint()).thenReturn(latestFailed);

		CheckpointStatsSnapshot snapshot = mock(CheckpointStatsSnapshot.class);
		when(snapshot.getCounts()).thenReturn(counts);
		when(snapshot.getSummaryStats()).thenReturn(summary);
		when(snapshot.getHistory()).thenReturn(history);
		when(snapshot.getLatestRestoredCheckpoint()).thenReturn(latestRestored);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getCheckpointStatsSnapshot()).thenReturn(snapshot);

		return new TestCheckpointStats(
			graph, counts, stateSizeSummary, durationSummary, alignmentBufferedSummary, summary,
			latestCompleted, latestSavepoint, latestFailed, latestRestored, inProgress,
			completedSavepoint, failed, history, snapshot
		);
	}

	private static void compareCheckpointStats(TestCheckpointStats checkpointStats, JsonNode rootNode) {
		CheckpointStatsCounts counts = checkpointStats.counts;
		JsonNode countNode = rootNode.get("counts");
		assertEquals(counts.getNumberOfRestoredCheckpoints(), countNode.get("restored").asLong());
		assertEquals(counts.getTotalNumberOfCheckpoints(), countNode.get("total").asLong());
		assertEquals(counts.getNumberOfInProgressCheckpoints(), countNode.get("in_progress").asLong());
		assertEquals(counts.getNumberOfCompletedCheckpoints(), countNode.get("completed").asLong());
		assertEquals(counts.getNumberOfFailedCheckpoints(), countNode.get("failed").asLong());

		MinMaxAvgStats stateSizeSummary = checkpointStats.stateSizeSummary;
		JsonNode summaryNode = rootNode.get("summary");
		JsonNode sizeSummaryNode = summaryNode.get("state_size");
		assertEquals(stateSizeSummary.getMinimum(), sizeSummaryNode.get("min").asLong());
		assertEquals(stateSizeSummary.getMaximum(), sizeSummaryNode.get("max").asLong());
		assertEquals(stateSizeSummary.getAverage(), sizeSummaryNode.get("avg").asLong());

		MinMaxAvgStats durationSummary = checkpointStats.durationSummary;
		JsonNode durationSummaryNode = summaryNode.get("end_to_end_duration");
		assertEquals(durationSummary.getMinimum(), durationSummaryNode.get("min").asLong());
		assertEquals(durationSummary.getMaximum(), durationSummaryNode.get("max").asLong());
		assertEquals(durationSummary.getAverage(), durationSummaryNode.get("avg").asLong());

		MinMaxAvgStats alignmentBufferedSummary = checkpointStats.alignmentBufferedSummary;
		JsonNode alignmentBufferedNode = summaryNode.get("alignment_buffered");
		assertEquals(alignmentBufferedSummary.getMinimum(), alignmentBufferedNode.get("min").asLong());
		assertEquals(alignmentBufferedSummary.getMaximum(), alignmentBufferedNode.get("max").asLong());
		assertEquals(alignmentBufferedSummary.getAverage(), alignmentBufferedNode.get("avg").asLong());

		CompletedCheckpointStats latestCompleted = checkpointStats.latestCompleted;
		JsonNode latestNode = rootNode.get("latest");
		JsonNode latestCheckpointNode = latestNode.get("completed");
		assertEquals(latestCompleted.getCheckpointId(), latestCheckpointNode.get("id").asLong());
		assertEquals(latestCompleted.getTriggerTimestamp(), latestCheckpointNode.get("trigger_timestamp").asLong());
		assertEquals(latestCompleted.getLatestAckTimestamp(), latestCheckpointNode.get("latest_ack_timestamp").asLong());
		assertEquals(latestCompleted.getStateSize(), latestCheckpointNode.get("state_size").asLong());
		assertEquals(latestCompleted.getEndToEndDuration(), latestCheckpointNode.get("end_to_end_duration").asLong());
		assertEquals(latestCompleted.getAlignmentBuffered(), latestCheckpointNode.get("alignment_buffered").asLong());
		assertEquals(latestCompleted.getExternalPath(), latestCheckpointNode.get("external_path").asText());

		CompletedCheckpointStats latestSavepoint = checkpointStats.latestSavepoint;
		JsonNode latestSavepointNode = latestNode.get("savepoint");
		assertEquals(latestSavepoint.getCheckpointId(), latestSavepointNode.get("id").asLong());
		assertEquals(latestSavepoint.getTriggerTimestamp(), latestSavepointNode.get("trigger_timestamp").asLong());
		assertEquals(latestSavepoint.getLatestAckTimestamp(), latestSavepointNode.get("latest_ack_timestamp").asLong());
		assertEquals(latestSavepoint.getStateSize(), latestSavepointNode.get("state_size").asLong());
		assertEquals(latestSavepoint.getEndToEndDuration(), latestSavepointNode.get("end_to_end_duration").asLong());
		assertEquals(latestSavepoint.getAlignmentBuffered(), latestSavepointNode.get("alignment_buffered").asLong());
		assertEquals(latestSavepoint.getExternalPath(), latestSavepointNode.get("external_path").asText());

		FailedCheckpointStats latestFailed = checkpointStats.latestFailed;
		JsonNode latestFailedNode = latestNode.get("failed");
		assertEquals(latestFailed.getCheckpointId(), latestFailedNode.get("id").asLong());
		assertEquals(latestFailed.getTriggerTimestamp(), latestFailedNode.get("trigger_timestamp").asLong());
		assertEquals(latestFailed.getLatestAckTimestamp(), latestFailedNode.get("latest_ack_timestamp").asLong());
		assertEquals(latestFailed.getStateSize(), latestFailedNode.get("state_size").asLong());
		assertEquals(latestFailed.getEndToEndDuration(), latestFailedNode.get("end_to_end_duration").asLong());
		assertEquals(latestFailed.getAlignmentBuffered(), latestFailedNode.get("alignment_buffered").asLong());
		assertEquals(latestFailed.getFailureTimestamp(), latestFailedNode.get("failure_timestamp").asLong());
		assertEquals(latestFailed.getFailureMessage(), latestFailedNode.get("failure_message").asText());

		RestoredCheckpointStats latestRestored = checkpointStats.latestRestored;
		JsonNode latestRestoredNode = latestNode.get("restored");
		assertEquals(latestRestored.getCheckpointId(), latestRestoredNode.get("id").asLong());
		assertEquals(latestRestored.getRestoreTimestamp(), latestRestoredNode.get("restore_timestamp").asLong());
		assertEquals(latestRestored.getProperties().isSavepoint(), latestRestoredNode.get("is_savepoint").asBoolean());
		assertEquals(latestRestored.getExternalPath(), latestRestoredNode.get("external_path").asText());

		JsonNode historyNode = rootNode.get("history");
		Iterator<JsonNode> it = historyNode.iterator();

		assertTrue(it.hasNext());
		JsonNode inProgressNode = it.next();

		PendingCheckpointStats inProgress = checkpointStats.inProgress;
		compareInProgressCheckpoint(inProgress, inProgressNode);

		assertTrue(it.hasNext());
		JsonNode completedSavepointNode = it.next();

		CompletedCheckpointStats completedSavepoint = checkpointStats.completedSavepoint;
		compareCompletedSavepoint(completedSavepoint, completedSavepointNode);

		assertTrue(it.hasNext());
		JsonNode failedNode = it.next();

		FailedCheckpointStats failed = checkpointStats.failed;
		compareFailedCheckpoint(failed, failedNode);

		assertFalse(it.hasNext());
	}

	private static void compareInProgressCheckpoint(PendingCheckpointStats inProgress, JsonNode inProgressNode) {
		assertEquals(inProgress.getCheckpointId(), inProgressNode.get("id").asLong());
		assertEquals(inProgress.getStatus().toString(), inProgressNode.get("status").asText());
		assertEquals(inProgress.getProperties().isSavepoint(), inProgressNode.get("is_savepoint").asBoolean());
		assertEquals(inProgress.getTriggerTimestamp(), inProgressNode.get("trigger_timestamp").asLong());
		assertEquals(inProgress.getLatestAckTimestamp(), inProgressNode.get("latest_ack_timestamp").asLong());
		assertEquals(inProgress.getStateSize(), inProgressNode.get("state_size").asLong());
		assertEquals(inProgress.getEndToEndDuration(), inProgressNode.get("end_to_end_duration").asLong());
		assertEquals(inProgress.getAlignmentBuffered(), inProgressNode.get("alignment_buffered").asLong());
		assertEquals(inProgress.getNumberOfSubtasks(), inProgressNode.get("num_subtasks").asInt());
		assertEquals(inProgress.getNumberOfAcknowledgedSubtasks(), inProgressNode.get("num_acknowledged_subtasks").asInt());
	}

	private static void compareCompletedSavepoint(CompletedCheckpointStats completedSavepoint, JsonNode completedSavepointNode) {
		assertEquals(completedSavepoint.getCheckpointId(), completedSavepointNode.get("id").asLong());
		assertEquals(completedSavepoint.getStatus().toString(), completedSavepointNode.get("status").asText());
		assertEquals(completedSavepoint.getProperties().isSavepoint(), completedSavepointNode.get("is_savepoint").asBoolean());
		assertEquals(completedSavepoint.getTriggerTimestamp(), completedSavepointNode.get("trigger_timestamp").asLong());
		assertEquals(completedSavepoint.getLatestAckTimestamp(), completedSavepointNode.get("latest_ack_timestamp").asLong());
		assertEquals(completedSavepoint.getStateSize(), completedSavepointNode.get("state_size").asLong());
		assertEquals(completedSavepoint.getEndToEndDuration(), completedSavepointNode.get("end_to_end_duration").asLong());
		assertEquals(completedSavepoint.getAlignmentBuffered(), completedSavepointNode.get("alignment_buffered").asLong());
		assertEquals(completedSavepoint.getNumberOfSubtasks(), completedSavepointNode.get("num_subtasks").asInt());
		assertEquals(completedSavepoint.getNumberOfAcknowledgedSubtasks(), completedSavepointNode.get("num_acknowledged_subtasks").asInt());

		assertEquals(completedSavepoint.getExternalPath(), completedSavepointNode.get("external_path").asText());
		assertEquals(completedSavepoint.isDiscarded(), completedSavepointNode.get("discarded").asBoolean());
	}

	private static void compareFailedCheckpoint(FailedCheckpointStats failed, JsonNode failedNode) {
		assertEquals(failed.getCheckpointId(), failedNode.get("id").asLong());
		assertEquals(failed.getStatus().toString(), failedNode.get("status").asText());
		assertEquals(failed.getProperties().isSavepoint(), failedNode.get("is_savepoint").asBoolean());
		assertEquals(failed.getTriggerTimestamp(), failedNode.get("trigger_timestamp").asLong());
		assertEquals(failed.getLatestAckTimestamp(), failedNode.get("latest_ack_timestamp").asLong());
		assertEquals(failed.getStateSize(), failedNode.get("state_size").asLong());
		assertEquals(failed.getEndToEndDuration(), failedNode.get("end_to_end_duration").asLong());
		assertEquals(failed.getAlignmentBuffered(), failedNode.get("alignment_buffered").asLong());
		assertEquals(failed.getNumberOfSubtasks(), failedNode.get("num_subtasks").asInt());
		assertEquals(failed.getNumberOfAcknowledgedSubtasks(), failedNode.get("num_acknowledged_subtasks").asInt());

		assertEquals(failed.getFailureTimestamp(), failedNode.get("failure_timestamp").asLong());
		assertEquals(failed.getFailureMessage(), failedNode.get("failure_message").asText());
	}

	private static class TestCheckpointStats {
		public final AccessExecutionGraph graph;
		public final CheckpointStatsCounts counts;
		public final MinMaxAvgStats stateSizeSummary;
		public final MinMaxAvgStats durationSummary;
		public final MinMaxAvgStats alignmentBufferedSummary;
		public final CompletedCheckpointStatsSummary summary;
		public final CompletedCheckpointStats latestCompleted;
		public final CompletedCheckpointStats latestSavepoint;
		public final FailedCheckpointStats latestFailed;
		public final RestoredCheckpointStats latestRestored;
		public final PendingCheckpointStats inProgress;
		public final CompletedCheckpointStats completedSavepoint;
		public final FailedCheckpointStats failed;
		public final CheckpointStatsHistory history;
		public final CheckpointStatsSnapshot snapshot;

		public TestCheckpointStats(
				AccessExecutionGraph graph,
				CheckpointStatsCounts counts,
				MinMaxAvgStats stateSizeSummary,
				MinMaxAvgStats durationSummary,
				MinMaxAvgStats alignmentBufferedSummary,
				CompletedCheckpointStatsSummary summary,
				CompletedCheckpointStats latestCompleted,
				CompletedCheckpointStats latestSavepoint,
				FailedCheckpointStats latestFailed,
				RestoredCheckpointStats latestRestored,
				PendingCheckpointStats inProgress,
				CompletedCheckpointStats completedSavepoint,
				FailedCheckpointStats failed,
				CheckpointStatsHistory history,
				CheckpointStatsSnapshot snapshot) {
			this.graph = graph;
			this.counts = counts;
			this.stateSizeSummary = stateSizeSummary;
			this.durationSummary = durationSummary;
			this.alignmentBufferedSummary = alignmentBufferedSummary;
			this.summary = summary;
			this.latestCompleted = latestCompleted;
			this.latestSavepoint = latestSavepoint;
			this.latestFailed = latestFailed;
			this.latestRestored = latestRestored;
			this.inProgress = inProgress;
			this.completedSavepoint = completedSavepoint;
			this.failed = failed;
			this.history = history;
			this.snapshot = snapshot;
		}
	}
}
