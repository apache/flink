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

package org.apache.flink.runtime.webmonitor.handlers.checkpoints;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
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
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.utils.JsonUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckpointStatsHandlerTest {

	/**
	 * Tests a complete checkpoint stats snapshot.
	 */
	@Test
	public void testCheckpointStatsRequest() throws Exception {
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
		when(latestSavepoint.getCheckpointId()).thenReturn(1992139L);
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
		when(latestRestored.getProperties()).thenReturn(CheckpointProperties.forStandardSavepoint());
		when(latestRestored.getExternalPath()).thenReturn("restored savepoint path");

		// History
		CheckpointStatsHistory history = mock(CheckpointStatsHistory.class);
		List<AbstractCheckpointStats> checkpoints = new ArrayList<>();

		PendingCheckpointStats inProgress = mock(PendingCheckpointStats.class);
		when(inProgress.getCheckpointId()).thenReturn(1992139L);
		when(inProgress.getStatus()).thenReturn(CheckpointStatsStatus.IN_PROGRESS);
		when(inProgress.getProperties()).thenReturn(CheckpointProperties.forStandardCheckpoint());
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
		when(completedSavepoint.getProperties()).thenReturn(CheckpointProperties.forStandardSavepoint());
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
		when(failed.getProperties()).thenReturn(CheckpointProperties.forStandardCheckpoint());
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

		CheckpointStatsHandler handler = new CheckpointStatsHandler(mock(ExecutionGraphHolder.class));
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap());

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(json);

		JsonNode countNode = rootNode.get(JsonUtils.Keys.COUNTS);
		assertEquals(counts.getNumberOfRestoredCheckpoints(), countNode.get(JsonUtils.Keys.RESTORED).asLong());
		assertEquals(counts.getTotalNumberOfCheckpoints(), countNode.get(JsonUtils.Keys.TOTAL).asLong());
		assertEquals(counts.getNumberOfInProgressCheckpoints(), countNode.get(JsonUtils.Keys.IN_PROGRESS).asLong());
		assertEquals(counts.getNumberOfCompletedCheckpoints(), countNode.get(JsonUtils.Keys.COMPLETED).asLong());
		assertEquals(counts.getNumberOfFailedCheckpoints(), countNode.get(JsonUtils.Keys.FAILED).asLong());

		JsonNode summaryNode = rootNode.get(JsonUtils.Keys.SUMMARY);
		JsonNode sizeSummaryNode = summaryNode.get(JsonUtils.Keys.STATE_SIZE);
		assertEquals(stateSizeSummary.getMinimum(), sizeSummaryNode.get(JsonUtils.Keys.MIN).asLong());
		assertEquals(stateSizeSummary.getMaximum(), sizeSummaryNode.get(JsonUtils.Keys.MAX).asLong());
		assertEquals(stateSizeSummary.getAverage(), sizeSummaryNode.get(JsonUtils.Keys.AVG).asLong());

		JsonNode durationSummaryNode = summaryNode.get(JsonUtils.Keys.ETE_DURATION);
		assertEquals(durationSummary.getMinimum(), durationSummaryNode.get(JsonUtils.Keys.MIN).asLong());
		assertEquals(durationSummary.getMaximum(), durationSummaryNode.get(JsonUtils.Keys.MAX).asLong());
		assertEquals(durationSummary.getAverage(), durationSummaryNode.get(JsonUtils.Keys.AVG).asLong());

		JsonNode alignmentBufferedNode = summaryNode.get(JsonUtils.Keys.ALIGNMENT_BUFFERED);
		assertEquals(alignmentBufferedSummary.getMinimum(), alignmentBufferedNode.get(JsonUtils.Keys.MIN).asLong());
		assertEquals(alignmentBufferedSummary.getMaximum(), alignmentBufferedNode.get(JsonUtils.Keys.MAX).asLong());
		assertEquals(alignmentBufferedSummary.getAverage(), alignmentBufferedNode.get(JsonUtils.Keys.AVG).asLong());

		JsonNode latestNode = rootNode.get(JsonUtils.Keys.LATEST);
		JsonNode latestCheckpointNode = latestNode.get(JsonUtils.Keys.COMPLETED);
		assertEquals(latestCompleted.getCheckpointId(), latestCheckpointNode.get(JsonUtils.Keys.ID).asLong());
		assertEquals(latestCompleted.getTriggerTimestamp(), latestCheckpointNode.get(JsonUtils.Keys.TRIGGER_TIMESTAMP).asLong());
		assertEquals(latestCompleted.getLatestAckTimestamp(), latestCheckpointNode.get(JsonUtils.Keys.LATEST_ACK_TIMESTAMP).asLong());
		assertEquals(latestCompleted.getStateSize(), latestCheckpointNode.get(JsonUtils.Keys.STATE_SIZE).asLong());
		assertEquals(latestCompleted.getEndToEndDuration(), latestCheckpointNode.get(JsonUtils.Keys.ETE_DURATION).asLong());
		assertEquals(latestCompleted.getAlignmentBuffered(), latestCheckpointNode.get(JsonUtils.Keys.ALIGNMENT_BUFFERED).asLong());
		assertEquals(latestCompleted.getExternalPath(), latestCheckpointNode.get(JsonUtils.Keys.EXTERNAL_PATH).asText());

		JsonNode latestSavepointNode = latestNode.get(JsonUtils.Keys.SAVEPOINT);
		assertEquals(latestSavepoint.getCheckpointId(), latestSavepointNode.get(JsonUtils.Keys.ID).asLong());
		assertEquals(latestSavepoint.getTriggerTimestamp(), latestSavepointNode.get(JsonUtils.Keys.TRIGGER_TIMESTAMP).asLong());
		assertEquals(latestSavepoint.getLatestAckTimestamp(), latestSavepointNode.get(JsonUtils.Keys.LATEST_ACK_TIMESTAMP).asLong());
		assertEquals(latestSavepoint.getStateSize(), latestSavepointNode.get(JsonUtils.Keys.STATE_SIZE).asLong());
		assertEquals(latestSavepoint.getEndToEndDuration(), latestSavepointNode.get(JsonUtils.Keys.ETE_DURATION).asLong());
		assertEquals(latestSavepoint.getAlignmentBuffered(), latestSavepointNode.get(JsonUtils.Keys.ALIGNMENT_BUFFERED).asLong());
		assertEquals(latestSavepoint.getExternalPath(), latestSavepointNode.get(JsonUtils.Keys.EXTERNAL_PATH).asText());

		JsonNode latestFailedNode = latestNode.get(JsonUtils.Keys.FAILED);
		assertEquals(latestFailed.getCheckpointId(), latestFailedNode.get(JsonUtils.Keys.ID).asLong());
		assertEquals(latestFailed.getTriggerTimestamp(), latestFailedNode.get(JsonUtils.Keys.TRIGGER_TIMESTAMP).asLong());
		assertEquals(latestFailed.getLatestAckTimestamp(), latestFailedNode.get(JsonUtils.Keys.LATEST_ACK_TIMESTAMP).asLong());
		assertEquals(latestFailed.getStateSize(), latestFailedNode.get(JsonUtils.Keys.STATE_SIZE).asLong());
		assertEquals(latestFailed.getEndToEndDuration(), latestFailedNode.get(JsonUtils.Keys.ETE_DURATION).asLong());
		assertEquals(latestFailed.getAlignmentBuffered(), latestFailedNode.get(JsonUtils.Keys.ALIGNMENT_BUFFERED).asLong());
		assertEquals(latestFailed.getFailureTimestamp(), latestFailedNode.get(JsonUtils.Keys.FAILURE_TIMESTAMP).asLong());
		assertEquals(latestFailed.getFailureMessage(), latestFailedNode.get(JsonUtils.Keys.FAILURE_MESSAGE).asText());

		JsonNode latestRestoredNode = latestNode.get(JsonUtils.Keys.RESTORED);
		assertEquals(latestRestored.getCheckpointId(), latestRestoredNode.get(JsonUtils.Keys.ID).asLong());
		assertEquals(latestRestored.getRestoreTimestamp(), latestRestoredNode.get(JsonUtils.Keys.RESTORE_TIMESTAMP).asLong());
		assertEquals(CheckpointProperties.isSavepoint(latestRestored.getProperties()), latestRestoredNode.get(JsonUtils.Keys.IS_SAVEPOINT).asBoolean());
		assertEquals(latestRestored.getExternalPath(), latestRestoredNode.get(JsonUtils.Keys.EXTERNAL_PATH).asText());

		JsonNode historyNode = rootNode.get(JsonUtils.Keys.HISTORY);
		Iterator<JsonNode> it = historyNode.iterator();

		assertTrue(it.hasNext());
		JsonNode inProgressNode = it.next();

		assertEquals(inProgress.getCheckpointId(), inProgressNode.get(JsonUtils.Keys.ID).asLong());
		assertEquals(inProgress.getStatus().toString(), inProgressNode.get(JsonUtils.Keys.STATUS).asText());
		assertEquals(CheckpointProperties.isSavepoint(inProgress.getProperties()), inProgressNode.get(JsonUtils.Keys.IS_SAVEPOINT).asBoolean());
		assertEquals(inProgress.getTriggerTimestamp(), inProgressNode.get(JsonUtils.Keys.TRIGGER_TIMESTAMP).asLong());
		assertEquals(inProgress.getLatestAckTimestamp(), inProgressNode.get(JsonUtils.Keys.LATEST_ACK_TIMESTAMP).asLong());
		assertEquals(inProgress.getStateSize(), inProgressNode.get(JsonUtils.Keys.STATE_SIZE).asLong());
		assertEquals(inProgress.getEndToEndDuration(), inProgressNode.get(JsonUtils.Keys.ETE_DURATION).asLong());
		assertEquals(inProgress.getAlignmentBuffered(), inProgressNode.get(JsonUtils.Keys.ALIGNMENT_BUFFERED).asLong());
		assertEquals(inProgress.getNumberOfSubtasks(), inProgressNode.get(JsonUtils.Keys.NUM_SUBTASKS).asInt());
		assertEquals(inProgress.getNumberOfAcknowledgedSubtasks(), inProgressNode.get(JsonUtils.Keys.NUM_ACK_SUBTASKS).asInt());

		assertTrue(it.hasNext());
		JsonNode completedSavepointNode = it.next();

		assertEquals(completedSavepoint.getCheckpointId(), completedSavepointNode.get(JsonUtils.Keys.ID).asLong());
		assertEquals(completedSavepoint.getStatus().toString(), completedSavepointNode.get(JsonUtils.Keys.STATUS).asText());
		assertEquals(CheckpointProperties.isSavepoint(completedSavepoint.getProperties()), completedSavepointNode.get(JsonUtils.Keys.IS_SAVEPOINT).asBoolean());
		assertEquals(completedSavepoint.getTriggerTimestamp(), completedSavepointNode.get(JsonUtils.Keys.TRIGGER_TIMESTAMP).asLong());
		assertEquals(completedSavepoint.getLatestAckTimestamp(), completedSavepointNode.get(JsonUtils.Keys.LATEST_ACK_TIMESTAMP).asLong());
		assertEquals(completedSavepoint.getStateSize(), completedSavepointNode.get(JsonUtils.Keys.STATE_SIZE).asLong());
		assertEquals(completedSavepoint.getEndToEndDuration(), completedSavepointNode.get(JsonUtils.Keys.ETE_DURATION).asLong());
		assertEquals(completedSavepoint.getAlignmentBuffered(), completedSavepointNode.get(JsonUtils.Keys.ALIGNMENT_BUFFERED).asLong());
		assertEquals(completedSavepoint.getNumberOfSubtasks(), completedSavepointNode.get(JsonUtils.Keys.NUM_SUBTASKS).asInt());
		assertEquals(completedSavepoint.getNumberOfAcknowledgedSubtasks(), completedSavepointNode.get(JsonUtils.Keys.NUM_ACK_SUBTASKS).asInt());

		assertEquals(completedSavepoint.getExternalPath(), completedSavepointNode.get(JsonUtils.Keys.EXTERNAL_PATH).asText());
		assertEquals(completedSavepoint.isDiscarded(), completedSavepointNode.get(JsonUtils.Keys.DISCARDED).asBoolean());

		assertTrue(it.hasNext());
		JsonNode failedNode = it.next();

		assertEquals(failed.getCheckpointId(), failedNode.get(JsonUtils.Keys.ID).asLong());
		assertEquals(failed.getStatus().toString(), failedNode.get(JsonUtils.Keys.STATUS).asText());
		assertEquals(CheckpointProperties.isSavepoint(failed.getProperties()), failedNode.get(JsonUtils.Keys.IS_SAVEPOINT).asBoolean());
		assertEquals(failed.getTriggerTimestamp(), failedNode.get(JsonUtils.Keys.TRIGGER_TIMESTAMP).asLong());
		assertEquals(failed.getLatestAckTimestamp(), failedNode.get(JsonUtils.Keys.LATEST_ACK_TIMESTAMP).asLong());
		assertEquals(failed.getStateSize(), failedNode.get(JsonUtils.Keys.STATE_SIZE).asLong());
		assertEquals(failed.getEndToEndDuration(), failedNode.get(JsonUtils.Keys.ETE_DURATION).asLong());
		assertEquals(failed.getAlignmentBuffered(), failedNode.get(JsonUtils.Keys.ALIGNMENT_BUFFERED).asLong());
		assertEquals(failed.getNumberOfSubtasks(), failedNode.get(JsonUtils.Keys.NUM_SUBTASKS).asInt());
		assertEquals(failed.getNumberOfAcknowledgedSubtasks(), failedNode.get(JsonUtils.Keys.NUM_ACK_SUBTASKS).asInt());

		assertEquals(failed.getFailureTimestamp(), failedNode.get(JsonUtils.Keys.FAILURE_TIMESTAMP).asLong());
		assertEquals(failed.getFailureMessage(), failedNode.get(JsonUtils.Keys.FAILURE_MESSAGE).asText());

		assertFalse(it.hasNext());
	}
}
