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
import org.apache.flink.runtime.checkpoint.CheckpointStatsHistory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.FailedCheckpointStats;
import org.apache.flink.runtime.checkpoint.PendingCheckpointStats;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CheckpointStatsDetailsHandlerTest {

	/**
	 * Tests request with illegal checkpoint ID param.
	 */
	@Test
	public void testIllegalCheckpointId() throws Exception {
		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		CheckpointStatsDetailsHandler handler = new CheckpointStatsDetailsHandler(mock(ExecutionGraphHolder.class), new CheckpointStatsCache(0));
		Map<String, String> params = new HashMap<>();
		params.put("checkpointid", "illegal checkpoint");
		String json = handler.handleRequest(graph, params);

		assertEquals("{}", json);
	}

	/**
	 * Tests request with missing checkpoint ID param.
	 */
	@Test
	public void testNoCheckpointIdParam() throws Exception {
		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		CheckpointStatsDetailsHandler handler = new CheckpointStatsDetailsHandler(mock(ExecutionGraphHolder.class), new CheckpointStatsCache(0));
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap());

		assertEquals("{}", json);
	}

	/**
	 * Test lookup of not existing checkpoint in history.
	 */
	@Test
	public void testCheckpointNotFound() throws Exception {
		CheckpointStatsHistory history = mock(CheckpointStatsHistory.class);
		when(history.getCheckpointById(anyLong())).thenReturn(null); // not found

		CheckpointStatsSnapshot snapshot = mock(CheckpointStatsSnapshot.class);
		when(snapshot.getHistory()).thenReturn(history);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);
		when(graph.getCheckpointStatsTracker()).thenReturn(tracker);
		when(tracker.createSnapshot()).thenReturn(snapshot);

		CheckpointStatsDetailsHandler handler = new CheckpointStatsDetailsHandler(mock(ExecutionGraphHolder.class), new CheckpointStatsCache(0));
		Map<String, String> params = new HashMap<>();
		params.put("checkpointid", "123");
		String json = handler.handleRequest(graph, params);

		assertEquals("{}", json);
		verify(history, times(1)).getCheckpointById(anyLong());
	}

	/**
	 * Tests a checkpoint details request for an in progress checkpoint.
	 */
	@Test
	public void testCheckpointDetailsRequestInProgressCheckpoint() throws Exception {
		PendingCheckpointStats checkpoint = mock(PendingCheckpointStats.class);
		when(checkpoint.getCheckpointId()).thenReturn(1992139L);
		when(checkpoint.getStatus()).thenReturn(CheckpointStatsStatus.IN_PROGRESS);
		when(checkpoint.getProperties()).thenReturn(CheckpointProperties.forStandardCheckpoint());
		when(checkpoint.getTriggerTimestamp()).thenReturn(1919191900L);
		when(checkpoint.getLatestAckTimestamp()).thenReturn(1977791901L);
		when(checkpoint.getStateSize()).thenReturn(111939272822L);
		when(checkpoint.getEndToEndDuration()).thenReturn(121191L);
		when(checkpoint.getAlignmentBuffered()).thenReturn(1L);
		when(checkpoint.getNumberOfSubtasks()).thenReturn(501);
		when(checkpoint.getNumberOfAcknowledgedSubtasks()).thenReturn(101);

		List<TaskStateStats> taskStats = new ArrayList<>();
		TaskStateStats task1 = createTaskStateStats();
		TaskStateStats task2 = createTaskStateStats();
		taskStats.add(task1);
		taskStats.add(task2);

		when(checkpoint.getAllTaskStateStats()).thenReturn(taskStats);

		JsonNode rootNode = triggerRequest(checkpoint);

		assertEquals(checkpoint.getCheckpointId(), rootNode.get("id").asLong());
		assertEquals(checkpoint.getStatus().toString(), rootNode.get("status").asText());
		assertEquals(CheckpointProperties.isSavepoint(checkpoint.getProperties()), rootNode.get("is_savepoint").asBoolean());
		assertEquals(checkpoint.getTriggerTimestamp(), rootNode.get("trigger_timestamp").asLong());
		assertEquals(checkpoint.getLatestAckTimestamp(), rootNode.get("latest_ack_timestamp").asLong());
		assertEquals(checkpoint.getStateSize(), rootNode.get("state_size").asLong());
		assertEquals(checkpoint.getEndToEndDuration(), rootNode.get("end_to_end_duration").asLong());
		assertEquals(checkpoint.getAlignmentBuffered(), rootNode.get("alignment_buffered").asLong());
		assertEquals(checkpoint.getNumberOfSubtasks(), rootNode.get("num_subtasks").asInt());
		assertEquals(checkpoint.getNumberOfAcknowledgedSubtasks(), rootNode.get("num_acknowledged_subtasks").asInt());

		verifyTaskNode(task1, rootNode);
		verifyTaskNode(task2, rootNode);
	}

	/**
	 * Tests a checkpoint details request for a completed checkpoint.
	 */
	@Test
	public void testCheckpointDetailsRequestCompletedCheckpoint() throws Exception {
		CompletedCheckpointStats checkpoint = mock(CompletedCheckpointStats.class);
		when(checkpoint.getCheckpointId()).thenReturn(1818213L);
		when(checkpoint.getStatus()).thenReturn(CheckpointStatsStatus.COMPLETED);
		when(checkpoint.getProperties()).thenReturn(CheckpointProperties.forStandardSavepoint());
		when(checkpoint.getTriggerTimestamp()).thenReturn(1818L);
		when(checkpoint.getLatestAckTimestamp()).thenReturn(11029222L);
		when(checkpoint.getStateSize()).thenReturn(925281L);
		when(checkpoint.getEndToEndDuration()).thenReturn(181819L);
		when(checkpoint.getAlignmentBuffered()).thenReturn(1010198L);
		when(checkpoint.getNumberOfSubtasks()).thenReturn(181271);
		when(checkpoint.getNumberOfAcknowledgedSubtasks()).thenReturn(29821);
		when(checkpoint.isDiscarded()).thenReturn(true);
		when(checkpoint.getExternalPath()).thenReturn("checkpoint-external-path");

		List<TaskStateStats> taskStats = new ArrayList<>();
		TaskStateStats task1 = createTaskStateStats();
		TaskStateStats task2 = createTaskStateStats();
		taskStats.add(task1);
		taskStats.add(task2);

		when(checkpoint.getAllTaskStateStats()).thenReturn(taskStats);

		JsonNode rootNode = triggerRequest(checkpoint);

		assertEquals(checkpoint.getCheckpointId(), rootNode.get("id").asLong());
		assertEquals(checkpoint.getStatus().toString(), rootNode.get("status").asText());
		assertEquals(CheckpointProperties.isSavepoint(checkpoint.getProperties()), rootNode.get("is_savepoint").asBoolean());
		assertEquals(checkpoint.getTriggerTimestamp(), rootNode.get("trigger_timestamp").asLong());
		assertEquals(checkpoint.getLatestAckTimestamp(), rootNode.get("latest_ack_timestamp").asLong());
		assertEquals(checkpoint.getStateSize(), rootNode.get("state_size").asLong());
		assertEquals(checkpoint.getEndToEndDuration(), rootNode.get("end_to_end_duration").asLong());
		assertEquals(checkpoint.getAlignmentBuffered(), rootNode.get("alignment_buffered").asLong());
		assertEquals(checkpoint.isDiscarded(), rootNode.get("discarded").asBoolean());
		assertEquals(checkpoint.getExternalPath(), rootNode.get("external_path").asText());
		assertEquals(checkpoint.getNumberOfSubtasks(), rootNode.get("num_subtasks").asInt());
		assertEquals(checkpoint.getNumberOfAcknowledgedSubtasks(), rootNode.get("num_acknowledged_subtasks").asInt());

		verifyTaskNode(task1, rootNode);
		verifyTaskNode(task2, rootNode);
	}

	/**
	 * Tests a checkpoint details request for a failed checkpoint.
	 */
	@Test
	public void testCheckpointDetailsRequestFailedCheckpoint() throws Exception {
		FailedCheckpointStats checkpoint = mock(FailedCheckpointStats.class);
		when(checkpoint.getCheckpointId()).thenReturn(1818213L);
		when(checkpoint.getStatus()).thenReturn(CheckpointStatsStatus.FAILED);
		when(checkpoint.getProperties()).thenReturn(CheckpointProperties.forStandardSavepoint());
		when(checkpoint.getTriggerTimestamp()).thenReturn(1818L);
		when(checkpoint.getLatestAckTimestamp()).thenReturn(11029222L);
		when(checkpoint.getStateSize()).thenReturn(925281L);
		when(checkpoint.getEndToEndDuration()).thenReturn(181819L);
		when(checkpoint.getAlignmentBuffered()).thenReturn(1010198L);
		when(checkpoint.getNumberOfSubtasks()).thenReturn(181271);
		when(checkpoint.getNumberOfAcknowledgedSubtasks()).thenReturn(29821);
		when(checkpoint.getFailureTimestamp()).thenReturn(123012890312093L);
		when(checkpoint.getFailureMessage()).thenReturn("failure-message");

		List<TaskStateStats> taskStats = new ArrayList<>();
		TaskStateStats task1 = createTaskStateStats();
		TaskStateStats task2 = createTaskStateStats();
		taskStats.add(task1);
		taskStats.add(task2);

		when(checkpoint.getAllTaskStateStats()).thenReturn(taskStats);

		JsonNode rootNode = triggerRequest(checkpoint);

		assertEquals(checkpoint.getCheckpointId(), rootNode.get("id").asLong());
		assertEquals(checkpoint.getStatus().toString(), rootNode.get("status").asText());
		assertEquals(CheckpointProperties.isSavepoint(checkpoint.getProperties()), rootNode.get("is_savepoint").asBoolean());
		assertEquals(checkpoint.getTriggerTimestamp(), rootNode.get("trigger_timestamp").asLong());
		assertEquals(checkpoint.getLatestAckTimestamp(), rootNode.get("latest_ack_timestamp").asLong());
		assertEquals(checkpoint.getStateSize(), rootNode.get("state_size").asLong());
		assertEquals(checkpoint.getEndToEndDuration(), rootNode.get("end_to_end_duration").asLong());
		assertEquals(checkpoint.getAlignmentBuffered(), rootNode.get("alignment_buffered").asLong());
		assertEquals(checkpoint.getFailureTimestamp(), rootNode.get("failure_timestamp").asLong());
		assertEquals(checkpoint.getFailureMessage(), rootNode.get("failure_message").asText());
		assertEquals(checkpoint.getNumberOfSubtasks(), rootNode.get("num_subtasks").asInt());
		assertEquals(checkpoint.getNumberOfAcknowledgedSubtasks(), rootNode.get("num_acknowledged_subtasks").asInt());

		verifyTaskNode(task1, rootNode);
		verifyTaskNode(task2, rootNode);
	}

	// ------------------------------------------------------------------------

	static JsonNode triggerRequest(AbstractCheckpointStats checkpoint) throws Exception {
		CheckpointStatsHistory history = mock(CheckpointStatsHistory.class);
		when(history.getCheckpointById(anyLong())).thenReturn(checkpoint);
		CheckpointStatsSnapshot snapshot = mock(CheckpointStatsSnapshot.class);
		when(snapshot.getHistory()).thenReturn(history);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);
		when(graph.getCheckpointStatsTracker()).thenReturn(tracker);
		when(tracker.createSnapshot()).thenReturn(snapshot);

		CheckpointStatsDetailsHandler handler = new CheckpointStatsDetailsHandler(mock(ExecutionGraphHolder.class), new CheckpointStatsCache(0));
		Map<String, String> params = new HashMap<>();
		params.put("checkpointid", "123");
		String json = handler.handleRequest(graph, params);

		ObjectMapper mapper = new ObjectMapper();
		return mapper.readTree(json);
	}

	static void verifyTaskNode(TaskStateStats task, JsonNode parentNode) {
		long duration = ThreadLocalRandom.current().nextInt(128);

		JsonNode taskNode = parentNode.get("tasks").get(task.getJobVertexId().toString());
		assertEquals(task.getLatestAckTimestamp(), taskNode.get("latest_ack_timestamp").asLong());
		assertEquals(task.getStateSize(), taskNode.get("state_size").asLong());
		assertEquals(task.getEndToEndDuration(task.getLatestAckTimestamp() - duration), taskNode.get("end_to_end_duration").asLong());
		assertEquals(task.getAlignmentBuffered(), taskNode.get("alignment_buffered").asLong());
		assertEquals(task.getNumberOfSubtasks(), taskNode.get("num_subtasks").asInt());
		assertEquals(task.getNumberOfAcknowledgedSubtasks(), taskNode.get("num_acknowledged_subtasks").asInt());
	}

	private static TaskStateStats createTaskStateStats() {
		ThreadLocalRandom rand = ThreadLocalRandom.current();

		TaskStateStats task = mock(TaskStateStats.class);
		when(task.getJobVertexId()).thenReturn(new JobVertexID());
		when(task.getLatestAckTimestamp()).thenReturn(rand.nextLong(1024) + 1);
		when(task.getStateSize()).thenReturn(rand.nextLong(1024) + 1);
		when(task.getEndToEndDuration(anyLong())).thenReturn(rand.nextLong(1024) + 1);
		when(task.getAlignmentBuffered()).thenReturn(rand.nextLong(1024) + 1);
		when(task.getNumberOfSubtasks()).thenReturn(rand.nextInt(1024) + 1);
		when(task.getNumberOfAcknowledgedSubtasks()).thenReturn(rand.nextInt(1024) + 1);
		return task;
	}
}
