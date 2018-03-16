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
import org.apache.flink.runtime.checkpoint.CheckpointStatsHistory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.FailedCheckpointStats;
import org.apache.flink.runtime.checkpoint.PendingCheckpointStats;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointStatsCache;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the CheckpointStatsDetailsHandler.
 */
public class CheckpointStatsDetailsHandlerTest {

	@Test
	public void testArchiver() throws IOException {
		JsonArchivist archivist = new CheckpointStatsDetailsHandler.CheckpointStatsDetailsJsonArchivist();

		CompletedCheckpointStats completedCheckpoint = createCompletedCheckpoint();
		FailedCheckpointStats failedCheckpoint = createFailedCheckpoint();
		List<AbstractCheckpointStats> checkpoints = new ArrayList<>();
		checkpoints.add(failedCheckpoint);
		checkpoints.add(completedCheckpoint);

		CheckpointStatsHistory history = mock(CheckpointStatsHistory.class);
		when(history.getCheckpoints()).thenReturn(checkpoints);
		CheckpointStatsSnapshot snapshot = mock(CheckpointStatsSnapshot.class);
		when(snapshot.getHistory()).thenReturn(history);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getCheckpointStatsSnapshot()).thenReturn(snapshot);
		when(graph.getJobID()).thenReturn(new JobID());

		ObjectMapper mapper = new ObjectMapper();

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(graph);
		Assert.assertEquals(2, archives.size());

		Iterator<ArchivedJson> iterator = archives.iterator();
		ArchivedJson archive1 = iterator.next();
		Assert.assertEquals(
			"/jobs/" + graph.getJobID() + "/checkpoints/details/" + failedCheckpoint.getCheckpointId(),
			archive1.getPath());
		compareFailedCheckpoint(failedCheckpoint, mapper.readTree(archive1.getJson()));

		ArchivedJson archive2 = iterator.next();
		Assert.assertEquals(
			"/jobs/" + graph.getJobID() + "/checkpoints/details/" + completedCheckpoint.getCheckpointId(),
			archive2.getPath());
		compareCompletedCheckpoint(completedCheckpoint, mapper.readTree(archive2.getJson()));
	}

	@Test
	public void testGetPaths() {
		CheckpointStatsDetailsHandler handler = new CheckpointStatsDetailsHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/checkpoints/details/:checkpointid", paths[0]);
	}

	/**
	 * Tests request with illegal checkpoint ID param.
	 */
	@Test
	public void testIllegalCheckpointId() throws Exception {
		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		CheckpointStatsDetailsHandler handler = new CheckpointStatsDetailsHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
		Map<String, String> params = new HashMap<>();
		params.put("checkpointid", "illegal checkpoint");
		String json = handler.handleRequest(graph, params).get();

		assertEquals("{}", json);
	}

	/**
	 * Tests request with missing checkpoint ID param.
	 */
	@Test
	public void testNoCheckpointIdParam() throws Exception {
		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		CheckpointStatsDetailsHandler handler = new CheckpointStatsDetailsHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap()).get();

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
		when(graph.getCheckpointStatsSnapshot()).thenReturn(snapshot);

		CheckpointStatsDetailsHandler handler = new CheckpointStatsDetailsHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
		Map<String, String> params = new HashMap<>();
		params.put("checkpointid", "123");
		String json = handler.handleRequest(graph, params).get();

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
		when(checkpoint.getProperties()).thenReturn(
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
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
		assertEquals(checkpoint.getProperties().isSavepoint(), rootNode.get("is_savepoint").asBoolean());
		assertEquals(checkpoint.getTriggerTimestamp(), rootNode.get("trigger_timestamp").asLong());
		assertEquals(checkpoint.getLatestAckTimestamp(), rootNode.get("latest_ack_timestamp").asLong());
		assertEquals(checkpoint.getStateSize(), rootNode.get("state_size").asLong());
		assertEquals(checkpoint.getEndToEndDuration(), rootNode.get("end_to_end_duration").asLong());
		assertEquals(checkpoint.getAlignmentBuffered(), rootNode.get("alignment_buffered").asLong());
		assertEquals(checkpoint.getNumberOfSubtasks(), rootNode.get("num_subtasks").asInt());
		assertEquals(checkpoint.getNumberOfAcknowledgedSubtasks(), rootNode.get("num_acknowledged_subtasks").asInt());

		verifyTaskNodes(taskStats, rootNode);
	}

	/**
	 * Tests a checkpoint details request for a completed checkpoint.
	 */
	@Test
	public void testCheckpointDetailsRequestCompletedCheckpoint() throws Exception {
		CompletedCheckpointStats checkpoint = createCompletedCheckpoint();

		JsonNode rootNode = triggerRequest(checkpoint);

		compareCompletedCheckpoint(checkpoint, rootNode);

		verifyTaskNodes(checkpoint.getAllTaskStateStats(), rootNode);
	}

	/**
	 * Tests a checkpoint details request for a failed checkpoint.
	 */
	@Test
	public void testCheckpointDetailsRequestFailedCheckpoint() throws Exception {
		FailedCheckpointStats checkpoint = createFailedCheckpoint();

		JsonNode rootNode = triggerRequest(checkpoint);

		compareFailedCheckpoint(checkpoint, rootNode);

		verifyTaskNodes(checkpoint.getAllTaskStateStats(), rootNode);
	}

	// ------------------------------------------------------------------------

	private static CompletedCheckpointStats createCompletedCheckpoint() {
		CompletedCheckpointStats checkpoint = mock(CompletedCheckpointStats.class);
		when(checkpoint.getCheckpointId()).thenReturn(1818213L);
		when(checkpoint.getStatus()).thenReturn(CheckpointStatsStatus.COMPLETED);
		when(checkpoint.getProperties()).thenReturn(CheckpointProperties.forSavepoint());
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

		return checkpoint;
	}

	private static void compareCompletedCheckpoint(CompletedCheckpointStats checkpoint, JsonNode rootNode) {
		assertEquals(checkpoint.getCheckpointId(), rootNode.get("id").asLong());
		assertEquals(checkpoint.getStatus().toString(), rootNode.get("status").asText());
		assertEquals(checkpoint.getProperties().isSavepoint(), rootNode.get("is_savepoint").asBoolean());
		assertEquals(checkpoint.getTriggerTimestamp(), rootNode.get("trigger_timestamp").asLong());
		assertEquals(checkpoint.getLatestAckTimestamp(), rootNode.get("latest_ack_timestamp").asLong());
		assertEquals(checkpoint.getStateSize(), rootNode.get("state_size").asLong());
		assertEquals(checkpoint.getEndToEndDuration(), rootNode.get("end_to_end_duration").asLong());
		assertEquals(checkpoint.getAlignmentBuffered(), rootNode.get("alignment_buffered").asLong());
		assertEquals(checkpoint.isDiscarded(), rootNode.get("discarded").asBoolean());
		assertEquals(checkpoint.getExternalPath(), rootNode.get("external_path").asText());
		assertEquals(checkpoint.getNumberOfSubtasks(), rootNode.get("num_subtasks").asInt());
		assertEquals(checkpoint.getNumberOfAcknowledgedSubtasks(), rootNode.get("num_acknowledged_subtasks").asInt());
	}

	private static FailedCheckpointStats createFailedCheckpoint() {
		FailedCheckpointStats checkpoint = mock(FailedCheckpointStats.class);
		when(checkpoint.getCheckpointId()).thenReturn(1818214L);
		when(checkpoint.getStatus()).thenReturn(CheckpointStatsStatus.FAILED);
		when(checkpoint.getProperties()).thenReturn(CheckpointProperties.forSavepoint());
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

		return checkpoint;
	}

	private static void compareFailedCheckpoint(FailedCheckpointStats checkpoint, JsonNode rootNode) {
		assertEquals(checkpoint.getCheckpointId(), rootNode.get("id").asLong());
		assertEquals(checkpoint.getStatus().toString(), rootNode.get("status").asText());
		assertEquals(checkpoint.getProperties().isSavepoint(), rootNode.get("is_savepoint").asBoolean());
		assertEquals(checkpoint.getTriggerTimestamp(), rootNode.get("trigger_timestamp").asLong());
		assertEquals(checkpoint.getLatestAckTimestamp(), rootNode.get("latest_ack_timestamp").asLong());
		assertEquals(checkpoint.getStateSize(), rootNode.get("state_size").asLong());
		assertEquals(checkpoint.getEndToEndDuration(), rootNode.get("end_to_end_duration").asLong());
		assertEquals(checkpoint.getAlignmentBuffered(), rootNode.get("alignment_buffered").asLong());
		assertEquals(checkpoint.getFailureTimestamp(), rootNode.get("failure_timestamp").asLong());
		assertEquals(checkpoint.getFailureMessage(), rootNode.get("failure_message").asText());
		assertEquals(checkpoint.getNumberOfSubtasks(), rootNode.get("num_subtasks").asInt());
		assertEquals(checkpoint.getNumberOfAcknowledgedSubtasks(), rootNode.get("num_acknowledged_subtasks").asInt());
	}

	private static JsonNode triggerRequest(AbstractCheckpointStats checkpoint) throws Exception {
		CheckpointStatsHistory history = mock(CheckpointStatsHistory.class);
		when(history.getCheckpointById(anyLong())).thenReturn(checkpoint);
		CheckpointStatsSnapshot snapshot = mock(CheckpointStatsSnapshot.class);
		when(snapshot.getHistory()).thenReturn(history);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getCheckpointStatsSnapshot()).thenReturn(snapshot);

		CheckpointStatsDetailsHandler handler = new CheckpointStatsDetailsHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
		Map<String, String> params = new HashMap<>();
		params.put("checkpointid", "123");
		String json = handler.handleRequest(graph, params).get();

		ObjectMapper mapper = new ObjectMapper();
		return mapper.readTree(json);
	}

	private static void verifyTaskNodes(Collection<TaskStateStats> tasks, JsonNode parentNode) {
		for (TaskStateStats task : tasks) {
			long duration = ThreadLocalRandom.current().nextInt(128);

			JsonNode taskNode = parentNode.get("tasks").get(task.getJobVertexId().toString());
			assertEquals(task.getLatestAckTimestamp(), taskNode.get("latest_ack_timestamp").asLong());
			assertEquals(task.getStateSize(), taskNode.get("state_size").asLong());
			assertEquals(task.getEndToEndDuration(task.getLatestAckTimestamp() - duration), taskNode.get("end_to_end_duration").asLong());
			assertEquals(task.getAlignmentBuffered(), taskNode.get("alignment_buffered").asLong());
			assertEquals(task.getNumberOfSubtasks(), taskNode.get("num_subtasks").asInt());
			assertEquals(task.getNumberOfAcknowledgedSubtasks(), taskNode.get("num_acknowledged_subtasks").asInt());
		}
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
