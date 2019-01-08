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
import org.apache.flink.runtime.checkpoint.CheckpointStatsHistory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.checkpoint.MinMaxAvgStats;
import org.apache.flink.runtime.checkpoint.PendingCheckpointStats;
import org.apache.flink.runtime.checkpoint.SubtaskStateStats;
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the CheckpointStatsSubtaskDetailsHandler.
 */
public class CheckpointStatsSubtaskDetailsHandlerTest {

	@Test
	public void testArchiver() throws Exception {
		JsonArchivist archivist = new CheckpointStatsDetailsSubtasksHandler.CheckpointStatsDetailsSubtasksJsonArchivist();
		ObjectMapper mapper = new ObjectMapper();

		PendingCheckpointStats checkpoint = mock(PendingCheckpointStats.class);
		when(checkpoint.getCheckpointId()).thenReturn(1992139L);
		when(checkpoint.getStatus()).thenReturn(CheckpointStatsStatus.IN_PROGRESS);
		when(checkpoint.getTriggerTimestamp()).thenReturn(0L); // ack timestamp = duration

		TaskStateStats task = createTaskStateStats(1237);
		when(checkpoint.getAllTaskStateStats()).thenReturn(Collections.singletonList(task));

		CheckpointStatsHistory history = mock(CheckpointStatsHistory.class);
		when(history.getCheckpoints()).thenReturn(Collections.<AbstractCheckpointStats>singletonList(checkpoint));
		CheckpointStatsSnapshot snapshot = mock(CheckpointStatsSnapshot.class);
		when(snapshot.getHistory()).thenReturn(history);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getCheckpointStatsSnapshot()).thenReturn(snapshot);
		when(graph.getJobID()).thenReturn(new JobID());

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(graph);
		Assert.assertEquals(1, archives.size());

		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals(
			"/jobs/" + graph.getJobID() + "/checkpoints/details/" + checkpoint.getCheckpointId() + "/subtasks/" + task.getJobVertexId(),
			archive.getPath());
		JsonNode rootNode = mapper.readTree(archive.getJson());
		assertEquals(checkpoint.getCheckpointId(), rootNode.get("id").asLong());
		assertEquals(checkpoint.getStatus().toString(), rootNode.get("status").asText());

		verifyTaskNode(rootNode, task, checkpoint.getTriggerTimestamp());
	}

	@Test
	public void testGetPaths() {
		CheckpointStatsDetailsSubtasksHandler handler = new CheckpointStatsDetailsSubtasksHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/checkpoints/details/:checkpointid/subtasks/:vertexid", paths[0]);
	}

	/**
	 * Tests a subtask details request.
	 */
	@Test
	public void testSubtaskRequest() throws Exception {
		PendingCheckpointStats checkpoint = mock(PendingCheckpointStats.class);
		when(checkpoint.getCheckpointId()).thenReturn(1992139L);
		when(checkpoint.getStatus()).thenReturn(CheckpointStatsStatus.IN_PROGRESS);
		when(checkpoint.getTriggerTimestamp()).thenReturn(0L); // ack timestamp = duration

		TaskStateStats task = createTaskStateStats(1237);
		when(checkpoint.getTaskStateStats(any(JobVertexID.class))).thenReturn(task);

		JsonNode rootNode = triggerRequest(checkpoint);
		assertEquals(checkpoint.getCheckpointId(), rootNode.get("id").asLong());
		assertEquals(checkpoint.getStatus().toString(), rootNode.get("status").asText());

		verifyTaskNode(rootNode, task, checkpoint.getTriggerTimestamp());
	}

	/**
	 * Tests a subtask details request.
	 */
	@Test
	public void testSubtaskRequestNoSummary() throws Exception {
		PendingCheckpointStats checkpoint = mock(PendingCheckpointStats.class);
		when(checkpoint.getCheckpointId()).thenReturn(1992139L);
		when(checkpoint.getStatus()).thenReturn(CheckpointStatsStatus.IN_PROGRESS);
		when(checkpoint.getTriggerTimestamp()).thenReturn(0L); // ack timestamp = duration

		TaskStateStats task = createTaskStateStats(0); // no acknowledged
		when(checkpoint.getTaskStateStats(any(JobVertexID.class))).thenReturn(task);

		JsonNode rootNode = triggerRequest(checkpoint);
		assertNull(rootNode.get("summary"));
	}

	/**
	 * Tests request with illegal checkpoint ID param.
	 */
	@Test
	public void testIllegalCheckpointId() throws Exception {
		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		CheckpointStatsDetailsSubtasksHandler handler = new CheckpointStatsDetailsSubtasksHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
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
		CheckpointStatsDetailsSubtasksHandler handler = new CheckpointStatsDetailsSubtasksHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
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

		CheckpointStatsDetailsSubtasksHandler handler = new CheckpointStatsDetailsSubtasksHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
		Map<String, String> params = new HashMap<>();
		params.put("checkpointid", "123");
		params.put("vertexid", new JobVertexID().toString());
		String json = handler.handleRequest(graph, params).get();

		assertEquals("{}", json);
		verify(history, times(1)).getCheckpointById(anyLong());
	}

	/**
	 * Tests request with illegal job vertex ID param.
	 */
	@Test
	public void testIllegalJobVertexIdParam() throws Exception {
		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		CheckpointStatsDetailsSubtasksHandler handler = new CheckpointStatsDetailsSubtasksHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
		Map<String, String> params = new HashMap<>();
		params.put("checkpointid", "1");
		params.put("vertexid", "illegal vertex id");
		String json = handler.handleRequest(graph, params).get();

		assertEquals("{}", json);
	}

	/**
	 * Tests request with missing job vertex ID param.
	 */
	@Test
	public void testNoJobVertexIdParam() throws Exception {
		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		CheckpointStatsDetailsSubtasksHandler handler = new CheckpointStatsDetailsSubtasksHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
		Map<String, String> params = new HashMap<>();
		params.put("checkpointid", "1");
		String json = handler.handleRequest(graph, params).get();

		assertEquals("{}", json);
	}

	/**
	 * Test lookup of not existing job vertex ID in checkpoint.
	 */
	@Test
	public void testJobVertexNotFound() throws Exception {
		PendingCheckpointStats inProgress = mock(PendingCheckpointStats.class);
		when(inProgress.getTaskStateStats(any(JobVertexID.class))).thenReturn(null); // not found
		CheckpointStatsHistory history = mock(CheckpointStatsHistory.class);
		when(history.getCheckpointById(anyLong())).thenReturn(inProgress);

		CheckpointStatsSnapshot snapshot = mock(CheckpointStatsSnapshot.class);
		when(snapshot.getHistory()).thenReturn(history);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getCheckpointStatsSnapshot()).thenReturn(snapshot);

		CheckpointStatsDetailsSubtasksHandler handler = new CheckpointStatsDetailsSubtasksHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
		Map<String, String> params = new HashMap<>();
		params.put("checkpointid", "123");
		params.put("vertexid", new JobVertexID().toString());
		String json = handler.handleRequest(graph, params).get();

		assertEquals("{}", json);
		verify(inProgress, times(1)).getTaskStateStats(any(JobVertexID.class));
	}

	// ------------------------------------------------------------------------

	private static JsonNode triggerRequest(AbstractCheckpointStats checkpoint) throws Exception {
		CheckpointStatsHistory history = mock(CheckpointStatsHistory.class);
		when(history.getCheckpointById(anyLong())).thenReturn(checkpoint);
		CheckpointStatsSnapshot snapshot = mock(CheckpointStatsSnapshot.class);
		when(snapshot.getHistory()).thenReturn(history);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getCheckpointStatsSnapshot()).thenReturn(snapshot);

		CheckpointStatsDetailsSubtasksHandler handler = new CheckpointStatsDetailsSubtasksHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), new CheckpointStatsCache(0));
		Map<String, String> params = new HashMap<>();
		params.put("checkpointid", "123");
		params.put("vertexid", new JobVertexID().toString());
		String json = handler.handleRequest(graph, params).get();

		ObjectMapper mapper = new ObjectMapper();
		return mapper.readTree(json);
	}

	private static TaskStateStats createTaskStateStats(int numAcknowledged) {
		ThreadLocalRandom rand = ThreadLocalRandom.current();

		TaskStateStats task = mock(TaskStateStats.class);
		when(task.getJobVertexId()).thenReturn(new JobVertexID());
		when(task.getLatestAckTimestamp()).thenReturn(rand.nextLong(1024) + 1);
		when(task.getStateSize()).thenReturn(rand.nextLong(1024) + 1);
		when(task.getEndToEndDuration(anyLong())).thenReturn(rand.nextLong(1024) + 1);
		when(task.getAlignmentBuffered()).thenReturn(rand.nextLong(1024) + 1);
		when(task.getNumberOfSubtasks()).thenReturn(rand.nextInt(1024) + 1);
		when(task.getNumberOfAcknowledgedSubtasks()).thenReturn(numAcknowledged);

		TaskStateStats.TaskStateStatsSummary summary = mock(TaskStateStats.TaskStateStatsSummary.class);

		doReturn(createMinMaxAvgStats(rand)).when(summary).getStateSizeStats();
		doReturn(createMinMaxAvgStats(rand)).when(summary).getAckTimestampStats();
		doReturn(createMinMaxAvgStats(rand)).when(summary).getAlignmentBufferedStats();
		doReturn(createMinMaxAvgStats(rand)).when(summary).getAlignmentDurationStats();
		doReturn(createMinMaxAvgStats(rand)).when(summary).getSyncCheckpointDurationStats();
		doReturn(createMinMaxAvgStats(rand)).when(summary).getAsyncCheckpointDurationStats();

		when(task.getSummaryStats()).thenReturn(summary);

		SubtaskStateStats[] subtasks = new SubtaskStateStats[3];
		subtasks[0] = createSubtaskStats(0, rand);
		subtasks[1] = createSubtaskStats(1, rand);
		subtasks[2] = null;

		when(task.getSubtaskStats()).thenReturn(subtasks);

		return task;
	}

	private static void verifyTaskNode(JsonNode taskNode, TaskStateStats task, long triggerTimestamp) {
		long duration = ThreadLocalRandom.current().nextInt(128);

		assertEquals(task.getLatestAckTimestamp(), taskNode.get("latest_ack_timestamp").asLong());
		assertEquals(task.getStateSize(), taskNode.get("state_size").asLong());
		assertEquals(task.getEndToEndDuration(task.getLatestAckTimestamp() - duration), taskNode.get("end_to_end_duration").asLong());
		assertEquals(task.getAlignmentBuffered(), taskNode.get("alignment_buffered").asLong());
		assertEquals(task.getNumberOfSubtasks(), taskNode.get("num_subtasks").asInt());
		assertEquals(task.getNumberOfAcknowledgedSubtasks(), taskNode.get("num_acknowledged_subtasks").asInt());

		TaskStateStats.TaskStateStatsSummary summary = task.getSummaryStats();
		verifyMinMaxAvgStats(summary.getStateSizeStats(), taskNode.get("summary").get("state_size"));
		verifyMinMaxAvgStats(summary.getSyncCheckpointDurationStats(), taskNode.get("summary").get("checkpoint_duration").get("sync"));
		verifyMinMaxAvgStats(summary.getAsyncCheckpointDurationStats(), taskNode.get("summary").get("checkpoint_duration").get("async"));
		verifyMinMaxAvgStats(summary.getAlignmentBufferedStats(), taskNode.get("summary").get("alignment").get("buffered"));
		verifyMinMaxAvgStats(summary.getAlignmentDurationStats(), taskNode.get("summary").get("alignment").get("duration"));

		JsonNode endToEndDurationNode = taskNode.get("summary").get("end_to_end_duration");
		assertEquals(summary.getAckTimestampStats().getMinimum() - triggerTimestamp, endToEndDurationNode.get("min").asLong());
		assertEquals(summary.getAckTimestampStats().getMaximum() - triggerTimestamp, endToEndDurationNode.get("max").asLong());
		assertEquals((long) summary.getAckTimestampStats().getAverage() - triggerTimestamp, endToEndDurationNode.get("avg").asLong());

		SubtaskStateStats[] subtasks = task.getSubtaskStats();
		Iterator<JsonNode> it = taskNode.get("subtasks").iterator();

		assertTrue(it.hasNext());
		verifySubtaskStats(it.next(), 0, subtasks[0]);

		assertTrue(it.hasNext());
		verifySubtaskStats(it.next(), 1, subtasks[1]);

		assertTrue(it.hasNext());
		verifySubtaskStats(it.next(), 2, subtasks[2]);

		assertFalse(it.hasNext());
	}

	private static SubtaskStateStats createSubtaskStats(int index, ThreadLocalRandom rand) {
		SubtaskStateStats subtask = mock(SubtaskStateStats.class);
		when(subtask.getSubtaskIndex()).thenReturn(index);
		when(subtask.getAckTimestamp()).thenReturn(rand.nextLong(1024));
		when(subtask.getAlignmentBuffered()).thenReturn(rand.nextLong(1024));
		when(subtask.getAlignmentDuration()).thenReturn(rand.nextLong(1024));
		when(subtask.getSyncCheckpointDuration()).thenReturn(rand.nextLong(1024));
		when(subtask.getAsyncCheckpointDuration()).thenReturn(rand.nextLong(1024));
		when(subtask.getAckTimestamp()).thenReturn(rand.nextLong(1024));
		when(subtask.getStateSize()).thenReturn(rand.nextLong(1024));
		when(subtask.getEndToEndDuration(anyLong())).thenReturn(rand.nextLong(1024));
		return subtask;
	}

	private static void verifySubtaskStats(JsonNode subtaskNode, int index, SubtaskStateStats subtask) {
		if (subtask == null) {
			assertEquals(index, subtaskNode.get("index").asInt());
			assertEquals("pending_or_failed", subtaskNode.get("status").asText());
		} else {
			assertEquals(subtask.getSubtaskIndex(), subtaskNode.get("index").asInt());
			assertEquals("completed", subtaskNode.get("status").asText());
			assertEquals(subtask.getAckTimestamp(), subtaskNode.get("ack_timestamp").asLong());
			assertEquals(subtask.getEndToEndDuration(0), subtaskNode.get("end_to_end_duration").asLong());
			assertEquals(subtask.getStateSize(), subtaskNode.get("state_size").asLong());
			assertEquals(subtask.getSyncCheckpointDuration(), subtaskNode.get("checkpoint").get("sync").asLong());
			assertEquals(subtask.getAsyncCheckpointDuration(), subtaskNode.get("checkpoint").get("async").asLong());
			assertEquals(subtask.getAlignmentBuffered(), subtaskNode.get("alignment").get("buffered").asLong());
			assertEquals(subtask.getAlignmentDuration(), subtaskNode.get("alignment").get("duration").asLong());
		}
	}

	private static MinMaxAvgStats createMinMaxAvgStats(ThreadLocalRandom rand) {
		MinMaxAvgStats mma = mock(MinMaxAvgStats.class);
		when(mma.getMinimum()).thenReturn(rand.nextLong(1024));
		when(mma.getMaximum()).thenReturn(rand.nextLong(1024));
		when(mma.getAverage()).thenReturn(rand.nextLong(1024));

		return mma;
	}

	private static void verifyMinMaxAvgStats(MinMaxAvgStats expected, JsonNode node) {
		assertEquals(expected.getMinimum(), node.get("min").asLong());
		assertEquals(expected.getMaximum(), node.get("max").asLong());
		assertEquals(expected.getAverage(), node.get("avg").asLong());
	}

}
