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

package org.apache.flink.runtime.webmonitor.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.runtime.checkpoint.stats.OperatorCheckpointStats;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import org.junit.Test;
import scala.Option;

import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobVertexCheckpointsHandlerTest {

	@Test
	public void testNoCoordinator() throws Exception {
		JobVertexCheckpointsHandler handler = new JobVertexCheckpointsHandler(
				mock(ExecutionGraphHolder.class));

		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
		when(vertex.getCheckpointStats())
			.thenReturn(Option.<OperatorCheckpointStats>empty());

		String response = handler.handleRequest(vertex, Collections.<String, String>emptyMap());

		// Expecting empty response
		assertEquals("{}", response);
	}

	@Test
	public void testNoStats() throws Exception {
		JobVertexCheckpointsHandler handler = new JobVertexCheckpointsHandler(
				mock(ExecutionGraphHolder.class));

		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);

		// No stats
		when(vertex.getCheckpointStats())
				.thenReturn(Option.<OperatorCheckpointStats>empty());

		String response = handler.handleRequest(vertex, Collections.<String, String>emptyMap());

		// Expecting empty response
		assertEquals("{}", response);
	}

	@Test
	public void testStats() throws Exception {
		JobVertexCheckpointsHandler handler = new JobVertexCheckpointsHandler(
				mock(ExecutionGraphHolder.class));

		JobVertexID vertexId = new JobVertexID();

		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);

		when(vertex.getJobVertexId()).thenReturn(vertexId);

		long[][] subTaskStats = new long[][] {
				new long[] { 1, 10 },
				new long[] { 2, 9 },
				new long[] { 3, 8 },
				new long[] { 4, 7 },
				new long[] { 5, 6 },
				new long[] { 6, 5 },
				new long[] { 7, 4 },
				new long[] { 8, 3 },
				new long[] { 9, 2 },
				new long[] { 10, 1 } };

		// Stats
		OperatorCheckpointStats stats = new OperatorCheckpointStats(
				3, 6812, 2800, 1024, subTaskStats);

		when(vertex.getCheckpointStats())
				.thenReturn(Option.apply(stats));

		// Request stats
		String response = handler.handleRequest(vertex, Collections.<String, String>emptyMap());

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(response);

		// Operator stats
		long checkpointId = rootNode.get("id").asLong();
		long timestamp = rootNode.get("timestamp").asLong();
		long duration = rootNode.get("duration").asLong();
		long size = rootNode.get("size").asLong();
		long parallelism = rootNode.get("parallelism").asLong();

		assertEquals(stats.getCheckpointId(), checkpointId);
		assertEquals(stats.getTriggerTimestamp(), timestamp);
		assertEquals(stats.getDuration(), duration);
		assertEquals(stats.getStateSize(), size);
		assertEquals(subTaskStats.length, parallelism);

		// Sub task stats
		JsonNode subTasksNode = rootNode.get("subtasks");
		assertNotNull(subTasksNode);
		assertTrue(subTasksNode.isArray());

		Iterator<JsonNode> it = subTasksNode.elements();

		for (int i = 0; i < subTaskStats.length; i++) {
			JsonNode node = it.next();

			assertEquals(i, node.get("subtask").asInt());
			assertEquals(subTaskStats[i][0], node.get("duration").asLong());
			assertEquals(subTaskStats[i][1], node.get("size").asLong());
		}

		assertFalse(it.hasNext());
	}
}
