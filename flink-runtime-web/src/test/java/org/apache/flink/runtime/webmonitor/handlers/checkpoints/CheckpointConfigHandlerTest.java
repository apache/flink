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
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckpointConfigHandlerTest {

	/**
	 * Tests a simple config.
	 */
	@Test
	public void testSimpleConfig() throws Exception {
		long interval = 18231823L;
		long timeout = 996979L;
		long minPause = 119191919L;
		int maxConcurrent = 12929329;
		ExternalizedCheckpointSettings externalized = ExternalizedCheckpointSettings.none();

		JobSnapshottingSettings settings = new JobSnapshottingSettings(
			Collections.<JobVertexID>emptyList(),
			Collections.<JobVertexID>emptyList(),
			Collections.<JobVertexID>emptyList(),
			interval,
			timeout,
			minPause,
			maxConcurrent,
			externalized,
			true);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getJobSnapshottingSettings()).thenReturn(settings);

		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphHolder.class));
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap());

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(json);

		assertEquals("exactly_once", rootNode.get("mode").asText());
		assertEquals(interval, rootNode.get("interval").asLong());
		assertEquals(timeout, rootNode.get("timeout").asLong());
		assertEquals(minPause, rootNode.get("min_pause").asLong());
		assertEquals(maxConcurrent, rootNode.get("max_concurrent").asInt());

		JsonNode externalizedNode = rootNode.get("externalization");
		assertNotNull(externalizedNode);
		assertEquals(false, externalizedNode.get("enabled").asBoolean());
	}

	/**
	 * Tests the that the isExactlyOnce flag is respected.
	 */
	@Test
	public void testAtLeastOnce() throws Exception {
		JobSnapshottingSettings settings = new JobSnapshottingSettings(
			Collections.<JobVertexID>emptyList(),
			Collections.<JobVertexID>emptyList(),
			Collections.<JobVertexID>emptyList(),
			996979L,
			1818L,
			1212L,
			12,
			ExternalizedCheckpointSettings.none(),
			false); // at least once

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getJobSnapshottingSettings()).thenReturn(settings);

		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphHolder.class));
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap());

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(json);

		assertEquals("at_least_once", rootNode.get("mode").asText());
	}

	/**
	 * Tests that the externalized checkpoint settings are forwarded.
	 */
	@Test
	public void testEnabledExternalizedCheckpointSettings() throws Exception {
		ExternalizedCheckpointSettings externalizedSettings = ExternalizedCheckpointSettings.externalizeCheckpoints(true);

		JobSnapshottingSettings settings = new JobSnapshottingSettings(
			Collections.<JobVertexID>emptyList(),
			Collections.<JobVertexID>emptyList(),
			Collections.<JobVertexID>emptyList(),
			996979L,
			1818L,
			1212L,
			12,
			externalizedSettings,
			false); // at least once

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getJobSnapshottingSettings()).thenReturn(settings);

		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphHolder.class));
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap());

		ObjectMapper mapper = new ObjectMapper();
		JsonNode externalizedNode = mapper.readTree(json).get("externalization");
		assertNotNull(externalizedNode);
		assertEquals(externalizedSettings.externalizeCheckpoints(), externalizedNode.get("enabled").asBoolean());
		assertEquals(externalizedSettings.deleteOnCancellation(), externalizedNode.get("delete_on_cancellation").asBoolean());
	}
}
