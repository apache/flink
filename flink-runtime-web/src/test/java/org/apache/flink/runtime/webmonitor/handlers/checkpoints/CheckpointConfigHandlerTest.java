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
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckpointConfigHandlerTest {

	@Test
	public void testArchiver() throws IOException {
		JsonArchivist archivist = new CheckpointConfigHandler.CheckpointConfigJsonArchivist();
		GraphAndSettings graphAndSettings = createGraphAndSettings(true, true);

		AccessExecutionGraph graph = graphAndSettings.graph;
		when(graph.getJobID()).thenReturn(new JobID());
		JobCheckpointingSettings settings = graphAndSettings.snapshottingSettings;
		ExternalizedCheckpointSettings externalizedSettings = graphAndSettings.externalizedSettings;
		
		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(graph);
		Assert.assertEquals(1, archives.size());
		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals("/jobs/" + graph.getJobID() + "/checkpoints/config", archive.getPath());

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(archive.getJson());

		Assert.assertEquals("exactly_once", rootNode.get("mode").asText());
		Assert.assertEquals(settings.getCheckpointInterval(), rootNode.get("interval").asLong());
		Assert.assertEquals(settings.getCheckpointTimeout(), rootNode.get("timeout").asLong());
		Assert.assertEquals(settings.getMinPauseBetweenCheckpoints(), rootNode.get("min_pause").asLong());
		Assert.assertEquals(settings.getMaxConcurrentCheckpoints(), rootNode.get("max_concurrent").asInt());

		JsonNode externalizedNode = rootNode.get("externalization");
		Assert.assertNotNull(externalizedNode);
		Assert.assertEquals(externalizedSettings.externalizeCheckpoints(), externalizedNode.get("enabled").asBoolean());
		Assert.assertEquals(externalizedSettings.deleteOnCancellation(), externalizedNode.get("delete_on_cancellation").asBoolean());

	}

	@Test
	public void testGetPaths() {
		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphHolder.class));
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/checkpoints/config", paths[0]);
	}

	/**
	 * Tests a simple config.
	 */
	@Test
	public void testSimpleConfig() throws Exception {
		GraphAndSettings graphAndSettings = createGraphAndSettings(false, true);

		AccessExecutionGraph graph = graphAndSettings.graph;
		JobCheckpointingSettings settings = graphAndSettings.snapshottingSettings;

		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphHolder.class));
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap());

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(json);

		assertEquals("exactly_once", rootNode.get("mode").asText());
		assertEquals(settings.getCheckpointInterval(), rootNode.get("interval").asLong());
		assertEquals(settings.getCheckpointTimeout(), rootNode.get("timeout").asLong());
		assertEquals(settings.getMinPauseBetweenCheckpoints(), rootNode.get("min_pause").asLong());
		assertEquals(settings.getMaxConcurrentCheckpoints(), rootNode.get("max_concurrent").asInt());

		JsonNode externalizedNode = rootNode.get("externalization");
		assertNotNull(externalizedNode);
		assertEquals(false, externalizedNode.get("enabled").asBoolean());
	}

	/**
	 * Tests the that the isExactlyOnce flag is respected.
	 */
	@Test
	public void testAtLeastOnce() throws Exception {
		GraphAndSettings graphAndSettings = createGraphAndSettings(false, false);

		AccessExecutionGraph graph = graphAndSettings.graph;

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
		GraphAndSettings graphAndSettings = createGraphAndSettings(true, false);

		AccessExecutionGraph graph = graphAndSettings.graph;
		ExternalizedCheckpointSettings externalizedSettings = graphAndSettings.externalizedSettings;

		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphHolder.class));
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap());

		ObjectMapper mapper = new ObjectMapper();
		JsonNode externalizedNode = mapper.readTree(json).get("externalization");
		assertNotNull(externalizedNode);
		assertEquals(externalizedSettings.externalizeCheckpoints(), externalizedNode.get("enabled").asBoolean());
		assertEquals(externalizedSettings.deleteOnCancellation(), externalizedNode.get("delete_on_cancellation").asBoolean());
	}

	private static GraphAndSettings createGraphAndSettings(boolean externalized, boolean exactlyOnce) {
		long interval = 18231823L;
		long timeout = 996979L;
		long minPause = 119191919L;
		int maxConcurrent = 12929329;
		ExternalizedCheckpointSettings externalizedSetting = externalized
			? ExternalizedCheckpointSettings.externalizeCheckpoints(true)
			: ExternalizedCheckpointSettings.none();

		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			Collections.<JobVertexID>emptyList(),
			Collections.<JobVertexID>emptyList(),
			Collections.<JobVertexID>emptyList(),
			interval,
			timeout,
			minPause,
			maxConcurrent,
			externalizedSetting,
			null,
			exactlyOnce);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getJobCheckpointingSettings()).thenReturn(settings);

		return new GraphAndSettings(graph, settings, externalizedSetting);
	}

	private static class GraphAndSettings {
		public final AccessExecutionGraph graph;
		public final JobCheckpointingSettings snapshottingSettings;
		public final ExternalizedCheckpointSettings externalizedSettings;

		public GraphAndSettings(
				AccessExecutionGraph graph,
				JobCheckpointingSettings snapshottingSettings,
				ExternalizedCheckpointSettings externalizedSettings) {
			this.graph = graph;
			this.snapshottingSettings = snapshottingSettings;
			this.externalizedSettings = externalizedSettings;
		}
	}
}
