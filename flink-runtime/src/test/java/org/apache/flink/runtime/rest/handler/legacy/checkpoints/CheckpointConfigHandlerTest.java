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
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the CheckpointConfigHandler.
 */
public class CheckpointConfigHandlerTest {

	@Test
	public void testArchiver() throws IOException {
		JsonArchivist archivist = new CheckpointConfigHandler.CheckpointConfigJsonArchivist();
		GraphAndSettings graphAndSettings = createGraphAndSettings(true, true);

		AccessExecutionGraph graph = graphAndSettings.graph;
		when(graph.getJobID()).thenReturn(new JobID());
		CheckpointCoordinatorConfiguration chkConfig = graphAndSettings.jobCheckpointingConfiguration;
		CheckpointRetentionPolicy retentionPolicy = graphAndSettings.retentionPolicy;

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(graph);
		Assert.assertEquals(1, archives.size());
		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals("/jobs/" + graph.getJobID() + "/checkpoints/config", archive.getPath());

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(archive.getJson());

		Assert.assertEquals("exactly_once", rootNode.get("mode").asText());
		Assert.assertEquals(chkConfig.getCheckpointInterval(), rootNode.get("interval").asLong());
		Assert.assertEquals(chkConfig.getCheckpointTimeout(), rootNode.get("timeout").asLong());
		Assert.assertEquals(chkConfig.getMinPauseBetweenCheckpoints(), rootNode.get("min_pause").asLong());
		Assert.assertEquals(chkConfig.getMaxConcurrentCheckpoints(), rootNode.get("max_concurrent").asInt());

		JsonNode externalizedNode = rootNode.get("externalization");
		Assert.assertNotNull(externalizedNode);
		Assert.assertEquals(retentionPolicy != CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION, externalizedNode.get("enabled").asBoolean());
		Assert.assertEquals(retentionPolicy != CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION, externalizedNode.get("delete_on_cancellation").asBoolean());

	}

	@Test
	public void testGetPaths() {
		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
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
		CheckpointCoordinatorConfiguration chkConfig = graphAndSettings.jobCheckpointingConfiguration;

		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap()).get();

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(json);

		assertEquals("exactly_once", rootNode.get("mode").asText());
		assertEquals(chkConfig.getCheckpointInterval(), rootNode.get("interval").asLong());
		assertEquals(chkConfig.getCheckpointTimeout(), rootNode.get("timeout").asLong());
		assertEquals(chkConfig.getMinPauseBetweenCheckpoints(), rootNode.get("min_pause").asLong());
		assertEquals(chkConfig.getMaxConcurrentCheckpoints(), rootNode.get("max_concurrent").asInt());

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

		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap()).get();

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
		CheckpointRetentionPolicy retentionPolicy = graphAndSettings.retentionPolicy;

		CheckpointConfigHandler handler = new CheckpointConfigHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
		String json = handler.handleRequest(graph, Collections.<String, String>emptyMap()).get();

		ObjectMapper mapper = new ObjectMapper();
		JsonNode externalizedNode = mapper.readTree(json).get("externalization");
		assertNotNull(externalizedNode);
		assertEquals(retentionPolicy != CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION, externalizedNode.get("enabled").asBoolean());
		assertEquals(retentionPolicy != CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION, externalizedNode.get("delete_on_cancellation").asBoolean());
	}

	private static GraphAndSettings createGraphAndSettings(boolean externalized, boolean exactlyOnce) {
		long interval = 18231823L;
		long timeout = 996979L;
		long minPause = 119191919L;
		int maxConcurrent = 12929329;

		CheckpointRetentionPolicy retentionPolicy = externalized
			? CheckpointRetentionPolicy.RETAIN_ON_FAILURE
			: CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;

		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			interval,
			timeout,
			minPause,
			maxConcurrent,
			retentionPolicy,
			exactlyOnce);

		AccessExecutionGraph graph = mock(AccessExecutionGraph.class);
		when(graph.getCheckpointCoordinatorConfiguration()).thenReturn(chkConfig);

		return new GraphAndSettings(graph, chkConfig, retentionPolicy);
	}

	private static class GraphAndSettings {
		public final AccessExecutionGraph graph;
		public final CheckpointCoordinatorConfiguration jobCheckpointingConfiguration;
		public final CheckpointRetentionPolicy retentionPolicy;

		public GraphAndSettings(
				AccessExecutionGraph graph,
				CheckpointCoordinatorConfiguration jobCheckpointingConfiguration,
				CheckpointRetentionPolicy retentionPolicy) {
			this.graph = graph;
			this.jobCheckpointingConfiguration = jobCheckpointingConfiguration;
			this.retentionPolicy = retentionPolicy;
		}
	}
}
