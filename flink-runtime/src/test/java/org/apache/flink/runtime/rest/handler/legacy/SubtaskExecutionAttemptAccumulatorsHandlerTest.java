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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedJobGenerationUtils;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.mockito.Mockito.mock;

/**
 * Tests for the SubtaskExecutionAttemptAccumulatorsHandler.
 */
public class SubtaskExecutionAttemptAccumulatorsHandlerTest extends TestLogger {

	@Test
	public void testArchiver() throws Exception {
		JsonArchivist archivist = new SubtaskExecutionAttemptAccumulatorsHandler.SubtaskExecutionAttemptAccumulatorsJsonArchivist();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();
		AccessExecution originalAttempt = ArchivedJobGenerationUtils.getTestAttempt();

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(originalJob);
		Assert.assertEquals(1, archives.size());

		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals(
			"/jobs/" + originalJob.getJobID() +
			"/vertices/" + originalTask.getJobVertexId() +
			"/subtasks/" + originalAttempt.getParallelSubtaskIndex() +
			"/attempts/" + originalAttempt.getAttemptNumber() +
			"/accumulators",
			archive.getPath());
		compareAttemptAccumulators(originalAttempt, archive.getJson());
	}

	@Test
	public void testGetPaths() {
		SubtaskExecutionAttemptAccumulatorsHandler handler = new SubtaskExecutionAttemptAccumulatorsHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/vertices/:vertexid/subtasks/:subtasknum/attempts/:attempt/accumulators", paths[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecution originalAttempt = ArchivedJobGenerationUtils.getTestAttempt();
		String json = SubtaskExecutionAttemptAccumulatorsHandler.createAttemptAccumulatorsJson(originalAttempt);

		compareAttemptAccumulators(originalAttempt, json);
	}

	private static void compareAttemptAccumulators(AccessExecution originalAttempt, String json) throws IOException {
		JsonNode result = ArchivedJobGenerationUtils.MAPPER.readTree(json);

		Assert.assertEquals(originalAttempt.getParallelSubtaskIndex(), result.get("subtask").asInt());
		Assert.assertEquals(originalAttempt.getAttemptNumber(), result.get("attempt").asInt());
		Assert.assertEquals(originalAttempt.getAttemptId().toString(), result.get("id").asText());

		ArchivedJobGenerationUtils.compareStringifiedAccumulators(originalAttempt.getUserAccumulatorsStringified(), (ArrayNode) result.get("user-accumulators"));
	}
}
