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

import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.mockito.Mockito.mock;

/**
 * Tests for the SubtasksAllAccumulatorsHandler.
 */
public class SubtasksAllAccumulatorsHandlerTest {

	@Test
	public void testArchiver() throws Exception {
		JsonArchivist archivist = new SubtasksAllAccumulatorsHandler.SubtasksAllAccumulatorsJsonArchivist();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(originalJob);
		Assert.assertEquals(1, archives.size());

		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals("/jobs/" + originalJob.getJobID() + "/vertices/" + originalTask.getJobVertexId() +
			"/subtasks/accumulators", archive.getPath());
		compareSubtaskAccumulators(originalTask, archive.getJson());
	}

	@Test
	public void testGetPaths() {
		SubtasksAllAccumulatorsHandler handler = new SubtasksAllAccumulatorsHandler(mock(ExecutionGraphHolder.class), Executors.directExecutor());
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/vertices/:vertexid/subtasks/accumulators", paths[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();
		String json = SubtasksAllAccumulatorsHandler.createSubtasksAccumulatorsJson(originalTask);
		compareSubtaskAccumulators(originalTask, json);
	}

	private static void compareSubtaskAccumulators(AccessExecutionJobVertex originalTask, String json) throws IOException {
		JsonNode result = ArchivedJobGenerationUtils.MAPPER.readTree(json);

		Assert.assertEquals(originalTask.getJobVertexId().toString(), result.get("id").asText());
		Assert.assertEquals(originalTask.getParallelism(), result.get("parallelism").asInt());

		ArrayNode subtasks = (ArrayNode) result.get("subtasks");

		Assert.assertEquals(originalTask.getTaskVertices().length, subtasks.size());
		for (int x = 0; x < originalTask.getTaskVertices().length; x++) {
			JsonNode subtask = subtasks.get(x);
			AccessExecutionVertex expectedSubtask = originalTask.getTaskVertices()[x];

			Assert.assertEquals(x, subtask.get("subtask").asInt());
			Assert.assertEquals(expectedSubtask.getCurrentExecutionAttempt().getAttemptNumber(), subtask.get("attempt").asInt());
			Assert.assertEquals(expectedSubtask.getCurrentAssignedResourceLocation().getHostname(), subtask.get("host").asText());

			ArchivedJobGenerationUtils.compareStringifiedAccumulators(
				expectedSubtask.getCurrentExecutionAttempt().getUserAccumulatorsStringified(),
				(ArrayNode) subtask.get("user-accumulators"));
		}
	}
}
