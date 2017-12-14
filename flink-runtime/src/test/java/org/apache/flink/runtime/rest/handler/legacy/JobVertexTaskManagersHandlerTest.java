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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedJobGenerationUtils;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
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
 * Tests for the JobVertexTaskManagersHandler.
 */
public class JobVertexTaskManagersHandlerTest extends TestLogger {

	@Test
	public void testArchiver() throws Exception {
		JsonArchivist archivist = new JobVertexTaskManagersHandler.JobVertexTaskManagersJsonArchivist();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();
		AccessExecutionVertex originalSubtask = ArchivedJobGenerationUtils.getTestSubtask();

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(originalJob);
		Assert.assertEquals(1, archives.size());

		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals("/jobs/" + originalJob.getJobID() + "/vertices/" + originalTask.getJobVertexId() + "/taskmanagers", archive.getPath());
		compareVertexTaskManagers(originalTask, originalSubtask, archive.getJson());
	}

	@Test
	public void testGetPaths() {
		JobVertexTaskManagersHandler handler = new JobVertexTaskManagersHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), null);
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/vertices/:vertexid/taskmanagers", paths[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();
		AccessExecutionVertex originalSubtask = ArchivedJobGenerationUtils.getTestSubtask();
		String json = JobVertexTaskManagersHandler.createVertexDetailsByTaskManagerJson(
			originalTask, ArchivedJobGenerationUtils.getTestJob().getJobID().toString(), null);

		compareVertexTaskManagers(originalTask, originalSubtask, json);
	}

	private static void compareVertexTaskManagers(AccessExecutionJobVertex originalTask, AccessExecutionVertex originalSubtask, String json) throws IOException {
		JsonNode result = ArchivedJobGenerationUtils.MAPPER.readTree(json);

		Assert.assertEquals(originalTask.getJobVertexId().toString(), result.get("id").asText());
		Assert.assertEquals(originalTask.getName(), result.get("name").asText());
		Assert.assertTrue(result.get("now").asLong() > 0);

		ArrayNode taskmanagers = (ArrayNode) result.get("taskmanagers");

		JsonNode taskManager = taskmanagers.get(0);

		TaskManagerLocation location = originalSubtask.getCurrentAssignedResourceLocation();
		String expectedLocationString = location.getHostname() + ':' + location.dataPort();
		Assert.assertEquals(expectedLocationString, taskManager.get("host").asText());
		Assert.assertEquals(ExecutionState.FINISHED.name(), taskManager.get("status").asText());

		Assert.assertEquals(3, taskManager.get("start-time").asLong());
		Assert.assertEquals(5, taskManager.get("end-time").asLong());
		Assert.assertEquals(2, taskManager.get("duration").asLong());

		JsonNode statusCounts = taskManager.get("status-counts");
		Assert.assertEquals(0, statusCounts.get(ExecutionState.CREATED.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.SCHEDULED.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.DEPLOYING.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.RUNNING.name()).asInt());
		Assert.assertEquals(1, statusCounts.get(ExecutionState.FINISHED.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.CANCELING.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.CANCELED.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.FAILED.name()).asInt());

		long expectedNumBytesIn = 0;
		long expectedNumBytesOut = 0;
		long expectedNumRecordsIn = 0;
		long expectedNumRecordsOut = 0;

		for (AccessExecutionVertex vertex : originalTask.getTaskVertices()) {
			IOMetrics ioMetrics = vertex.getCurrentExecutionAttempt().getIOMetrics();

			expectedNumBytesIn += ioMetrics.getNumBytesInLocal() + ioMetrics.getNumBytesInRemote();
			expectedNumBytesOut += ioMetrics.getNumBytesOut();
			expectedNumRecordsIn += ioMetrics.getNumRecordsIn();
			expectedNumRecordsOut += ioMetrics.getNumRecordsOut();
		}

		JsonNode metrics = taskManager.get("metrics");

		Assert.assertEquals(expectedNumBytesIn, metrics.get("read-bytes").asLong());
		Assert.assertEquals(expectedNumBytesOut, metrics.get("write-bytes").asLong());
		Assert.assertEquals(expectedNumRecordsIn, metrics.get("read-records").asLong());
		Assert.assertEquals(expectedNumRecordsOut, metrics.get("write-records").asLong());
	}
}
