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
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;
import org.junit.Assert;
import org.junit.Test;

public class SubtasksTimesHandlerTest {
	@Test
	public void testGetPaths() {
		SubtasksTimesHandler handler = new SubtasksTimesHandler(null);
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/vertices/:vertexid/subtasktimes", paths[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();
		AccessExecution originalAttempt = ArchivedJobGenerationUtils.getTestAttempt();
		String json = SubtasksTimesHandler.createSubtaskTimesJson(originalTask);

		JsonNode result = ArchivedJobGenerationUtils.mapper.readTree(json);

		Assert.assertEquals(originalTask.getJobVertexId().toString(), result.get("id").asText());
		Assert.assertEquals(originalTask.getName(), result.get("name").asText());
		Assert.assertTrue(result.get("now").asLong() > 0L);

		ArrayNode subtasks = (ArrayNode) result.get("subtasks");

		JsonNode subtask = subtasks.get(0);
		Assert.assertEquals(0, subtask.get("subtask").asInt());
		Assert.assertEquals(originalAttempt.getAssignedResourceLocation().getHostname(), subtask.get("host").asText());
		Assert.assertEquals(originalAttempt.getStateTimestamp(originalAttempt.getState()) - originalAttempt.getStateTimestamp(ExecutionState.SCHEDULED), subtask.get("duration").asLong());

		JsonNode timestamps = subtask.get("timestamps");

		Assert.assertEquals(originalAttempt.getStateTimestamp(ExecutionState.CREATED), timestamps.get(ExecutionState.CREATED.name()).asLong());
		Assert.assertEquals(originalAttempt.getStateTimestamp(ExecutionState.SCHEDULED), timestamps.get(ExecutionState.SCHEDULED.name()).asLong());
		Assert.assertEquals(originalAttempt.getStateTimestamp(ExecutionState.DEPLOYING), timestamps.get(ExecutionState.DEPLOYING.name()).asLong());
		Assert.assertEquals(originalAttempt.getStateTimestamp(ExecutionState.RUNNING), timestamps.get(ExecutionState.RUNNING.name()).asLong());
		Assert.assertEquals(originalAttempt.getStateTimestamp(ExecutionState.FINISHED), timestamps.get(ExecutionState.FINISHED.name()).asLong());
		Assert.assertEquals(originalAttempt.getStateTimestamp(ExecutionState.CANCELING), timestamps.get(ExecutionState.CANCELING.name()).asLong());
		Assert.assertEquals(originalAttempt.getStateTimestamp(ExecutionState.CANCELED), timestamps.get(ExecutionState.CANCELED.name()).asLong());
		Assert.assertEquals(originalAttempt.getStateTimestamp(ExecutionState.FAILED), timestamps.get(ExecutionState.FAILED.name()).asLong());
	}
}
