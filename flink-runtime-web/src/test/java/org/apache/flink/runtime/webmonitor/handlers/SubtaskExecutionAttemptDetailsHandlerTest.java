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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;
import org.junit.Assert;
import org.junit.Test;

public class SubtaskExecutionAttemptDetailsHandlerTest {
	@Test
	public void testGetPaths() {
		SubtaskExecutionAttemptDetailsHandler handler = new SubtaskExecutionAttemptDetailsHandler(null, null);
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/vertices/:vertexid/subtasks/:subtasknum/attempts/:attempt", paths[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();
		AccessExecution originalAttempt = ArchivedJobGenerationUtils.getTestAttempt();
		String json = SubtaskExecutionAttemptDetailsHandler.createAttemptDetailsJson(
			originalAttempt, originalJob.getJobID().toString(), originalTask.getJobVertexId().toString(), null);

		JsonNode result = ArchivedJobGenerationUtils.mapper.readTree(json);

		Assert.assertEquals(originalAttempt.getParallelSubtaskIndex(), result.get("subtask").asInt());
		Assert.assertEquals(originalAttempt.getState().name(), result.get("status").asText());
		Assert.assertEquals(originalAttempt.getAttemptNumber(), result.get("attempt").asInt());
		Assert.assertEquals(originalAttempt.getAssignedResourceLocation().getHostname(), result.get("host").asText());
		long start = originalAttempt.getStateTimestamp(ExecutionState.DEPLOYING);
		Assert.assertEquals(start, result.get("start-time").asLong());
		long end = originalAttempt.getStateTimestamp(ExecutionState.FINISHED);
		Assert.assertEquals(end, result.get("end-time").asLong());
		Assert.assertEquals(end - start, result.get("duration").asLong());

		ArchivedJobGenerationUtils.compareIoMetrics(originalAttempt.getIOMetrics(), result.get("metrics"));
	}
}
