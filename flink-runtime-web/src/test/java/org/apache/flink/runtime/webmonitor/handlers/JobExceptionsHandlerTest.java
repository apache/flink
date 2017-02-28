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
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;
import org.apache.flink.util.ExceptionUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JobExceptionsHandlerTest {
	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		String json = JobExceptionsHandler.createJobExceptionsJson(originalJob);

		JsonNode result = ArchivedJobGenerationUtils.mapper.readTree(json);

		assertEquals(originalJob.getFailureCauseAsString(), result.get("root-exception").asText());

		ArrayNode exceptions = (ArrayNode) result.get("all-exceptions");

		int x = 0;
		for (AccessExecutionVertex expectedSubtask : originalJob.getAllExecutionVertices()) {
			if (!expectedSubtask.getFailureCauseAsString().equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
				JsonNode exception = exceptions.get(x);

				assertEquals(expectedSubtask.getFailureCauseAsString(), exception.get("exception").asText());
				assertEquals(expectedSubtask.getTaskNameWithSubtaskIndex(), exception.get("task").asText());

				TaskManagerLocation location = expectedSubtask.getCurrentAssignedResourceLocation();
				String expectedLocationString = location.getFQDNHostname() + ':' + location.dataPort();
				assertEquals(expectedLocationString, exception.get("location").asText());
			}
			x++;
		}
		assertEquals(x > JobExceptionsHandler.MAX_NUMBER_EXCEPTION_TO_REPORT, result.get("truncated").asBoolean());
	}
}
