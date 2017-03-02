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
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;
import org.apache.flink.util.ExceptionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

public class JobExceptionsHandlerTest {

	@Test
	public void testArchiver() throws Exception {
		JsonArchivist archivist = new JobExceptionsHandler.JobExceptionsJsonArchivist();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(originalJob);
		Assert.assertEquals(1, archives.size());

		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals("/jobs/" + originalJob.getJobID() + "/exceptions", archive.getPath());
		compareExceptions(originalJob, archive.getJson());
	}

	@Test
	public void testGetPaths() {
		JobExceptionsHandler handler = new JobExceptionsHandler(null);
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/exceptions", paths[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		String json = JobExceptionsHandler.createJobExceptionsJson(originalJob);

		compareExceptions(originalJob, json);
	}

	private static void compareExceptions(AccessExecutionGraph originalJob, String json) throws IOException {
		JsonNode result = ArchivedJobGenerationUtils.mapper.readTree(json);

		Assert.assertEquals(originalJob.getFailureCauseAsString(), result.get("root-exception").asText());

		ArrayNode exceptions = (ArrayNode) result.get("all-exceptions");

		int x = 0;
		for (AccessExecutionVertex expectedSubtask : originalJob.getAllExecutionVertices()) {
			if (!expectedSubtask.getFailureCauseAsString().equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
				JsonNode exception = exceptions.get(x);

				Assert.assertEquals(expectedSubtask.getFailureCauseAsString(), exception.get("exception").asText());
				Assert.assertEquals(expectedSubtask.getTaskNameWithSubtaskIndex(), exception.get("task").asText());

				TaskManagerLocation location = expectedSubtask.getCurrentAssignedResourceLocation();
				String expectedLocationString = location.getFQDNHostname() + ':' + location.dataPort();
				Assert.assertEquals(expectedLocationString, exception.get("location").asText());
			}
			x++;
		}
		Assert.assertEquals(x > JobExceptionsHandler.MAX_NUMBER_EXCEPTION_TO_REPORT, result.get("truncated").asBoolean());
	}
}
