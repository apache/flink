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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedJobGenerationUtils;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

/**
 * Tests for the CurrentJobsOverviewHandler.
 */
public class JobsOverviewHandlerTest extends TestLogger {

	@Test
	public void testArchiver() throws Exception {
		JsonArchivist archivist = new JobsOverviewHandler.CurrentJobsOverviewJsonArchivist();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		JobDetails expectedDetails = WebMonitorUtils.createDetailsForJob(originalJob);

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(originalJob);
		Assert.assertEquals(1, archives.size());

		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals(JobsOverviewHeaders.URL, archive.getPath());

		JsonNode result = ArchivedJobGenerationUtils.MAPPER.readTree(archive.getJson());
		ArrayNode jobs = (ArrayNode) result.get("jobs");
		Assert.assertEquals(1, jobs.size());

		compareJobOverview(expectedDetails, jobs.get(0).toString());
	}

	@Test
	public void testGetPaths() {
		JobsOverviewHandler handlerAll = new JobsOverviewHandler(Executors.directExecutor(), Time.seconds(0L));
		String[] pathsAll = handlerAll.getPaths();
		Assert.assertEquals(1, pathsAll.length);
		Assert.assertEquals(JobsOverviewHeaders.URL, pathsAll[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		JobDetails expectedDetails = WebMonitorUtils.createDetailsForJob(originalJob);
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = ArchivedJobGenerationUtils.JACKSON_FACTORY.createGenerator(writer)) {
			JobDetails.JobDetailsSerializer serializer = new JobDetails.JobDetailsSerializer();
			serializer.serialize(expectedDetails, gen, null);
		}
		compareJobOverview(expectedDetails, writer.toString());
	}

	private static void compareJobOverview(JobDetails expectedDetails, String answer) throws IOException {
		JsonNode result = ArchivedJobGenerationUtils.MAPPER.readTree(answer);

		Assert.assertEquals(expectedDetails.getJobId().toString(), result.get("jid").asText());
		Assert.assertEquals(expectedDetails.getJobName(), result.get("name").asText());
		Assert.assertEquals(expectedDetails.getStatus().name(), result.get("state").asText());

		Assert.assertEquals(expectedDetails.getStartTime(), result.get("start-time").asLong());
		Assert.assertEquals(expectedDetails.getEndTime(), result.get("end-time").asLong());
		Assert.assertEquals(expectedDetails.getEndTime() - expectedDetails.getStartTime(), result.get("duration").asLong());
		Assert.assertEquals(expectedDetails.getLastUpdateTime(), result.get("last-modification").asLong());

		JsonNode tasks = result.get("tasks");
		Assert.assertEquals(expectedDetails.getNumTasks(), tasks.get("total").asInt());
		int[] tasksPerState = expectedDetails.getTasksPerState();

		for (ExecutionState executionState : ExecutionState.values()) {
			Assert.assertEquals(tasksPerState[executionState.ordinal()], tasks.get(executionState.name().toLowerCase()).asInt());
		}
	}
}
