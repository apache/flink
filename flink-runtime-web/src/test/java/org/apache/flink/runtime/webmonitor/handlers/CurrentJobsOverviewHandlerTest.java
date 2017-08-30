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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

/**
 * Tests for the CurrentJobsOverviewHandler.
 */
public class CurrentJobsOverviewHandlerTest {

	@Test
	public void testArchiver() throws Exception {
		JsonArchivist archivist = new CurrentJobsOverviewHandler.CurrentJobsOverviewJsonArchivist();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		JobDetails expectedDetails = WebMonitorUtils.createDetailsForJob(originalJob);

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(originalJob);
		Assert.assertEquals(1, archives.size());

		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals("/joboverview", archive.getPath());

		JsonNode result = ArchivedJobGenerationUtils.MAPPER.readTree(archive.getJson());
		ArrayNode running = (ArrayNode) result.get("running");
		Assert.assertEquals(0, running.size());

		ArrayNode finished = (ArrayNode) result.get("finished");
		Assert.assertEquals(1, finished.size());

		compareJobOverview(expectedDetails, finished.get(0).toString());
	}

	@Test
	public void testGetPaths() {
		CurrentJobsOverviewHandler handlerAll = new CurrentJobsOverviewHandler(Time.seconds(0L), true, true);
		String[] pathsAll = handlerAll.getPaths();
		Assert.assertEquals(1, pathsAll.length);
		Assert.assertEquals("/joboverview", pathsAll[0]);

		CurrentJobsOverviewHandler handlerRunning = new CurrentJobsOverviewHandler(Time.seconds(0L), true, false);
		String[] pathsRunning = handlerRunning.getPaths();
		Assert.assertEquals(1, pathsRunning.length);
		Assert.assertEquals("/joboverview/running", pathsRunning[0]);

		CurrentJobsOverviewHandler handlerCompleted = new CurrentJobsOverviewHandler(Time.seconds(0L), false, true);
		String[] pathsCompleted = handlerCompleted.getPaths();
		Assert.assertEquals(1, pathsCompleted.length);
		Assert.assertEquals("/joboverview/completed", pathsCompleted[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		JobDetails expectedDetails = WebMonitorUtils.createDetailsForJob(originalJob);
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = ArchivedJobGenerationUtils.JACKSON_FACTORY.createGenerator(writer)) {
			CurrentJobsOverviewHandler.writeJobDetailOverviewAsJson(expectedDetails, gen, 0);
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
		int[] tasksPerState = expectedDetails.getNumVerticesPerExecutionState();
		Assert.assertEquals(
			tasksPerState[ExecutionState.CREATED.ordinal()] + tasksPerState[ExecutionState.SCHEDULED.ordinal()] + tasksPerState[ExecutionState.DEPLOYING.ordinal()],
			tasks.get("pending").asInt());
		Assert.assertEquals(tasksPerState[ExecutionState.RUNNING.ordinal()], tasks.get("running").asInt());
		Assert.assertEquals(tasksPerState[ExecutionState.FINISHED.ordinal()], tasks.get("finished").asInt());
		Assert.assertEquals(tasksPerState[ExecutionState.CANCELING.ordinal()], tasks.get("canceling").asInt());
		Assert.assertEquals(tasksPerState[ExecutionState.CANCELED.ordinal()], tasks.get("canceled").asInt());
		Assert.assertEquals(tasksPerState[ExecutionState.FAILED.ordinal()], tasks.get("failed").asInt());
	}
}
