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
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedJobGenerationUtils;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Tests for the JobDetailsHandler.
 */
public class JobDetailsHandlerTest extends TestLogger {

	@Test
	public void testArchiver() throws Exception {
		JsonArchivist archivist = new JobDetailsHandler.JobDetailsJsonArchivist();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(originalJob);
		Assert.assertEquals(2, archives.size());

		Iterator<ArchivedJson> iterator = archives.iterator();
		ArchivedJson archive1 = iterator.next();
		Assert.assertEquals("/jobs/" + originalJob.getJobID(), archive1.getPath());
		compareJobDetails(originalJob, archive1.getJson());

		ArchivedJson archive2 = iterator.next();
		Assert.assertEquals("/jobs/" + originalJob.getJobID() + "/vertices", archive2.getPath());
		compareJobDetails(originalJob, archive2.getJson());
	}

	@Test
	public void testGetPaths() {
		JobDetailsHandler handler = new JobDetailsHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), null);
		String[] paths = handler.getPaths();
		Assert.assertEquals(2, paths.length);
		List<String> pathsList = Lists.newArrayList(paths);
		Assert.assertTrue(pathsList.contains("/jobs/:jobid"));
		Assert.assertTrue(pathsList.contains("/jobs/:jobid/vertices"));
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		String json = JobDetailsHandler.createJobDetailsJson(originalJob, null);

		compareJobDetails(originalJob, json);
	}

	private static void compareJobDetails(AccessExecutionGraph originalJob, String json) throws IOException {
		JsonNode result = ArchivedJobGenerationUtils.MAPPER.readTree(json);

		Assert.assertEquals(originalJob.getJobID().toString(), result.get("jid").asText());
		Assert.assertEquals(originalJob.getJobName(), result.get("name").asText());
		Assert.assertEquals(originalJob.isStoppable(), result.get("isStoppable").asBoolean());
		Assert.assertEquals(originalJob.getState().name(), result.get("state").asText());

		Assert.assertEquals(originalJob.getStatusTimestamp(JobStatus.CREATED), result.get("start-time").asLong());
		Assert.assertEquals(originalJob.getStatusTimestamp(originalJob.getState()), result.get("end-time").asLong());
		Assert.assertEquals(
			originalJob.getStatusTimestamp(originalJob.getState()) - originalJob.getStatusTimestamp(JobStatus.CREATED),
			result.get("duration").asLong()
		);

		JsonNode timestamps = result.get("timestamps");
		for (JobStatus status : JobStatus.values()) {
			Assert.assertEquals(originalJob.getStatusTimestamp(status), timestamps.get(status.name()).asLong());
		}

		ArrayNode tasks = (ArrayNode) result.get("vertices");
		int x = 0;
		for (AccessExecutionJobVertex expectedTask : originalJob.getVerticesTopologically()) {
			JsonNode task = tasks.get(x);

			Assert.assertEquals(expectedTask.getJobVertexId().toString(), task.get("id").asText());
			Assert.assertEquals(expectedTask.getName(), task.get("name").asText());
			Assert.assertEquals(expectedTask.getParallelism(), task.get("parallelism").asInt());
			Assert.assertEquals(expectedTask.getAggregateState().name(), task.get("status").asText());

			Assert.assertEquals(3, task.get("start-time").asLong());
			Assert.assertEquals(5, task.get("end-time").asLong());
			Assert.assertEquals(2, task.get("duration").asLong());

			JsonNode subtasksPerState = task.get("tasks");
			Assert.assertEquals(0, subtasksPerState.get(ExecutionState.CREATED.name()).asInt());
			Assert.assertEquals(0, subtasksPerState.get(ExecutionState.SCHEDULED.name()).asInt());
			Assert.assertEquals(0, subtasksPerState.get(ExecutionState.DEPLOYING.name()).asInt());
			Assert.assertEquals(0, subtasksPerState.get(ExecutionState.RUNNING.name()).asInt());
			Assert.assertEquals(1, subtasksPerState.get(ExecutionState.FINISHED.name()).asInt());
			Assert.assertEquals(0, subtasksPerState.get(ExecutionState.CANCELING.name()).asInt());
			Assert.assertEquals(0, subtasksPerState.get(ExecutionState.CANCELED.name()).asInt());
			Assert.assertEquals(0, subtasksPerState.get(ExecutionState.FAILED.name()).asInt());

			long expectedNumBytesIn = 0;
			long expectedNumBytesOut = 0;
			long expectedNumRecordsIn = 0;
			long expectedNumRecordsOut = 0;

			for (AccessExecutionVertex vertex : expectedTask.getTaskVertices()) {
				IOMetrics ioMetrics = vertex.getCurrentExecutionAttempt().getIOMetrics();

				expectedNumBytesIn += ioMetrics.getNumBytesInLocal() + ioMetrics.getNumBytesInRemote();
				expectedNumBytesOut += ioMetrics.getNumBytesOut();
				expectedNumRecordsIn += ioMetrics.getNumRecordsIn();
				expectedNumRecordsOut += ioMetrics.getNumRecordsOut();
			}

			JsonNode metrics = task.get("metrics");

			Assert.assertEquals(expectedNumBytesIn, metrics.get("read-bytes").asLong());
			Assert.assertEquals(expectedNumBytesOut, metrics.get("write-bytes").asLong());
			Assert.assertEquals(expectedNumRecordsIn, metrics.get("read-records").asLong());
			Assert.assertEquals(expectedNumRecordsOut, metrics.get("write-records").asLong());

			x++;
		}
		Assert.assertEquals(1, tasks.size());

		JsonNode statusCounts = result.get("status-counts");
		Assert.assertEquals(0, statusCounts.get(ExecutionState.CREATED.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.SCHEDULED.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.DEPLOYING.name()).asInt());
		Assert.assertEquals(1, statusCounts.get(ExecutionState.RUNNING.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.FINISHED.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.CANCELING.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.CANCELED.name()).asInt());
		Assert.assertEquals(0, statusCounts.get(ExecutionState.FAILED.name()).asInt());

		Assert.assertEquals(ArchivedJobGenerationUtils.MAPPER.readTree(originalJob.getJsonPlan()), result.get("plan"));
	}
}
