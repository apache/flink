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
import com.google.common.collect.Lists;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class JobDetailsHandlerTest {
	@Test
	public void testGetPaths() {
		JobDetailsHandler handler = new JobDetailsHandler(null, null);
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

		JsonNode result = ArchivedJobGenerationUtils.mapper.readTree(json);

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

		Assert.assertEquals(ArchivedJobGenerationUtils.mapper.readTree(originalJob.getJsonPlan()), result.get("plan"));
	}
}
