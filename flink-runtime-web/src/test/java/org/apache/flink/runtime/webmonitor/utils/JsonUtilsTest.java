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
package org.apache.flink.runtime.webmonitor.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.util.ExceptionUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonUtilsTest {
	private final ObjectMapper mapper = new ObjectMapper();
	private static final JsonFactory jacksonFactory = new JsonFactory()
		.enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
		.disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT);

	private static ArchivedExecutionGraph originalJob;
	private static ArchivedExecutionJobVertex originalTask;
	private static ArchivedExecutionVertex originalSubtask;
	private static ArchivedExecution originalAttempt;

	@BeforeClass
	public static void generateObjects() throws UnknownHostException {
		// Attempt
		StringifiedAccumulatorResult acc1 = new StringifiedAccumulatorResult("name1", "type1", "value1");
		StringifiedAccumulatorResult acc2 = new StringifiedAccumulatorResult("name2", "type2", "value2");
		TaskManagerLocation location = new TaskManagerLocation(new ResourceID("hello"), InetAddress.getLocalHost(), 1234);
		originalAttempt = BuilderUtils.createArchivedExecution()
			.setStateTimestamps(new long[]{1, 2, 3, 4, 5, 6, 7, 8})
			.setParallelSubtaskIndex(1)
			.setAttemptNumber(3)
			.setAssignedResourceLocation(location)
			.setUserAccumulators(new StringifiedAccumulatorResult[]{acc1, acc2})
			.setState(ExecutionState.FINISHED)
			.setFailureCause("attemptException")
			.finish();
		// Subtask
		originalSubtask = BuilderUtils.createArchivedExecutionVertex()
			.setSubtaskIndex(originalAttempt.getParallelSubtaskIndex())
			.setTaskNameWithSubtask("hello(1/1)")
			.setCurrentExecution(originalAttempt)
			.finish();
		// Task
		originalTask = BuilderUtils.createArchivedExecutionJobVertex()
			.setTaskVertices(new ArchivedExecutionVertex[]{originalSubtask})
			.finish();
		// Job
		Map<JobVertexID, ArchivedExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(originalTask.getJobVertexId(), originalTask);
		originalJob = BuilderUtils.createArchivedExecutionGraph()
			.setJobID(new JobID())
			.setTasks(tasks)
			.setFailureCause("jobException")
			.setArchivedUserAccumulators(new StringifiedAccumulatorResult[]{acc1, acc2})
			.finish();
	}

	// ========================================================================
	// Main test methods
	// ========================================================================
	@Test
	public void writeConfigAsJsonTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			long refreshInterval = 12345;
			TimeZone timeZone = TimeZone.getDefault();
			EnvironmentInformation.RevisionInformation revision = EnvironmentInformation.getRevisionInformation();

			JsonUtils.writeConfigAsJson(gen, refreshInterval);
			gen.close();
			JsonNode result = mapper.readTree(writer.toString());

			assertEquals(refreshInterval, result.get(JsonUtils.Keys.REFRESH_INTERVAL).asLong());
			assertEquals(timeZone.getDisplayName(), result.get(JsonUtils.Keys.TIMEZONE_NAME).asText());
			assertEquals(timeZone.getRawOffset(), result.get(JsonUtils.Keys.TIMEZONE_OFFSET).asLong());
			assertEquals(EnvironmentInformation.getVersion(), result.get(JsonUtils.Keys.FLINK_VERSION).asText());
			assertEquals(revision.commitId + " @ " + revision.commitDate, result.get(JsonUtils.Keys.FLINK_REVISION).asText());
		}
	}

	@Test
	public void writeJobDetailOverviewAsJsonTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			JsonUtils.writeJobDetailOverviewAsJson(originalJob, gen);
			gen.close();

			JsonNode result = mapper.readTree(writer.toString());
			JobDetails expectedDetails = WebMonitorUtils.createDetailsForJob(originalJob);

			assertEquals(expectedDetails.getJobId().toString(), result.get(JsonUtils.Keys.JOB_ID).asText());
			assertEquals(expectedDetails.getJobName(), result.get(JsonUtils.Keys.NAME).asText());
			assertEquals(expectedDetails.getStatus().name(), result.get(JsonUtils.Keys.STATE).asText());

			assertEquals(expectedDetails.getStartTime(), result.get(JsonUtils.Keys.START_TIME).asLong());
			assertEquals(expectedDetails.getEndTime(), result.get(JsonUtils.Keys.END_TIME).asLong());
			assertEquals(expectedDetails.getEndTime() - expectedDetails.getStartTime(), result.get(JsonUtils.Keys.DURATION).asLong());
			assertEquals(expectedDetails.getLastUpdateTime(), result.get(JsonUtils.Keys.LAST_MODIFICATION).asLong());

			JsonNode tasks = result.get(JsonUtils.Keys.TASKS);
			assertEquals(expectedDetails.getNumTasks(), tasks.get(JsonUtils.Keys.TOTAL).asInt());
			int[] tasksPerState = expectedDetails.getNumVerticesPerExecutionState();
			assertEquals(
				tasksPerState[ExecutionState.CREATED.ordinal()] + tasksPerState[ExecutionState.SCHEDULED.ordinal()] + tasksPerState[ExecutionState.DEPLOYING.ordinal()],
				tasks.get(JsonUtils.Keys.PENDING).asInt());
			assertEquals(tasksPerState[ExecutionState.RUNNING.ordinal()], tasks.get(JsonUtils.Keys.RUNNING).asInt());
			assertEquals(tasksPerState[ExecutionState.FINISHED.ordinal()], tasks.get(JsonUtils.Keys.FINISHED).asInt());
			assertEquals(tasksPerState[ExecutionState.CANCELING.ordinal()], tasks.get(JsonUtils.Keys.CANCELING).asInt());
			assertEquals(tasksPerState[ExecutionState.CANCELED.ordinal()], tasks.get(JsonUtils.Keys.CANCELED).asInt());
			assertEquals(tasksPerState[ExecutionState.FAILED.ordinal()], tasks.get(JsonUtils.Keys.FAILED).asInt());
		}
	}

	@Test
	public void writeJobDetailsAsJsonTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			JsonUtils.writeJobDetailsAsJson(originalJob, null, gen);
			gen.close();

			JsonNode result = mapper.readTree(writer.toString());

			assertEquals(originalJob.getJobID().toString(), result.get(JsonUtils.Keys.JOB_ID).asText());
			assertEquals(originalJob.getJobName(), result.get(JsonUtils.Keys.NAME).asText());
			assertEquals(originalJob.isStoppable(), result.get(JsonUtils.Keys.IS_STOPPABLE).asBoolean());
			assertEquals(originalJob.getState().name(), result.get(JsonUtils.Keys.STATE).asText());

			assertEquals(originalJob.getStatusTimestamp(JobStatus.CREATED), result.get(JsonUtils.Keys.START_TIME).asLong());
			assertEquals(originalJob.getStatusTimestamp(originalJob.getState()), result.get(JsonUtils.Keys.END_TIME).asLong());
			assertEquals(
				originalJob.getStatusTimestamp(originalJob.getState()) - originalJob.getStatusTimestamp(JobStatus.CREATED),
				result.get(JsonUtils.Keys.DURATION).asLong()
			);

			JsonNode timestamps = result.get(JsonUtils.Keys.TIMESTAMPS);
			for (JobStatus status : JobStatus.values()) {
				assertEquals(originalJob.getStatusTimestamp(status), timestamps.get(status.name()).asLong());
			}

			ArrayNode tasks = (ArrayNode) result.get(JsonUtils.Keys.VERTICES);
			int x = 0;
			for (AccessExecutionJobVertex expectedTask : originalJob.getVerticesTopologically()) {
				JsonNode task = tasks.get(x);

				assertEquals(expectedTask.getJobVertexId().toString(), task.get(JsonUtils.Keys.ID).asText());
				assertEquals(expectedTask.getName(), task.get(JsonUtils.Keys.NAME).asText());
				assertEquals(expectedTask.getParallelism(), task.get(JsonUtils.Keys.PARALLELISM).asInt());
				assertEquals(expectedTask.getAggregateState().name(), task.get(JsonUtils.Keys.STATE).asText());

				assertEquals(3, task.get(JsonUtils.Keys.START_TIME).asLong());
				assertEquals(5, task.get(JsonUtils.Keys.END_TIME).asLong());
				assertEquals(2, task.get(JsonUtils.Keys.DURATION).asLong());

				JsonNode subtasksPerState = task.get(JsonUtils.Keys.TASKS);
				assertEquals(0, subtasksPerState.get(ExecutionState.CREATED.name()).asInt());
				assertEquals(0, subtasksPerState.get(ExecutionState.SCHEDULED.name()).asInt());
				assertEquals(0, subtasksPerState.get(ExecutionState.DEPLOYING.name()).asInt());
				assertEquals(0, subtasksPerState.get(ExecutionState.RUNNING.name()).asInt());
				assertEquals(1, subtasksPerState.get(ExecutionState.FINISHED.name()).asInt());
				assertEquals(0, subtasksPerState.get(ExecutionState.CANCELING.name()).asInt());
				assertEquals(0, subtasksPerState.get(ExecutionState.CANCELED.name()).asInt());
				assertEquals(0, subtasksPerState.get(ExecutionState.FAILED.name()).asInt());

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

				JsonNode metrics = task.get(JsonUtils.Keys.METRICS);

				assertEquals(expectedNumBytesIn, metrics.get(JsonUtils.Keys.READ_BYTES).asLong());
				assertEquals(expectedNumBytesOut, metrics.get(JsonUtils.Keys.WRITE_BYTES).asLong());
				assertEquals(expectedNumRecordsIn, metrics.get(JsonUtils.Keys.READ_RECORDS).asLong());
				assertEquals(expectedNumRecordsOut, metrics.get(JsonUtils.Keys.WRITE_RECORDS).asLong());

				x++;
			}
			assertEquals(1, tasks.size());

			JsonNode statusCounts = result.get(JsonUtils.Keys.STATUS_COUNTS);
			assertEquals(0, statusCounts.get(ExecutionState.CREATED.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.SCHEDULED.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.DEPLOYING.name()).asInt());
			assertEquals(1, statusCounts.get(ExecutionState.RUNNING.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.FINISHED.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.CANCELING.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.CANCELED.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.FAILED.name()).asInt());

			assertEquals(mapper.readTree(originalJob.getJsonPlan()), result.get(JsonUtils.Keys.PLAN));
		}
	}

	@Test
	public void writeJobExceptionsAsJsonTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			JsonUtils.writeJobExceptionsAsJson(originalJob, gen);
			gen.close();

			JsonNode result = mapper.readTree(writer.toString());

			assertEquals(originalJob.getFailureCauseAsString(), result.get(JsonUtils.Keys.ROOT_EXCEPTION).asText());

			ArrayNode exceptions = (ArrayNode) result.get(JsonUtils.Keys.ALL_EXCEPTIONS);

			int x = 0;
			for (AccessExecutionVertex expectedSubtask : originalJob.getAllExecutionVertices()) {
				if (!expectedSubtask.getFailureCauseAsString().equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
					JsonNode exception = exceptions.get(x);

					assertEquals(expectedSubtask.getFailureCauseAsString(), exception.get(JsonUtils.Keys.EXCEPTION).asText());
					assertEquals(expectedSubtask.getTaskNameWithSubtaskIndex(), exception.get(JsonUtils.Keys.TASK).asText());

					TaskManagerLocation location = expectedSubtask.getCurrentAssignedResourceLocation();
					String expectedLocationString = location.getFQDNHostname() + ':' + location.dataPort();
					assertEquals(expectedLocationString, exception.get(JsonUtils.Keys.LOCATION).asText());
				}
				x++;
			}
			assertEquals(x > JsonUtils.MAX_NUMBER_EXCEPTION_TO_REPORT, result.get(JsonUtils.Keys.TRUNCATED).asBoolean());

		}

	}

	@Test
	public void writeJobAccumulatorsAsJsonTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			JsonUtils.writeJobAccumulatorsAsJson(originalJob, gen);
			gen.close();

			JsonNode result = mapper.readTree(writer.toString());

			ArrayNode accs = (ArrayNode) result.get(JsonUtils.Keys.JOB_ACCUMULATORS);
			assertEquals(0, accs.size());

			assertTrue(originalJob.getAccumulatorResultsStringified().length > 0);
			compareStringifiedAccumulators(
				originalJob.getAccumulatorResultsStringified(),
				(ArrayNode) result.get(JsonUtils.Keys.USER_TASK_ACCUMULATORS));
		}
	}

	@Test
	public void writeVertexDetailsAsJsonTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			JsonUtils.writeVertexDetailsAsJson(originalTask, null, null, gen);
			gen.close();

			JsonNode result = mapper.readTree(writer.toString());

			assertEquals(originalTask.getJobVertexId().toString(), result.get(JsonUtils.Keys.ID).asText());
			assertEquals(originalTask.getName(), result.get(JsonUtils.Keys.NAME).asText());
			assertEquals(originalTask.getParallelism(), result.get(JsonUtils.Keys.PARALLELISM).asInt());
			assertTrue(result.get(JsonUtils.Keys.NOW).asLong() > 0);

			ArrayNode subtasks = (ArrayNode) result.get(JsonUtils.Keys.SUBTASKS);

			assertEquals(originalTask.getTaskVertices().length, subtasks.size());
			for (int x = 0; x < originalTask.getTaskVertices().length; x++) {
				AccessExecutionVertex expectedSubtask = originalTask.getTaskVertices()[x];
				JsonNode subtask = subtasks.get(x);

				assertEquals(x, subtask.get(JsonUtils.Keys.SUBTASK).asInt());
				assertEquals(expectedSubtask.getExecutionState().name(), subtask.get(JsonUtils.Keys.STATUS).asText());
				assertEquals(expectedSubtask.getCurrentExecutionAttempt().getAttemptNumber(), subtask.get(JsonUtils.Keys.ATTEMPT).asInt());

				TaskManagerLocation location = expectedSubtask.getCurrentAssignedResourceLocation();
				String expectedLocationString = location.getHostname() + ":" + location.dataPort();
				assertEquals(expectedLocationString, subtask.get(JsonUtils.Keys.HOST).asText());
				long start = expectedSubtask.getStateTimestamp(ExecutionState.DEPLOYING);
				assertEquals(start, subtask.get(JsonUtils.Keys.START_TIME).asLong());
				long end = expectedSubtask.getStateTimestamp(ExecutionState.FINISHED);
				assertEquals(end, subtask.get(JsonUtils.Keys.END_TIME).asLong());
				assertEquals(end - start, subtask.get(JsonUtils.Keys.DURATION).asLong());

				compareIoMetrics(expectedSubtask.getCurrentExecutionAttempt().getIOMetrics(), subtask.get(JsonUtils.Keys.METRICS));
			}
		}
	}

	@Test
	public void writeVertexAccumulatorsAsJsonTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			JsonUtils.writeVertexAccumulatorsAsJson(originalTask, gen);
			gen.close();

			JsonNode result = mapper.readTree(writer.toString());

			assertEquals(originalTask.getJobVertexId().toString(), result.get(JsonUtils.Keys.ID).asText());

			ArrayNode accs = (ArrayNode) result.get(JsonUtils.Keys.USER_ACCUMULATORS);
			StringifiedAccumulatorResult[] expectedAccs = originalTask.getAggregatedUserAccumulatorsStringified();

			compareStringifiedAccumulators(expectedAccs, accs);
		}
	}

	@Test
	public void writeSubtasksAccumulatorsAsJsonTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			JsonUtils.writeSubtasksAccumulatorsAsJson(originalTask, gen);
			gen.close();

			JsonNode result = mapper.readTree(writer.toString());

			assertEquals(originalTask.getJobVertexId().toString(), result.get(JsonUtils.Keys.ID).asText());
			assertEquals(originalTask.getParallelism(), result.get(JsonUtils.Keys.PARALLELISM).asInt());

			ArrayNode subtasks = (ArrayNode) result.get(JsonUtils.Keys.SUBTASKS);

			assertEquals(originalTask.getTaskVertices().length, subtasks.size());
			for (int x = 0; x < originalTask.getTaskVertices().length; x++) {
				JsonNode subtask = subtasks.get(x);
				AccessExecutionVertex expectedSubtask = originalTask.getTaskVertices()[x];

				assertEquals(x, subtask.get(JsonUtils.Keys.SUBTASK).asInt());
				assertEquals(expectedSubtask.getCurrentExecutionAttempt().getAttemptNumber(), subtask.get(JsonUtils.Keys.ATTEMPT).asInt());
				assertEquals(expectedSubtask.getCurrentAssignedResourceLocation().getHostname(), subtask.get(JsonUtils.Keys.HOST).asText());

				compareStringifiedAccumulators(
					expectedSubtask.getCurrentExecutionAttempt().getUserAccumulatorsStringified(),
					(ArrayNode) subtask.get(JsonUtils.Keys.USER_ACCUMULATORS));
			}
		}
	}

	@Test
	public void writeSubtaskDetailsAsJsonTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			JsonUtils.writeSubtaskDetailsAsJson(originalTask, gen);
			gen.close();

			JsonNode result = mapper.readTree(writer.toString());

			assertEquals(originalTask.getJobVertexId().toString(), result.get(JsonUtils.Keys.ID).asText());
			assertEquals(originalTask.getName(), result.get(JsonUtils.Keys.NAME).asText());
			assertTrue(result.get(JsonUtils.Keys.NOW).asLong() > 0L);

			ArrayNode subtasks = (ArrayNode) result.get(JsonUtils.Keys.SUBTASKS);

			JsonNode subtask = subtasks.get(0);
			assertEquals(0, subtask.get(JsonUtils.Keys.SUBTASK).asInt());
			assertEquals(originalAttempt.getAssignedResourceLocation().getHostname(), subtask.get(JsonUtils.Keys.HOST).asText());
			assertEquals(originalAttempt.getStateTimestamp(originalAttempt.getState()) - originalAttempt.getStateTimestamp(ExecutionState.SCHEDULED), subtask.get(JsonUtils.Keys.DURATION).asLong());

			JsonNode timestamps = subtask.get(JsonUtils.Keys.TIMESTAMPS);

			assertEquals(originalAttempt.getStateTimestamp(ExecutionState.CREATED), timestamps.get(ExecutionState.CREATED.name()).asLong());
			assertEquals(originalAttempt.getStateTimestamp(ExecutionState.SCHEDULED), timestamps.get(ExecutionState.SCHEDULED.name()).asLong());
			assertEquals(originalAttempt.getStateTimestamp(ExecutionState.DEPLOYING), timestamps.get(ExecutionState.DEPLOYING.name()).asLong());
			assertEquals(originalAttempt.getStateTimestamp(ExecutionState.RUNNING), timestamps.get(ExecutionState.RUNNING.name()).asLong());
			assertEquals(originalAttempt.getStateTimestamp(ExecutionState.FINISHED), timestamps.get(ExecutionState.FINISHED.name()).asLong());
			assertEquals(originalAttempt.getStateTimestamp(ExecutionState.CANCELING), timestamps.get(ExecutionState.CANCELING.name()).asLong());
			assertEquals(originalAttempt.getStateTimestamp(ExecutionState.CANCELED), timestamps.get(ExecutionState.CANCELED.name()).asLong());
			assertEquals(originalAttempt.getStateTimestamp(ExecutionState.FAILED), timestamps.get(ExecutionState.FAILED.name()).asLong());
		}
	}

	@Test
	public void writeAttemptDetailsTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			JsonUtils.writeAttemptDetails(originalAttempt, null, null, null, gen);
			gen.close();

			JsonNode result = mapper.readTree(writer.toString());

			assertEquals(originalAttempt.getParallelSubtaskIndex(), result.get(JsonUtils.Keys.SUBTASK).asInt());
			assertEquals(originalAttempt.getState().name(), result.get(JsonUtils.Keys.STATUS).asText());
			assertEquals(originalAttempt.getAttemptNumber(), result.get(JsonUtils.Keys.ATTEMPT).asInt());
			assertEquals(originalAttempt.getAssignedResourceLocation().getHostname(), result.get(JsonUtils.Keys.HOST).asText());
			long start = originalAttempt.getStateTimestamp(ExecutionState.DEPLOYING);
			assertEquals(start, result.get(JsonUtils.Keys.START_TIME).asLong());
			long end = originalAttempt.getStateTimestamp(ExecutionState.FINISHED);
			assertEquals(end, result.get(JsonUtils.Keys.END_TIME).asLong());
			assertEquals(end - start, result.get(JsonUtils.Keys.DURATION).asLong());

			compareIoMetrics(originalAttempt.getIOMetrics(), result.get(JsonUtils.Keys.METRICS));
		}
	}

	@Test
	public void writeAttemptAccumulatorsTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			JsonUtils.writeAttemptAccumulators(originalAttempt, gen);
			gen.close();

			JsonNode result = mapper.readTree(writer.toString());

			assertEquals(1, result.get(JsonUtils.Keys.SUBTASK).asInt());
			assertEquals(3, result.get(JsonUtils.Keys.ATTEMPT).asInt());
			assertEquals(originalAttempt.getAttemptId().toString(), result.get(JsonUtils.Keys.ID).asText());

			compareStringifiedAccumulators(originalAttempt.getUserAccumulatorsStringified(), (ArrayNode) result.get(JsonUtils.Keys.USER_ACCUMULATORS));
		}
	}

	@Test
	public void writeVertexDetailsByTaskManagerAsJsonTest() throws IOException {
		StringWriter writer = new StringWriter();
		try (JsonGenerator gen = jacksonFactory.createGenerator(writer)) {
			JsonUtils.writeVertexDetailsByTaskManagerAsJson(originalTask, null, null, gen);
			gen.close();

			JsonNode result = mapper.readTree(writer.toString());

			assertEquals(originalTask.getJobVertexId().toString(), result.get(JsonUtils.Keys.ID).asText());
			assertEquals(originalTask.getName(), result.get(JsonUtils.Keys.NAME).asText());
			assertTrue(result.get(JsonUtils.Keys.NOW).asLong() > 0);

			ArrayNode taskmanagers = (ArrayNode) result.get(JsonUtils.Keys.TASKMANAGERS);

			JsonNode taskManager = taskmanagers.get(0);

			TaskManagerLocation location = originalSubtask.getCurrentAssignedResourceLocation();
			String expectedLocationString = location.getHostname() + ':' + location.dataPort();
			assertEquals(expectedLocationString, taskManager.get(JsonUtils.Keys.HOST).asText());
			assertEquals(ExecutionState.FINISHED.name(), taskManager.get(JsonUtils.Keys.STATUS).asText());

			assertEquals(3, taskManager.get(JsonUtils.Keys.START_TIME).asLong());
			assertEquals(5, taskManager.get(JsonUtils.Keys.END_TIME).asLong());
			assertEquals(2, taskManager.get(JsonUtils.Keys.DURATION).asLong());

			JsonNode statusCounts = taskManager.get(JsonUtils.Keys.STATUS_COUNTS);
			assertEquals(0, statusCounts.get(ExecutionState.CREATED.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.SCHEDULED.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.DEPLOYING.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.RUNNING.name()).asInt());
			assertEquals(1, statusCounts.get(ExecutionState.FINISHED.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.CANCELING.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.CANCELED.name()).asInt());
			assertEquals(0, statusCounts.get(ExecutionState.FAILED.name()).asInt());

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

			JsonNode metrics = taskManager.get(JsonUtils.Keys.METRICS);

			assertEquals(expectedNumBytesIn, metrics.get(JsonUtils.Keys.READ_BYTES).asLong());
			assertEquals(expectedNumBytesOut, metrics.get(JsonUtils.Keys.WRITE_BYTES).asLong());
			assertEquals(expectedNumRecordsIn, metrics.get(JsonUtils.Keys.READ_RECORDS).asLong());
			assertEquals(expectedNumRecordsOut, metrics.get(JsonUtils.Keys.WRITE_RECORDS).asLong());
		}
	}

	// ========================================================================
	// utility methods
	// ========================================================================
	private static void compareStringifiedAccumulators(StringifiedAccumulatorResult[] expectedAccs, ArrayNode writtenAccs) {
		assertEquals(expectedAccs.length, writtenAccs.size());
		for (int x = 0; x < expectedAccs.length; x++) {
			JsonNode acc = writtenAccs.get(x);

			assertEquals(expectedAccs[x].getName(), acc.get(JsonUtils.Keys.NAME).asText());
			assertEquals(expectedAccs[x].getType(), acc.get(JsonUtils.Keys.TYPE).asText());
			assertEquals(expectedAccs[x].getValue(), acc.get(JsonUtils.Keys.VALUE).asText());
		}
	}

	private static void compareIoMetrics(IOMetrics expectedMetrics, JsonNode writtenMetrics) {
		assertEquals(expectedMetrics.getNumBytesInTotal(), writtenMetrics.get(JsonUtils.Keys.READ_BYTES).asLong());
		assertEquals(expectedMetrics.getNumBytesOut(), writtenMetrics.get(JsonUtils.Keys.WRITE_BYTES).asLong());
		assertEquals(expectedMetrics.getNumRecordsIn(), writtenMetrics.get(JsonUtils.Keys.READ_RECORDS).asLong());
		assertEquals(expectedMetrics.getNumRecordsOut(), writtenMetrics.get(JsonUtils.Keys.WRITE_RECORDS).asLong());

	}
}
