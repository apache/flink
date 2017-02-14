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
package org.apache.flink.runtime.webmonitor.history;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.webmonitor.utils.BuilderUtils;
import org.apache.flink.runtime.webmonitor.utils.JsonUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This test verifies that the HistoryServer's file generation is working correctly. This includes the setup of initial
 * files on startup, proper generation of job specific files for a given ExecutionGraph, and the update of the job list.
 */
public class HistoryServerFilesTest {
	private final ObjectMapper mapper = new ObjectMapper();
	private static final JsonFactory jacksonFactory = new JsonFactory()
		.enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
		.disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT);

	@Test
	public void testHistoryServer() throws IOException {
		File webRoot = CommonTestUtils.createTempDirectory();
		Configuration config = new Configuration();
		config.setString(HistoryServerOptions.HISTORY_SERVER_DIR, webRoot.getAbsolutePath());

		ActorSystem system = AkkaUtils.createLocalActorSystem(config);
		Props props = Props.create(HistoryServer.class, config, null);
		TestActorRef<HistoryServer> actorRef = TestActorRef.create(system, props);
		HistoryServer actor = actorRef.underlyingActor();

		verifyInitialFiles(webRoot);
		AccessExecutionGraph graph = createArchivedExecutionGraph();
		actor.onReceive(graph);

		verifyJobFiles(webRoot, graph);

		verifyUpdatedOverview(new File(webRoot, "joboverview.json"), graph);
	}

	private void verifyInitialFiles(File webRoot) throws IOException {
		verifyConfigFile(new File(webRoot, "config.json"));
		verifyInitialOverviewFile(new File(webRoot, "joboverview.json"));
	}

	private void verifyConfigFile(File file) throws IOException {
		JsonNode configNode = mapper.readTree(file);

		assertEquals(10000, configNode.get("refresh-interval").asInt());
		assertNotNull(configNode.get("timezone-offset"));
		assertNotNull(configNode.get("timezone-name"));
		assertNotNull(configNode.get("flink-version"));
	}

	private void verifyInitialOverviewFile(File file) throws IOException {
		JsonNode overviewNode = mapper.readTree(file);

		ArrayNode jobs = (ArrayNode) overviewNode.get("finished");
		assertNotNull(jobs);
		assertEquals(0, jobs.size());
	}

	private void verifyUpdatedOverview(File file, AccessExecutionGraph job) throws IOException {
		JsonNode overviewNode = mapper.readTree(file);
		ArrayNode jobs = (ArrayNode) overviewNode.get("finished");
		assertNotNull(jobs);
		assertEquals(1, jobs.size());

		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeJobDetailOverviewAsJson(job, gen);
		gen.close();

		JsonNode expected = mapper.readTree(sw.toString());
		JsonNode written = jobs.get(0);

		assertEquals(expected, written);
	}

	// ========================================================================
	// Job
	// ========================================================================
	private void verifyJobFiles(File webRoot, AccessExecutionGraph job) throws IOException {
		File currentFolder = new File(webRoot, "jobs");
		assertTrue(currentFolder.exists());
		assertTrue(currentFolder.isDirectory());
		verifyJobDetailsFile(new File(currentFolder, job.getJobID() + ".json"), job);

		currentFolder = new File(currentFolder, job.getJobID().toString());
		assertTrue(currentFolder.exists());
		assertTrue(currentFolder.isDirectory());

		verifyJobPlanFile(new File(currentFolder, "plan.json"), job);
		verifyJobConfigFile(new File(currentFolder, "config.json"), job);
		verifyJobExceptionsFile(new File(currentFolder, "exceptions.json"), job);
		verifyJobAccumulatorsFile(new File(currentFolder, "accumulators.json"), job);
		assertTrue(new File(currentFolder, "metrics.json").exists());

		for (AccessExecutionJobVertex task : job.getAllVertices().values()) {
			verifyTaskFiles(currentFolder, task);
		}
	}

	private void verifyJobDetailsFile(File file, AccessExecutionGraph job) throws IOException {
		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeJobDetailsAsJson(job, null, gen);
		gen.close();

		JobDetails expected = mapper.readValue(sw.toString(), JobDetails.class);
		JobDetails written = mapper.readValue(file, JobDetails.class);

		assertEquals(expected, written);
	}

	private void verifyJobPlanFile(File file, AccessExecutionGraph job) throws IOException {
		JsonNode expected = mapper.readTree(job.getJsonPlan());
		JsonNode written = mapper.readTree(file);

		assertEquals(expected, written);
	}

	private void verifyJobConfigFile(File file, AccessExecutionGraph job) throws IOException {
		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeJobConfigAsJson(job, gen);
		gen.close();

		JsonNode expected = mapper.readTree(sw.toString());
		JsonNode written = mapper.readTree(file);

		assertEquals(expected, written);
	}

	private void verifyJobExceptionsFile(File file, AccessExecutionGraph job) throws IOException {
		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeJobExceptionsAsJson(job, gen);
		gen.close();

		JsonNode expected = mapper.readTree(sw.toString());
		JsonNode written = mapper.readTree(file);

		assertEquals(expected, written);
	}

	private void verifyJobAccumulatorsFile(File file, AccessExecutionGraph job) throws IOException {
		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeJobAccumulatorsAsJson(job, gen);
		gen.close();

		JsonNode expected = mapper.readTree(sw.toString());
		JsonNode written = mapper.readTree(file);

		assertEquals(expected, written);
	}

	// ========================================================================
	// Task
	// ========================================================================
	private void verifyTaskFiles(File jobRoot, AccessExecutionJobVertex task) throws IOException {
		File currentFolder = new File(jobRoot, "vertices");
		assertTrue(currentFolder.exists());
		assertTrue(currentFolder.isDirectory());
		assertTrue(new File(currentFolder, task.getJobVertexId() + ".json").exists());

		currentFolder = new File(currentFolder, task.getJobVertexId().toString());
		assertTrue(currentFolder.exists());
		assertTrue(currentFolder.isDirectory());

		verifyTaskAccumulatorsFile(new File(currentFolder, "accumulators.json"), task);
		assertTrue(new File(currentFolder, "backpressure.json").exists());
		assertTrue(new File(currentFolder, "metrics.json").exists());
		verifyTaskSubtaskTimesFile(new File(currentFolder, "subtasktimes.json"), task);
		verifyTaskByTaskManagerFile(new File(currentFolder, "taskmanagers.json"), task);

		for (AccessExecutionVertex subtask : task.getTaskVertices()) {
			verifySubtaskFiles(currentFolder, task, subtask);
		}
	}

	private void verifyTaskAccumulatorsFile(File file, AccessExecutionJobVertex task) throws IOException {
		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeVertexAccumulatorsAsJson(task, gen);
		gen.close();

		JsonNode expected = mapper.readTree(sw.toString());
		JsonNode written = mapper.readTree(file);

		assertEquals(expected, written);
	}

	private void verifyTaskSubtaskTimesFile(File file, AccessExecutionJobVertex task) throws IOException {
		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeSubtaskDetailsAsJson(task, gen);
		gen.close();

		TaskSubtaskDetails expected = mapper.readValue(sw.toString(), TaskSubtaskDetails.class);
		TaskSubtaskDetails written = mapper.readValue(file, TaskSubtaskDetails.class);

		assertEquals(expected, written);
	}

	private void verifyTaskByTaskManagerFile(File file, AccessExecutionJobVertex task) throws IOException {
		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeVertexDetailsByTaskManagerAsJson(task, null, null, gen);
		gen.close();

		TaskDetails expected = mapper.readValue(sw.toString(), TaskDetails.class);
		TaskDetails written = mapper.readValue(file, TaskDetails.class);

		assertEquals(expected, written);
	}

	// ========================================================================
	// Subtask
	// ========================================================================
	private void verifySubtaskFiles(File taskRoot, AccessExecutionJobVertex task, AccessExecutionVertex subtask) throws IOException {
		File currentFolder = new File(taskRoot, "subtasks");
		assertTrue(currentFolder.exists());
		assertTrue(currentFolder.isDirectory());

		verifySubtaskAccumulatorsFile(new File(currentFolder, "accumulators.json"), task);

		currentFolder = new File(currentFolder, String.valueOf(subtask.getParallelSubtaskIndex()));
		assertTrue(currentFolder.exists());
		assertTrue(currentFolder.isDirectory());

		verifySubtaskDetailsFile(new File(currentFolder, subtask.getParallelSubtaskIndex() + ".json"), subtask);

		verifyAttemptFiles(currentFolder, subtask.getCurrentExecutionAttempt());
		for (AccessExecution attempt : subtask.getPriorExecutionAttempts()) {
			verifyAttemptFiles(currentFolder, attempt);
		}
	}

	private void verifySubtaskAccumulatorsFile(File file, AccessExecutionJobVertex task) throws IOException {
		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeSubtasksAccumulatorsAsJson(task, gen);
		gen.close();

		JsonNode expected = mapper.readTree(sw.toString());
		JsonNode written = mapper.readTree(file);

		assertEquals(expected, written);
	}

	private void verifySubtaskDetailsFile(File file, AccessExecutionVertex subtask) throws IOException {
		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeAttemptDetails(subtask.getCurrentExecutionAttempt(), null, null, null, gen);
		gen.close();

		JsonNode expected = mapper.readTree(sw.toString());
		JsonNode written = mapper.readTree(file);

		assertEquals(expected, written);
	}

	// ========================================================================
	// Attempt
	// ========================================================================
	private void verifyAttemptFiles(File subtaskRoot, AccessExecution attempt) throws IOException {
		File currentFolder = new File(subtaskRoot, "attempts");
		assertTrue(currentFolder.exists());
		assertTrue(currentFolder.isDirectory());
		verifyAttemptDetailsFile(new File(currentFolder, attempt.getAttemptNumber() + ".json"), attempt);

		currentFolder = new File(currentFolder, String.valueOf(attempt.getAttemptNumber()));
		assertTrue(currentFolder.exists());
		assertTrue(currentFolder.isDirectory());
		verifyAttemptAccumulatorsFile(new File(currentFolder, "accumulators.json"), attempt);
	}

	private void verifyAttemptDetailsFile(File file, AccessExecution attempt) throws IOException {
		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeAttemptDetails(attempt, null, null, null, gen);
		gen.close();

		JsonNode expected = mapper.readTree(sw.toString());
		JsonNode written = mapper.readTree(file);

		assertEquals(expected, written);
	}

	private void verifyAttemptAccumulatorsFile(File file, AccessExecution attempt) throws IOException {
		StringWriter sw = new StringWriter();
		JsonGenerator gen = jacksonFactory.createGenerator(sw);
		JsonUtils.writeAttemptAccumulators(attempt, gen);
		gen.close();

		JsonNode expected = mapper.readTree(sw.toString());
		JsonNode written = mapper.readTree(file);

		assertEquals(expected, written);
	}


	// ========================================================================
	// Utils
	// ========================================================================
	private static AccessExecutionGraph createArchivedExecutionGraph() throws UnknownHostException {
		JobID jobID = new JobID();
		JobVertexID jobVertexID1 = new JobVertexID();
		int subtaskIndex1 = 1;
		int attemptNumber1 = 2;

		ArchivedExecution attempt = BuilderUtils.createArchivedExecution()
			.setFailureCause("Stuff happened somewhere else")
			.setState(ExecutionState.CANCELED)
			.setParallelSubtaskIndex(subtaskIndex1)
			.setAttemptNumber(attemptNumber1)
			.finish();

		ArchivedExecutionVertex subtask = BuilderUtils.createArchivedExecutionVertex()
			.setCurrentExecution(attempt)
			.setSubtaskIndex(subtaskIndex1)
			.finish();

		ArchivedExecutionJobVertex task = BuilderUtils.createArchivedExecutionJobVertex()
			.setTaskVertices(new ArchivedExecutionVertex[]{subtask})
			.setId(jobVertexID1)
			.finish();

		Map<JobVertexID, ArchivedExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(jobVertexID1, task);

		ArchivedExecutionGraph job = BuilderUtils.createArchivedExecutionGraph()
			.setJobID(jobID)
			.setTasks(tasks)
			.finish();

		return job;
	}

	// ========================================================================
	// POJOs - These are necessary to exclude certain dynamic fields from the comparisons, while still making sure
	// that they were in-fact written
	// ========================================================================
	private static class JobDetails {
		@JsonProperty
		private String jid;
		@JsonProperty
		private String name;
		@JsonProperty
		private boolean isStoppable;
		@JsonProperty
		private String state;
		@JsonProperty(JsonUtils.Keys.START_TIME)
		private long startTime;
		@JsonProperty(JsonUtils.Keys.END_TIME)
		private long endTime;
		@JsonProperty
		private long duration;
		@JsonProperty
		private long now;
		@JsonProperty
		private JsonNode timestamps;
		@JsonProperty
		private List<JsonNode> vertices;
		@JsonProperty(JsonUtils.Keys.STATUS_COUNTS)
		private JsonNode statusCounts;
		@JsonProperty
		private JsonNode plan;

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (other == null) {
				return false;
			}
			if (other instanceof JobDetails) {
				JobDetails details = (JobDetails) other;
				// we ignore "now" since the value is generate dynamically when the json is created
				return jid.equals(details.jid) &&
					name.equals(details.name) &&
					isStoppable == details.isStoppable &&
					state.equals(details.state) &&
					startTime == details.startTime &&
					endTime == details.endTime &&
					duration == details.duration &&
					timestamps.equals(details.timestamps) &&
					vertices.equals(details.vertices) &&
					statusCounts.equals(details.statusCounts) &&
					plan.equals(details.plan);
			} else {
				return false;
			}
		}
	}

	private static class TaskDetails {
		@JsonProperty
		private String id;
		@JsonProperty
		private String name;
		@JsonProperty
		private long now;
		@JsonProperty
		private List<TaskByTaskManagerDetails> taskmanagers;

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (other == null) {
				return false;
			}
			if (other instanceof TaskDetails) {
				TaskDetails details = (TaskDetails) other;
				// we ignore "now" since the value is generate dynamically when the json is created
				return id.equals(details.id) &&
					name.equals(details.name) &&
					taskmanagers.equals(details.taskmanagers);
			} else {
				return false;
			}
		}
	}

	private static class TaskByTaskManagerDetails {
		@JsonProperty
		private String host;
		@JsonProperty
		private String status;
		@JsonProperty(JsonUtils.Keys.START_TIME)
		private long startTime;
		@JsonProperty(JsonUtils.Keys.END_TIME)
		private long endTime;
		@JsonProperty
		private long duration;
		@JsonProperty
		private JsonNode metrics;
		@JsonProperty(JsonUtils.Keys.STATUS_COUNTS)
		private JsonNode statusCounts;

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (other == null) {
				return false;
			}
			if (other instanceof TaskByTaskManagerDetails) {
				TaskByTaskManagerDetails details = (TaskByTaskManagerDetails) other;
				// we ignore "duration" since the value is under certain circumstances
				// generated dynamically when the json is created
				return host.equals(details.host) &&
					status.equals(details.status) &&
					startTime == details.startTime &&
					endTime == details.endTime &&
					metrics.equals(details.metrics) &&
					statusCounts.equals(details.statusCounts);
			} else {
				return false;
			}
		}
	}

	private static class TaskSubtaskDetails {
		@JsonProperty
		private String id;
		@JsonProperty
		private String name;
		@JsonProperty
		private long now;
		@JsonProperty
		private List<SubtaskDetails> subtasks;

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (other == null) {
				return false;
			}
			if (other instanceof TaskSubtaskDetails) {
				TaskSubtaskDetails details = (TaskSubtaskDetails) other;
				// we ignore "now" since the value is generate dynamically when the json is created
				return id.equals(details.id) &&
					name.equals(details.name) &&
					subtasks.equals(details.subtasks);
			} else {
				return false;
			}
		}
	}

	private static class SubtaskDetails {
		@JsonProperty
		private int subtask;
		@JsonProperty
		private String host;
		@JsonProperty
		private long duration;
		@JsonProperty
		private JsonNode timestamps;

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (other == null) {
				return false;
			}
			if (other instanceof SubtaskDetails) {
				SubtaskDetails details = (SubtaskDetails) other;
				// we ignore "duration" since the value is under certain circumstances
				// generated dynamically when the json is created
				return subtask == details.subtask &&
					host.equals(details.host) &&
					timestamps.equals(details.timestamps);
			} else {
				return false;
			}
		}
	}
}
