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

package org.apache.flink.runtime.rest.handler.legacy.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Common entry-point for accessing generated ArchivedExecution* components.
 */
public class ArchivedJobGenerationUtils {
	public static final ObjectMapper MAPPER = new ObjectMapper();
	public static final JsonFactory JACKSON_FACTORY = new JsonFactory()
		.enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
		.disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT);

	private static ArchivedExecutionGraph originalJob;
	private static ArchivedExecutionJobVertex originalTask;
	private static ArchivedExecutionVertex originalSubtask;
	private static ArchivedExecution originalAttempt;

	private static final Object lock = new Object();

	private ArchivedJobGenerationUtils() {
	}

	public static AccessExecutionGraph getTestJob() throws Exception {
		synchronized (lock) {
			if (originalJob == null) {
				generateArchivedJob();
			}
		}
		return originalJob;
	}

	public static AccessExecutionJobVertex getTestTask() throws Exception {
		synchronized (lock) {
			if (originalJob == null) {
				generateArchivedJob();
			}
		}
		return originalTask;
	}

	public static AccessExecutionVertex getTestSubtask() throws Exception {
		synchronized (lock) {
			if (originalJob == null) {
				generateArchivedJob();
			}
		}
		return originalSubtask;
	}

	public static AccessExecution getTestAttempt() throws Exception {
		synchronized (lock) {
			if (originalJob == null) {
				generateArchivedJob();
			}
		}
		return originalAttempt;
	}

	private static void generateArchivedJob() throws Exception {
		// Attempt
		StringifiedAccumulatorResult acc1 = new StringifiedAccumulatorResult("name1", "type1", "value1");
		StringifiedAccumulatorResult acc2 = new StringifiedAccumulatorResult("name2", "type2", "value2");
		TaskManagerLocation location = new TaskManagerLocation(new ResourceID("hello"), InetAddress.getLocalHost(), 1234);
		originalAttempt = new ArchivedExecutionBuilder()
			.setStateTimestamps(new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9})
			.setParallelSubtaskIndex(1)
			.setAttemptNumber(0)
			.setAssignedResourceLocation(location)
			.setUserAccumulators(new StringifiedAccumulatorResult[]{acc1, acc2})
			.setState(ExecutionState.FINISHED)
			.setFailureCause("attemptException")
			.build();
		// Subtask
		originalSubtask = new ArchivedExecutionVertexBuilder()
			.setSubtaskIndex(originalAttempt.getParallelSubtaskIndex())
			.setTaskNameWithSubtask("hello(1/1)")
			.setCurrentExecution(originalAttempt)
			.build();
		// Task
		originalTask = new ArchivedExecutionJobVertexBuilder()
			.setTaskVertices(new ArchivedExecutionVertex[]{originalSubtask})
			.build();
		// Job
		Map<JobVertexID, ArchivedExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(originalTask.getJobVertexId(), originalTask);
		originalJob = new ArchivedExecutionGraphBuilder()
			.setJobID(new JobID())
			.setTasks(tasks)
			.setFailureCause(new ErrorInfo(new Exception("jobException"), originalAttempt.getStateTimestamp(ExecutionState.FAILED)))
			.setState(JobStatus.FINISHED)
			.setStateTimestamps(new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
			.setArchivedUserAccumulators(new StringifiedAccumulatorResult[]{acc1, acc2})
			.build();
	}

	// ========================================================================
	// utility methods
	// ========================================================================

	public static void compareStringifiedAccumulators(StringifiedAccumulatorResult[] expectedAccs, ArrayNode writtenAccs) {
		assertEquals(expectedAccs.length, writtenAccs.size());
		for (int x = 0; x < expectedAccs.length; x++) {
			JsonNode acc = writtenAccs.get(x);

			assertEquals(expectedAccs[x].getName(), acc.get("name").asText());
			assertEquals(expectedAccs[x].getType(), acc.get("type").asText());
			assertEquals(expectedAccs[x].getValue(), acc.get("value").asText());
		}
	}

	public static void compareIoMetrics(IOMetrics expectedMetrics, JsonNode writtenMetrics) {
		assertEquals(expectedMetrics.getNumBytesInTotal(), writtenMetrics.get("read-bytes").asLong());
		assertEquals(expectedMetrics.getNumBytesOut(), writtenMetrics.get("write-bytes").asLong());
		assertEquals(expectedMetrics.getNumRecordsIn(), writtenMetrics.get("read-records").asLong());
		assertEquals(expectedMetrics.getNumRecordsOut(), writtenMetrics.get("write-records").asLong());
	}
}
