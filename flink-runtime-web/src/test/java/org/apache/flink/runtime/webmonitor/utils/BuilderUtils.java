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

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class BuilderUtils {
	private static final Random RANDOM = new Random();

	private BuilderUtils() {
	}

	public static ArchivedExecutionConfigBuilder createArchivedExecutionConfig() {
		return new ArchivedExecutionConfigBuilder();
	}

	public static class ArchivedExecutionConfigBuilder {
		private String executionMode;
		private String restartStrategyDescription;
		private int parallelism;
		private boolean objectReuseEnabled;
		private Map<String, String> globalJobParameters;

		private ArchivedExecutionConfigBuilder() {
		}

		public ArchivedExecutionConfigBuilder setExecutionMode(String executionMode) {
			this.executionMode = executionMode;
			return this;
		}

		public ArchivedExecutionConfigBuilder setRestartStrategyDescription(String restartStrategyDescription) {
			this.restartStrategyDescription = restartStrategyDescription;
			return this;
		}

		public ArchivedExecutionConfigBuilder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public ArchivedExecutionConfigBuilder setObjectReuseEnabled(boolean objectReuseEnabled) {
			this.objectReuseEnabled = objectReuseEnabled;
			return this;
		}

		public ArchivedExecutionConfigBuilder setGlobalJobParameters(Map<String, String> globalJobParameters) {
			this.globalJobParameters = globalJobParameters;
			return this;
		}

		public ArchivedExecutionConfig finish() {
			return new ArchivedExecutionConfig(
				executionMode != null ? executionMode : ExecutionMode.PIPELINED.name(),
				restartStrategyDescription != null ? restartStrategyDescription : "default",
				parallelism,
				objectReuseEnabled,
				globalJobParameters != null ? globalJobParameters : Collections.<String, String>emptyMap()
			);
		}
	}

	public static ArchivedExecutionGraphBuilder createArchivedExecutionGraph() {
		return new ArchivedExecutionGraphBuilder();
	}

	public static class ArchivedExecutionGraphBuilder {
		private JobID jobID;
		private String jobName;
		private Map<JobVertexID, ArchivedExecutionJobVertex> tasks;
		private List<ArchivedExecutionJobVertex> verticesInCreationOrder;
		private long[] stateTimestamps;
		private JobStatus state;
		private String failureCause;
		private String jsonPlan;
		private StringifiedAccumulatorResult[] archivedUserAccumulators;
		private ArchivedExecutionConfig archivedExecutionConfig;
		private boolean isStoppable;
		private Map<String, SerializedValue<Object>> serializedUserAccumulators;

		private ArchivedExecutionGraphBuilder() {
		}

		public ArchivedExecutionGraphBuilder setJobID(JobID jobID) {
			this.jobID = jobID;
			return this;
		}

		public ArchivedExecutionGraphBuilder setJobName(String jobName) {
			this.jobName = jobName;
			return this;
		}

		public ArchivedExecutionGraphBuilder setTasks(Map<JobVertexID, ArchivedExecutionJobVertex> tasks) {
			this.tasks = tasks;
			return this;
		}

		public ArchivedExecutionGraphBuilder setVerticesInCreationOrder(List<ArchivedExecutionJobVertex> verticesInCreationOrder) {
			this.verticesInCreationOrder = verticesInCreationOrder;
			return this;
		}

		public ArchivedExecutionGraphBuilder setStateTimestamps(long[] stateTimestamps) {
			this.stateTimestamps = stateTimestamps;
			return this;
		}

		public ArchivedExecutionGraphBuilder setState(JobStatus state) {
			this.state = state;
			return this;
		}

		public ArchivedExecutionGraphBuilder setFailureCause(String failureCause) {
			this.failureCause = failureCause;
			return this;
		}

		public ArchivedExecutionGraphBuilder setJsonPlan(String jsonPlan) {
			this.jsonPlan = jsonPlan;
			return this;
		}

		public ArchivedExecutionGraphBuilder setArchivedUserAccumulators(StringifiedAccumulatorResult[] archivedUserAccumulators) {
			this.archivedUserAccumulators = archivedUserAccumulators;
			return this;
		}

		public ArchivedExecutionGraphBuilder setArchivedExecutionConfig(ArchivedExecutionConfig archivedExecutionConfig) {
			this.archivedExecutionConfig = archivedExecutionConfig;
			return this;
		}

		public ArchivedExecutionGraphBuilder setStoppable(boolean stoppable) {
			isStoppable = stoppable;
			return this;
		}

		public ArchivedExecutionGraphBuilder setSerializedUserAccumulators(Map<String, SerializedValue<Object>> serializedUserAccumulators) {
			this.serializedUserAccumulators = serializedUserAccumulators;
			return this;
		}

		public ArchivedExecutionGraph finish() {
			Preconditions.checkNotNull(tasks, "Tasks must not be null.");
			JobID jobID = this.jobID != null ? this.jobID : new JobID();
			String jobName = this.jobName != null ? this.jobName : "job_" + RANDOM.nextInt();
			return new ArchivedExecutionGraph(
				jobID,
				jobName,
				tasks,
				verticesInCreationOrder != null ? verticesInCreationOrder : new ArrayList<>(tasks.values()),
				stateTimestamps != null ? stateTimestamps : new long[]{1, 2, 3, 4, 5, 5, 5, 5, 0},
				state != null ? state : JobStatus.FINISHED,
				failureCause != null ? failureCause : "(null)",
				jsonPlan != null ? jsonPlan : "{\"" + JsonUtils.Keys.JOB_ID + "\":\"" + jobID + "\", \"" + JsonUtils.Keys.NAME + "\":\"" + jobName + "\", \"nodes\":[]}",
				archivedUserAccumulators != null ? archivedUserAccumulators : new StringifiedAccumulatorResult[0],
				serializedUserAccumulators != null ? serializedUserAccumulators : Collections.<String, SerializedValue<Object>>emptyMap(),
				archivedExecutionConfig != null ? archivedExecutionConfig : new ArchivedExecutionConfigBuilder().finish(),
				isStoppable,
				null,
				null
			);
		}
	}

	public static ArchivedExecutionJobVertexBuilder createArchivedExecutionJobVertex() {
		return new ArchivedExecutionJobVertexBuilder();
	}

	public static class ArchivedExecutionJobVertexBuilder {
		private ArchivedExecutionVertex[] taskVertices;
		private JobVertexID id;
		private String name;
		private int parallelism;
		private int maxParallelism;
		private StringifiedAccumulatorResult[] archivedUserAccumulators;

		private ArchivedExecutionJobVertexBuilder() {
		}

		public ArchivedExecutionJobVertexBuilder setTaskVertices(ArchivedExecutionVertex[] taskVertices) {
			this.taskVertices = taskVertices;
			return this;
		}

		public ArchivedExecutionJobVertexBuilder setId(JobVertexID id) {
			this.id = id;
			return this;
		}

		public ArchivedExecutionJobVertexBuilder setName(String name) {
			this.name = name;
			return this;
		}

		public ArchivedExecutionJobVertexBuilder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public ArchivedExecutionJobVertexBuilder setMaxParallelism(int maxParallelism) {
			this.maxParallelism = maxParallelism;
			return this;
		}

		public ArchivedExecutionJobVertexBuilder setArchivedUserAccumulators(StringifiedAccumulatorResult[] archivedUserAccumulators) {
			this.archivedUserAccumulators = archivedUserAccumulators;
			return this;
		}

		public ArchivedExecutionJobVertex finish() {
			Preconditions.checkNotNull(taskVertices);
			return new ArchivedExecutionJobVertex(
				taskVertices,
				id != null ? id : new JobVertexID(),
				name != null ? name : "task_" + RANDOM.nextInt(),
				parallelism,
				maxParallelism,
				archivedUserAccumulators != null ? archivedUserAccumulators : new StringifiedAccumulatorResult[0]
			);
		}

	}

	public static ArchivedExecutionVertexBuilder createArchivedExecutionVertex() {
		return new ArchivedExecutionVertexBuilder();
	}

	public static class ArchivedExecutionVertexBuilder {
		private int subtaskIndex;
		private EvictingBoundedList<ArchivedExecution> priorExecutions;
		private String taskNameWithSubtask;
		private ArchivedExecution currentExecution;

		private ArchivedExecutionVertexBuilder() {
		}

		public ArchivedExecutionVertexBuilder setSubtaskIndex(int subtaskIndex) {
			this.subtaskIndex = subtaskIndex;
			return this;
		}

		public ArchivedExecutionVertexBuilder setPriorExecutions(List<ArchivedExecution> priorExecutions) {
			this.priorExecutions = new EvictingBoundedList<>(priorExecutions);
			return this;
		}

		public ArchivedExecutionVertexBuilder setTaskNameWithSubtask(String taskNameWithSubtask) {
			this.taskNameWithSubtask = taskNameWithSubtask;
			return this;
		}

		public ArchivedExecutionVertexBuilder setCurrentExecution(ArchivedExecution currentExecution) {
			this.currentExecution = currentExecution;
			return this;
		}

		public ArchivedExecutionVertex finish() {
			Preconditions.checkNotNull(currentExecution);
			return new ArchivedExecutionVertex(
				subtaskIndex,
				taskNameWithSubtask != null ? taskNameWithSubtask : "task_" + RANDOM.nextInt() + "_" + subtaskIndex,
				currentExecution,
				priorExecutions != null ? priorExecutions : new EvictingBoundedList<ArchivedExecution>(0)
			);
		}
	}

	public static ArchivedExecutionBuilder createArchivedExecution() {
		return new ArchivedExecutionBuilder();
	}

	public static class ArchivedExecutionBuilder {
		private ExecutionAttemptID attemptId;
		private long[] stateTimestamps;
		private int attemptNumber;
		private ExecutionState state;
		private String failureCause;
		private TaskManagerLocation assignedResourceLocation;
		private StringifiedAccumulatorResult[] userAccumulators;
		private IOMetrics ioMetrics;
		private int parallelSubtaskIndex;

		private ArchivedExecutionBuilder() {
		}

		public ArchivedExecutionBuilder setAttemptId(ExecutionAttemptID attemptId) {
			this.attemptId = attemptId;
			return this;
		}

		public ArchivedExecutionBuilder setStateTimestamps(long[] stateTimestamps) {
			Preconditions.checkArgument(stateTimestamps.length == ExecutionState.values().length);
			this.stateTimestamps = stateTimestamps;
			return this;
		}

		public ArchivedExecutionBuilder setAttemptNumber(int attemptNumber) {
			this.attemptNumber = attemptNumber;
			return this;
		}

		public ArchivedExecutionBuilder setState(ExecutionState state) {
			this.state = state;
			return this;
		}

		public ArchivedExecutionBuilder setFailureCause(String failureCause) {
			this.failureCause = failureCause;
			return this;
		}

		public ArchivedExecutionBuilder setAssignedResourceLocation(TaskManagerLocation assignedResourceLocation) {
			this.assignedResourceLocation = assignedResourceLocation;
			return this;
		}

		public ArchivedExecutionBuilder setUserAccumulators(StringifiedAccumulatorResult[] userAccumulators) {
			this.userAccumulators = userAccumulators;
			return this;
		}

		public ArchivedExecutionBuilder setParallelSubtaskIndex(int parallelSubtaskIndex) {
			this.parallelSubtaskIndex = parallelSubtaskIndex;
			return this;
		}

		public ArchivedExecutionBuilder setIOMetrics(IOMetrics ioMetrics) {
			this.ioMetrics = ioMetrics;
			return this;
		}

		public ArchivedExecution finish() throws UnknownHostException {
			return new ArchivedExecution(
				userAccumulators != null ? userAccumulators : new StringifiedAccumulatorResult[0],
				ioMetrics != null ? ioMetrics : new TestIOMetrics(),
				attemptId != null ? attemptId : new ExecutionAttemptID(),
				attemptNumber,
				state != null ? state : ExecutionState.FINISHED,
				failureCause != null ? failureCause : "(null)",
				assignedResourceLocation != null ? assignedResourceLocation : new TaskManagerLocation(new ResourceID("tm"), InetAddress.getLocalHost(), 1234),
				parallelSubtaskIndex,
				stateTimestamps != null ? stateTimestamps : new long[]{1, 2, 3, 4, 5, 5, 5, 5}
			);
		}
	}

	private static class TestIOMetrics extends IOMetrics {
		private static final long serialVersionUID = -5920076211680012555L;

		public TestIOMetrics() {
			super(
				new MeterView(new TestCounter(1), 0),
				new MeterView(new TestCounter(2), 0),
				new MeterView(new TestCounter(3), 0),
				new MeterView(new TestCounter(4), 0),
				new MeterView(new TestCounter(5), 0));
		}
	}

	private static class TestCounter implements Counter {
		private final long count;

		private TestCounter(long count) {
			this.count = count;
		}

		@Override
		public void inc() {
		}

		@Override
		public void inc(long n) {
		}

		@Override
		public void dec() {
		}

		@Override
		public void dec(long n) {
		}

		@Override
		public long getCount() {
			return count;
		}
	}
}
