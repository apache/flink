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

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointStatsCounts;
import org.apache.flink.runtime.checkpoint.CheckpointStatsHistory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStatsSummary;
import org.apache.flink.runtime.checkpoint.FailedCheckpointStats;
import org.apache.flink.runtime.checkpoint.MinMaxAvgStats;
import org.apache.flink.runtime.checkpoint.RestoredCheckpointStats;
import org.apache.flink.runtime.checkpoint.SubtaskStateStats;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.metrics.MetricFetcher;
import org.apache.flink.runtime.webmonitor.metrics.MetricStore;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class JsonUtils {
	static final int MAX_NUMBER_EXCEPTION_TO_REPORT = 20;

	@Public
	public static final class Keys {
		public static final String TASKMANAGERS = "taskmanagers";
		public static final String JOB_ID = "jid";
		public static final String ID = "id";
		public static final String NAME = "name";
		public static final String STATE = "state";
		public static final String IS_STOPPABLE = "isStoppable";
		public static final String PARALLELISM = "parallelism";
		public static final String PLAN = "plan";

		public static final String START_TIME = "start-time";
		public static final String END_TIME = "end-time";
		public static final String DURATION = "duration";
		public static final String NOW = "now";
		public static final String LAST_MODIFICATION = "last-modification";

		public static final String TIMESTAMP = "timestamp";
		public static final String TIMESTAMPS = "timestamps";
		public static final String STATUS_COUNTS = "status-counts";

		public static final String REFRESH_INTERVAL = "refresh-interval";
		public static final String TIMEZONE_OFFSET = "timezone-offset";
		public static final String TIMEZONE_NAME = "timezone-name";
		public static final String FLINK_VERSION = "flink-version";
		public static final String FLINK_REVISION = "flink-revision";

		public static final String EXECUTION_CONFIG = "execution-config";
		public static final String MODE = "mode";
		public static final String EXECUTION_MODE = "execution-mode";
		public static final String RESTART_STRATEGY = "restart-strategy";
		public static final String JOB_PARALLELISM = "job-parallelism";
		public static final String OBJECT_REUSE_MODE = "object-reuse-mode";
		public static final String USER_CONFIG = "user-config";

		public static final String ROOT_EXCEPTION = "root-exception";
		public static final String ALL_EXCEPTIONS = "all-exceptions";
		public static final String EXCEPTION = "exception";
		public static final String TRUNCATED = "truncated";

		public static final String HOST = "host";
		public static final String LOCATION = "location";

		public static final String VERTICES = "vertices";
		public static final String TASKS = "tasks";
		public static final String TASK = "task";
		public static final String SUBTASKS = "subtasks";
		public static final String SUBTASK = "subtask";
		public static final String ATTEMPT = "attempt";

		public static final String STATUS = "status";
		public static final String TOTAL = "total";
		public static final String PENDING = "pending";
		public static final String RUNNING = "running";
		public static final String FINISHED = "finished";
		public static final String CANCELING = "canceling";
		public static final String CANCELED = "canceled";
		public static final String FAILED = "failed";
		public static final String RESTORED = "restored";
		public static final String PENDING_OR_FAILED = "pending_or_failed";
		public static final String DISCARDED = "discarded";
		public static final String IN_PROGRESS = "in_progress";
		public static final String COMPLETED = "completed";

		public static final String METRICS = "metrics";
		public static final String WRITE_BYTES = "write-bytes";
		public static final String READ_BYTES = "read-bytes";
		public static final String WRITE_RECORDS = "write-records";
		public static final String READ_RECORDS = "read-records";
		public static final String TYPE = "type";
		public static final String VALUE = "value";

		public static final String MIN = "min";
		public static final String MAX = "max";
		public static final String AVG = "avg";

		public static final String JOB_ACCUMULATORS = "job-accumulators";
		public static final String USER_ACCUMULATORS = "user-accumulators";
		public static final String USER_TASK_ACCUMULATORS = "user-task-accumulators";
		
		public static final String COUNTS = "counts";
		public static final String EXTERNALIZATION = "externalization";
		public static final String EXTERNAL_PATH = "external-path";
		public static final String DELETE_ON_CANCEL = "delete_on_cancellation";
		public static final String HISTORY = "history";

		public static final String SUMMARY = "summary";
		public static final String STATE_SIZE = "state_size";
		public static final String ETE_DURATION = "end_to_end_duration";
		public static final String ALIGNMENT_BUFFERED = "alignment_buffered";
		public static final String SAVEPOINT = "savepoint";
		public static final String IS_SAVEPOINT = "is_savepoint";
		public static final String CHECKPOINT = "checkpoint";
		public static final String CHECKPOINT_DURATION = "checkpoint_duration";
		public static final String SYNC = "sync";
		public static final String ASYNC = "async";
		public static final String ALIGNMENT = "alignment";
		public static final String BUFFERED = "buffered";
		
		public static final String LATEST = "latest";
		
		public static final String FAILURE_TIMESTAMP = "failure_timestamp";
		public static final String FAILURE_MESSAGE = "failure_message";
		public static final String RESTORE_TIMESTAMP = "restore_timestamp";
		
		public static final String TRIGGER_TIMESTAMP = "trigger_timestamp";
		public static final String ACK_TIMESTAMP = "ack_timestamp";
		public static final String LATEST_ACK_TIMESTAMP = "latest_ack_timestamp";
		
		public static final String NUM_SUBTASKS = "num_subtasks";
		public static final String NUM_ACK_SUBTASKS = "num_acknowledged_subtasks";
		public static final String INDEX = "index";
		public static final String INTERVAL = "interval";
		public static final String ENABLED = "enabled";
		public static final String TIMEOUT = "timeout";
		public static final String MIN_PAUSE = "min_pause";
		public static final String MAX_CONCURRENT = "max_concurrent";

		private Keys() {
		}
	}

	public static void writeConfigAsJson(JsonGenerator gen, long refreshInterval) throws IOException 
	{
		TimeZone timeZone = TimeZone.getDefault();
		String timeZoneName = timeZone.getDisplayName();
		long timeZoneOffset = timeZone.getRawOffset();

		gen.writeStartObject();
		gen.writeNumberField(Keys.REFRESH_INTERVAL, refreshInterval);
		gen.writeNumberField(Keys.TIMEZONE_OFFSET, timeZoneOffset);
		gen.writeStringField(Keys.TIMEZONE_NAME, timeZoneName);
		gen.writeStringField(Keys.FLINK_VERSION, EnvironmentInformation.getVersion());

		EnvironmentInformation.RevisionInformation revision = EnvironmentInformation.getRevisionInformation();
		if (revision != null) {
			gen.writeStringField(Keys.FLINK_REVISION, revision.commitId + " @ " + revision.commitDate);
		}

		gen.writeEndObject();
	}

	public static void writeJobDetailOverviewAsJson(AccessExecutionGraph graph, JsonGenerator gen) throws IOException {
		writeJobDetailOverviewAsJson(WebMonitorUtils.createDetailsForJob(graph), gen, System.currentTimeMillis());
	}

	public static void writeJobDetailOverviewAsJson(JobDetails details, JsonGenerator gen, long now) throws IOException {
		gen.writeStartObject();

		gen.writeStringField(Keys.JOB_ID, details.getJobId().toString());
		gen.writeStringField(Keys.NAME, details.getJobName());
		gen.writeStringField(Keys.STATE, details.getStatus().name());

		gen.writeNumberField(Keys.START_TIME, details.getStartTime());
		gen.writeNumberField(Keys.END_TIME, details.getEndTime());
		gen.writeNumberField(Keys.DURATION, (details.getEndTime() <= 0 ? now : details.getEndTime()) - details.getStartTime());
		gen.writeNumberField(Keys.LAST_MODIFICATION, details.getLastUpdateTime());

		gen.writeObjectFieldStart(Keys.TASKS);
		gen.writeNumberField(Keys.TOTAL, details.getNumTasks());

		final int[] perState = details.getNumVerticesPerExecutionState();
		gen.writeNumberField(Keys.PENDING, perState[ExecutionState.CREATED.ordinal()] +
			perState[ExecutionState.SCHEDULED.ordinal()] +
			perState[ExecutionState.DEPLOYING.ordinal()]);
		gen.writeNumberField(Keys.RUNNING, perState[ExecutionState.RUNNING.ordinal()]);
		gen.writeNumberField(Keys.FINISHED, perState[ExecutionState.FINISHED.ordinal()]);
		gen.writeNumberField(Keys.CANCELING, perState[ExecutionState.CANCELING.ordinal()]);
		gen.writeNumberField(Keys.CANCELED, perState[ExecutionState.CANCELED.ordinal()]);
		gen.writeNumberField(Keys.FAILED, perState[ExecutionState.FAILED.ordinal()]);
		gen.writeEndObject();

		gen.writeEndObject();
	}

	public static void writeJobDetailsAsJson(AccessExecutionGraph graph, MetricFetcher fetcher, JsonGenerator gen) throws IOException {
		final long now = System.currentTimeMillis();

		gen.writeStartObject();

		// basic info
		gen.writeStringField(Keys.JOB_ID, graph.getJobID().toString());
		gen.writeStringField(Keys.NAME, graph.getJobName());
		gen.writeBooleanField(Keys.IS_STOPPABLE, graph.isStoppable());
		gen.writeStringField(Keys.STATE, graph.getState().name());

		// times and duration
		final long jobStartTime = graph.getStatusTimestamp(JobStatus.CREATED);
		final long jobEndTime = graph.getState().isGloballyTerminalState() ?
			graph.getStatusTimestamp(graph.getState()) : -1L;
		gen.writeNumberField(Keys.START_TIME, jobStartTime);
		gen.writeNumberField(Keys.END_TIME, jobEndTime);
		gen.writeNumberField(Keys.DURATION, (jobEndTime > 0 ? jobEndTime : now) - jobStartTime);
		gen.writeNumberField(Keys.NOW, now);

		// timestamps
		gen.writeObjectFieldStart(Keys.TIMESTAMPS);
		for (JobStatus status : JobStatus.values()) {
			gen.writeNumberField(status.name(), graph.getStatusTimestamp(status));
		}
		gen.writeEndObject();

		// job vertices
		int[] jobVerticesPerState = new int[ExecutionState.values().length];
		gen.writeArrayFieldStart(Keys.VERTICES);

		for (AccessExecutionJobVertex ejv : graph.getVerticesTopologically()) {
			int[] tasksPerState = new int[ExecutionState.values().length];
			long startTime = Long.MAX_VALUE;
			long endTime = 0;
			boolean allFinished = true;

			for (AccessExecutionVertex vertex : ejv.getTaskVertices()) {
				final ExecutionState state = vertex.getExecutionState();
				tasksPerState[state.ordinal()]++;

				// take the earliest start time
				long started = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
				if (started > 0) {
					startTime = Math.min(startTime, started);
				}

				allFinished &= state.isTerminal();
				endTime = Math.max(endTime, vertex.getStateTimestamp(state));
			}

			long duration;
			if (startTime < Long.MAX_VALUE) {
				if (allFinished) {
					duration = endTime - startTime;
				} else {
					endTime = -1L;
					duration = now - startTime;
				}
			} else {
				startTime = -1L;
				endTime = -1L;
				duration = -1L;
			}

			ExecutionState jobVertexState =
				ExecutionJobVertex.getAggregateJobVertexState(tasksPerState, ejv.getParallelism());
			jobVerticesPerState[jobVertexState.ordinal()]++;

			gen.writeStartObject();
			gen.writeStringField(Keys.ID, ejv.getJobVertexId().toString());
			gen.writeStringField(Keys.NAME, ejv.getName());
			gen.writeNumberField(Keys.PARALLELISM, ejv.getParallelism());
			gen.writeStringField(Keys.STATE, jobVertexState.name());

			gen.writeNumberField(Keys.START_TIME, startTime);
			gen.writeNumberField(Keys.END_TIME, endTime);
			gen.writeNumberField(Keys.DURATION, duration);

			gen.writeObjectFieldStart(Keys.TASKS);
			for (ExecutionState state : ExecutionState.values()) {
				gen.writeNumberField(state.name(), tasksPerState[state.ordinal()]);
			}
			gen.writeEndObject();

			MutableIOMetrics counts = new MutableIOMetrics();

			for (AccessExecutionVertex vertex : ejv.getTaskVertices()) {
				addIOMetrics(
					counts, 
					vertex.getCurrentExecutionAttempt().getState(),
					vertex.getCurrentExecutionAttempt().getIOMetrics(),
					fetcher,
					graph.getJobID().toString(),
					ejv.getJobVertexId().toString(),
					vertex.getParallelSubtaskIndex());
			}

			writeIOMetrics(gen, counts);

			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeObjectFieldStart(Keys.STATUS_COUNTS);
		for (ExecutionState state : ExecutionState.values()) {
			gen.writeNumberField(state.name(), jobVerticesPerState[state.ordinal()]);
		}
		gen.writeEndObject();

		gen.writeFieldName(Keys.PLAN);
		gen.writeRawValue(graph.getJsonPlan());

		gen.writeEndObject();
	}

	public static void writeJobConfigAsJson(AccessExecutionGraph graph, JsonGenerator gen) throws IOException {
		gen.writeStartObject();
		gen.writeStringField(Keys.JOB_ID, graph.getJobID().toString());
		gen.writeStringField(Keys.NAME, graph.getJobName());

		final ArchivedExecutionConfig summary = graph.getArchivedExecutionConfig();

		if (summary != null) {
			gen.writeObjectFieldStart(Keys.EXECUTION_CONFIG);

			gen.writeStringField(Keys.EXECUTION_MODE, summary.getExecutionMode());

			gen.writeStringField(Keys.RESTART_STRATEGY, summary.getRestartStrategyDescription());
			gen.writeNumberField(Keys.JOB_PARALLELISM, summary.getParallelism());
			gen.writeBooleanField(Keys.OBJECT_REUSE_MODE, summary.getObjectReuseEnabled());

			Map<String, String> ucVals = summary.getGlobalJobParameters();
			if (ucVals != null) {
				gen.writeObjectFieldStart(Keys.USER_CONFIG);

				for (Map.Entry<String, String> ucVal : ucVals.entrySet()) {
					gen.writeStringField(ucVal.getKey(), ucVal.getValue());
				}

				gen.writeEndObject();
			}

			gen.writeEndObject();
		}
		gen.writeEndObject();

	}

	public static void writeJobExceptionsAsJson(AccessExecutionGraph graph, JsonGenerator gen) throws IOException {

		gen.writeStartObject();

		// most important is the root failure cause
		String rootException = graph.getFailureCauseAsString();
		if (!rootException.equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
			gen.writeStringField(Keys.ROOT_EXCEPTION, rootException);
		}

		// we additionally collect all exceptions (up to a limit) that occurred in the individual tasks
		gen.writeArrayFieldStart(Keys.ALL_EXCEPTIONS);

		int numExceptionsSoFar = 0;
		boolean truncated = false;

		for (AccessExecutionVertex task : graph.getAllExecutionVertices()) {
			String t = task.getFailureCauseAsString();
			if (!t.equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
				if (numExceptionsSoFar >= MAX_NUMBER_EXCEPTION_TO_REPORT) {
					truncated = true;
					break;
				}

				TaskManagerLocation location = task.getCurrentAssignedResourceLocation();
				String locationString = location != null ?
					location.getFQDNHostname() + ':' + location.dataPort() : "(unassigned)";

				gen.writeStartObject();
				gen.writeStringField(Keys.EXCEPTION, t);
				gen.writeStringField(Keys.TASK, task.getTaskNameWithSubtaskIndex());
				gen.writeStringField(Keys.LOCATION, locationString);
				gen.writeEndObject();
				numExceptionsSoFar++;
			}
		}
		gen.writeEndArray();

		gen.writeBooleanField(Keys.TRUNCATED, truncated);
		gen.writeEndObject();
	}

	public static void writeJobAccumulatorsAsJson(AccessExecutionGraph graph, JsonGenerator gen) throws IOException {
		StringifiedAccumulatorResult[] allAccumulators = graph.getAccumulatorResultsStringified();

		gen.writeStartObject();

		gen.writeArrayFieldStart(Keys.JOB_ACCUMULATORS);
		// empty for now
		gen.writeEndArray();

		gen.writeArrayFieldStart(Keys.USER_TASK_ACCUMULATORS);
		for (StringifiedAccumulatorResult acc : allAccumulators) {
			gen.writeStartObject();
			gen.writeStringField(Keys.NAME, acc.getName());
			gen.writeStringField(Keys.TYPE, acc.getType());
			gen.writeStringField(Keys.VALUE, acc.getValue());
			gen.writeEndObject();
		}
		gen.writeEndArray();
		gen.writeEndObject();

	}

	public static void writeVertexDetailsAsJson(AccessExecutionJobVertex jobVertex, String jobID, MetricFetcher fetcher, JsonGenerator gen) throws IOException {
		final long now = System.currentTimeMillis();

		gen.writeStartObject();

		gen.writeStringField(Keys.ID, jobVertex.getJobVertexId().toString());
		gen.writeStringField(Keys.NAME, jobVertex.getName());
		gen.writeNumberField(Keys.PARALLELISM, jobVertex.getParallelism());
		gen.writeNumberField(Keys.NOW, now);

		gen.writeArrayFieldStart(Keys.SUBTASKS);
		int num = 0;
		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
			final ExecutionState status = vertex.getExecutionState();

			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname() + ":" + location.dataPort();

			long startTime = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
			if (startTime == 0) {
				startTime = -1;
			}
			long endTime = status.isTerminal() ? vertex.getStateTimestamp(status) : -1;
			long duration = startTime > 0 ? ((endTime > 0 ? endTime : now) - startTime) : -1;

			gen.writeStartObject();
			gen.writeNumberField(Keys.SUBTASK, num);
			gen.writeStringField(Keys.STATUS, status.name());
			gen.writeNumberField(Keys.ATTEMPT, vertex.getCurrentExecutionAttempt().getAttemptNumber());
			gen.writeStringField(Keys.HOST, locationString);
			gen.writeNumberField(Keys.START_TIME, startTime);
			gen.writeNumberField(Keys.END_TIME, endTime);
			gen.writeNumberField(Keys.DURATION, duration);

			MutableIOMetrics counts = new MutableIOMetrics();

			addIOMetrics(
				counts,
				status,
				vertex.getCurrentExecutionAttempt().getIOMetrics(),
				fetcher,
				jobID,
				jobVertex.getJobVertexId().toString(),
				vertex.getParallelSubtaskIndex()
			);

			writeIOMetrics(gen, counts);

			gen.writeEndObject();

			num++;
		}
		gen.writeEndArray();

		gen.writeEndObject();
	}

	public static void writeVertexAccumulatorsAsJson(AccessExecutionJobVertex jobVertex, JsonGenerator gen) throws IOException {
		StringifiedAccumulatorResult[] accs = jobVertex.getAggregatedUserAccumulatorsStringified();

		gen.writeStartObject();
		gen.writeStringField(Keys.ID, jobVertex.getJobVertexId().toString());

		gen.writeArrayFieldStart(Keys.USER_ACCUMULATORS);
		for (StringifiedAccumulatorResult acc : accs) {
			gen.writeStartObject();
			gen.writeStringField(Keys.NAME, acc.getName());
			gen.writeStringField(Keys.TYPE, acc.getType());
			gen.writeStringField(Keys.VALUE, acc.getValue());
			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeEndObject();
	}

	public static void writeSubtasksAccumulatorsAsJson(AccessExecutionJobVertex jobVertex, JsonGenerator gen) throws IOException {

		gen.writeStartObject();
		gen.writeStringField(Keys.ID, jobVertex.getJobVertexId().toString());
		gen.writeNumberField(Keys.PARALLELISM, jobVertex.getParallelism());

		gen.writeArrayFieldStart(Keys.SUBTASKS);

		int num = 0;
		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {

			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname();

			gen.writeStartObject();

			gen.writeNumberField(Keys.SUBTASK, num++);
			gen.writeNumberField(Keys.ATTEMPT, vertex.getCurrentExecutionAttempt().getAttemptNumber());
			gen.writeStringField(Keys.HOST, locationString);

			StringifiedAccumulatorResult[] accs = vertex.getCurrentExecutionAttempt().getUserAccumulatorsStringified();
			gen.writeArrayFieldStart(Keys.USER_ACCUMULATORS);
			for (StringifiedAccumulatorResult acc : accs) {
				gen.writeStartObject();
				gen.writeStringField(Keys.NAME, acc.getName());
				gen.writeStringField(Keys.TYPE, acc.getType());
				gen.writeStringField(Keys.VALUE, acc.getValue());
				gen.writeEndObject();
			}
			gen.writeEndArray();

			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeEndObject();
	}

	public static void writeSubtaskDetailsAsJson(AccessExecutionJobVertex jobVertex, JsonGenerator gen) throws IOException {
		final long now = System.currentTimeMillis();

		gen.writeStartObject();

		gen.writeStringField(Keys.ID, jobVertex.getJobVertexId().toString());
		gen.writeStringField(Keys.NAME, jobVertex.getName());
		gen.writeNumberField(Keys.NOW, now);

		gen.writeArrayFieldStart(Keys.SUBTASKS);

		int num = 0;
		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {

			long[] timestamps = vertex.getCurrentExecutionAttempt().getStateTimestamps();
			ExecutionState status = vertex.getExecutionState();

			long scheduledTime = timestamps[ExecutionState.SCHEDULED.ordinal()];

			long start = scheduledTime > 0 ? scheduledTime : -1;
			long end = status.isTerminal() ? timestamps[status.ordinal()] : now;
			long duration = start >= 0 ? end - start : -1L;

			gen.writeStartObject();
			gen.writeNumberField(Keys.SUBTASK, num++);

			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname();
			gen.writeStringField(Keys.HOST, locationString);

			gen.writeNumberField(Keys.DURATION, duration);

			gen.writeObjectFieldStart(Keys.TIMESTAMPS);
			for (ExecutionState state : ExecutionState.values()) {
				gen.writeNumberField(state.name(), timestamps[state.ordinal()]);
			}
			gen.writeEndObject();

			gen.writeEndObject();
		}

		gen.writeEndArray();
		gen.writeEndObject();
	}

	public static void writeAttemptDetails(AccessExecution execAttempt, String jobID, String vertexID, MetricFetcher fetcher, JsonGenerator gen) throws IOException {
		final ExecutionState status = execAttempt.getState();
		final long now = System.currentTimeMillis();

		TaskManagerLocation location = execAttempt.getAssignedResourceLocation();
		String locationString = location == null ? "(unassigned)" : location.getHostname();

		long startTime = execAttempt.getStateTimestamp(ExecutionState.DEPLOYING);
		if (startTime == 0) {
			startTime = -1;
		}
		long endTime = status.isTerminal() ? execAttempt.getStateTimestamp(status) : -1;
		long duration = startTime > 0 ? ((endTime > 0 ? endTime : now) - startTime) : -1;

		gen.writeStartObject();
		gen.writeNumberField(Keys.SUBTASK, execAttempt.getParallelSubtaskIndex());
		gen.writeStringField(Keys.STATUS, status.name());
		gen.writeNumberField(Keys.ATTEMPT, execAttempt.getAttemptNumber());
		gen.writeStringField(Keys.HOST, locationString);
		gen.writeNumberField(Keys.START_TIME, startTime);
		gen.writeNumberField(Keys.END_TIME, endTime);
		gen.writeNumberField(Keys.DURATION, duration);

		MutableIOMetrics counts = new MutableIOMetrics();

		addIOMetrics(
			counts,
			execAttempt.getState(),
			execAttempt.getIOMetrics(),
			fetcher,
			jobID,
			vertexID,
			execAttempt.getParallelSubtaskIndex()
		);

		writeIOMetrics(gen, counts);

		gen.writeEndObject();
	}

	public static void writeAttemptAccumulators(AccessExecution execAttempt, JsonGenerator gen) throws IOException {
		final StringifiedAccumulatorResult[] accs = execAttempt.getUserAccumulatorsStringified();

		gen.writeStartObject();

		gen.writeNumberField(Keys.SUBTASK, execAttempt.getParallelSubtaskIndex());
		gen.writeNumberField(Keys.ATTEMPT, execAttempt.getAttemptNumber());
		gen.writeStringField(Keys.ID, execAttempt.getAttemptId().toString());

		gen.writeArrayFieldStart(Keys.USER_ACCUMULATORS);
		for (StringifiedAccumulatorResult acc : accs) {
			gen.writeStartObject();
			gen.writeStringField(Keys.NAME, acc.getName());
			gen.writeStringField(Keys.TYPE, acc.getType());
			gen.writeStringField(Keys.VALUE, acc.getValue());
			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeEndObject();
	}

	public static void writeVertexDetailsByTaskManagerAsJson(AccessExecutionJobVertex jobVertex, String jobID, MetricFetcher fetcher, JsonGenerator gen) throws IOException {
		// Build a map that groups tasks by TaskManager
		Map<String, List<AccessExecutionVertex>> taskManagerVertices = new HashMap<>();

		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String taskManager = location == null ? "(unassigned)" : location.getHostname() + ":" + location.dataPort();

			List<AccessExecutionVertex> vertices = taskManagerVertices.get(taskManager);

			if (vertices == null) {
				vertices = new ArrayList<>();
				taskManagerVertices.put(taskManager, vertices);
			}

			vertices.add(vertex);
		}

		// Build JSON response
		final long now = System.currentTimeMillis();


		gen.writeStartObject();

		gen.writeStringField(Keys.ID, jobVertex.getJobVertexId().toString());
		gen.writeStringField(Keys.NAME, jobVertex.getName());
		gen.writeNumberField(Keys.NOW, now);

		gen.writeArrayFieldStart(Keys.TASKMANAGERS);
		for (Map.Entry<String, List<AccessExecutionVertex>> entry : taskManagerVertices.entrySet()) {
			String host = entry.getKey();
			List<AccessExecutionVertex> taskVertices = entry.getValue();

			int[] tasksPerState = new int[ExecutionState.values().length];

			long startTime = Long.MAX_VALUE;
			long endTime = 0;
			boolean allFinished = true;

			MutableIOMetrics counts = new MutableIOMetrics();

			for (AccessExecutionVertex vertex : taskVertices) {
				final ExecutionState state = vertex.getExecutionState();
				tasksPerState[state.ordinal()]++;

				// take the earliest start time
				long started = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
				if (started > 0) {
					startTime = Math.min(startTime, started);
				}

				allFinished &= state.isTerminal();
				endTime = Math.max(endTime, vertex.getStateTimestamp(state));

				addIOMetrics(
					counts, 
					vertex.getExecutionState(), 
					vertex.getCurrentExecutionAttempt().getIOMetrics(),
					fetcher,
					jobID,
					jobVertex.getJobVertexId().toString(),
					vertex.getParallelSubtaskIndex());
			}

			long duration;
			if (startTime < Long.MAX_VALUE) {
				if (allFinished) {
					duration = endTime - startTime;
				} else {
					endTime = -1L;
					duration = now - startTime;
				}
			} else {
				startTime = -1L;
				endTime = -1L;
				duration = -1L;
			}

			ExecutionState jobVertexState =
				ExecutionJobVertex.getAggregateJobVertexState(tasksPerState, taskVertices.size());

			gen.writeStartObject();

			gen.writeStringField(Keys.HOST, host);
			gen.writeStringField(Keys.STATUS, jobVertexState.name());

			gen.writeNumberField(Keys.START_TIME, startTime);
			gen.writeNumberField(Keys.END_TIME, endTime);
			gen.writeNumberField(Keys.DURATION, duration);

			writeIOMetrics(gen, counts);

			gen.writeObjectFieldStart(Keys.STATUS_COUNTS);
			for (ExecutionState state : ExecutionState.values()) {
				gen.writeNumberField(state.name(), tasksPerState[state.ordinal()]);
			}
			gen.writeEndObject();

			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeEndObject();
	}

	public static void writeCheckpointStatsAsJson(AccessExecutionGraph graph, JsonGenerator gen) throws IOException {
		CheckpointStatsSnapshot snapshot = graph.getCheckpointStatsSnapshot();
		
		if (snapshot == null) {
			gen.writeRaw("{}");
		} else {
			gen.writeStartObject();

			// Counts
			writeCounts(gen, snapshot.getCounts());

			// Summary
			writeSummary(gen, snapshot.getSummaryStats());
			CompletedCheckpointStatsSummary summary = snapshot.getSummaryStats();
			gen.writeObjectFieldStart(Keys.SUMMARY);
			gen.writeObjectFieldStart(Keys.STATE_SIZE);
			writeMinMaxAvg(gen, summary.getStateSizeStats());
			gen.writeEndObject();

			gen.writeObjectFieldStart(Keys.ETE_DURATION);
			writeMinMaxAvg(gen, summary.getEndToEndDurationStats());
			gen.writeEndObject();

			gen.writeObjectFieldStart(Keys.ALIGNMENT_BUFFERED);
			writeMinMaxAvg(gen, summary.getAlignmentBufferedStats());
			gen.writeEndObject();
			gen.writeEndObject();

			CheckpointStatsHistory history = snapshot.getHistory();

			// Latest
			writeLatestCheckpoints(
				gen,
				history.getLatestCompletedCheckpoint(),
				history.getLatestSavepoint(),
				history.getLatestFailedCheckpoint(),
				snapshot.getLatestRestoredCheckpoint());

			// History
			writeHistory(gen, snapshot.getHistory());

			gen.writeEndObject();
		}
	}

	private static void writeCounts(JsonGenerator gen, CheckpointStatsCounts counts) throws IOException {
		gen.writeObjectFieldStart(Keys.COUNTS);
		gen.writeNumberField(Keys.RESTORED, counts.getNumberOfRestoredCheckpoints());
		gen.writeNumberField(Keys.TOTAL, counts.getTotalNumberOfCheckpoints());
		gen.writeNumberField(Keys.IN_PROGRESS, counts.getNumberOfInProgressCheckpoints());
		gen.writeNumberField(Keys.COMPLETED, counts.getNumberOfCompletedCheckpoints());
		gen.writeNumberField(Keys.FAILED, counts.getNumberOfFailedCheckpoints());
		gen.writeEndObject();
	}

	private static void writeSummary(
		JsonGenerator gen,
		CompletedCheckpointStatsSummary summary) throws IOException {
		gen.writeObjectFieldStart(Keys.SUMMARY);
		gen.writeObjectFieldStart(Keys.STATE_SIZE);
		writeMinMaxAvg(gen, summary.getStateSizeStats());
		gen.writeEndObject();

		gen.writeObjectFieldStart(Keys.ETE_DURATION);
		writeMinMaxAvg(gen, summary.getEndToEndDurationStats());
		gen.writeEndObject();

		gen.writeObjectFieldStart(Keys.ALIGNMENT_BUFFERED);
		writeMinMaxAvg(gen, summary.getAlignmentBufferedStats());
		gen.writeEndObject();
		gen.writeEndObject();
	}

	private static void writeMinMaxAvg(JsonGenerator gen, MinMaxAvgStats minMaxAvg) throws IOException {
		gen.writeNumberField(Keys.MIN, minMaxAvg.getMinimum());
		gen.writeNumberField(Keys.MAX, minMaxAvg.getMaximum());
		gen.writeNumberField(Keys.AVG, minMaxAvg.getAverage());
	}

	private static void writeLatestCheckpoints(
		JsonGenerator gen,
		@Nullable CompletedCheckpointStats completed,
		@Nullable CompletedCheckpointStats savepoint,
		@Nullable FailedCheckpointStats failed,
		@Nullable RestoredCheckpointStats restored) throws IOException {

		gen.writeObjectFieldStart(Keys.LATEST);
		// Completed checkpoint
		if (completed != null) {
			gen.writeObjectFieldStart(Keys.COMPLETED);
			writeCheckpoint(gen, completed);

			String externalPath = completed.getExternalPath();
			if (externalPath != null) {
				gen.writeStringField(Keys.EXTERNAL_PATH, completed.getExternalPath());
			}

			gen.writeEndObject();
		}

		// Completed savepoint
		if (savepoint != null) {
			gen.writeObjectFieldStart(Keys.SAVEPOINT);
			writeCheckpoint(gen, savepoint);

			String externalPath = savepoint.getExternalPath();
			if (externalPath != null) {
				gen.writeStringField(Keys.EXTERNAL_PATH, savepoint.getExternalPath());
			}
			gen.writeEndObject();
		}

		// Failed checkpoint
		if (failed != null) {
			gen.writeObjectFieldStart(Keys.FAILED);
			writeCheckpoint(gen, failed);

			gen.writeNumberField(Keys.FAILURE_TIMESTAMP, failed.getFailureTimestamp());
			String failureMsg = failed.getFailureMessage();
			if (failureMsg != null) {
				gen.writeStringField(Keys.FAILURE_MESSAGE, failureMsg);
			}
			gen.writeEndObject();
		}

		// Restored checkpoint
		if (restored != null) {
			gen.writeObjectFieldStart(Keys.RESTORED);
			gen.writeNumberField(Keys.ID, restored.getCheckpointId());
			gen.writeNumberField(Keys.RESTORE_TIMESTAMP, restored.getRestoreTimestamp());
			gen.writeBooleanField(Keys.IS_SAVEPOINT, CheckpointProperties.isSavepoint(restored.getProperties()));

			String externalPath = restored.getExternalPath();
			if (externalPath != null) {
				gen.writeStringField(Keys.EXTERNAL_PATH, externalPath);
			}
			gen.writeEndObject();
		}
		gen.writeEndObject();
	}

	private static void writeCheckpoint(JsonGenerator gen, AbstractCheckpointStats checkpoint) throws IOException {
		gen.writeNumberField(Keys.ID, checkpoint.getCheckpointId());
		gen.writeNumberField(Keys.TRIGGER_TIMESTAMP, checkpoint.getTriggerTimestamp());
		gen.writeNumberField(Keys.LATEST_ACK_TIMESTAMP, checkpoint.getLatestAckTimestamp());
		gen.writeNumberField(Keys.STATE_SIZE, checkpoint.getStateSize());
		gen.writeNumberField(Keys.ETE_DURATION, checkpoint.getEndToEndDuration());
		gen.writeNumberField(Keys.ALIGNMENT_BUFFERED, checkpoint.getAlignmentBuffered());

	}

	private static void writeHistory(JsonGenerator gen, CheckpointStatsHistory history) throws IOException {
		gen.writeArrayFieldStart(Keys.HISTORY);
		for (AbstractCheckpointStats checkpoint : history.getCheckpoints()) {
			gen.writeStartObject();
			gen.writeNumberField(Keys.ID, checkpoint.getCheckpointId());
			gen.writeStringField(Keys.STATUS, checkpoint.getStatus().toString());
			gen.writeBooleanField(Keys.IS_SAVEPOINT, CheckpointProperties.isSavepoint(checkpoint.getProperties()));
			gen.writeNumberField(Keys.TRIGGER_TIMESTAMP, checkpoint.getTriggerTimestamp());
			gen.writeNumberField(Keys.LATEST_ACK_TIMESTAMP, checkpoint.getLatestAckTimestamp());
			gen.writeNumberField(Keys.STATE_SIZE, checkpoint.getStateSize());
			gen.writeNumberField(Keys.ETE_DURATION, checkpoint.getEndToEndDuration());
			gen.writeNumberField(Keys.ALIGNMENT_BUFFERED, checkpoint.getAlignmentBuffered());
			gen.writeNumberField(Keys.NUM_SUBTASKS, checkpoint.getNumberOfSubtasks());
			gen.writeNumberField(Keys.NUM_ACK_SUBTASKS, checkpoint.getNumberOfAcknowledgedSubtasks());

			if (checkpoint.getStatus().isCompleted()) {
				// --- Completed ---
				CompletedCheckpointStats completed = (CompletedCheckpointStats) checkpoint;

				String externalPath = completed.getExternalPath();
				if (externalPath != null) {
					gen.writeStringField(Keys.EXTERNAL_PATH, externalPath);
				}

				gen.writeBooleanField(Keys.DISCARDED, completed.isDiscarded());
			}
			else if (checkpoint.getStatus().isFailed()) {
				// --- Failed ---
				FailedCheckpointStats failed = (FailedCheckpointStats) checkpoint;

				gen.writeNumberField(Keys.FAILURE_TIMESTAMP, failed.getFailureTimestamp());

				String failureMsg = failed.getFailureMessage();
				if (failureMsg != null) {
					gen.writeStringField(Keys.FAILURE_MESSAGE, failureMsg);
				}
			}

			gen.writeEndObject();
		}
		gen.writeEndArray();
	}

	public static void writeCheckpointConfigAsJson(AccessExecutionGraph graph, JsonGenerator gen) throws IOException {
		JobSnapshottingSettings settings = graph.getJobSnapshottingSettings();

		if (settings == null) {
			gen.writeRaw("{}");
		} else {
			gen.writeStartObject();
			{
				gen.writeStringField(Keys.MODE, settings.isExactlyOnce() ? "exactly_once" : "at_least_once");
				gen.writeNumberField(Keys.INTERVAL, settings.getCheckpointInterval());
				gen.writeNumberField(Keys.TIMEOUT, settings.getCheckpointTimeout());
				gen.writeNumberField(Keys.MIN_PAUSE, settings.getMinPauseBetweenCheckpoints());
				gen.writeNumberField(Keys.MAX_CONCURRENT, settings.getMaxConcurrentCheckpoints());

				ExternalizedCheckpointSettings externalization = settings.getExternalizedCheckpointSettings();
				gen.writeObjectFieldStart(Keys.EXTERNALIZATION);
				{
					if (externalization.externalizeCheckpoints()) {
						gen.writeBooleanField(Keys.ENABLED, true);
						gen.writeBooleanField(Keys.DELETE_ON_CANCEL, externalization.deleteOnCancellation());
					} else {
						gen.writeBooleanField(Keys.ENABLED, false);
					}
				}
				gen.writeEndObject();

			}
			gen.writeEndObject();
		}
	}

	public static void writeCheckpointDetailsAsJson(AbstractCheckpointStats checkpoint, JsonGenerator gen) throws IOException {
		gen.writeStartObject();

		gen.writeNumberField(Keys.ID, checkpoint.getCheckpointId());
		gen.writeStringField(Keys.STATUS, checkpoint.getStatus().toString());
		gen.writeBooleanField(Keys.IS_SAVEPOINT, CheckpointProperties.isSavepoint(checkpoint.getProperties()));
		gen.writeNumberField(Keys.TRIGGER_TIMESTAMP, checkpoint.getTriggerTimestamp());
		gen.writeNumberField(Keys.LATEST_ACK_TIMESTAMP, checkpoint.getLatestAckTimestamp());
		gen.writeNumberField(Keys.STATE_SIZE, checkpoint.getStateSize());
		gen.writeNumberField(Keys.ETE_DURATION, checkpoint.getEndToEndDuration());
		gen.writeNumberField(Keys.ALIGNMENT_BUFFERED, checkpoint.getAlignmentBuffered());
		gen.writeNumberField(Keys.NUM_SUBTASKS, checkpoint.getNumberOfSubtasks());
		gen.writeNumberField(Keys.NUM_ACK_SUBTASKS, checkpoint.getNumberOfAcknowledgedSubtasks());

		if (checkpoint.getStatus().isCompleted()) {
			// --- Completed ---
			CompletedCheckpointStats completed = (CompletedCheckpointStats) checkpoint;

			String externalPath = completed.getExternalPath();
			if (externalPath != null) {
				gen.writeStringField(Keys.EXTERNAL_PATH, externalPath);
			}

			gen.writeBooleanField(Keys.DISCARDED, completed.isDiscarded());
		}
		else if (checkpoint.getStatus().isFailed()) {
			// --- Failed ---
			FailedCheckpointStats failed = (FailedCheckpointStats) checkpoint;

			gen.writeNumberField(Keys.FAILURE_TIMESTAMP, failed.getFailureTimestamp());

			String failureMsg = failed.getFailureMessage();
			if (failureMsg != null) {
				gen.writeStringField(Keys.FAILURE_MESSAGE, failureMsg);
			}
		}

		gen.writeObjectFieldStart(Keys.TASKS);
		for (TaskStateStats taskStats : checkpoint.getAllTaskStateStats()) {
			gen.writeObjectFieldStart(taskStats.getJobVertexId().toString());

			gen.writeNumberField(Keys.LATEST_ACK_TIMESTAMP, taskStats.getLatestAckTimestamp());
			gen.writeNumberField(Keys.STATE_SIZE, taskStats.getStateSize());
			gen.writeNumberField(Keys.ETE_DURATION, taskStats.getEndToEndDuration(checkpoint.getTriggerTimestamp()));
			gen.writeNumberField(Keys.ALIGNMENT_BUFFERED, taskStats.getAlignmentBuffered());
			gen.writeNumberField(Keys.NUM_SUBTASKS, taskStats.getNumberOfSubtasks());
			gen.writeNumberField(Keys.NUM_ACK_SUBTASKS, taskStats.getNumberOfAcknowledgedSubtasks());

			gen.writeEndObject();
		}
		gen.writeEndObject();

		gen.writeEndObject();
	}

	public static void writeSubtaskCheckpointDetailsAsJson(AbstractCheckpointStats checkpoint, TaskStateStats taskStats, JsonGenerator gen) throws IOException {
		gen.writeStartObject();
		// Overview
		gen.writeNumberField(Keys.ID, checkpoint.getCheckpointId());
		gen.writeStringField(Keys.STATUS, checkpoint.getStatus().toString());
		gen.writeNumberField(Keys.LATEST_ACK_TIMESTAMP, taskStats.getLatestAckTimestamp());
		gen.writeNumberField(Keys.STATE_SIZE, taskStats.getStateSize());
		gen.writeNumberField(Keys.ETE_DURATION, taskStats.getEndToEndDuration(checkpoint.getTriggerTimestamp()));
		gen.writeNumberField(Keys.ALIGNMENT_BUFFERED, taskStats.getAlignmentBuffered());
		gen.writeNumberField(Keys.NUM_SUBTASKS, taskStats.getNumberOfSubtasks());
		gen.writeNumberField(Keys.NUM_ACK_SUBTASKS, taskStats.getNumberOfAcknowledgedSubtasks());

		if (taskStats.getNumberOfAcknowledgedSubtasks() > 0) {
			gen.writeObjectFieldStart(Keys.SUMMARY);
			gen.writeObjectFieldStart(Keys.STATE_SIZE);
			writeMinMaxAvg(gen, taskStats.getSummaryStats().getStateSizeStats());
			gen.writeEndObject();

			gen.writeObjectFieldStart(Keys.ETE_DURATION);
			MinMaxAvgStats ackTimestampStats = taskStats.getSummaryStats().getAckTimestampStats();
			gen.writeNumberField(Keys.MIN, Math.max(0, ackTimestampStats.getMinimum() - checkpoint.getTriggerTimestamp()));
			gen.writeNumberField(Keys.MAX, Math.max(0, ackTimestampStats.getMaximum() - checkpoint.getTriggerTimestamp()));
			gen.writeNumberField(Keys.AVG, Math.max(0, ackTimestampStats.getAverage() - checkpoint.getTriggerTimestamp()));
			gen.writeEndObject();

			gen.writeObjectFieldStart(Keys.CHECKPOINT_DURATION);
			gen.writeObjectFieldStart(Keys.SYNC);
			writeMinMaxAvg(gen, taskStats.getSummaryStats().getSyncCheckpointDurationStats());
			gen.writeEndObject();
			gen.writeObjectFieldStart(Keys.ASYNC);
			writeMinMaxAvg(gen, taskStats.getSummaryStats().getAsyncCheckpointDurationStats());
			gen.writeEndObject();
			gen.writeEndObject();

			gen.writeObjectFieldStart(Keys.ALIGNMENT);
			gen.writeObjectFieldStart(Keys.BUFFERED);
			writeMinMaxAvg(gen, taskStats.getSummaryStats().getAlignmentBufferedStats());
			gen.writeEndObject();
			gen.writeObjectFieldStart(Keys.DURATION);
			writeMinMaxAvg(gen, taskStats.getSummaryStats().getAlignmentDurationStats());
			gen.writeEndObject();
			gen.writeEndObject();
			gen.writeEndObject();
		}

		SubtaskStateStats[] subtasks = taskStats.getSubtaskStats();

		gen.writeArrayFieldStart(Keys.SUBTASKS);
		for (int i = 0; i < subtasks.length; i++) {
			SubtaskStateStats subtask = subtasks[i];

			gen.writeStartObject();
			gen.writeNumberField(Keys.INDEX, i);

			if (subtask != null) {
				gen.writeStringField(Keys.STATUS, Keys.COMPLETED);
				gen.writeNumberField(Keys.ACK_TIMESTAMP, subtask.getAckTimestamp());
				gen.writeNumberField(Keys.ETE_DURATION, subtask.getEndToEndDuration(checkpoint.getTriggerTimestamp()));
				gen.writeNumberField(Keys.STATE_SIZE, subtask.getStateSize());

				gen.writeObjectFieldStart(Keys.CHECKPOINT);
				gen.writeNumberField(Keys.SYNC, subtask.getSyncCheckpointDuration());
				gen.writeNumberField(Keys.ASYNC, subtask.getAsyncCheckpointDuration());
				gen.writeEndObject();

				gen.writeObjectFieldStart(Keys.ALIGNMENT);
				gen.writeNumberField(Keys.BUFFERED, subtask.getAlignmentBuffered());
				gen.writeNumberField(Keys.DURATION, subtask.getAlignmentDuration());
				gen.writeEndObject();
			} else {
				gen.writeStringField(Keys.STATUS, Keys.PENDING_OR_FAILED);
			}
			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeEndObject();
	}

	private static void addIOMetrics(MutableIOMetrics summedMetrics, ExecutionState state, @Nullable IOMetrics ioMetrics, @Nullable MetricFetcher fetcher, String jobID, String taskID, int subtaskIndex) {
		if (state.isTerminal()) {
			if (ioMetrics != null) { // execAttempt is already finished, use final metrics stored in ExecutionGraph
				summedMetrics.addNumBytesInLocal(ioMetrics.getNumBytesInLocal());
				summedMetrics.addNumBytesInRemote(ioMetrics.getNumBytesInRemote());
				summedMetrics.addNumBytesOut(ioMetrics.getNumBytesOut());
				summedMetrics.addNumRecordsIn(ioMetrics.getNumRecordsIn());
				summedMetrics.addNumRecordsOut(ioMetrics.getNumRecordsOut());
			}
		} else { // execAttempt is still running, use MetricQueryService instead
			if (fetcher != null) {
				fetcher.update();
				MetricStore.SubtaskMetricStore metrics = fetcher.getMetricStore().getSubtaskMetricStore(jobID, taskID, subtaskIndex);
				if (metrics != null) {
					summedMetrics.addNumBytesInLocal(Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_LOCAL, "0")));
					summedMetrics.addNumBytesInRemote(Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_REMOTE, "0")));
					summedMetrics.addNumBytesOut(Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_OUT, "0")));
					summedMetrics.addNumRecordsIn(Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_IN, "0")));
					summedMetrics.addNumRecordsOut(Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_OUT, "0")));
				}
			}
		}
	}
	
	private static void writeIOMetrics(JsonGenerator gen, IOMetrics metrics) throws IOException {
		gen.writeObjectFieldStart(Keys.METRICS);
		gen.writeNumberField(Keys.READ_BYTES, metrics.getNumBytesInLocal() + metrics.getNumBytesInRemote());
		gen.writeNumberField(Keys.WRITE_BYTES, metrics.getNumBytesInRemote());
		gen.writeNumberField(Keys.READ_RECORDS, metrics.getNumRecordsIn());
		gen.writeNumberField(Keys.WRITE_RECORDS, metrics.getNumRecordsOut());
		gen.writeEndObject();
	}

	private static class MutableIOMetrics extends IOMetrics {
		public MutableIOMetrics() {
			super(0, 0, 0, 0, 0, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D);
		}

		public void addNumBytesInLocal(long toAdd) {
			this.numBytesInLocal += toAdd;
		}

		public void addNumBytesInRemote(long toAdd) {
			this.numBytesInRemote += toAdd;
		}

		public void addNumBytesOut(long toAdd) {
			this.numBytesOut += toAdd;
		}

		public void addNumRecordsIn(long toAdd) {
			this.numRecordsIn += toAdd;
		}

		public void addNumRecordsOut(long toAdd) {
			this.numRecordsOut += toAdd;
		}
	}
}
