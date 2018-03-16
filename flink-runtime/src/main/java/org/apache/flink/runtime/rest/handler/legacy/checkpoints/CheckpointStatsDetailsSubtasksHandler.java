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

package org.apache.flink.runtime.rest.handler.legacy.checkpoints;

import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsHistory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.MinMaxAvgStats;
import org.apache.flink.runtime.checkpoint.SubtaskStateStats;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointStatsCache;
import org.apache.flink.runtime.rest.handler.legacy.AbstractExecutionGraphRequestHandler;
import org.apache.flink.runtime.rest.handler.legacy.AbstractJobVertexRequestHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Request handler that returns checkpoint stats for a single job vertex with
 * the summary stats and all subtasks.
 */
public class CheckpointStatsDetailsSubtasksHandler extends AbstractExecutionGraphRequestHandler {

	private static final String CHECKPOINT_STATS_DETAILS_SUBTASKS_REST_PATH = "/jobs/:jobid/checkpoints/details/:checkpointid/subtasks/:vertexid";

	private final CheckpointStatsCache cache;

	public CheckpointStatsDetailsSubtasksHandler(ExecutionGraphCache executionGraphHolder, Executor executor, CheckpointStatsCache cache) {
		super(executionGraphHolder, executor);
		this.cache = checkNotNull(cache);
	}

	@Override
	public String[] getPaths() {
		return new String[]{CHECKPOINT_STATS_DETAILS_SUBTASKS_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(
			Map<String, String> pathParams,
			Map<String, String> queryParams,
			JobManagerGateway jobManagerGateway) {
		return super.handleJsonRequest(pathParams, queryParams, jobManagerGateway);
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionGraph graph, Map<String, String> params) {
		long checkpointId = CheckpointStatsDetailsHandler.parseCheckpointId(params);
		if (checkpointId == -1) {
			return CompletableFuture.completedFuture("{}");
		}

		JobVertexID vertexId = AbstractJobVertexRequestHandler.parseJobVertexId(params);
		if (vertexId == null) {
			return CompletableFuture.completedFuture("{}");
		}

		CheckpointStatsSnapshot snapshot = graph.getCheckpointStatsSnapshot();
		if (snapshot == null) {
			return CompletableFuture.completedFuture("{}");
		}

		AbstractCheckpointStats checkpoint = snapshot.getHistory().getCheckpointById(checkpointId);

		if (checkpoint != null) {
			cache.tryAdd(checkpoint);
		} else {
			checkpoint = cache.tryGet(checkpointId);

			if (checkpoint == null) {
				return CompletableFuture.completedFuture("{}");
			}
		}

		TaskStateStats taskStats = checkpoint.getTaskStateStats(vertexId);
		if (taskStats == null) {
			return CompletableFuture.completedFuture("{}");
		}

		try {
			return CompletableFuture.completedFuture(createSubtaskCheckpointDetailsJson(checkpoint, taskStats));
		} catch (IOException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	/**
	 * Archivist for the CheckpointStatsDetailsSubtasksHandler.
	 */
	public static class CheckpointStatsDetailsSubtasksJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			CheckpointStatsSnapshot stats = graph.getCheckpointStatsSnapshot();
			if (stats == null) {
				return Collections.emptyList();
			}
			CheckpointStatsHistory history = stats.getHistory();
			List<ArchivedJson> archive = new ArrayList<>();
			for (AbstractCheckpointStats checkpoint : history.getCheckpoints()) {
				for (TaskStateStats subtaskStats : checkpoint.getAllTaskStateStats()) {
					String json = createSubtaskCheckpointDetailsJson(checkpoint, subtaskStats);
					String path = CHECKPOINT_STATS_DETAILS_SUBTASKS_REST_PATH
						.replace(":jobid", graph.getJobID().toString())
						.replace(":checkpointid", String.valueOf(checkpoint.getCheckpointId()))
						.replace(":vertexid", subtaskStats.getJobVertexId().toString());
					archive.add(new ArchivedJson(path, json));
				}
			}
			return archive;
		}
	}

	private static String createSubtaskCheckpointDetailsJson(AbstractCheckpointStats checkpoint, TaskStateStats taskStats) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		gen.writeStartObject();
		// Overview
		gen.writeNumberField("id", checkpoint.getCheckpointId());
		gen.writeStringField("status", checkpoint.getStatus().toString());
		gen.writeNumberField("latest_ack_timestamp", taskStats.getLatestAckTimestamp());
		gen.writeNumberField("state_size", taskStats.getStateSize());
		gen.writeNumberField("end_to_end_duration", taskStats.getEndToEndDuration(checkpoint.getTriggerTimestamp()));
		gen.writeNumberField("alignment_buffered", taskStats.getAlignmentBuffered());
		gen.writeNumberField("num_subtasks", taskStats.getNumberOfSubtasks());
		gen.writeNumberField("num_acknowledged_subtasks", taskStats.getNumberOfAcknowledgedSubtasks());

		if (taskStats.getNumberOfAcknowledgedSubtasks() > 0) {
			gen.writeObjectFieldStart("summary");
			gen.writeObjectFieldStart("state_size");
			CheckpointStatsHandler.writeMinMaxAvg(gen, taskStats.getSummaryStats().getStateSizeStats());
			gen.writeEndObject();

			gen.writeObjectFieldStart("end_to_end_duration");
			MinMaxAvgStats ackTimestampStats = taskStats.getSummaryStats().getAckTimestampStats();
			gen.writeNumberField("min", Math.max(0, ackTimestampStats.getMinimum() - checkpoint.getTriggerTimestamp()));
			gen.writeNumberField("max", Math.max(0, ackTimestampStats.getMaximum() - checkpoint.getTriggerTimestamp()));
			gen.writeNumberField("avg", Math.max(0, ackTimestampStats.getAverage() - checkpoint.getTriggerTimestamp()));
			gen.writeEndObject();

			gen.writeObjectFieldStart("checkpoint_duration");
			gen.writeObjectFieldStart("sync");
			CheckpointStatsHandler.writeMinMaxAvg(gen, taskStats.getSummaryStats().getSyncCheckpointDurationStats());
			gen.writeEndObject();
			gen.writeObjectFieldStart("async");
			CheckpointStatsHandler.writeMinMaxAvg(gen, taskStats.getSummaryStats().getAsyncCheckpointDurationStats());
			gen.writeEndObject();
			gen.writeEndObject();

			gen.writeObjectFieldStart("alignment");
			gen.writeObjectFieldStart("buffered");
			CheckpointStatsHandler.writeMinMaxAvg(gen, taskStats.getSummaryStats().getAlignmentBufferedStats());
			gen.writeEndObject();
			gen.writeObjectFieldStart("duration");
			CheckpointStatsHandler.writeMinMaxAvg(gen, taskStats.getSummaryStats().getAlignmentDurationStats());
			gen.writeEndObject();
			gen.writeEndObject();
			gen.writeEndObject();
		}

		SubtaskStateStats[] subtasks = taskStats.getSubtaskStats();

		gen.writeArrayFieldStart("subtasks");
		for (int i = 0; i < subtasks.length; i++) {
			SubtaskStateStats subtask = subtasks[i];

			gen.writeStartObject();
			gen.writeNumberField("index", i);

			if (subtask != null) {
				gen.writeStringField("status", "completed");
				gen.writeNumberField("ack_timestamp", subtask.getAckTimestamp());
				gen.writeNumberField("end_to_end_duration", subtask.getEndToEndDuration(checkpoint.getTriggerTimestamp()));
				gen.writeNumberField("state_size", subtask.getStateSize());

				gen.writeObjectFieldStart("checkpoint");
				gen.writeNumberField("sync", subtask.getSyncCheckpointDuration());
				gen.writeNumberField("async", subtask.getAsyncCheckpointDuration());
				gen.writeEndObject();

				gen.writeObjectFieldStart("alignment");
				gen.writeNumberField("buffered", subtask.getAlignmentBuffered());
				gen.writeNumberField("duration", subtask.getAlignmentDuration());
				gen.writeEndObject();
			} else {
				gen.writeStringField("status", "pending_or_failed");
			}
			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeEndObject();
		gen.close();

		return writer.toString();
	}

}
