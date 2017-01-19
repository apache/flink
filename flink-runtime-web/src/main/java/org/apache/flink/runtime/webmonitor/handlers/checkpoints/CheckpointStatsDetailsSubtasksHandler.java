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

package org.apache.flink.runtime.webmonitor.handlers.checkpoints;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.MinMaxAvgStats;
import org.apache.flink.runtime.checkpoint.SubtaskStateStats;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.handlers.AbstractExecutionGraphRequestHandler;
import org.apache.flink.runtime.webmonitor.handlers.AbstractJobVertexRequestHandler;
import org.apache.flink.runtime.webmonitor.handlers.JsonFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Request handler that returns checkpoint stats for a single job vertex with
 * the summary stats and all subtasks.
 */
public class CheckpointStatsDetailsSubtasksHandler extends AbstractExecutionGraphRequestHandler {

	private final CheckpointStatsCache cache;

	public CheckpointStatsDetailsSubtasksHandler(ExecutionGraphHolder executionGraphHolder, CheckpointStatsCache cache) {
		super(executionGraphHolder);
		this.cache = checkNotNull(cache);
	}

	@Override
	public String handleJsonRequest(
		Map<String, String> pathParams,
		Map<String, String> queryParams,
		ActorGateway jobManager) throws Exception {
		return super.handleJsonRequest(pathParams, queryParams, jobManager);
	}

	@Override
	public String handleRequest(AccessExecutionGraph graph, Map<String, String> params) throws Exception {
		long checkpointId = CheckpointStatsDetailsHandler.parseCheckpointId(params);
		if (checkpointId == -1) {
			return "{}";
		}

		JobVertexID vertexId = AbstractJobVertexRequestHandler.parseJobVertexId(params);
		if (vertexId == null) {
			return "{}";
		}

		CheckpointStatsTracker tracker = graph.getCheckpointStatsTracker();
		CheckpointStatsSnapshot snapshot = tracker.createSnapshot();

		AbstractCheckpointStats checkpoint = snapshot.getHistory().getCheckpointById(checkpointId);

		if (checkpoint != null) {
			cache.tryAdd(checkpoint);
		} else {
			checkpoint = cache.tryGet(checkpointId);

			if (checkpoint == null) {
				return "{}";
			}
		}

		return writeResponse(checkpoint, vertexId);
	}

	private String writeResponse(AbstractCheckpointStats checkpoint, JobVertexID vertexId) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);
		gen.writeStartObject();

		TaskStateStats taskStats = checkpoint.getTaskStateStats(vertexId);
		if (taskStats == null) {
			return "{}";
		}

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
			writeMinMaxAvg(gen, taskStats.getSummaryStats().getStateSizeStats());
			gen.writeEndObject();

			gen.writeObjectFieldStart("end_to_end_duration");
			MinMaxAvgStats ackTimestampStats = taskStats.getSummaryStats().getAckTimestampStats();
			gen.writeNumberField("min", Math.max(0, ackTimestampStats.getMinimum() - checkpoint.getTriggerTimestamp()));
			gen.writeNumberField("max", Math.max(0, ackTimestampStats.getMaximum() - checkpoint.getTriggerTimestamp()));
			gen.writeNumberField("avg", Math.max(0, ackTimestampStats.getAverage() - checkpoint.getTriggerTimestamp()));
			gen.writeEndObject();

			gen.writeObjectFieldStart("checkpoint_duration");
			gen.writeObjectFieldStart("sync");
			writeMinMaxAvg(gen, taskStats.getSummaryStats().getSyncCheckpointDurationStats());
			gen.writeEndObject();
			gen.writeObjectFieldStart("async");
			writeMinMaxAvg(gen, taskStats.getSummaryStats().getAsyncCheckpointDurationStats());
			gen.writeEndObject();
			gen.writeEndObject();

			gen.writeObjectFieldStart("alignment");
			gen.writeObjectFieldStart("buffered");
			writeMinMaxAvg(gen, taskStats.getSummaryStats().getAlignmentBufferedStats());
			gen.writeEndObject();
			gen.writeObjectFieldStart("duration");
			writeMinMaxAvg(gen, taskStats.getSummaryStats().getAlignmentDurationStats());
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

	private void writeMinMaxAvg(JsonGenerator gen, MinMaxAvgStats minMaxAvg) throws IOException {
		gen.writeNumberField("min", minMaxAvg.getMinimum());
		gen.writeNumberField("max", minMaxAvg.getMaximum());
		gen.writeNumberField("avg", minMaxAvg.getAverage());
	}

}
