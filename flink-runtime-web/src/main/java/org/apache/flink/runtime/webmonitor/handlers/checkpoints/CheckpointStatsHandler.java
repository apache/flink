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
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointStatsCounts;
import org.apache.flink.runtime.checkpoint.CheckpointStatsHistory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStatsSummary;
import org.apache.flink.runtime.checkpoint.FailedCheckpointStats;
import org.apache.flink.runtime.checkpoint.MinMaxAvgStats;
import org.apache.flink.runtime.checkpoint.RestoredCheckpointStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.handlers.AbstractExecutionGraphRequestHandler;
import org.apache.flink.runtime.webmonitor.handlers.JsonFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

/**
 * Handler that returns checkpoint statistics for a job.
 */
public class CheckpointStatsHandler extends AbstractExecutionGraphRequestHandler {

	public CheckpointStatsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(AccessExecutionGraph graph, Map<String, String> params) throws Exception {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		CheckpointStatsSnapshot snapshot = graph.getCheckpointStatsSnapshot();
		if (snapshot == null) {
			return "{}";
		}

		gen.writeStartObject();

		// Counts
		writeCounts(gen, snapshot.getCounts());

		// Summary
		writeSummary(gen, snapshot.getSummaryStats());

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
		gen.close();

		return writer.toString();
	}

	private void writeCounts(JsonGenerator gen, CheckpointStatsCounts counts) throws IOException {
		gen.writeObjectFieldStart("counts");
		gen.writeNumberField("restored", counts.getNumberOfRestoredCheckpoints());
		gen.writeNumberField("total", counts.getTotalNumberOfCheckpoints());
		gen.writeNumberField("in_progress", counts.getNumberOfInProgressCheckpoints());
		gen.writeNumberField("completed", counts.getNumberOfCompletedCheckpoints());
		gen.writeNumberField("failed", counts.getNumberOfFailedCheckpoints());
		gen.writeEndObject();
	}

	private void writeSummary(
		JsonGenerator gen,
		CompletedCheckpointStatsSummary summary) throws IOException {
		gen.writeObjectFieldStart("summary");
		gen.writeObjectFieldStart("state_size");
		writeMinMaxAvg(gen, summary.getStateSizeStats());
		gen.writeEndObject();

		gen.writeObjectFieldStart("end_to_end_duration");
		writeMinMaxAvg(gen, summary.getEndToEndDurationStats());
		gen.writeEndObject();

		gen.writeObjectFieldStart("alignment_buffered");
		writeMinMaxAvg(gen, summary.getAlignmentBufferedStats());
		gen.writeEndObject();
		gen.writeEndObject();
	}

	private void writeMinMaxAvg(JsonGenerator gen, MinMaxAvgStats minMaxAvg) throws IOException {
		gen.writeNumberField("min", minMaxAvg.getMinimum());
		gen.writeNumberField("max", minMaxAvg.getMaximum());
		gen.writeNumberField("avg", minMaxAvg.getAverage());
	}

	private void writeLatestCheckpoints(
		JsonGenerator gen,
		@Nullable CompletedCheckpointStats completed,
		@Nullable CompletedCheckpointStats savepoint,
		@Nullable FailedCheckpointStats failed,
		@Nullable RestoredCheckpointStats restored) throws IOException {

		gen.writeObjectFieldStart("latest");
		// Completed checkpoint
		if (completed != null) {
			gen.writeObjectFieldStart("completed");
			writeCheckpoint(gen, completed);

			String externalPath = completed.getExternalPath();
			if (externalPath != null) {
				gen.writeStringField("external_path", completed.getExternalPath());
			}

			gen.writeEndObject();
		}

		// Completed savepoint
		if (savepoint != null) {
			gen.writeObjectFieldStart("savepoint");
			writeCheckpoint(gen, savepoint);

			String externalPath = savepoint.getExternalPath();
			if (externalPath != null) {
				gen.writeStringField("external_path", savepoint.getExternalPath());
			}
			gen.writeEndObject();
		}

		// Failed checkpoint
		if (failed != null) {
			gen.writeObjectFieldStart("failed");
			writeCheckpoint(gen, failed);

			gen.writeNumberField("failure_timestamp", failed.getFailureTimestamp());
			String failureMsg = failed.getFailureMessage();
			if (failureMsg != null) {
				gen.writeStringField("failure_message", failureMsg);
			}
			gen.writeEndObject();
		}

		// Restored checkpoint
		if (restored != null) {
			gen.writeObjectFieldStart("restored");
			gen.writeNumberField("id", restored.getCheckpointId());
			gen.writeNumberField("restore_timestamp", restored.getRestoreTimestamp());
			gen.writeBooleanField("is_savepoint", CheckpointProperties.isSavepoint(restored.getProperties()));

			String externalPath = restored.getExternalPath();
			if (externalPath != null) {
				gen.writeStringField("external_path", externalPath);
			}
			gen.writeEndObject();
		}
		gen.writeEndObject();
	}

	private void writeCheckpoint(JsonGenerator gen, AbstractCheckpointStats checkpoint) throws IOException {
		gen.writeNumberField("id", checkpoint.getCheckpointId());
		gen.writeNumberField("trigger_timestamp", checkpoint.getTriggerTimestamp());
		gen.writeNumberField("latest_ack_timestamp", checkpoint.getLatestAckTimestamp());
		gen.writeNumberField("state_size", checkpoint.getStateSize());
		gen.writeNumberField("end_to_end_duration", checkpoint.getEndToEndDuration());
		gen.writeNumberField("alignment_buffered", checkpoint.getAlignmentBuffered());

	}

	private void writeHistory(JsonGenerator gen, CheckpointStatsHistory history) throws IOException {
		gen.writeArrayFieldStart("history");
		for (AbstractCheckpointStats checkpoint : history.getCheckpoints()) {
			gen.writeStartObject();
			gen.writeNumberField("id", checkpoint.getCheckpointId());
			gen.writeStringField("status", checkpoint.getStatus().toString());
			gen.writeBooleanField("is_savepoint", CheckpointProperties.isSavepoint(checkpoint.getProperties()));
			gen.writeNumberField("trigger_timestamp", checkpoint.getTriggerTimestamp());
			gen.writeNumberField("latest_ack_timestamp", checkpoint.getLatestAckTimestamp());
			gen.writeNumberField("state_size", checkpoint.getStateSize());
			gen.writeNumberField("end_to_end_duration", checkpoint.getEndToEndDuration());
			gen.writeNumberField("alignment_buffered", checkpoint.getAlignmentBuffered());
			gen.writeNumberField("num_subtasks", checkpoint.getNumberOfSubtasks());
			gen.writeNumberField("num_acknowledged_subtasks", checkpoint.getNumberOfAcknowledgedSubtasks());

			if (checkpoint.getStatus().isCompleted()) {
				// --- Completed ---
				CompletedCheckpointStats completed = (CompletedCheckpointStats) checkpoint;

				String externalPath = completed.getExternalPath();
				if (externalPath != null) {
					gen.writeStringField("external_path", externalPath);
				}

				gen.writeBooleanField("discarded", completed.isDiscarded());
			}
			else if (checkpoint.getStatus().isFailed()) {
				// --- Failed ---
				FailedCheckpointStats failed = (FailedCheckpointStats) checkpoint;

				gen.writeNumberField("failure_timestamp", failed.getFailureTimestamp());

				String failureMsg = failed.getFailureMessage();
				if (failureMsg != null) {
					gen.writeStringField("failure_message", failureMsg);
				}
			}

			gen.writeEndObject();
		}
		gen.writeEndArray();
	}
}
