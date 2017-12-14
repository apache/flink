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
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.FailedCheckpointStats;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointStatsCache;
import org.apache.flink.runtime.rest.handler.legacy.AbstractExecutionGraphRequestHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Request handler that returns checkpoint stats for a single job vertex.
 */
public class CheckpointStatsDetailsHandler extends AbstractExecutionGraphRequestHandler {

	private static final String CHECKPOINT_STATS_DETAILS_REST_PATH = "/jobs/:jobid/checkpoints/details/:checkpointid";

	private final CheckpointStatsCache cache;

	public CheckpointStatsDetailsHandler(ExecutionGraphCache executionGraphHolder, Executor executor, CheckpointStatsCache cache) {
		super(executionGraphHolder, executor);
		this.cache = cache;
	}

	@Override
	public String[] getPaths() {
		return new String[]{CHECKPOINT_STATS_DETAILS_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionGraph graph, Map<String, String> params) {
		return CompletableFuture.supplyAsync(
			() -> {
				long checkpointId = parseCheckpointId(params);
				if (checkpointId == -1) {
					return "{}";
				}

				CheckpointStatsSnapshot snapshot = graph.getCheckpointStatsSnapshot();
				if (snapshot == null) {
					return "{}";
				}

				AbstractCheckpointStats checkpoint = snapshot.getHistory().getCheckpointById(checkpointId);

				if (checkpoint != null) {
					cache.tryAdd(checkpoint);
				} else {
					checkpoint = cache.tryGet(checkpointId);

					if (checkpoint == null) {
						return "{}";
					}
				}

				try {
					return createCheckpointDetailsJson(checkpoint);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not create checkpoint details json.", e));
				}
			},
			executor);
	}

	/**
	 * Archivist for the CheckpointStatsDetails.
	 */
	public static class CheckpointStatsDetailsJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			CheckpointStatsSnapshot stats = graph.getCheckpointStatsSnapshot();
			if (stats == null) {
				return Collections.emptyList();
			}
			CheckpointStatsHistory history = stats.getHistory();
			List<ArchivedJson> archive = new ArrayList<>();
			for (AbstractCheckpointStats checkpoint : history.getCheckpoints()) {
				String json = createCheckpointDetailsJson(checkpoint);
				String path = CHECKPOINT_STATS_DETAILS_REST_PATH
					.replace(":jobid", graph.getJobID().toString())
					.replace(":checkpointid", String.valueOf(checkpoint.getCheckpointId()));
				archive.add(new ArchivedJson(path, json));
			}
			return archive;
		}
	}

	public static String createCheckpointDetailsJson(AbstractCheckpointStats checkpoint) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
		gen.writeStartObject();

		gen.writeNumberField("id", checkpoint.getCheckpointId());
		gen.writeStringField("status", checkpoint.getStatus().toString());
		gen.writeBooleanField("is_savepoint", checkpoint.getProperties().isSavepoint());
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

		gen.writeObjectFieldStart("tasks");
		for (TaskStateStats taskStats : checkpoint.getAllTaskStateStats()) {
			gen.writeObjectFieldStart(taskStats.getJobVertexId().toString());

			gen.writeNumberField("latest_ack_timestamp", taskStats.getLatestAckTimestamp());
			gen.writeNumberField("state_size", taskStats.getStateSize());
			gen.writeNumberField("end_to_end_duration", taskStats.getEndToEndDuration(checkpoint.getTriggerTimestamp()));
			gen.writeNumberField("alignment_buffered", taskStats.getAlignmentBuffered());
			gen.writeNumberField("num_subtasks", taskStats.getNumberOfSubtasks());
			gen.writeNumberField("num_acknowledged_subtasks", taskStats.getNumberOfAcknowledgedSubtasks());

			gen.writeEndObject();
		}
		gen.writeEndObject();

		gen.writeEndObject();
		gen.close();

		return writer.toString();
	}

	/**
	 * Returns the checkpoint ID parsed from the provided parameters.
	 *
	 * @param params Path parameters
	 * @return Parsed checkpoint ID or <code>-1</code> if not available.
	 */
	static long parseCheckpointId(Map<String, String> params) {
		String param = params.get("checkpointid");
		if (param == null) {
			return -1;
		}

		try {
			return Long.parseLong(param);
		} catch (NumberFormatException ignored) {
			return -1;
		}
	}
}
