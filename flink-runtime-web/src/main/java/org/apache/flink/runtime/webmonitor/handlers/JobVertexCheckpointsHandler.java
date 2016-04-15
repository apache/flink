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

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.OperatorCheckpointStats;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import scala.Option;

import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler that returns checkpoint stats for a single job vertex.
 */
public class JobVertexCheckpointsHandler extends AbstractJobVertexRequestHandler {

	public JobVertexCheckpointsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionJobVertex jobVertex, Map<String, String> params) throws Exception {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);
		gen.writeStartObject();

		CheckpointStatsTracker tracker = jobVertex.getGraph().getCheckpointStatsTracker();

		if (tracker != null) {
			Option<OperatorCheckpointStats> statsOption = tracker
					.getOperatorStats(jobVertex.getJobVertexId());

			if (statsOption.isDefined()) {
				OperatorCheckpointStats stats = statsOption.get();

				gen.writeNumberField("id", stats.getCheckpointId());
				gen.writeNumberField("timestamp", stats.getTriggerTimestamp());
				gen.writeNumberField("duration", stats.getDuration());
				gen.writeNumberField("size", stats.getStateSize());
				gen.writeNumberField("parallelism", stats.getNumberOfSubTasks());

				gen.writeArrayFieldStart("subtasks");
				for (int i = 0; i < stats.getNumberOfSubTasks(); i++) {
					gen.writeStartObject();
					gen.writeNumberField("subtask", i);
					gen.writeNumberField("duration", stats.getSubTaskDuration(i));
					gen.writeNumberField("size", stats.getSubTaskStateSize(i));
					gen.writeEndObject();
				}
				gen.writeEndArray();
			}
		}

		gen.writeEndObject();
		gen.close();

		return writer.toString();
	}

}
