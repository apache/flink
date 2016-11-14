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
import org.apache.flink.runtime.checkpoint.stats.CheckpointStats;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.JobCheckpointStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import scala.Option;

import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler that returns checkpoint stats for a job.
 */
public class JobCheckpointsHandler extends AbstractExecutionGraphRequestHandler {

	public JobCheckpointsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(AccessExecutionGraph graph, Map<String, String> params) throws Exception {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		CheckpointStatsTracker tracker = graph.getCheckpointStatsTracker();

		gen.writeStartObject();

		if (tracker != null) {
			Option<JobCheckpointStats> stats = tracker.getJobStats();

			if (stats.isDefined()) {
				JobCheckpointStats jobStats = stats.get();

				// Total number of checkpoints
				gen.writeNumberField("count", jobStats.getCount());

				// Optional external path
				if (jobStats.getExternalPath() != null) {
					gen.writeStringField("external-path", jobStats.getExternalPath());
				}

				// Duration
				gen.writeFieldName("duration");
				gen.writeStartObject();
				gen.writeNumberField("min", jobStats.getMinDuration());
				gen.writeNumberField("max", jobStats.getMaxDuration());
				gen.writeNumberField("avg", jobStats.getAverageDuration());
				gen.writeEndObject();

				// State size
				gen.writeFieldName("size");
				gen.writeStartObject();
				gen.writeNumberField("min", jobStats.getMinStateSize());
				gen.writeNumberField("max", jobStats.getMaxStateSize());
				gen.writeNumberField("avg", jobStats.getAverageStateSize());
				gen.writeEndObject();

				// Recent history
				gen.writeArrayFieldStart("history");
				for (CheckpointStats checkpoint : jobStats.getRecentHistory()) {
					gen.writeStartObject();
					gen.writeNumberField("id", checkpoint.getCheckpointId());
					gen.writeNumberField("timestamp", checkpoint.getTriggerTimestamp());
					gen.writeNumberField("duration", checkpoint.getDuration());
					gen.writeNumberField("size", checkpoint.getStateSize());
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
