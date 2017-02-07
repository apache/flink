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
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.handlers.AbstractExecutionGraphRequestHandler;
import org.apache.flink.runtime.webmonitor.handlers.JsonFactory;

import java.io.StringWriter;
import java.util.Map;

/**
 * Handler that returns a job's snapshotting settings.
 */
public class CheckpointConfigHandler extends AbstractExecutionGraphRequestHandler {

	public CheckpointConfigHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(AccessExecutionGraph graph, Map<String, String> params) throws Exception {
		StringWriter writer = new StringWriter();

		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);
		JobSnapshottingSettings settings = graph.getJobSnapshottingSettings();

		if (settings == null) {
			return "{}";
		}

		gen.writeStartObject();
		{
			gen.writeStringField("mode", settings.isExactlyOnce() ? "exactly_once" : "at_least_once");
			gen.writeNumberField("interval", settings.getCheckpointInterval());
			gen.writeNumberField("timeout", settings.getCheckpointTimeout());
			gen.writeNumberField("min_pause", settings.getMinPauseBetweenCheckpoints());
			gen.writeNumberField("max_concurrent", settings.getMaxConcurrentCheckpoints());

			ExternalizedCheckpointSettings externalization = settings.getExternalizedCheckpointSettings();
			gen.writeObjectFieldStart("externalization");
			{
				if (externalization.externalizeCheckpoints()) {
					gen.writeBooleanField("enabled", true);
					gen.writeBooleanField("delete_on_cancellation", externalization.deleteOnCancellation());
				} else {
					gen.writeBooleanField("enabled", false);
				}
			}
			gen.writeEndObject();

		}
		gen.writeEndObject();

		gen.close();

		return writer.toString();
	}
}
