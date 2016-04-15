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
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.webmonitor.BackPressureStatsTracker;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.OperatorBackPressureStats;
import scala.Option;

import java.io.StringWriter;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Request handler that returns back pressure stats for a single job vertex and
 * all its sub tasks.
 */
public class JobVertexBackPressureHandler extends AbstractJobVertexRequestHandler {

	/** Back pressure stats tracker. */
	private final BackPressureStatsTracker backPressureStatsTracker;

	/** Time after which stats are considered outdated. */
	private final int refreshInterval;

	public JobVertexBackPressureHandler(
			ExecutionGraphHolder executionGraphHolder,
			BackPressureStatsTracker backPressureStatsTracker,
			int refreshInterval) {

		super(executionGraphHolder);
		this.backPressureStatsTracker = checkNotNull(backPressureStatsTracker, "Stats tracker");
		checkArgument(refreshInterval >= 0, "Negative timeout");
		this.refreshInterval = refreshInterval;
	}

	@Override
	public String handleRequest(
			ExecutionJobVertex jobVertex,
			Map<String, String> params) throws Exception {

		try (StringWriter writer = new StringWriter();
				JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer)) {

			gen.writeStartObject();

			Option<OperatorBackPressureStats> statsOption = backPressureStatsTracker
					.getOperatorBackPressureStats(jobVertex);

			if (statsOption.isDefined()) {
				OperatorBackPressureStats stats = statsOption.get();

				// Check whether we need to refresh
				if (refreshInterval <= System.currentTimeMillis() - stats.getEndTimestamp()) {
					backPressureStatsTracker.triggerStackTraceSample(jobVertex);
					gen.writeStringField("status", "deprecated");
				} else {
					gen.writeStringField("status", "ok");
				}

				gen.writeStringField("backpressure-level", getBackPressureLevel(stats.getMaxBackPressureRatio()));
				gen.writeNumberField("end-timestamp", stats.getEndTimestamp());

				// Sub tasks
				gen.writeArrayFieldStart("subtasks");
				int numSubTasks = stats.getNumberOfSubTasks();
				for (int i = 0; i < numSubTasks; i++) {
					double ratio = stats.getBackPressureRatio(i);

					gen.writeStartObject();
					gen.writeNumberField("subtask", i);
					gen.writeStringField("backpressure-level", getBackPressureLevel(ratio));
					gen.writeNumberField("ratio", ratio);
					gen.writeEndObject();
				}
				gen.writeEndArray();
			} else {
				backPressureStatsTracker.triggerStackTraceSample(jobVertex);
				gen.writeStringField("status", "deprecated");
			}

			gen.writeEndObject();
			gen.close();

			return writer.toString();
		}
	}

	/**
	 * Returns the back pressure level as a String.
	 *
	 * @param backPressureRatio Ratio of back pressures samples to total number of samples.
	 *
	 * @return Back pressure level ('no', 'low', or 'high')
	 */
	static String getBackPressureLevel(double backPressureRatio) {
		if (backPressureRatio <= 0.10) {
			return "ok";
		} else if (backPressureRatio <= 0.5) {
			return "low";
		} else {
			return "high";
		}
	}

}
