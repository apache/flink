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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTrackerImpl;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Request handler that returns back pressure stats for a single job vertex and
 * all its sub tasks.
 */
public class JobVertexBackPressureHandler extends AbstractJobVertexRequestHandler {

	private static final String JOB_VERTEX_BACKPRESSURE_REST_PATH = "/jobs/:jobid/vertices/:vertexid/backpressure";

	/** Back pressure stats tracker. */
	private final BackPressureStatsTrackerImpl backPressureStatsTrackerImpl;

	/** Time after which stats are considered outdated. */
	private final int refreshInterval;

	public JobVertexBackPressureHandler(
			ExecutionGraphCache executionGraphHolder,
			Executor executor,
			BackPressureStatsTrackerImpl backPressureStatsTrackerImpl,
			int refreshInterval) {

		super(executionGraphHolder, executor);
		this.backPressureStatsTrackerImpl = checkNotNull(backPressureStatsTrackerImpl, "Stats tracker");
		checkArgument(refreshInterval >= 0, "Negative timeout");
		this.refreshInterval = refreshInterval;
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_VERTEX_BACKPRESSURE_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(
			AccessExecutionJobVertex accessJobVertex,
			Map<String, String> params) {
		if (accessJobVertex instanceof ArchivedExecutionJobVertex) {
			return CompletableFuture.completedFuture("");
		}
		ExecutionJobVertex jobVertex = (ExecutionJobVertex) accessJobVertex;
		try (StringWriter writer = new StringWriter();
				JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer)) {

			gen.writeStartObject();

			Optional<OperatorBackPressureStats> statsOption = backPressureStatsTrackerImpl
					.getOperatorBackPressureStats(jobVertex);

			if (statsOption.isPresent()) {
				OperatorBackPressureStats stats = statsOption.get();

				// Check whether we need to refresh
				if (refreshInterval <= System.currentTimeMillis() - stats.getEndTimestamp()) {
					backPressureStatsTrackerImpl.triggerStackTraceSample(jobVertex);
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
				backPressureStatsTrackerImpl.triggerStackTraceSample(jobVertex);
				gen.writeStringField("status", "deprecated");
			}

			gen.writeEndObject();
			gen.close();

			return CompletableFuture.completedFuture(writer.toString());
		} catch (IOException e) {
			return FutureUtils.completedExceptionally(e);
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
