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

package org.apache.flink.runtime.rest.handler.legacy.backpressure;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TestingSlotProvider;
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Utility methods for {@link BackPressureStatsTrackerImplTest} and {@link BackPressureRequestCoordinatorTest}.
 */
public class BackPressureTrackerTestUtils {

	public static ExecutionJobVertex createExecutionJobVertex() throws Exception {
		return new ExecutionJobVertex(
			createExecutionGraph(),
			new JobVertex("TestingJobVertex", new JobVertexID()),
			4,
			JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE.defaultValue(),
			Time.milliseconds(10000),
			1L,
			System.currentTimeMillis());
	}

	public static ExecutionGraph createExecutionGraph() throws IOException {
		final ExecutionGraph executionGraph = new ExecutionGraph(
			new JobInformation(
				new JobID(),
				"BackPressureStatsTrackerImplTest",
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.emptyList(),
				Collections.emptyList()),
			new DirectScheduledExecutorService(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new RestartAllStrategy.Factory(),
			new TestingSlotProvider(ignored -> new CompletableFuture<>()),
			ClassLoader.getSystemClassLoader(),
			VoidBlobWriter.getInstance(),
			Time.milliseconds(60000));
		executionGraph.transitionToRunning();
		return executionGraph;
	}
}
