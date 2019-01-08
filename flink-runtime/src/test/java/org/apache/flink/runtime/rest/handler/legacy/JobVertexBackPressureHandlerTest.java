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

import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTrackerImpl;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for back pressure handler responses.
 */
public class JobVertexBackPressureHandlerTest extends TestLogger {
	@Test
	public void testGetPaths() {
		JobVertexBackPressureHandler handler = new JobVertexBackPressureHandler(mock(ExecutionGraphCache.class), Executors.directExecutor(), mock(BackPressureStatsTrackerImpl.class), 0);
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/vertices/:vertexid/backpressure", paths[0]);
	}

	/** Tests the response when no stats are available. */
	@Test
	public void testResponseNoStatsAvailable() throws Exception {
		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		BackPressureStatsTrackerImpl statsTracker = mock(BackPressureStatsTrackerImpl.class);

		when(statsTracker.getOperatorBackPressureStats(any(ExecutionJobVertex.class)))
				.thenReturn(Optional.empty());

		JobVertexBackPressureHandler handler = new JobVertexBackPressureHandler(
				mock(ExecutionGraphCache.class),
				Executors.directExecutor(),
				statsTracker,
				9999);

		String response = handler.handleRequest(jobVertex, Collections.<String, String>emptyMap()).get();

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(response);

		// Single element
		assertEquals(1, rootNode.size());

		// Status
		JsonNode status = rootNode.get("status");
		assertNotNull(status);
		assertEquals("deprecated", status.textValue());

		verify(statsTracker).triggerStackTraceSample(any(ExecutionJobVertex.class));
	}

	/** Tests the response when stats are available. */
	@Test
	public void testResponseStatsAvailable() throws Exception {
		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		BackPressureStatsTrackerImpl statsTracker = mock(BackPressureStatsTrackerImpl.class);

		OperatorBackPressureStats stats = new OperatorBackPressureStats(
				0, System.currentTimeMillis(), new double[] { 0.31, 0.48, 1.0, 0.0 });

		when(statsTracker.getOperatorBackPressureStats(any(ExecutionJobVertex.class)))
				.thenReturn(Optional.of(stats));

		JobVertexBackPressureHandler handler = new JobVertexBackPressureHandler(
				mock(ExecutionGraphCache.class),
				Executors.directExecutor(),
				statsTracker,
				9999);

		String response = handler.handleRequest(jobVertex, Collections.<String, String>emptyMap()).get();

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(response);

		// Single element
		assertEquals(4, rootNode.size());

		// Status
		JsonNode status = rootNode.get("status");
		assertNotNull(status);
		assertEquals("ok", status.textValue());

		// Back pressure level
		JsonNode backPressureLevel = rootNode.get("backpressure-level");
		assertNotNull(backPressureLevel);
		assertEquals("high", backPressureLevel.textValue());

		// End time stamp
		JsonNode endTimeStamp = rootNode.get("end-timestamp");
		assertNotNull(endTimeStamp);
		assertEquals(stats.getEndTimestamp(), endTimeStamp.longValue());

		// Subtasks
		JsonNode subTasks = rootNode.get("subtasks");
		assertEquals(stats.getNumberOfSubTasks(), subTasks.size());
		for (int i = 0; i < subTasks.size(); i++) {
			JsonNode subTask = subTasks.get(i);

			JsonNode index = subTask.get("subtask");
			assertEquals(i, index.intValue());

			JsonNode level = subTask.get("backpressure-level");
			assertEquals(JobVertexBackPressureHandler
					.getBackPressureLevel(stats.getBackPressureRatio(i)), level.textValue());

			JsonNode ratio = subTask.get("ratio");
			assertEquals(stats.getBackPressureRatio(i), ratio.doubleValue(), 0.0);
		}

		// Verify not triggered
		verify(statsTracker, never()).triggerStackTraceSample(any(ExecutionJobVertex.class));
	}

	/** Tests that after the refresh interval another sample is triggered. */
	@Test
	public void testResponsePassedRefreshInterval() throws Exception {
		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		BackPressureStatsTrackerImpl statsTracker = mock(BackPressureStatsTrackerImpl.class);

		OperatorBackPressureStats stats = new OperatorBackPressureStats(
				0, System.currentTimeMillis(), new double[] { 0.31, 0.48, 1.0, 0.0 });

		when(statsTracker.getOperatorBackPressureStats(any(ExecutionJobVertex.class)))
				.thenReturn(Optional.of(stats));

		JobVertexBackPressureHandler handler = new JobVertexBackPressureHandler(
				mock(ExecutionGraphCache.class),
				Executors.directExecutor(),
				statsTracker,
				0); // <----- refresh interval should fire immediately

		String response = handler.handleRequest(jobVertex, Collections.<String, String>emptyMap()).get();

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(response);

		// Single element
		assertEquals(4, rootNode.size());

		// Status
		JsonNode status = rootNode.get("status");
		assertNotNull(status);
		// Interval passed, hence deprecated
		assertEquals("deprecated", status.textValue());

		// Back pressure level
		JsonNode backPressureLevel = rootNode.get("backpressure-level");
		assertNotNull(backPressureLevel);
		assertEquals("high", backPressureLevel.textValue());

		// End time stamp
		JsonNode endTimeStamp = rootNode.get("end-timestamp");
		assertNotNull(endTimeStamp);
		assertEquals(stats.getEndTimestamp(), endTimeStamp.longValue());

		// Subtasks
		JsonNode subTasks = rootNode.get("subtasks");
		assertEquals(stats.getNumberOfSubTasks(), subTasks.size());
		for (int i = 0; i < subTasks.size(); i++) {
			JsonNode subTask = subTasks.get(i);

			JsonNode index = subTask.get("subtask");
			assertEquals(i, index.intValue());

			JsonNode level = subTask.get("backpressure-level");
			assertEquals(JobVertexBackPressureHandler
					.getBackPressureLevel(stats.getBackPressureRatio(i)), level.textValue());

			JsonNode ratio = subTask.get("ratio");
			assertEquals(stats.getBackPressureRatio(i), ratio.doubleValue(), 0.0);
		}

		// Verify triggered
		verify(statsTracker).triggerStackTraceSample(any(ExecutionJobVertex.class));
	}
}
