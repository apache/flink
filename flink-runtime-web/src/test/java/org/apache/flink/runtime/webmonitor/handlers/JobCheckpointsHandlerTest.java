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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStats;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.JobCheckpointStats;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import org.junit.Test;
import scala.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobCheckpointsHandlerTest {

	@Test
	public void testNoCoordinator() throws Exception {
		JobCheckpointsHandler handler = new JobCheckpointsHandler(
				mock(ExecutionGraphHolder.class));

		ExecutionGraph graph = mock(ExecutionGraph.class);

		// No coordinator
		when(graph.getCheckpointStatsTracker()).thenReturn(null);

		String response = handler.handleRequest(graph, Collections.<String, String>emptyMap());

		// Expecting empty response
		assertEquals("{}", response);
	}

	@Test
	public void testNoStats() throws Exception {
		JobCheckpointsHandler handler = new JobCheckpointsHandler(
				mock(ExecutionGraphHolder.class));

		ExecutionGraph graph = mock(ExecutionGraph.class);
		CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);

		when(graph.getCheckpointStatsTracker()).thenReturn(tracker);

		// No stats
		when(tracker.getJobStats()).thenReturn(Option.<JobCheckpointStats>empty());

		String response = handler.handleRequest(graph, Collections.<String, String>emptyMap());

		// Expecting empty response
		assertEquals("{}", response);
	}

	@Test
	public void testStats() throws Exception {
		JobCheckpointsHandler handler = new JobCheckpointsHandler(
				mock(ExecutionGraphHolder.class));

		ExecutionGraph graph = mock(ExecutionGraph.class);
		CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);

		when(graph.getCheckpointStatsTracker()).thenReturn(tracker);

		final List<CheckpointStats> history = new ArrayList<>();
		history.add(new CheckpointStats(0, 1, 1, 124));
		history.add(new CheckpointStats(1, 5, 177, 0));
		history.add(new CheckpointStats(2, 6, 8282, 2));
		history.add(new CheckpointStats(3, 6812, 2800, 1024));

		JobCheckpointStats stats = new JobCheckpointStats() {
			@Override
			public List<CheckpointStats> getRecentHistory() {
				return history;
			}

			@Override
			public long getCount() {
				return 4;
			}

			@Override
			public long getMinDuration() {
				return 1;
			}

			@Override
			public long getMaxDuration() {
				return 8282;
			}

			@Override
			public long getAverageDuration() {
				return 2815;
			}

			@Override
			public long getMinStateSize() {
				return 0;
			}

			@Override
			public long getMaxStateSize() {
				return 1024;
			}

			@Override
			public long getAverageStateSize() {
				return 287;
			}
		};

		when(tracker.getJobStats()).thenReturn(Option.apply(stats));

		// Request stats
		String response = handler.handleRequest(graph, Collections.<String, String>emptyMap());

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readTree(response);

		// Count
		int count = rootNode.get("count").asInt();
		assertEquals(stats.getCount(), count);

		// Duration
		JsonNode durationNode = rootNode.get("duration");
		assertNotNull(durationNode);

		long minDuration = durationNode.get("min").asLong();
		long maxDuration = durationNode.get("max").asLong();
		long avgDuration = durationNode.get("avg").asLong();

		assertEquals(stats.getMinDuration(), minDuration);
		assertEquals(stats.getMaxDuration(), maxDuration);
		assertEquals(stats.getAverageDuration(), avgDuration);

		// State size
		JsonNode sizeNode = rootNode.get("size");
		assertNotNull(sizeNode);

		long minSize = sizeNode.get("min").asLong();
		long maxSize = sizeNode.get("max").asLong();
		long avgSize = sizeNode.get("avg").asLong();

		assertEquals(stats.getMinStateSize(), minSize);
		assertEquals(stats.getMaxStateSize(), maxSize);
		assertEquals(stats.getAverageStateSize(), avgSize);

		JsonNode historyNode = rootNode.get("history");
		assertNotNull(historyNode);
		assertTrue(historyNode.isArray());

		Iterator<JsonNode> it = historyNode.elements();

		for (int i = 0; i < history.size(); i++) {
			CheckpointStats s = history.get(i);

			JsonNode node = it.next();

			long checkpointId = node.get("id").asLong();
			long timestamp = node.get("timestamp").asLong();
			long duration = node.get("duration").asLong();
			long size = node.get("size").asLong();

			assertEquals(s.getCheckpointId(), checkpointId);
			assertEquals(s.getTriggerTimestamp(), timestamp);
			assertEquals(s.getDuration(), duration);
			assertEquals(s.getStateSize(), size);
		}

		assertFalse(it.hasNext());
	}
}
