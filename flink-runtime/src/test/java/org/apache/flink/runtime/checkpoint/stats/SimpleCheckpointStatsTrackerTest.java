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

package org.apache.flink.runtime.checkpoint.stats;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimpleCheckpointStatsTrackerTest {

	private static final Random RAND = new Random();
	
	@Test
	public void testNoCompletedCheckpointYet() throws Exception {
		CheckpointStatsTracker tracker = new SimpleCheckpointStatsTracker(
				0, Collections.<ExecutionJobVertex>emptyList());

		assertFalse(tracker.getJobStats().isDefined());
		assertFalse(tracker.getOperatorStats(new JobVertexID()).isDefined());
	}

	@Test
	public void testRandomStats() throws Exception {
		CompletedCheckpoint[] checkpoints = generateRandomCheckpoints(16);
		List<ExecutionJobVertex> tasksToWaitFor = createTasksToWaitFor(checkpoints[0]);
		CheckpointStatsTracker tracker = new SimpleCheckpointStatsTracker(10, tasksToWaitFor);

		for (int i = 0; i < checkpoints.length; i++) {
			CompletedCheckpoint checkpoint = checkpoints[i];

			tracker.onCompletedCheckpoint(checkpoint);

			verifyJobStats(tracker, 10, Arrays.copyOfRange(checkpoints, 0, i + 1));
			verifySubtaskStats(tracker, tasksToWaitFor, checkpoint);
		}
	}

	@Test
	public void testIllegalOperatorId() throws Exception {
		CompletedCheckpoint[] checkpoints = generateRandomCheckpoints(16);
		List<ExecutionJobVertex> tasksToWaitFor = createTasksToWaitFor(checkpoints[0]);
		CheckpointStatsTracker tracker = new SimpleCheckpointStatsTracker(10, tasksToWaitFor);

		for (CompletedCheckpoint checkpoint : checkpoints) {
			tracker.onCompletedCheckpoint(checkpoint);
		}

		assertTrue(tracker.getJobStats().isDefined());

		assertTrue(tracker.getOperatorStats(new JobVertexID()).isEmpty());
	}

	@Test
	public void testCompletedCheckpointReordering() throws Exception {
		CompletedCheckpoint[] checkpoints = generateRandomCheckpoints(2);
		List<ExecutionJobVertex> tasksToWaitFor = createTasksToWaitFor(checkpoints[0]);
		CheckpointStatsTracker tracker = new SimpleCheckpointStatsTracker(10, tasksToWaitFor);

		// First the second checkpoint notifies
		tracker.onCompletedCheckpoint(checkpoints[1]);
		verifyJobStats(tracker, 10, new CompletedCheckpoint[] { checkpoints[1] });
		verifySubtaskStats(tracker, tasksToWaitFor, checkpoints[1]);

		// Then the first one
		tracker.onCompletedCheckpoint(checkpoints[0]);
		verifyJobStats(tracker, 10, checkpoints);

		// This should not alter the results for the subtasks
		verifySubtaskStats(tracker, tasksToWaitFor, checkpoints[1]);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testOperatorStateCachedClearedOnNewCheckpoint() throws Exception {
		CompletedCheckpoint[] checkpoints = generateRandomCheckpoints(2);
		List<ExecutionJobVertex> tasksToWaitFor = createTasksToWaitFor(checkpoints[0]);
		CheckpointStatsTracker tracker = new SimpleCheckpointStatsTracker(10, tasksToWaitFor);

		tracker.onCompletedCheckpoint(checkpoints[0]);

		Set<JobVertexID> jobVerticesID = checkpoints[0].getTaskStates().keySet();

		Iterator<JobVertexID> jobVertexIDIterator = jobVerticesID.iterator();

		JobVertexID operatorId = null;

		if (jobVertexIDIterator.hasNext()) {
			operatorId = jobVertexIDIterator.next();
		}

		assertNotNull(operatorId);

		assertNotNull(tracker.getOperatorStats(operatorId));

		// Get the cache
		Field f = tracker.getClass().getDeclaredField("operatorStatsCache");
		f.setAccessible(true);
		Map<JobVertexID, OperatorCheckpointStats> cache =
				(Map<JobVertexID, OperatorCheckpointStats>) f.get(tracker);

		// Cache contains result
		assertTrue(cache.containsKey(operatorId));

		// Add new checkpoint
		tracker.onCompletedCheckpoint(checkpoints[1]);

		assertTrue(cache.isEmpty());
	}

	// ------------------------------------------------------------------------

	private static void verifyJobStats(
			CheckpointStatsTracker tracker,
			int historySize,
			CompletedCheckpoint[] checkpoints) {

		assertTrue(tracker.getJobStats().isDefined());
		JobCheckpointStats jobStats = tracker.getJobStats().get();

		// History
		List<CheckpointStats> history = jobStats.getRecentHistory();

		if (historySize > checkpoints.length) {
			assertEquals(checkpoints.length, history.size());
		}
		else {
			assertEquals(historySize, history.size());
		}

		// Recently completed checkpoint stats
		assertTrue(checkpoints.length >= history.size());

		for (int i = 0; i < history.size(); i++) {
			CheckpointStats actualStats = history.get(history.size() - i - 1);

			CompletedCheckpoint checkpoint = checkpoints[checkpoints.length - 1 - i];

			long stateSize = checkpoint.getStateSize();

			CheckpointStats expectedStats = new CheckpointStats(
					checkpoint.getCheckpointID(),
					checkpoint.getTimestamp(),
					checkpoint.getDuration(),
					stateSize);

			assertEquals(expectedStats, actualStats);
		}

		// Stats
		long minDuration = Long.MAX_VALUE;
		long maxDuration = Long.MIN_VALUE;
		long totalDuration = 0;

		long minStateSize = Long.MAX_VALUE;
		long maxStateSize = Long.MIN_VALUE;
		long totalStateSize = 0;

		long count = 0;

		// Compute the expected stats
		for (CompletedCheckpoint checkpoint : checkpoints) {
			count++;

			if (checkpoint.getDuration() < minDuration) {
				minDuration = checkpoint.getDuration();
			}

			if (checkpoint.getDuration() > maxDuration) {
				maxDuration = checkpoint.getDuration();
			}

			totalDuration += checkpoint.getDuration();

			long stateSize = checkpoint.getStateSize();

			// State size
			if (stateSize < minStateSize) {
				minStateSize = stateSize;
			}

			if (stateSize > maxStateSize) {
				maxStateSize = stateSize;
			}

			totalStateSize += stateSize;
		}

		// Verify
		assertEquals(count, jobStats.getCount());
		assertEquals(minDuration, jobStats.getMinDuration());
		assertEquals(maxDuration, jobStats.getMaxDuration());
		assertEquals(totalDuration / count, jobStats.getAverageDuration());
		assertEquals(minStateSize, jobStats.getMinStateSize());
		assertEquals(maxStateSize, jobStats.getMaxStateSize());
		assertEquals(totalStateSize / count, jobStats.getAverageStateSize());
	}

	private static void verifySubtaskStats(
			CheckpointStatsTracker tracker,
			List<ExecutionJobVertex> tasksToWaitFor,
			CompletedCheckpoint checkpoint) {

		for (ExecutionJobVertex vertex : tasksToWaitFor) {
			JobVertexID operatorId = vertex.getJobVertexId();
			int parallelism = vertex.getParallelism();
			TaskState taskState = checkpoint.getTaskState(operatorId);

			assertNotNull(taskState);

			OperatorCheckpointStats actualStats = tracker.getOperatorStats(operatorId).get();

			long operatorDuration = Long.MIN_VALUE;
			long operatorStateSize = 0;

			long[][] expectedSubTaskStats = new long[parallelism][2];

			for (int i = 0; i < parallelism; i++) {
				SubtaskState subtaskState = taskState.getState(i);

				expectedSubTaskStats[i][0] = subtaskState.getDuration();
				expectedSubTaskStats[i][1] = subtaskState.getStateSize();
			}

			OperatorCheckpointStats expectedStats = new OperatorCheckpointStats(
					checkpoint.getCheckpointID(),
					checkpoint.getTimestamp(),
					operatorDuration, // we want the max duration of all subtasks
					operatorStateSize,
					expectedSubTaskStats);

			assertEquals(expectedStats, actualStats);
		}
	}

	private static CompletedCheckpoint[] generateRandomCheckpoints(
			int numCheckpoints) throws IOException {

		// Config
		JobID jobId = new JobID();
		int minNumOperators = 4;
		int maxNumOperators = 32;
		int minParallelism = 4;
		int maxParallelism = 16;

		// Use yuge numbers here in order to test that summing up state sizes
		// does not overflow. This was a bug in the initial version, because
		// the individual state sizes (longs) were summed up in an int.
		long minStateSize = Integer.MAX_VALUE;
		long maxStateSize = Long.MAX_VALUE;
		CompletedCheckpoint[] checkpoints = new CompletedCheckpoint[numCheckpoints];

		int numOperators = RAND.nextInt(maxNumOperators - minNumOperators + 1) + minNumOperators;

		// Setup
		JobVertexID[] operatorIds = new JobVertexID[numOperators];
		int[] operatorParallelism = new int[numOperators];

		for (int i = 0; i < numOperators; i++) {
			operatorIds[i] = new JobVertexID();
			operatorParallelism[i] = RAND.nextInt(maxParallelism - minParallelism + 1) + minParallelism;
		}

		// Generate checkpoints
		for (int i = 0; i < numCheckpoints; i++) {
			long triggerTimestamp = System.currentTimeMillis();
			int maxDuration = RAND.nextInt(128 + 1);

			Map<JobVertexID, TaskState> taskGroupStates = new HashMap<>();

			// The maximum random duration is used as time to completion
			int completionDuration = 0;

			// Generate states for same set of operators
			for (int operatorIndex = 0; operatorIndex < numOperators; operatorIndex++) {
				JobVertexID operatorId = operatorIds[operatorIndex];
				int parallelism = operatorParallelism[operatorIndex];

				TaskState taskState = new TaskState(operatorId, parallelism);

				taskGroupStates.put(operatorId, taskState);

				for (int subtaskIndex = 0; subtaskIndex < parallelism; subtaskIndex++) {
					int duration = RAND.nextInt(maxDuration + 1);

					if (duration > completionDuration) {
						completionDuration = duration;
					}

					SubtaskState subtaskState = new SubtaskState(
						new SerializedValue<StateHandle<?>>(null),
						minStateSize + ((long) (RAND.nextDouble() * (maxStateSize - minStateSize))),
						duration);

					taskState.putState(subtaskIndex, subtaskState);
				}
			}

			// Add some random delay
			final long completionTimestamp = triggerTimestamp + completionDuration + RAND.nextInt(10);

			checkpoints[i] = new CompletedCheckpoint(
					jobId, i, triggerTimestamp, completionTimestamp, taskGroupStates);
		}

		return checkpoints;
	}

	private List<ExecutionJobVertex> createTasksToWaitFor(CompletedCheckpoint checkpoint) {

		List<ExecutionJobVertex> jobVertices = new ArrayList<>(checkpoint.getTaskStates().size());

		for (Map.Entry<JobVertexID, TaskState> entry : checkpoint.getTaskStates().entrySet()) {
			JobVertexID operatorId = entry.getKey();
			int parallelism = entry.getValue().getParallelism();
			ExecutionJobVertex v = mock(ExecutionJobVertex.class);
			when(v.getJobVertexId()).thenReturn(operatorId);
			when(v.getParallelism()).thenReturn(parallelism);
			
			jobVertices.add(v);
		}

		return jobVertices;
	}
}
