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

import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.StateForTask;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import scala.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A simple checkpoint stats tracker.
 */
public class SimpleCheckpointStatsTracker implements CheckpointStatsTracker {

	/** Lock guarding access to the stats */
	private final Object statsLock = new Object();

	/** The maximum number of recent checkpoint stats to remember. */
	private final int historySize;

	/** A bounded list of detailed stats. */
	private final ArrayList<CheckpointStats> history = new ArrayList<>();

	/**
	 * Expected parallelism of tasks, which acknowledge checkpoints. Used for
	 * per sub task state size computation.
	 */
	private final Map<JobVertexID, Integer> taskParallelism;

	/**
	 * Stats per operator. The long[parallelism][2], where [i][0] holds the
	 * duration, [i][1] the state size for sub task i of the operator.
	 */
	private Map<JobVertexID, long[][]> subTaskStats;

	/**
	 * Last computed job-specific statistic. Cleared on every completed
	 * checkpoint. And computed only on call to {@link #getJobStats()}.
	 */
	private JobCheckpointStats lastJobStats;

	/**
	 * A map caching computed operator-specific statistics. Cleared on every
	 * completed checkpoint. And computed only on call to {@link #getOperatorStats(JobVertexID)}.
	 */
	private Map<JobVertexID, OperatorCheckpointStats> operatorStatsCache = new HashMap<>();

	/**
	 * The total number of completed checkpoints. This does not always
	 * equal the last completed ID, because some checkpoints may have been
	 * cancelled after incrementing the ID counter.
	 */
	private long overallCount;

	/** The minimum checkpoint completion duration (over all checkpoints). */
	private long overallMinDuration = Long.MAX_VALUE;

	/** The maximum checkpoint completion duration (over all checkpoints). */
	private long overallMaxDuration = Long.MIN_VALUE;

	/**
	 * The total checkpoint completion duration of all completed checkpoints.
	 * Used for computing the average duration.
	 */
	private long overallTotalDuration;

	/** The minimum checkpoint state size (over all checkpoints). */
	private long overallMinStateSize = Long.MAX_VALUE;

	/** The maximum checkpoint state size (over all checkpoints). */
	private long overallMaxStateSize = Long.MIN_VALUE;

	/**
	 * The total checkpoint state size (over all checkpoints). Used for
	 * computing the overall average state size.
	 */
	private long overallTotalStateSize;

	/**
	 * The latest completed checkpoint (highest ID) or <code>null</code>.
	 */
	private CompletedCheckpoint latestCompletedCheckpoint;

	public SimpleCheckpointStatsTracker(
			int historySize,
			ExecutionVertex[] tasksToWaitFor) {

		checkArgument(historySize >= 0);
		this.historySize = historySize;

		// We know upfront, which tasks will ack the checkpoints.
		if (tasksToWaitFor != null && tasksToWaitFor.length > 0) {
			taskParallelism = new HashMap<>();

			for (ExecutionVertex vertex : tasksToWaitFor) {
				taskParallelism.put(
						vertex.getJobvertexId(),
						vertex.getTotalNumberOfParallelSubtasks());
			}
		}
		else {
			taskParallelism = Collections.emptyMap();
		}
	}

	@Override
	public void onCompletedCheckpoint(CompletedCheckpoint checkpoint) {
		// Sanity check
		if (taskParallelism.isEmpty()) {
			return;
		}

		synchronized (statsLock) {
			int overallStateSize = 0;

			// Operator stats
			Map<JobVertexID, long[][]> statsForSubTasks = new HashMap<>();

			for (StateForTask state : checkpoint.getStates()) {
				// Job-level checkpoint size is sum of all state sizes
				overallStateSize += state.getStateSize();

				// Subtask stats
				JobVertexID opId = state.getOperatorId();
				long[][] statsPerSubtask = statsForSubTasks.get(opId);

				if (statsPerSubtask == null) {
					int parallelism = taskParallelism.get(opId);
					statsPerSubtask = new long[parallelism][2];
					statsForSubTasks.put(opId, statsPerSubtask);
				}

				int subTaskIndex = state.getSubtask();
				if (subTaskIndex < statsPerSubtask.length) {
					statsPerSubtask[subTaskIndex][0] = state.getDuration();
					statsPerSubtask[subTaskIndex][1] = state.getStateSize();
				}
			}

			// It is possible that completed checkpoints are added out of
			// order. Make sure that in this case the last completed
			// checkpoint is not updated.
			boolean isInOrder = latestCompletedCheckpoint != null &&
					checkpoint.getCheckpointID() > latestCompletedCheckpoint.getCheckpointID();

			// Clear this in each case
			lastJobStats = null;

			if (overallCount == 0 || isInOrder) {
				latestCompletedCheckpoint = checkpoint;

				// Clear cached stats
				operatorStatsCache.clear();

				// Update the stats per sub task
				subTaskStats = statsForSubTasks;
			}

			long checkpointId = checkpoint.getCheckpointID();
			long checkpointTriggerTimestamp = checkpoint.getTimestamp();
			long checkpointDuration = checkpoint.getDuration();

			overallCount++;

			// Duration stats
			if (checkpointDuration > overallMaxDuration) {
				overallMaxDuration = checkpointDuration;
			}

			if (checkpointDuration < overallMinDuration) {
				overallMinDuration = checkpointDuration;
			}

			overallTotalDuration += checkpointDuration;

			// State size stats
			if (overallStateSize < overallMinStateSize) {
				overallMinStateSize = overallStateSize;
			}

			if (overallStateSize > overallMaxStateSize) {
				overallMaxStateSize = overallStateSize;
			}

			this.overallTotalStateSize += overallStateSize;

			// Recent history
			if (historySize > 0) {
				CheckpointStats stats = new CheckpointStats(
						checkpointId,
						checkpointTriggerTimestamp,
						checkpointDuration,
						overallStateSize);

				if (isInOrder) {
					if (history.size() == historySize) {
						history.remove(0);
					}

					history.add(stats);
				}
				else {
					final int size = history.size();

					// Only remove it if it the new checkpoint is not too old
					if (size == historySize) {
						if (checkpointId > history.get(0).getCheckpointId()) {
							history.remove(0);
						}
					}

					int pos = 0;

					// Find position
					for (int i = 0; i < size; i++) {
						pos = i;

						if (checkpointId < history.get(i).getCheckpointId()) {
							break;
						}
					}

					history.add(pos, stats);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Option<JobCheckpointStats> getJobStats() {
		synchronized (statsLock) {
			if (lastJobStats != null) {
				return Option.apply(lastJobStats);
			}
			else if (latestCompletedCheckpoint != null) {
				long overallAverageDuration = overallCount == 0
						? 0
						: overallTotalDuration / overallCount;

				long overallAverageStateSize = overallCount == 0
						? 0
						: overallTotalStateSize / overallCount;

				lastJobStats = new JobCheckpointStatsSnapshot(
						// Need to clone in order to have a consistent snapshot.
						// We can safely update it afterwards.
						(List<CheckpointStats>) history.clone(),
						overallCount,
						overallMinDuration,
						overallMaxDuration,
						overallAverageDuration,
						overallMinStateSize,
						overallMaxStateSize,
						overallAverageStateSize);

				return Option.apply(lastJobStats);
			}
			else {
				return Option.empty();
			}
		}
	}

	@Override
	public Option<OperatorCheckpointStats> getOperatorStats(JobVertexID operatorId) {
		synchronized (statsLock) {
			OperatorCheckpointStats stats = operatorStatsCache.get(operatorId);

			if (stats != null) {
				return Option.apply(stats);
			}
			else if (latestCompletedCheckpoint != null && subTaskStats != null) {
				long[][] subTaskStats = this.subTaskStats.get(operatorId);

				if (subTaskStats == null) {
					return Option.empty();
				}
				else {
					long maxDuration = Long.MIN_VALUE;
					long stateSize = 0;

					for (long[] subTaskStat : subTaskStats) {
						if (subTaskStat[0] > maxDuration) {
							maxDuration = subTaskStat[0];
						}

						stateSize += subTaskStat[1];
					}

					stats = new OperatorCheckpointStats(
							latestCompletedCheckpoint.getCheckpointID(),
							latestCompletedCheckpoint.getTimestamp(),
							maxDuration,
							stateSize,
							subTaskStats);

					// Remember this and don't recompute if requested again
					operatorStatsCache.put(operatorId, stats);

					return Option.apply(stats);
				}
			}
			else {
				return Option.empty();
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A snapshot of checkpoint stats.
	 */
	private static class JobCheckpointStatsSnapshot implements JobCheckpointStats {

		// General
		private final List<CheckpointStats> recentHistory;
		private final long count;

		// Duration
		private final long minDuration;
		private final long maxDuration;
		private final long averageDuration;

		// State size
		private final long minStateSize;
		private final long maxStateSize;
		private final long averageStateSize;

		public JobCheckpointStatsSnapshot(
				List<CheckpointStats> recentHistory,
				long count,
				long minDuration,
				long maxDuration,
				long averageDuration,
				long minStateSize,
				long maxStateSize,
				long averageStateSize) {

			this.recentHistory = recentHistory;
			this.count = count;

			this.minDuration = minDuration;
			this.maxDuration = maxDuration;
			this.averageDuration = averageDuration;

			this.minStateSize = minStateSize;
			this.maxStateSize = maxStateSize;
			this.averageStateSize = averageStateSize;
		}

		@Override
		public List<CheckpointStats> getRecentHistory() {
			return recentHistory;
		}

		@Override
		public long getCount() {
			return count;
		}

		@Override
		public long getMinDuration() {
			return minDuration;
		}

		@Override
		public long getMaxDuration() {
			return maxDuration;
		}

		@Override
		public long getAverageDuration() {
			return averageDuration;
		}

		@Override
		public long getMinStateSize() {
			return minStateSize;
		}

		@Override
		public long getMaxStateSize() {
			return maxStateSize;
		}

		@Override
		public long getAverageStateSize() {
			return averageStateSize;
		}
	}
}
