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

import org.apache.flink.configuration.ConfigConstants;

import java.util.List;

/**
 * Snapshot of checkpoint statistics for a job.
 */
public interface JobCheckpointStats {

	// ------------------------------------------------------------------------
	// General stats
	// ------------------------------------------------------------------------

	/**
	 * Returns a list of recently completed checkpoints stats.
	 *
	 * <p>The history size is configurable via {@link ConfigConstants#JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE}.
	 *
	 * @return List of recently completed checkpoints stats.
	 */
	List<CheckpointStats> getRecentHistory();

	/**
	 * Returns the total number of completed checkpoints.
	 *
	 * @return Total number of completed checkpoints.
	 */
	long getCount();

	// ------------------------------------------------------------------------
	// Duration
	// ------------------------------------------------------------------------

	/**
	 * Returns the minimum checkpoint duration ever seen over all completed
	 * checkpoints.
	 *
	 * @return Minimum checkpoint duration over all completed checkpoints.
	 */
	long getMinDuration();

	/**
	 * Returns the maximum checkpoint duration ever seen over all completed
	 * checkpoints.
	 *
	 * @return Maximum checkpoint duration over all completed checkpoints.
	 */
	long getMaxDuration();

	/**
	 * Returns the average checkpoint duration ever seen over all completed
	 * checkpoints.
	 *
	 * @return Average checkpoint duration over all completed checkpoints.
	 */
	long getAverageDuration();

	// ------------------------------------------------------------------------
	// State size
	// ------------------------------------------------------------------------

	/**
	 * Returns the minimum checkpoint state size ever seen over all completed
	 * checkpoints.
	 *
	 * @return Minimum checkpoint state size over all completed checkpoints.
	 */
	long getMinStateSize();

	/**
	 * Returns the maximum checkpoint state size ever seen over all completed
	 * checkpoints.
	 *
	 * @return Maximum checkpoint state size over all completed checkpoints.
	 */
	long getMaxStateSize();

	/**
	 * Average the minimum checkpoint state size ever seen over all completed
	 * checkpoints.
	 *
	 * @return Average checkpoint state size over all completed checkpoints.
	 */
	long getAverageStateSize();

}
