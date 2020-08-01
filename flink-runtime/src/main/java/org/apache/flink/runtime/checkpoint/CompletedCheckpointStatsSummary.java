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

package org.apache.flink.runtime.checkpoint;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Summary over <strong>all</strong> completed checkpoints.
 */
public class CompletedCheckpointStatsSummary implements Serializable {

	private static final long serialVersionUID = 5784360461635814038L;

	/** State size statistics for all completed checkpoints. */
	private final MinMaxAvgStats stateSize;

	/** Duration statistics for all completed checkpoints. */
	private final MinMaxAvgStats duration;

	CompletedCheckpointStatsSummary() {
		this(new MinMaxAvgStats(), new MinMaxAvgStats());
	}

	private CompletedCheckpointStatsSummary(
			MinMaxAvgStats stateSize,
			MinMaxAvgStats duration) {

		this.stateSize = checkNotNull(stateSize);
		this.duration = checkNotNull(duration);
	}

	/**
	 * Updates the summary with the given completed checkpoint.
	 *
	 * @param completed Completed checkpoint to update the summary with.
	 */
	void updateSummary(CompletedCheckpointStats completed) {
		stateSize.add(completed.getStateSize());
		duration.add(completed.getEndToEndDuration());
	}

	/**
	 * Creates a snapshot of the current state.
	 *
	 * @return A snapshot of the current state.
	 */
	CompletedCheckpointStatsSummary createSnapshot() {
		return new CompletedCheckpointStatsSummary(
				stateSize.createSnapshot(),
				duration.createSnapshot());
	}

	/**
	 * Returns the summary stats for the state size of completed checkpoints.
	 *
	 * @return Summary stats for the state size.
	 */
	public MinMaxAvgStats getStateSizeStats() {
		return stateSize;
	}

	/**
	 * Returns the summary stats for the duration of completed checkpoints.
	 *
	 * @return Summary stats for the duration.
	 */
	public MinMaxAvgStats getEndToEndDurationStats() {
		return duration;
	}
}
