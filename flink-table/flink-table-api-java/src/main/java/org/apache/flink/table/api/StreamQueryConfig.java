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

package org.apache.flink.table.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.time.Time;

/**
 * The {@link StreamQueryConfig} holds parameters to configure the behavior of streaming queries.
 *
 * @deprecated Set the configuration on {@link TableConfig}.
 */
@Deprecated
@PublicEvolving
public class StreamQueryConfig implements QueryConfig {

	/**
	 * The minimum time until state which was not updated will be retained.
	 * State might be cleared and removed if it was not updated for the defined period of time.
	 */
	private long minIdleStateRetentionTime = 0L;

	/**
	 * The maximum time until state which was not updated will be retained.
	 * State will be cleared and removed if it was not updated for the defined period of time.
	 */
	private long maxIdleStateRetentionTime = 0L;

	@Internal
	public StreamQueryConfig(long minIdleStateRetentionTime, long maxIdleStateRetentionTime) {
		this.minIdleStateRetentionTime = minIdleStateRetentionTime;
		this.maxIdleStateRetentionTime = maxIdleStateRetentionTime;
	}

	public StreamQueryConfig() {
	}

	/**
	 * Specifies a minimum and a maximum time interval for how long idle state, i.e., state which
	 * was not updated, will be retained.
	 * State will never be cleared until it was idle for less than the minimum time and will never
	 * be kept if it was idle for more than the maximum time.
	 *
	 * <p>When new data arrives for previously cleaned-up state, the new data will be handled as if it
	 * was the first data. This can result in previous results being overwritten.
	 *
	 * <p>Set to 0 (zero) to never clean-up the state.
	 *
	 * <p>NOTE: Cleaning up state requires additional bookkeeping which becomes less expensive for
	 * larger differences of minTime and maxTime. The difference between minTime and maxTime must be
	 * at least 5 minutes.
	 *
	 * @param minTime The minimum time interval for which idle state is retained. Set to 0 (zero) to
	 *                never clean-up the state.
	 * @param maxTime The maximum time interval for which idle state is retained. Must be at least
	 *                5 minutes greater than minTime. Set to 0 (zero) to never clean-up the state.
	 */
	public StreamQueryConfig withIdleStateRetentionTime(Time minTime, Time maxTime) {

		if (maxTime.toMilliseconds() - minTime.toMilliseconds() < 300000 &&
				!(maxTime.toMilliseconds() == 0 && minTime.toMilliseconds() == 0)) {
			throw new IllegalArgumentException(
					"Difference between minTime: " + minTime.toString() + " and maxTime: " + maxTime.toString() +
					"shoud be at least 5 minutes.");
		}
		minIdleStateRetentionTime = minTime.toMilliseconds();
		maxIdleStateRetentionTime = maxTime.toMilliseconds();
		return this;
	}

	/**
	 * @return The minimum time until state which was not updated will be retained.
	 */
	public long getMinIdleStateRetentionTime() {
		return minIdleStateRetentionTime;
	}

	/**
	 * @return The maximum time until state which was not updated will be retained.
	 */
	public long getMaxIdleStateRetentionTime() {
		return maxIdleStateRetentionTime;
	}
}
