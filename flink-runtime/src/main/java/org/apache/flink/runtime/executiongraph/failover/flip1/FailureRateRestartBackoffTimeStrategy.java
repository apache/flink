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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.clock.Clock;
import org.apache.flink.runtime.util.clock.SystemClock;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.apache.flink.configuration.RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_FAILURE_RATE_BACKOFF_TIME;
import static org.apache.flink.configuration.RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL;
import static org.apache.flink.configuration.RestartBackoffTimeStrategyOptions.RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Restart strategy which can restart when failure rate is not exceeded.
 */
public class FailureRateRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {

	private final long failuresIntervalMS;

	private final long backoffTimeMS;

	private final int maxFailuresPerInterval;

	private final Deque<Long> failureTimestamps;

	private final String strategyString;

	private final Clock clock;

	FailureRateRestartBackoffTimeStrategy(Clock clock, int maxFailuresPerInterval, long failuresIntervalMS, long backoffTimeMS) {

		checkArgument(maxFailuresPerInterval > 0, "Maximum number of restart attempts per time unit must be greater than 0.");
		checkArgument(failuresIntervalMS > 0, "Failures interval must be greater than 0 ms.");
		checkArgument(backoffTimeMS >= 0, "Backoff time must be at least 0 ms.");

		this.failuresIntervalMS = failuresIntervalMS;
		this.backoffTimeMS = backoffTimeMS;
		this.maxFailuresPerInterval = maxFailuresPerInterval;
		this.failureTimestamps = new ArrayDeque<>(maxFailuresPerInterval);
		this.strategyString = generateStrategyString();
		this.clock = checkNotNull(clock);
	}

	@Override
	public boolean canRestart() {
		if (isFailureTimestampsQueueFull()) {
			Long now = clock.absoluteTimeMillis();
			Long earliestFailure = failureTimestamps.peek();

			return (now - earliestFailure) > failuresIntervalMS;
		} else {
			return true;
		}
	}

	@Override
	public long getBackoffTime() {
		return backoffTimeMS;
	}

	@Override
	public void notifyFailure(Throwable cause) {
		if (isFailureTimestampsQueueFull()) {
			failureTimestamps.remove();
		}
		failureTimestamps.add(clock.absoluteTimeMillis());
	}

	@Override
	public String toString() {
		return strategyString;
	}

	private boolean isFailureTimestampsQueueFull() {
		return failureTimestamps.size() >= maxFailuresPerInterval;
	}

	private String generateStrategyString() {
		StringBuilder str = new StringBuilder("FailureRateRestartBackoffTimeStrategy(");
		str.append("FailureRateRestartBackoffTimeStrategy(failuresIntervalMS=");
		str.append(failuresIntervalMS);
		str.append(",backoffTimeMS=");
		str.append(backoffTimeMS);
		str.append(",maxFailuresPerInterval=");
		str.append(maxFailuresPerInterval);
		str.append(")");

		return str.toString();
	}

	public static FailureRateRestartBackoffTimeStrategyFactory createFactory(final Configuration configuration) {
		return new FailureRateRestartBackoffTimeStrategyFactory(
				configuration.getInteger(RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL),
				configuration.getLong(RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL),
				configuration.getLong(RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_FAILURE_RATE_BACKOFF_TIME));
	}

	/**
	 * The factory for creating {@link FailureRateRestartBackoffTimeStrategy}.
	 */
	public static class FailureRateRestartBackoffTimeStrategyFactory implements RestartBackoffTimeStrategy.Factory {

		private final int maxFailuresPerInterval;

		private final long failuresIntervalMS;

		private final long backoffTimeMS;

		public FailureRateRestartBackoffTimeStrategyFactory(
				int maxFailuresPerInterval,
				long failuresIntervalMS,
				long backoffTimeMS) {

			this.maxFailuresPerInterval = maxFailuresPerInterval;
			this.failuresIntervalMS = failuresIntervalMS;
			this.backoffTimeMS = backoffTimeMS;
		}

		@Override
		public RestartBackoffTimeStrategy create() {
			return new FailureRateRestartBackoffTimeStrategy(
				SystemClock.getInstance(),
				maxFailuresPerInterval,
				failuresIntervalMS,
				backoffTimeMS);
		}
	}
}
