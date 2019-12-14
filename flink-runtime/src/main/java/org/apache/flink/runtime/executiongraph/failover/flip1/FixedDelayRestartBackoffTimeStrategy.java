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
import org.apache.flink.configuration.RestartStrategyOptions;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Restart strategy which tries to restart a fixed number of times with a fixed backoff time in between.
 */
public class FixedDelayRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {

	private final int maxNumberRestartAttempts;

	private final long backoffTimeMS;

	private final String strategyString;

	private int currentRestartAttempt;

	FixedDelayRestartBackoffTimeStrategy(int maxNumberRestartAttempts, long backoffTimeMS) {
		checkArgument(maxNumberRestartAttempts >= 0, "Maximum number of restart attempts must be at least 0.");
		checkArgument(backoffTimeMS >= 0, "Backoff time between restart attempts must be at least 0 ms.");

		this.maxNumberRestartAttempts = maxNumberRestartAttempts;
		this.backoffTimeMS = backoffTimeMS;
		this.currentRestartAttempt = 0;
		this.strategyString = generateStrategyString();
	}

	@Override
	public boolean canRestart() {
		return currentRestartAttempt <= maxNumberRestartAttempts;
	}

	@Override
	public long getBackoffTime() {
		return backoffTimeMS;
	}

	public int getMaxNumberRestartAttempts() {
		return maxNumberRestartAttempts;
	}

	@Override
	public void notifyFailure(Throwable cause) {
		currentRestartAttempt++;
	}

	@Override
	public String toString() {
		return strategyString;
	}

	private String generateStrategyString() {
		StringBuilder str = new StringBuilder("FixedDelayRestartBackoffTimeStrategy(");
		str.append("maxNumberRestartAttempts=");
		str.append(maxNumberRestartAttempts);
		str.append(", backoffTimeMS=");
		str.append(backoffTimeMS);
		str.append(")");

		return str.toString();
	}

	public static FixedDelayRestartBackoffTimeStrategyFactory createFactory(final Configuration configuration) {
		int maxAttempts = configuration.getInteger(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);
		long delay = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY).toMillis();

		return new FixedDelayRestartBackoffTimeStrategyFactory(maxAttempts, delay);
	}

	/**
	 * The factory for creating {@link FixedDelayRestartBackoffTimeStrategy}.
	 */
	public static class FixedDelayRestartBackoffTimeStrategyFactory implements RestartBackoffTimeStrategy.Factory {

		private final int maxNumberRestartAttempts;

		private final long backoffTimeMS;

		public FixedDelayRestartBackoffTimeStrategyFactory(int maxNumberRestartAttempts, long backoffTimeMS) {
			this.maxNumberRestartAttempts = maxNumberRestartAttempts;
			this.backoffTimeMS = backoffTimeMS;
		}

		@Override
		public RestartBackoffTimeStrategy create() {
			return new FixedDelayRestartBackoffTimeStrategy(
					maxNumberRestartAttempts,
					backoffTimeMS);
		}
	}
}
