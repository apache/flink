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

package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.util.Preconditions;
import scala.concurrent.duration.Duration;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

import static akka.dispatch.Futures.future;

/**
 * Restart strategy which tries to restart the given {@link ExecutionGraph} when failure rate exceeded
 * with a fixed time delay in between.
 */
public class FailureRateRestartStrategy implements RestartStrategy {
	private final Duration failuresInterval;
	private final Duration delayInterval;
	private final int maxFailuresPerInterval;
	private final ArrayDeque<Long> restartTimestampsDeque;
	private boolean disabled = false;

	public FailureRateRestartStrategy(int maxFailuresPerInterval, Duration failuresInterval, Duration delayInterval) {
		Preconditions.checkNotNull(failuresInterval, "Failures interval cannot be null.");
		Preconditions.checkNotNull(delayInterval, "Delay interval cannot be null.");
		Preconditions.checkArgument(maxFailuresPerInterval > 0, "Maximum number of restart attempts per time unit must be greater than 0.");
		Preconditions.checkArgument(failuresInterval.length() > 0, "Failures interval must be greater than 0 ms.");
		Preconditions.checkArgument(delayInterval.length() >= 0, "Delay interval must be at least 0 ms.");

		this.failuresInterval = failuresInterval;
		this.delayInterval = delayInterval;
		this.maxFailuresPerInterval = maxFailuresPerInterval;
		this.restartTimestampsDeque = new ArrayDeque<>(maxFailuresPerInterval);
	}

	@Override
	public boolean canRestart() {
		return !disabled && canRestartJob();
	}

	private boolean canRestartJob() {
		if (isRestartTimestampsQueueFull()) {
			Long now = System.currentTimeMillis();
			Long earliestFailure = restartTimestampsDeque.peek();
			return Duration.apply(now - earliestFailure, TimeUnit.MILLISECONDS).gt(failuresInterval);
		} else {
			return true;
		}
	}

	@Override
	public void restart(final ExecutionGraph executionGraph) {
		if (isRestartTimestampsQueueFull()) {
			restartTimestampsDeque.remove();
		}
		restartTimestampsDeque.add(System.currentTimeMillis());
		future(ExecutionGraphRestarter.restartWithDelay(executionGraph, delayInterval.toMillis()), executionGraph.getExecutionContext());
	}

	private boolean isRestartTimestampsQueueFull() {
		return restartTimestampsDeque.size() == maxFailuresPerInterval;
	}

	@Override
	public void disable() {
		disabled = true;
	}

	public static FailureRateRestartStrategyFactory createFactory(Configuration configuration) throws Exception {
		int maxFailuresPerInterval = configuration.getInteger(ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 1);
		String failuresIntervalString = configuration.getString(
				ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL, Duration.apply(1, TimeUnit.MINUTES).toString()
		);
		String timeoutString = configuration.getString(ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL, ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT);
		String delayString = configuration.getString(ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_DELAY, timeoutString);
		return new FailureRateRestartStrategyFactory(maxFailuresPerInterval, Duration.apply(failuresIntervalString), Duration.apply(delayString));
	}

	public static class FailureRateRestartStrategyFactory extends RestartStrategyFactory {
		private final int maxFailuresPerInterval;
		private final Duration failuresInterval;
		private final Duration delayInterval;

		public FailureRateRestartStrategyFactory(int maxFailuresPerInterval, Duration failuresInterval, Duration delayInterval) {
			this.maxFailuresPerInterval = maxFailuresPerInterval;
			this.failuresInterval = failuresInterval;
			this.delayInterval = delayInterval;
		}

		@Override
		public RestartStrategy createRestartStrategy() {
			return new FailureRateRestartStrategy(maxFailuresPerInterval, failuresInterval, delayInterval);
		}
	}
}