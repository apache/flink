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

import com.google.common.base.Preconditions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static akka.dispatch.Futures.future;

/**
 * Restart strategy which tries to restart the given {@link ExecutionGraph} when failure rate exceeded
 * with a fixed time delay in between.
 */
public class FailureRateRestartStrategy implements RestartStrategy {
	private final int maxFailuresPerUnit;
	private final TimeUnit failureRateUnit;
	private final long delayBetweenRestartAttempts;
	private List<Long> restartTimestamps = new ArrayList<>();
	private boolean disabled = false;

	public FailureRateRestartStrategy(int maxFailuresPerUnit, TimeUnit failureRateUnit, long delayBetweenRestartAttempts) {
		Preconditions.checkArgument(maxFailuresPerUnit > 0, "Maximum number of restart attempts per time unit must be greater than 0.");
		Preconditions.checkArgument(delayBetweenRestartAttempts >= 0, "Delay between restart attempts must be positive");

		this.maxFailuresPerUnit = maxFailuresPerUnit;
		this.failureRateUnit = failureRateUnit;
		this.delayBetweenRestartAttempts = delayBetweenRestartAttempts;
	}

	@Override
	public boolean canRestart() {
		return !disabled && canRestartJob();
	}

	private boolean canRestartJob() {
		int restartsInWindowSoFar = restartTimestamps.size();
		if (restartsInWindowSoFar >= maxFailuresPerUnit) {
			List<Long> lastFailures = restartTimestamps.subList(restartsInWindowSoFar - maxFailuresPerUnit, restartsInWindowSoFar);
			restartTimestamps = lastFailures; //deallocating not needed timestamps
			Long earliestFailure = lastFailures.get(0);
			Long now = System.currentTimeMillis();
			return Duration.apply(now - earliestFailure, TimeUnit.MILLISECONDS).gt(Duration.apply(1, failureRateUnit));
		} else {
			return true;
		}
	}

	@Override
	public void restart(final ExecutionGraph executionGraph) {
		restartTimestamps.add(System.currentTimeMillis());
		future(ExecutionGraphRestarter.restartWithDelay(executionGraph, delayBetweenRestartAttempts), executionGraph.getExecutionContext());
	}

	@Override
	public void disable() {
		disabled = true;
	}

	public static FailureRateRestartStrategyFactory createFactory(Configuration configuration) throws Exception {
		int maxFailuresPerUnit = configuration.getInteger(ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_UNIT, 1);
		String failureRateUnitString = configuration.getString(ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_UNIT, TimeUnit.MINUTES.name());
		TimeUnit failureRateUnit = TimeUnit.valueOf(failureRateUnitString);
		String timeoutString = configuration.getString(ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL, ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT);
		String delayString = configuration.getString(ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_DELAY, timeoutString);
		long delay = Duration.apply(delayString).toMillis();
		return new FailureRateRestartStrategyFactory(maxFailuresPerUnit, failureRateUnit, delay);
	}

	public static class FailureRateRestartStrategyFactory extends RestartStrategyFactory {
		private final int maxFailuresPerUnit;
		private final TimeUnit failureRateUnit;
		private final long delayBetweenRestartAttempts;

		public FailureRateRestartStrategyFactory(int maxFailuresPerUnit, TimeUnit failureRateUnit, long delayBetweenRestartAttempts) {
			this.maxFailuresPerUnit = maxFailuresPerUnit;
			this.failureRateUnit = failureRateUnit;
			this.delayBetweenRestartAttempts = delayBetweenRestartAttempts;
		}

		@Override
		public RestartStrategy createRestartStrategy() {
			return new FailureRateRestartStrategy(maxFailuresPerUnit, failureRateUnit, delayBetweenRestartAttempts);
		}
	}
}