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

package org.apache.flink.api.common.restartstrategy;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestartStrategyOptions;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * This class defines methods to generate RestartStrategyConfigurations. These configurations are
 * used to create RestartStrategies at runtime.
 *
 * <p>The RestartStrategyConfigurations are used to decouple the core module from the runtime module.
 */
@PublicEvolving
public class RestartStrategies {

	/**
	 * Generates NoRestartStrategyConfiguration.
	 *
	 * @return NoRestartStrategyConfiguration
	 */
	public static RestartStrategyConfiguration noRestart() {
		return new NoRestartStrategyConfiguration();
	}

	public static RestartStrategyConfiguration fallBackRestart() {
		return new FallbackRestartStrategyConfiguration();
	}

	/**
	 * Generates a FixedDelayRestartStrategyConfiguration.
	 *
	 * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
	 * @param delayBetweenAttempts Delay in-between restart attempts for the FixedDelayRestartStrategy
	 * @return FixedDelayRestartStrategy
	 */
	public static RestartStrategyConfiguration fixedDelayRestart(int restartAttempts, long delayBetweenAttempts) {
		return fixedDelayRestart(restartAttempts, Time.of(delayBetweenAttempts, TimeUnit.MILLISECONDS));
	}

	/**
	 * Generates a FixedDelayRestartStrategyConfiguration.
	 *
	 * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
	 * @param delayInterval Delay in-between restart attempts for the FixedDelayRestartStrategy
	 * @return FixedDelayRestartStrategy
	 */
	public static RestartStrategyConfiguration fixedDelayRestart(int restartAttempts, Time delayInterval) {
		return new FixedDelayRestartStrategyConfiguration(restartAttempts, delayInterval);
	}

	/**
	 * Generates a FailureRateRestartStrategyConfiguration.
	 *
	 * @param failureRate Maximum number of restarts in given interval {@code failureInterval} before failing a job
	 * @param failureInterval Time interval for failures
	 * @param delayInterval Delay in-between restart attempts
	 */
	public static FailureRateRestartStrategyConfiguration failureRateRestart(
			int failureRate, Time failureInterval, Time delayInterval) {
		return new FailureRateRestartStrategyConfiguration(failureRate, failureInterval, delayInterval);
	}

	/**
	 * Abstract configuration for restart strategies.
	 */
	public abstract static class RestartStrategyConfiguration implements Serializable {
		private static final long serialVersionUID = 6285853591578313960L;

		private RestartStrategyConfiguration() {}

		/**
		 * Returns a description which is shown in the web interface.
		 *
		 * @return Description of the restart strategy
		 */
		public abstract String getDescription();

		@Override
		public String toString() {
			return getDescription();
		}
	}

	/**
	 * Configuration representing no restart strategy.
	 */
	public static final class NoRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = -5894362702943349962L;

		@Override
		public String getDescription() {
			return "Restart deactivated.";
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			return o instanceof NoRestartStrategyConfiguration;
		}

		@Override
		public int hashCode() {
			return Objects.hash();
		}
	}

	/**
	 * Configuration representing a fixed delay restart strategy.
	 */
	public static final class FixedDelayRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = 4149870149673363190L;

		private final int restartAttempts;
		private final Time delayBetweenAttemptsInterval;

		FixedDelayRestartStrategyConfiguration(int restartAttempts, Time delayBetweenAttemptsInterval) {
			this.restartAttempts = restartAttempts;
			this.delayBetweenAttemptsInterval = delayBetweenAttemptsInterval;
		}

		public int getRestartAttempts() {
			return restartAttempts;
		}

		public Time getDelayBetweenAttemptsInterval() {
			return delayBetweenAttemptsInterval;
		}

		@Override
		public int hashCode() {
			int result = restartAttempts;
			result = 31 * result + (delayBetweenAttemptsInterval != null ? delayBetweenAttemptsInterval.hashCode() : 0);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof FixedDelayRestartStrategyConfiguration) {
				FixedDelayRestartStrategyConfiguration other = (FixedDelayRestartStrategyConfiguration) obj;

				return restartAttempts == other.restartAttempts &&
					delayBetweenAttemptsInterval.equals(other.delayBetweenAttemptsInterval);
			} else {
				return false;
			}
		}

		@Override
		public String getDescription() {
			return String.format(
				"Restart with fixed delay (%s). #%d restart attempts.",
				delayBetweenAttemptsInterval,
				restartAttempts);
		}
	}

	/**
	 * Configuration representing a failure rate restart strategy.
	 */
	public static final class FailureRateRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = 1195028697539661739L;
		private final int maxFailureRate;

		private final Time failureInterval;
		private final Time delayBetweenAttemptsInterval;

		public FailureRateRestartStrategyConfiguration(
				int maxFailureRate,
				Time failureInterval,
				Time delayBetweenAttemptsInterval) {
			this.maxFailureRate = maxFailureRate;
			this.failureInterval = failureInterval;
			this.delayBetweenAttemptsInterval = delayBetweenAttemptsInterval;
		}

		public int getMaxFailureRate() {
			return maxFailureRate;
		}

		public Time getFailureInterval() {
			return failureInterval;
		}

		public Time getDelayBetweenAttemptsInterval() {
			return delayBetweenAttemptsInterval;
		}

		@Override
		public String getDescription() {
			return String.format(
				"Failure rate restart with maximum of %d failures within interval %s and fixed delay %s.",
				maxFailureRate,
				failureInterval.toString(),
				delayBetweenAttemptsInterval.toString());
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			FailureRateRestartStrategyConfiguration that = (FailureRateRestartStrategyConfiguration) o;
			return maxFailureRate == that.maxFailureRate &&
				Objects.equals(failureInterval, that.failureInterval) &&
				Objects.equals(delayBetweenAttemptsInterval, that.delayBetweenAttemptsInterval);
		}

		@Override
		public int hashCode() {
			return Objects.hash(maxFailureRate, failureInterval, delayBetweenAttemptsInterval);
		}
	}

	/**
	 * Restart strategy configuration that could be used by jobs to use cluster level restart
	 * strategy. Useful especially when one has a custom implementation of restart strategy set via
	 * flink-conf.yaml.
	 */
	public static final class FallbackRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = -4441787204284085544L;

		@Override
		public String getDescription() {
			return "Cluster level default restart strategy";
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			return o instanceof FallbackRestartStrategyConfiguration;
		}

		@Override
		public int hashCode() {
			return Objects.hash();
		}
	}

	/**
	 * Reads a {@link RestartStrategyConfiguration} from a given {@link ReadableConfig}.
	 *
	 * @param configuration configuration object to retrieve parameters from
	 * @return {@link Optional#empty()} when no restart strategy parameters provided
	 */
	public static Optional<RestartStrategyConfiguration> fromConfiguration(ReadableConfig configuration) {
		return configuration.getOptional(RestartStrategyOptions.RESTART_STRATEGY)
			.map(confName -> parseConfiguration(confName, configuration));
	}

	private static RestartStrategyConfiguration parseConfiguration(
			String restartstrategyKind,
			ReadableConfig configuration) {
		switch (restartstrategyKind.toLowerCase()) {
			case "none":
			case "off":
			case "disable":
				return noRestart();
			case "fixeddelay":
			case "fixed-delay":
				int attempts = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);
				Duration delay = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY);
				return fixedDelayRestart(attempts, delay.toMillis());
			case "failurerate":
			case "failure-rate":
				int maxFailures = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL);
				Duration failureRateInterval = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL);
				Duration failureRateDelay = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY);
				return failureRateRestart(
					maxFailures,
					Time.milliseconds(failureRateInterval.toMillis()),
					Time.milliseconds(failureRateDelay.toMillis()));
			default:
				throw new IllegalArgumentException("Unknown restart strategy " + restartstrategyKind + ".");
		}
	}
}
