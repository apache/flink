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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * This class defines methods to generate RestartStrategyConfigurations. These configurations are
 * used to create RestartStrategies at runtime.
 *
 * The RestartStrategyConfigurations are used to decouple the core module from the runtime module.
 */
@PublicEvolving
public class RestartStrategies {

	/**
	 * Generates NoRestartStrategyConfiguration
	 *
	 * @return NoRestartStrategyConfiguration
	 */
	public static RestartStrategyConfiguration noRestart() {
		return new NoRestartStrategyConfiguration();
	}

	/**
	 * Generates a FixedDelayRestartStrategyConfiguration.
	 *
	 * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
	 * @param delayBetweenAttempts Delay in-between restart attempts for the FixedDelayRestartStrategy
	 * @return FixedDelayRestartStrategy
	 */
	public static RestartStrategyConfiguration fixedDelayRestart(
		int restartAttempts,
		long delayBetweenAttempts) {

		return new FixedDelayRestartStrategyConfiguration(restartAttempts, delayBetweenAttempts);
	}

	/**
	 * Generates a FailureRateRestartStrategyConfiguration.
	 *
	 * @param maxFailureRate Maximum number of restarts in given time unit {@code failureRateUnit} before failing a job
	 * @param failureRateUnit Time unit for measuring failure rate
	 * @param delayBetweenRestartAttempts Delay in-between restart attempts
	 */
	public static FailureRateRestartStrategyConfiguration failureRateRestart(
			int maxFailureRate, TimeUnit failureRateUnit, long delayBetweenRestartAttempts) {
		return new FailureRateRestartStrategyConfiguration(maxFailureRate, failureRateUnit, delayBetweenRestartAttempts);
	}

	public abstract static class RestartStrategyConfiguration implements Serializable {
		private static final long serialVersionUID = 6285853591578313960L;

		private RestartStrategyConfiguration() {}

		/**
		 * Returns a description which is shown in the web interface
		 *
		 * @return Description of the restart strategy
		 */
		public abstract String getDescription();
	}

	final public static class NoRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = -5894362702943349962L;

		@Override
		public String getDescription() {
			return "Restart deactivated.";
		}
	}

	final public static class FixedDelayRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = 4149870149673363190L;

		private final int restartAttempts;
		private final long delayBetweenAttempts;

		FixedDelayRestartStrategyConfiguration(int restartAttempts, long delayBetweenAttempts) {
			this.restartAttempts = restartAttempts;
			this.delayBetweenAttempts = delayBetweenAttempts;
		}

		public int getRestartAttempts() {
			return restartAttempts;
		}

		public long getDelayBetweenAttempts() {
			return delayBetweenAttempts;
		}

		@Override
		public int hashCode() {
			return 31 * restartAttempts + (int)(delayBetweenAttempts ^ (delayBetweenAttempts >>> 32));
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof FixedDelayRestartStrategyConfiguration) {
				FixedDelayRestartStrategyConfiguration other = (FixedDelayRestartStrategyConfiguration) obj;

				return restartAttempts == other.restartAttempts && delayBetweenAttempts == other.delayBetweenAttempts;
			} else {
				return false;
			}
		}

		@Override
		public String getDescription() {
			return "Restart with fixed delay (" + delayBetweenAttempts + " ms). #"
				+ restartAttempts + " restart attempts.";
		}
	}

	final public static class FailureRateRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private final int maxFailureRate;
		private final TimeUnit failureRateUnit;
		private final long delayBetweenRestartAttempts;

		public FailureRateRestartStrategyConfiguration(int maxFailureRate, TimeUnit failureRateUnit, long delayBetweenRestartAttempts) {
			this.maxFailureRate = maxFailureRate;
			this.failureRateUnit = failureRateUnit;
			this.delayBetweenRestartAttempts = delayBetweenRestartAttempts;
		}

		public int getMaxFailureRate() {
			return maxFailureRate;
		}

		public TimeUnit getFailureRateUnit() {
			return failureRateUnit;
		}

		public long getDelayBetweenRestartAttempts() {
			return delayBetweenRestartAttempts;
		}

		@Override
		public String getDescription() {
			return "Failure rate restart with " + maxFailureRate + " max failures per " + failureRateUnit
					+ " and fixed delay " + delayBetweenRestartAttempts;
		}
	}
}
