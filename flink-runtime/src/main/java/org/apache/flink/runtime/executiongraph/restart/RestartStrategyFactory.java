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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public abstract class RestartStrategyFactory implements Serializable {
	private static final long serialVersionUID = 7320252552640522191L;

	private static final Logger LOG = LoggerFactory.getLogger(RestartStrategyFactory.class);
	private static final String CREATE_METHOD = "createFactory";

	/**
	 * Factory method to create a restart strategy
	 * @return The created restart strategy
	 */
	public abstract RestartStrategy createRestartStrategy();

	/**
	 * Creates a {@link RestartStrategy} instance from the given {@link org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration}.
	 *
	 * @param restartStrategyConfiguration Restart strategy configuration which specifies which
	 *                                     restart strategy to instantiate
	 * @return RestartStrategy instance
	 */
	public static RestartStrategy createRestartStrategy(RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration) {
		if (restartStrategyConfiguration instanceof RestartStrategies.NoRestartStrategyConfiguration) {
			return new NoRestartStrategy();
		} else if (restartStrategyConfiguration instanceof RestartStrategies.FixedDelayRestartStrategyConfiguration) {
			RestartStrategies.FixedDelayRestartStrategyConfiguration fixedDelayConfig =
				(RestartStrategies.FixedDelayRestartStrategyConfiguration) restartStrategyConfiguration;

			return new FixedDelayRestartStrategy(
				fixedDelayConfig.getRestartAttempts(),
				fixedDelayConfig.getDelayBetweenAttemptsInterval().toMilliseconds());
		} else if (restartStrategyConfiguration instanceof RestartStrategies.FailureRateRestartStrategyConfiguration) {
			RestartStrategies.FailureRateRestartStrategyConfiguration config =
					(RestartStrategies.FailureRateRestartStrategyConfiguration) restartStrategyConfiguration;
			return new FailureRateRestartStrategy(
					config.getMaxFailureRate(),
					config.getFailureInterval(),
					config.getDelayBetweenAttemptsInterval()
			);
		} else if (restartStrategyConfiguration instanceof RestartStrategies.FallbackRestartStrategyConfiguration) {
			return null;
		} else {
			throw new IllegalArgumentException("Unknown restart strategy configuration " +
				restartStrategyConfiguration + ".");
		}
	}

	/**
	 * Creates a {@link RestartStrategy} instance from the given {@link Configuration}.
	 *
	 * @return RestartStrategy instance
	 * @throws Exception which indicates that the RestartStrategy could not be instantiated.
	 */
	public static RestartStrategyFactory createRestartStrategyFactory(Configuration configuration) throws Exception {
		String restartStrategyName = configuration.getString(ConfigConstants.RESTART_STRATEGY, null);

		if (restartStrategyName == null) {
			// support deprecated ConfigConstants values
			final int numberExecutionRetries = configuration.getInteger(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
				ConfigConstants.DEFAULT_EXECUTION_RETRIES);
			String pauseString = configuration.getString(AkkaOptions.WATCH_HEARTBEAT_PAUSE);
			String delayString = configuration.getString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY,
				pauseString);

			long delay;

			try {
				delay = Duration.apply(delayString).toMillis();
			} catch (NumberFormatException nfe) {
				if (delayString.equals(pauseString)) {
					throw new Exception("Invalid config value for " +
						AkkaOptions.WATCH_HEARTBEAT_PAUSE.key() + ": " + pauseString +
						". Value must be a valid duration (such as '10 s' or '1 min')");
				} else {
					throw new Exception("Invalid config value for " +
						ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY + ": " + delayString +
						". Value must be a valid duration (such as '100 milli' or '10 s')");
				}
			}

			if (numberExecutionRetries > 0 && delay >= 0) {
				return new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(numberExecutionRetries, delay);
			} else {
				return new NoOrFixedIfCheckpointingEnabledRestartStrategyFactory();
			}
		}

		switch (restartStrategyName.toLowerCase()) {
			case "none":
			case "off":
			case "disable":
				return NoRestartStrategy.createFactory(configuration);
			case "fixeddelay":
			case "fixed-delay":
				return FixedDelayRestartStrategy.createFactory(configuration);
			case "failurerate":
			case "failure-rate":
				return FailureRateRestartStrategy.createFactory(configuration);
			default:
				try {
					Class<?> clazz = Class.forName(restartStrategyName);

					if (clazz != null) {
						Method method = clazz.getMethod(CREATE_METHOD, Configuration.class);

						if (method != null) {
							Object result = method.invoke(null, configuration);

							if (result != null) {
								return (RestartStrategyFactory) result;
							}
						}
					}
				} catch (ClassNotFoundException cnfe) {
					LOG.warn("Could not find restart strategy class {}.", restartStrategyName);
				} catch (NoSuchMethodException nsme) {
					LOG.warn("Class {} does not has static method {}.", restartStrategyName, CREATE_METHOD);
				} catch (InvocationTargetException ite) {
					LOG.warn("Cannot call static method {} from class {}.", CREATE_METHOD, restartStrategyName);
				} catch (IllegalAccessException iae) {
					LOG.warn("Illegal access while calling method {} from class {}.", CREATE_METHOD, restartStrategyName);
				}

				// fallback in case of an error
				return new NoOrFixedIfCheckpointingEnabledRestartStrategyFactory();
		}
	}
}
