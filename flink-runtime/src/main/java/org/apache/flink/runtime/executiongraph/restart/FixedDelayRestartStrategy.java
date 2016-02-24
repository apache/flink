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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.concurrent.Callable;

import static akka.dispatch.Futures.future;

/**
 * Restart strategy which tries to restart the given {@link ExecutionGraph} a fixed number of times
 * with a fixed time delay in between.
 */
public class FixedDelayRestartStrategy implements RestartStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(FixedDelayRestartStrategy.class);


	private final int maxNumberRestartAttempts;
	private final long delayBetweenRestartAttempts;
	private int currentRestartAttempt;

	public FixedDelayRestartStrategy(
		int maxNumberRestartAttempts,
		long delayBetweenRestartAttempts) {

		Preconditions.checkArgument(maxNumberRestartAttempts >= 0, "Maximum number of restart attempts must be positive.");
		Preconditions.checkArgument(delayBetweenRestartAttempts >= 0, "Delay between restart attempts must be positive");

		this.maxNumberRestartAttempts = maxNumberRestartAttempts;
		this.delayBetweenRestartAttempts = delayBetweenRestartAttempts;
		currentRestartAttempt = 0;
	}

	public int getCurrentRestartAttempt() {
		return currentRestartAttempt;
	}

	@Override
	public boolean canRestart() {
		return currentRestartAttempt < maxNumberRestartAttempts;
	}

	@Override
	public void restart(final ExecutionGraph executionGraph) {
		currentRestartAttempt++;

		future(new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				try {
					LOG.info("Delaying retry of job execution for {} ms ...", delayBetweenRestartAttempts);
					// do the delay
					Thread.sleep(delayBetweenRestartAttempts);
				} catch(InterruptedException e) {
					// should only happen on shutdown
				}
				executionGraph.restart();
				return null;
			}
		}, executionGraph.getExecutionContext());
	}

	/**
	 * Creates a FixedDelayRestartStrategy from the given Configuration.
	 *
	 * @param configuration Configuration containing the parameter values for the restart strategy
	 * @return Initialized instance of FixedDelayRestartStrategy
	 * @throws Exception
	 */
	public static FixedDelayRestartStrategy create(Configuration configuration) throws Exception {
		int maxAttempts = configuration.getInteger(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);

		String timeoutString = configuration.getString(
			ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL,
			ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT);

		String delayString = configuration.getString(
			ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY,
			timeoutString
		);

		long delay;

		try {
			delay = Duration.apply(delayString).toMillis();
		} catch (NumberFormatException nfe) {
			if (delayString.equals(timeoutString)) {
				throw new Exception("Invalid config value for " +
					ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE + ": " + timeoutString +
					". Value must be a valid duration (such as '10 s' or '1 min')");
			} else {
				throw new Exception("Invalid config value for " +
					ConfigConstants.EXECUTION_RETRY_DELAY_KEY + ": " + delayString +
					". Value must be a valid duration (such as '100 milli' or '10 s')");
			}
		}

		return new FixedDelayRestartStrategy(maxAttempts, delay);
	}

	@Override
	public String toString() {
		return "FixedDelayRestartStrategy(" +
				"maxNumberRestartAttempts=" + maxNumberRestartAttempts +
				", delayBetweenRestartAttempts=" + delayBetweenRestartAttempts +
				')';
	}
}
