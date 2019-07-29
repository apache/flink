/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnegative;

import java.util.concurrent.CompletableFuture;

/**
 * A restart strategy which fails a predefined amount of times.
 */
public class FailingRestartStrategy implements RestartStrategy {

	public static final ConfigOption<Integer> NUM_FAILURES_CONFIG_OPTION = ConfigOptions
		.key("restart-strategy.failing.failures")
		.defaultValue(1);

	private final int numberOfFailures;

	private int restartedTimes;

	public FailingRestartStrategy(@Nonnegative int numberOfFailures) {
		this.numberOfFailures = numberOfFailures;
	}

	@Override
	public boolean canRestart() {
		return true;
	}

	@Override
	public CompletableFuture<Void> restart(RestartCallback restarter, ScheduledExecutor executor) {
		++restartedTimes;

		if (restartedTimes <= numberOfFailures) {
			return FutureUtils.completedExceptionally(new FlinkRuntimeException("Fail to restart for " + restartedTimes + " time(s)."));
		} else {
			return FutureUtils.scheduleWithDelay(restarter::triggerFullRecovery, Time.milliseconds(0L), executor);
		}
	}

	/**
	 * Creates a {@link FailingRestartStrategyFactory} from the given Configuration.
	 */
	public static FailingRestartStrategyFactory createFactory(Configuration configuration) {
		int numberOfFailures = configuration.getInteger(NUM_FAILURES_CONFIG_OPTION);
		return new FailingRestartStrategyFactory(numberOfFailures);
	}

	/**
	 * Factory for {@link FailingRestartStrategy}.
	 */
	public static class FailingRestartStrategyFactory extends RestartStrategyFactory {
		private static final long serialVersionUID = 1L;

		private final int numberOfFailures;

		public FailingRestartStrategyFactory(int numberOfFailures) {
			this.numberOfFailures = numberOfFailures;
		}

		@Override
		public RestartStrategy createRestartStrategy() {
			return new FailingRestartStrategy(numberOfFailures);
		}
	}
}
