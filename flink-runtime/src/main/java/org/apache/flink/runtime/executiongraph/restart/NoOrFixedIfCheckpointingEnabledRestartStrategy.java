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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;

/**
 * Default restart strategy that resolves either to {@link NoRestartStrategy} or {@link FixedDelayRestartStrategy}
 * depending if checkpointing was enabled.
 */
public class NoOrFixedIfCheckpointingEnabledRestartStrategy implements RestartStrategy {

	private static final long DEFAULT_RESTART_DELAY = 0;

	private final RestartStrategy resolvedStrategy;

	/**
	 * Creates a NoOrFixedIfCheckpointingEnabledRestartStrategyFactory instance.
	 *
	 * @param configuration Configuration object which is ignored
	 * @return NoOrFixedIfCheckpointingEnabledRestartStrategyFactory instance
	 */
	public static NoOrFixedIfCheckpointingEnabledRestartStrategyFactory createFactory(Configuration configuration) {
		return new NoOrFixedIfCheckpointingEnabledRestartStrategyFactory();
	}

	/**
	 * Creates instance of NoOrFixedIfCheckpointingEnabledRestartStrategy
	 *
	 * @param isCheckpointingEnabled if true resolves to {@link FixedDelayRestartStrategy}
	 * otherwise to {@link NoRestartStrategy}
	 */
	public NoOrFixedIfCheckpointingEnabledRestartStrategy(boolean isCheckpointingEnabled) {
		if (isCheckpointingEnabled) {
			resolvedStrategy = new FixedDelayRestartStrategy(Integer.MAX_VALUE, DEFAULT_RESTART_DELAY);
		} else {
			resolvedStrategy = new NoRestartStrategy();
		}
	}

	@Override
	public boolean canRestart() {
		return resolvedStrategy.canRestart();
	}

	@Override
	public void restart(RestartCallback restarter, ScheduledExecutor executor) {
		resolvedStrategy.restart(restarter, executor);
	}

	public static class NoOrFixedIfCheckpointingEnabledRestartStrategyFactory extends RestartStrategyFactory {

		private static final long serialVersionUID = -1809462525812787862L;

		@Override
		public RestartStrategy createRestartStrategy() {
			return createRestartStrategy(false);
		}

		RestartStrategy createRestartStrategy(boolean isCheckpointingEnabled) {
			return new NoOrFixedIfCheckpointingEnabledRestartStrategy(isCheckpointingEnabled);
		}
	}

	@VisibleForTesting
	public RestartStrategy getResolvedStrategy() {
		return resolvedStrategy;
	}
}
