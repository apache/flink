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

import javax.annotation.Nullable;

/**
 * Utility method for resolving {@link RestartStrategy}.
 */
public final class RestartStrategyResolving {

	private static final long DEFAULT_RESTART_DELAY = 0;

	/**
	 * Resolves which {@link RestartStrategy} to use. It should be used only on the server side.
	 * The resolving strategy is as follows:
	 * <ol>
	 * <li>Strategy set within job graph.</li>
	 * <li>Strategy set flink-conf.yaml on the server set, unless is set to {@link NoRestartStrategy} and checkpointing is enabled.</li>
	 * <li>If no strategy was set on client and server side and checkpointing was enabled then {@link FixedDelayRestartStrategy} is used</li>
	 * </ol>
	 *
	 * @param clientConfiguration    restart configuration given within the job graph
	 * @param serverStrategyFactory  default server side strategy factory
	 * @param isCheckpointingEnabled if checkpointing was enabled for the job
	 * @return resolved strategy
	 */
	public static RestartStrategy resolve(
			@Nullable RestartStrategies.RestartStrategyConfiguration clientConfiguration,
			RestartStrategyFactory serverStrategyFactory,
			boolean isCheckpointingEnabled) {

		final RestartStrategy serverSideRestartStrategy = serverStrategyFactory.createRestartStrategy();

		RestartStrategy clientSideRestartStrategy = null;
		if (clientConfiguration != null) {
			if (clientConfiguration instanceof RestartStrategies.FallbackRestartStrategyConfiguration) {
				clientSideRestartStrategy = serverSideRestartStrategy;
			} else {
				clientSideRestartStrategy = RestartStrategyFactory.createRestartStrategy(clientConfiguration);
			}
		}

		if (clientSideRestartStrategy == null && serverSideRestartStrategy instanceof NoRestartStrategy &&
			isCheckpointingEnabled) {
			return new FixedDelayRestartStrategy(Integer.MAX_VALUE, DEFAULT_RESTART_DELAY);
		} else if (clientSideRestartStrategy != null) {
			return clientSideRestartStrategy;
		} else {
			return serverSideRestartStrategy;
		}
	}

	private RestartStrategyResolving() {
	}
}
