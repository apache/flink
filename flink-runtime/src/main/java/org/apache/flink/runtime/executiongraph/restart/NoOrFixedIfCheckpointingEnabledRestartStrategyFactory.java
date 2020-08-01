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

import java.time.Duration;

/**
 * Default restart strategy that resolves either to {@link NoRestartStrategy} or {@link FixedDelayRestartStrategy}
 * depending if checkpointing was enabled.
 */
public class NoOrFixedIfCheckpointingEnabledRestartStrategyFactory extends RestartStrategyFactory {
	private static final long serialVersionUID = -1809462525812787862L;

	public static final int DEFAULT_RESTART_ATTEMPTS = Integer.MAX_VALUE;

	public static final long DEFAULT_RESTART_DELAY = Duration.ofSeconds(1L).toMillis();

	@Override
	public RestartStrategy createRestartStrategy() {
		return createRestartStrategy(false);
	}

	RestartStrategy createRestartStrategy(boolean isCheckpointingEnabled) {
		if (isCheckpointingEnabled) {
			return new FixedDelayRestartStrategy(DEFAULT_RESTART_ATTEMPTS, DEFAULT_RESTART_DELAY);
		} else {
			return new NoRestartStrategy();
		}
	}
}
