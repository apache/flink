/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link RestartStrategyFactory}.
 */
public class RestartStrategyFactoryTest extends TestLogger {

	@Test
	public void createRestartStrategyFactory_noRestartStrategyConfigured_returnsNoOrFixedIfCheckpointingEnabledRestartStrategyFactory() throws Exception {
		final Configuration configuration = new Configuration();

		final RestartStrategyFactory restartStrategyFactory = RestartStrategyFactory.createRestartStrategyFactory(configuration);

		assertThat(restartStrategyFactory, instanceOf(NoOrFixedIfCheckpointingEnabledRestartStrategyFactory.class));
	}

	@Test
	public void createRestartStrategyFactory_noRestartStrategyButAttemptsConfigured_returnsNoOrFixedIfCheckpointingEnabledRestartStrategyFactory() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setInteger(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);

		final RestartStrategyFactory restartStrategyFactory = RestartStrategyFactory.createRestartStrategyFactory(configuration);

		assertThat(restartStrategyFactory, instanceOf(NoOrFixedIfCheckpointingEnabledRestartStrategyFactory.class));
	}

	@Test
	public void createRestartStrategyFactory_fixedDelayRestartStrategyConfigured_returnsConfiguredFixedDelayRestartStrategy() throws Exception {
		final int attempts = 42;
		final Duration delayBetweenRestartAttempts = Duration.ofSeconds(1337L);
		final Configuration configuration = new Configuration();
		configuration.setString(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
		configuration.setInteger(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, attempts);
		configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, delayBetweenRestartAttempts);

		final RestartStrategyFactory restartStrategyFactory = RestartStrategyFactory.createRestartStrategyFactory(configuration);

		assertThat(restartStrategyFactory, instanceOf(FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory.class));
		final FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory fixedDelayRestartStrategyFactory = (FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory) restartStrategyFactory;

		assertThat(fixedDelayRestartStrategyFactory.getMaxNumberRestartAttempts(), is(attempts));
		assertThat(fixedDelayRestartStrategyFactory.getDelayBetweenRestartAttempts(), is(delayBetweenRestartAttempts.toMillis()));
	}
}
