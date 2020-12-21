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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

/**
 * Unit tests for {@link RestartBackoffTimeStrategyFactoryLoader}.
 */
public class RestartBackoffTimeStrategyFactoryLoaderTest extends TestLogger {

	private static final RestartStrategies.RestartStrategyConfiguration DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION =
		RestartStrategies.fallBackRestart();

	@Test
	public void testNoRestartStrategySpecifiedInJobConfig() {
		final Configuration conf = new Configuration();
		conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");

		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
				RestartStrategies.noRestart(),
				conf,
				false);

		assertEquals(NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE, factory);
	}

	@Test
	public void testFixedDelayRestartStrategySpecifiedInJobConfig() {
		final Configuration conf = new Configuration();
		conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");

		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
				RestartStrategies.fixedDelayRestart(1, Time.milliseconds(1000)),
				conf,
				false);

		assertThat(
			factory,
			instanceOf(FixedDelayRestartBackoffTimeStrategy.FixedDelayRestartBackoffTimeStrategyFactory.class));
	}

	@Test
	public void testFailureRateRestartStrategySpecifiedInJobConfig() {
		final Configuration conf = new Configuration();
		conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");

		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
				RestartStrategies.failureRateRestart(1, Time.milliseconds(1000), Time.milliseconds(1000)),
				conf,
				false);

		assertThat(
			factory,
			instanceOf(FailureRateRestartBackoffTimeStrategy.FailureRateRestartBackoffTimeStrategyFactory.class));
	}

	@Test
	public void testNoRestartStrategySpecifiedInClusterConfig() {
		final Configuration conf = new Configuration();
		conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "none");

		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
				DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION,
				conf,
				false);

		assertEquals(NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE, factory);
	}

	@Test
	public void testFixedDelayStrategySpecifiedInClusterConfig() {
		final Configuration conf = new Configuration();
		conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");

		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
				DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION,
				conf,
				false);

		assertThat(
			factory,
			instanceOf(FixedDelayRestartBackoffTimeStrategy.FixedDelayRestartBackoffTimeStrategyFactory.class));
	}

	@Test
	public void testFailureRateStrategySpecifiedInClusterConfig() {
		final Configuration conf = new Configuration();
		conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");

		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
				DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION,
				conf,
				false);

		assertThat(
			factory,
			instanceOf(FailureRateRestartBackoffTimeStrategy.FailureRateRestartBackoffTimeStrategyFactory.class));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidStrategySpecifiedInClusterConfig() {
		final Configuration conf = new Configuration();
		conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "invalid-strategy");

		RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
			DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION,
			conf,
			false);
	}

	@Test
	public void testNoStrategySpecifiedWhenCheckpointingEnabled() {
		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
				DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION,
				new Configuration(),
				true);

		RestartBackoffTimeStrategy strategy = factory.create();
		assertThat(
			strategy,
			instanceOf(FixedDelayRestartBackoffTimeStrategy.class));

		FixedDelayRestartBackoffTimeStrategy fixedDelayStrategy = (FixedDelayRestartBackoffTimeStrategy) strategy;
		assertEquals(
			RestartBackoffTimeStrategyFactoryLoader.DEFAULT_RESTART_DELAY,
			fixedDelayStrategy.getBackoffTime());
		assertEquals(
			RestartBackoffTimeStrategyFactoryLoader.DEFAULT_RESTART_ATTEMPTS,
			fixedDelayStrategy.getMaxNumberRestartAttempts());
	}

	@Test
	public void testNoStrategySpecifiedWhenCheckpointingDisabled() {
		final RestartBackoffTimeStrategy.Factory factory =
			RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
				DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION,
				new Configuration(),
				false);

		assertThat(
			factory,
			instanceOf(NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.class));
	}
}
