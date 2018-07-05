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

import org.apache.flink.api.common.time.Time;

import org.junit.Test;

import static org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link RestartStrategyResolving}.
 */
public class RestartStrategyResolvingTest {

	@Test
	public void testClientSideHighestPriority() {

		RestartStrategy resolvedStrategy = RestartStrategyResolving.resolve(noRestart(),
			new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(2, 1000L),
			true);

		assertTrue(resolvedStrategy instanceof NoRestartStrategy);
	}

	@Test
	public void testFixedStrategySetWhenCheckpointingEnabled() {

		RestartStrategy resolvedStrategy = RestartStrategyResolving.resolve(null,
			new NoRestartStrategy.NoRestartStrategyFactory(),
			true);

		assertTrue(resolvedStrategy instanceof FixedDelayRestartStrategy);
	}

	@Test
	public void testServerStrategyIsUsedSetWhenCheckpointingEnabled() {

		RestartStrategy resolvedStrategy = RestartStrategyResolving.resolve(null,
			new FailureRateRestartStrategy.FailureRateRestartStrategyFactory(5, Time.seconds(5), Time.seconds(2)),
			true);

		assertTrue(resolvedStrategy instanceof FailureRateRestartStrategy);
	}

	@Test
	public void testServerStrategyIsUsedSetWhenCheckpointingDisabled() {

		RestartStrategy resolvedStrategy = RestartStrategyResolving.resolve(null,
			new FailureRateRestartStrategy.FailureRateRestartStrategyFactory(5, Time.seconds(5), Time.seconds(2)),
			false);

		assertTrue(resolvedStrategy instanceof FailureRateRestartStrategy);
	}
}
