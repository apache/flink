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

package org.apache.flink.streaming.api;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link RestartStrategies}.
 */
public class RestartStrategyTest extends TestLogger {

	/**
	 * Tests that in a streaming use case where checkpointing is enabled, there is no default strategy set on the
	 * client side.
	 */
	@Test
	public void testFallbackStrategyOnClientSideWhenCheckpointingEnabled() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);

		env.fromElements(1).print();

		StreamGraph graph = env.getStreamGraph();
		JobGraph jobGraph = graph.getJobGraph();

		RestartStrategies.RestartStrategyConfiguration restartStrategy =
			jobGraph.getSerializedExecutionConfig().deserializeValue(getClass().getClassLoader()).getRestartStrategy();

		Assert.assertNotNull(restartStrategy);
		Assert.assertTrue(restartStrategy instanceof RestartStrategies.FallbackRestartStrategyConfiguration);
	}

	/**
	 * Checks that in a streaming use case where checkpointing is enabled and the number
	 * of execution retries is set to 0, restarting is deactivated.
	 */
	@Test
	public void testNoRestartingWhenCheckpointingAndExplicitExecutionRetriesZero() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setNumberOfExecutionRetries(0);

		env.fromElements(1).print();

		StreamGraph graph = env.getStreamGraph();
		JobGraph jobGraph = graph.getJobGraph();

		RestartStrategies.RestartStrategyConfiguration restartStrategy =
			jobGraph.getSerializedExecutionConfig().deserializeValue(getClass().getClassLoader()).getRestartStrategy();

		Assert.assertNotNull(restartStrategy);
		Assert.assertTrue(restartStrategy instanceof RestartStrategies.NoRestartStrategyConfiguration);
	}

	/**
	 * Checks that in a streaming use case where checkpointing is enabled and the number
	 * of execution retries is set to 42 and the delay to 1337, fixed delay restarting is used.
	 */
	@Test
	public void testFixedRestartingWhenCheckpointingAndExplicitExecutionRetriesNonZero() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setNumberOfExecutionRetries(42);
		env.getConfig().setExecutionRetryDelay(1337);

		env.fromElements(1).print();

		StreamGraph graph = env.getStreamGraph();
		JobGraph jobGraph = graph.getJobGraph();

		RestartStrategies.RestartStrategyConfiguration restartStrategy =
			jobGraph.getSerializedExecutionConfig().deserializeValue(getClass().getClassLoader()).getRestartStrategy();

		Assert.assertNotNull(restartStrategy);
		Assert.assertTrue(restartStrategy instanceof RestartStrategies.FixedDelayRestartStrategyConfiguration);
		Assert.assertEquals(42, ((RestartStrategies.FixedDelayRestartStrategyConfiguration) restartStrategy).getRestartAttempts());
		Assert.assertEquals(1337, ((RestartStrategies.FixedDelayRestartStrategyConfiguration) restartStrategy).getDelayBetweenAttemptsInterval().toMilliseconds());
	}
}
