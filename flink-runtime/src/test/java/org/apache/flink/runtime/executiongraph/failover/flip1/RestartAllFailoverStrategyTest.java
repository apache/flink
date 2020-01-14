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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link RestartAllFailoverStrategy}.
 */
public class RestartAllFailoverStrategyTest extends TestLogger {

	@Test
	public void testGetTasksNeedingRestart() {
		final TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		final TestFailoverTopology.TestFailoverVertex v1 = topologyBuilder.newVertex();
		final TestFailoverTopology.TestFailoverVertex v2 = topologyBuilder.newVertex();
		final TestFailoverTopology.TestFailoverVertex v3 = topologyBuilder.newVertex();

		topologyBuilder.connect(v1, v2, ResultPartitionType.PIPELINED);
		topologyBuilder.connect(v2, v3, ResultPartitionType.BLOCKING);

		final TestFailoverTopology topology = topologyBuilder.build();

		final RestartAllFailoverStrategy strategy = new RestartAllFailoverStrategy(topology);

		assertEquals(
			new HashSet<>(Arrays.asList(v1.getId(), v2.getId(), v3.getId())),
			strategy.getTasksNeedingRestart(v1.getId(), new Exception("Test failure")));
	}
}
