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

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests the failure handling logic of the {@link RestartPipelinedRegionStrategy}.
 */
public class RestartPipelinedRegionStrategyTest extends TestLogger {

	/**
	 * Tests for scenes that a task fails for its own error, in which case only the
	 * region containing the failed task should be restarted.
	 * <pre>
	 *     (v1)
	 *
	 *     (v2)
	 *
	 *     (v3)
	 * </pre>
	 */
	@Test
	public void testRegionFailoverForTaskInternalErrors() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex v1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v3 = topologyBuilder.newVertex();

		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion r1 = strategy.getFailoverRegion(v1.getExecutionVertexID());
		FailoverRegion r2 = strategy.getFailoverRegion(v2.getExecutionVertexID());
		FailoverRegion r3 = strategy.getFailoverRegion(v3.getExecutionVertexID());

		assertEquals(r1.getAllExecutionVertexIDs(),
			strategy.getTasksNeedingRestart(v1.getExecutionVertexID(), new Exception("Test failure")));
		assertEquals(r2.getAllExecutionVertexIDs(),
			strategy.getTasksNeedingRestart(v2.getExecutionVertexID(), new Exception("Test failure")));
		assertEquals(r3.getAllExecutionVertexIDs(),
			strategy.getTasksNeedingRestart(v3.getExecutionVertexID(), new Exception("Test failure")));
	}
}
