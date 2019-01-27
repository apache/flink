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

package org.apache.flink.runtime.schedule;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link GraphManagerPluginFactory}.
 */
public class GraphManagerPluginFactoryTest extends TestLogger {

	/**
	 * Test the factory can create graph manager plugin correctly.
	 */
	@Test
	public void testCreateGraphManagerPlugin() throws Exception {
		Configuration jobConfig = new Configuration();
		SchedulingConfig config = new SchedulingConfig(jobConfig, this.getClass().getClassLoader());
		GraphManagerPlugin graphManagerPlugin;

		jobConfig.setString(ScheduleMode.class.getName(), ScheduleMode.EAGER.toString());
		graphManagerPlugin = GraphManagerPluginFactory.createGraphManagerPlugin(
			jobConfig, this.getClass().getClassLoader());
		graphManagerPlugin.open(mock(VertexScheduler.class), mock(JobGraph.class), config);
		assertTrue(graphManagerPlugin instanceof EagerSchedulingPlugin);
		assertFalse(graphManagerPlugin.allowLazyDeployment());

		jobConfig.setString(ScheduleMode.class.getName(), ScheduleMode.LAZY_FROM_SOURCES.toString());
		graphManagerPlugin = GraphManagerPluginFactory.createGraphManagerPlugin(
			jobConfig, this.getClass().getClassLoader());
		graphManagerPlugin.open(mock(VertexScheduler.class), mock(JobGraph.class), config);
		assertTrue(graphManagerPlugin instanceof StepwiseSchedulingPlugin);
		assertTrue(graphManagerPlugin.allowLazyDeployment());

		jobConfig.setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN,
			"org.apache.flink.runtime.schedule.SchedulingTestUtils$TestGraphManagerPlugin");
		graphManagerPlugin = GraphManagerPluginFactory.createGraphManagerPlugin(
			jobConfig, Thread.currentThread().getContextClassLoader());
		graphManagerPlugin.open(mock(VertexScheduler.class), mock(JobGraph.class), config);
		assertTrue(graphManagerPlugin instanceof SchedulingTestUtils.TestGraphManagerPlugin);

	}
}
