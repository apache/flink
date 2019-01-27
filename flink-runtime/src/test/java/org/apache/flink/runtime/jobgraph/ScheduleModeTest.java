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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.schedule.GraphManagerPlugin;
import org.apache.flink.runtime.schedule.GraphManagerPluginFactory;
import org.apache.flink.runtime.schedule.SchedulingConfig;
import org.apache.flink.runtime.schedule.VertexScheduler;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ScheduleModeTest {

	/**
	 * Test that schedule modes set the lazy deployment flag correctly.
	 */
	@Test
	public void testAllowLazyDeployment() throws Exception {
		GraphManagerPlugin graphManagerPlugin;

		Configuration conf = new Configuration();
		SchedulingConfig config = new SchedulingConfig(conf, this.getClass().getClassLoader());
		conf.setString(ScheduleMode.class.getName(), ScheduleMode.LAZY_FROM_SOURCES.toString());
		graphManagerPlugin = GraphManagerPluginFactory.createGraphManagerPlugin(conf, this.getClass().getClassLoader());
		graphManagerPlugin.open(mock(VertexScheduler.class), mock(JobGraph.class), config);
		assertTrue(graphManagerPlugin.allowLazyDeployment());

		conf.setString(ScheduleMode.class.getName(), ScheduleMode.EAGER.toString());
		graphManagerPlugin = GraphManagerPluginFactory.createGraphManagerPlugin(conf, this.getClass().getClassLoader());
		graphManagerPlugin.open(mock(VertexScheduler.class), mock(JobGraph.class), config);
		assertFalse(graphManagerPlugin.allowLazyDeployment());
	}
}
