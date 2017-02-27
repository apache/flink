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

package org.apache.flink.yarn;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.heartbeat.HeartbeatManagerImpl;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.TestingSerialRpcService;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class YarnResourceManagerTest extends TestLogger {

	private YarnResourceManager yarnResourceManager;

	@Before
	public void setup() {
		Configuration conf = new Configuration();
		ResourceManagerConfiguration rmConfig =
				new ResourceManagerConfiguration(Time.seconds(1), Time.seconds(10));
		yarnResourceManager = new YarnResourceManager(conf, null, new TestingSerialRpcService(), rmConfig,
				mock(HighAvailabilityServices.class), mock(SlotManagerFactory.class),
				mock(MetricRegistry.class),	 mock(JobLeaderIdService.class),
                mock(FatalErrorHandler.class));
	}

	@After
	public void teardown() {
	}

	@Test
	public void testGeneratePriority() throws Exception {
		ResourceProfile resourceProfile = new ResourceProfile(1, 1024);
		int priority = yarnResourceManager.generatePriority(resourceProfile);
		assertEquals(priority, 0);

        ResourceProfile resourceProfile2 = new ResourceProfile(2, 100);
		priority = yarnResourceManager.generatePriority(resourceProfile2);
		assertEquals(priority, 1);

		priority = yarnResourceManager.generatePriority(resourceProfile);
		assertEquals(priority, 0);
	}

	@Test
	public void testGetResourceProfile() throws Exception {
		ResourceProfile resourceProfile = new ResourceProfile(1, 1024);
		int priority = yarnResourceManager.generatePriority(resourceProfile);
		assertEquals(priority, 0);

        ResourceProfile resourceProfile2 = new ResourceProfile(2, 100);
		priority = yarnResourceManager.generatePriority(resourceProfile2);
		assertEquals(priority, 1);

		ResourceProfile profile = yarnResourceManager.getResourceProfile(0);
		assertEquals(profile, resourceProfile);

		profile = yarnResourceManager.getResourceProfile(2);
		assertEquals(profile, null);
	}

}
