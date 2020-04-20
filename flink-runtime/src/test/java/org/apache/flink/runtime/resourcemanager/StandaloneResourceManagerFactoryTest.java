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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

/**
 * Tests for the {@link StandaloneResourceManagerFactory}.
 */
public class StandaloneResourceManagerFactoryTest extends TestLogger {

	@Test
	public void createResourceManager_WithLessMemoryThanContainerizedHeapCutoffMin_ShouldSucceed() throws Exception {
		final StandaloneResourceManagerFactory resourceManagerFactory = StandaloneResourceManagerFactory.INSTANCE;

		final TestingRpcService rpcService = new TestingRpcService();
		try {
			final Configuration configuration = new Configuration();
			configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, new MemorySize(128 * 1024 * 1024));
			configuration.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, 600);

			final ResourceManager<ResourceID> ignored = resourceManagerFactory.createResourceManager(
				configuration,
				ResourceID.generate(),
				rpcService,
				new TestingHighAvailabilityServices(),
				new TestingHeartbeatServices(),
				new TestingFatalErrorHandler(),
				new ClusterInformation("foobar", 1234),
				null,
				UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());
		} finally {
			RpcUtils.terminateRpcService(rpcService, Time.seconds(10L));
		}
	}

}
