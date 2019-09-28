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
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link ActiveResourceManagerFactory}.
 */
public class ActiveResourceManagerFactoryTest extends TestLogger {

	/**
	 * Test which ensures that the {@link ActiveResourceManagerFactory} sets the correct managed
	 * memory when creating a resource manager.
	 */
	@Test
	public void createResourceManager_WithDefaultConfiguration_ShouldSetManagedMemory() throws Exception {
		final Configuration configuration = new Configuration();

		final TestingActiveResourceManagerFactory resourceManagerFactory = new TestingActiveResourceManagerFactory();

		final TestingRpcService rpcService = new TestingRpcService();

		try {
			final ResourceManager<ResourceID> ignored = resourceManagerFactory.createResourceManager(
				configuration,
				ResourceID.generate(),
				rpcService,
				new TestingHighAvailabilityServices(),
				new TestingHeartbeatServices(),
				NoOpMetricRegistry.INSTANCE,
				new TestingFatalErrorHandler(),
				new ClusterInformation("foobar", 1234),
				null,
				UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup());
		} finally {
			RpcUtils.terminateRpcService(rpcService, Time.seconds(10L));
		}
	}

	private static final class TestingActiveResourceManagerFactory extends ActiveResourceManagerFactory<ResourceID> {

		@Override
		protected ResourceManager<ResourceID> createActiveResourceManager(
				Configuration configuration,
				ResourceID resourceId,
				RpcService rpcService,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				MetricRegistry metricRegistry,
				FatalErrorHandler fatalErrorHandler,
				ClusterInformation clusterInformation,
				@Nullable String webInterfaceUrl,
				JobManagerMetricGroup jobManagerMetricGroup) {
			assertThat(configuration.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE), is(true));

			return null;
		}
	}
}
