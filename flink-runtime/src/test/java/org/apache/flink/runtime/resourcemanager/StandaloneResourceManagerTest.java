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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;

import org.apache.flink.util.TestLogger;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.SupplierWithException;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the Standalone Resource Manager.
 */
public class StandaloneResourceManagerTest extends TestLogger {

	@ClassRule
	public static final TestingRpcServiceResource RPC_SERVICE = new TestingRpcServiceResource();

	private static final Time TIMEOUT = Time.seconds(10);

	private final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();

	@Test
	public void testStartupPeriod() throws Exception {
		final StandaloneResourceManager rm = createResourceManager(Time.milliseconds(1L));
		final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10L));

		assertHappensUntil(() -> isFailingUnfulfillableRequest(rm), deadline);

		rm.close();
	}

	@Test
	public void testNoStartupPeriod() throws Exception {
		final StandaloneResourceManager rm = createResourceManager(Time.milliseconds(-1L));

		// startup includes initialization and granting leadership, so by the time we are
		// here, the initialization method scheduling the startup period will have been executed.

		assertThat(fatalErrorHandler.hasExceptionOccurred(), is(false));

		assertThat(isFailingUnfulfillableRequest(rm), is(false));

		rm.close();
	}

	private StandaloneResourceManager createResourceManager(Time startupPeriod) throws Exception {

		final MockResourceManagerRuntimeServices rmServices = new MockResourceManagerRuntimeServices(
			RPC_SERVICE.getTestingRpcService(),
			TIMEOUT);

		final StandaloneResourceManager rm = new StandaloneResourceManager(
			rmServices.rpcService,
			UUID.randomUUID().toString(),
			ResourceID.generate(),
			rmServices.highAvailabilityServices,
			rmServices.heartbeatServices,
			rmServices.slotManager,
			rmServices.metricRegistry,
			rmServices.jobLeaderIdService,
			new ClusterInformation("localhost", 1234),
			fatalErrorHandler,
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			startupPeriod);

		rm.start();
		rmServices.grantLeadership();

		return rm;
	}

	private static boolean isFailingUnfulfillableRequest(
			StandaloneResourceManager resourceManager) throws InterruptedException {
		try {
			return resourceManager.isFailingUnfulfillableRequestAsync().get();
		} catch (ExecutionException e) {
			ExceptionUtils.rethrow(e);
			return false; // should be unreachable
		}
	}

	private static void assertHappensUntil(
			SupplierWithException<Boolean, InterruptedException> condition,
			Deadline until) throws InterruptedException {
		while (!condition.get()) {
			if (!until.hasTimeLeft()) {
				fail("condition was not fulfilled before the deadline");
			}
			Thread.sleep(2);
		}
	}
}
