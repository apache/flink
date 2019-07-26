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
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.TestingSlotManagerFactory;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

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
		final LinkedBlockingQueue<Boolean> setFailUnfulfillableRequestInvokes = new LinkedBlockingQueue<>();
		final SlotManager slotManager = new TestingSlotManagerFactory()
			.setSetFailUnfulfillableRequestConsumer(invoke -> setFailUnfulfillableRequestInvokes.add(invoke))
			.createSlotManager();
		final TestingStandaloneResourceManager rm = createResourceManager(Time.milliseconds(1L), slotManager);

		final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(1));
		assertHappensUntil(() -> setFailUnfulfillableRequestInvokes.size() == 2, deadline);
		assertThat(setFailUnfulfillableRequestInvokes.poll(), is(false));
		assertThat(setFailUnfulfillableRequestInvokes.poll(), is(true));

		rm.close();
	}

	@Test
	public void testNoStartupPeriod() throws Exception {
		final LinkedBlockingQueue<Boolean> setFailUnfulfillableRequestInvokes = new LinkedBlockingQueue<>();
		final SlotManager slotManager = new TestingSlotManagerFactory()
			.setSetFailUnfulfillableRequestConsumer(invoke -> setFailUnfulfillableRequestInvokes.add(invoke))
			.createSlotManager();
		final TestingStandaloneResourceManager rm = createResourceManager(Time.milliseconds(-1L), slotManager);

		final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(1));
		assertNotHappens(() -> setFailUnfulfillableRequestInvokes.size() > 1, deadline);
		assertThat(setFailUnfulfillableRequestInvokes.size(), is(1));
		assertThat(setFailUnfulfillableRequestInvokes.poll(), is(false));

		rm.close();
	}

	@Test
	public void testStartUpPeriodAfterLeadershipSwitch() throws Exception {
		final LinkedBlockingQueue<Boolean> setFailUnfulfillableRequestInvokes = new LinkedBlockingQueue<>();
		final SlotManager slotManager = new TestingSlotManagerFactory()
			.setSetFailUnfulfillableRequestConsumer(invoke -> setFailUnfulfillableRequestInvokes.add(invoke))
			.createSlotManager();
		final TestingStandaloneResourceManager rm = createResourceManager(Time.milliseconds(1L), slotManager);

		final Deadline deadline1 = Deadline.fromNow(Duration.ofSeconds(1));
		assertHappensUntil(() -> setFailUnfulfillableRequestInvokes.size() == 2, deadline1);
		assertThat(setFailUnfulfillableRequestInvokes.poll(), is(false));
		assertThat(setFailUnfulfillableRequestInvokes.poll(), is(true));

		rm.rmServices.revokeLeadership();
		rm.rmServices.grantLeadership();

		final Deadline deadline2 = Deadline.fromNow(Duration.ofSeconds(1L));
		assertHappensUntil(() -> setFailUnfulfillableRequestInvokes.size() == 2, deadline2);
		assertThat(setFailUnfulfillableRequestInvokes.poll(), is(false));
		assertThat(setFailUnfulfillableRequestInvokes.poll(), is(true));
	}

	private TestingStandaloneResourceManager createResourceManager(Time startupPeriod, SlotManager slotManager) throws Exception {

		final MockResourceManagerRuntimeServices rmServices = new MockResourceManagerRuntimeServices(
			RPC_SERVICE.getTestingRpcService(),
			TIMEOUT,
			slotManager);

		final TestingStandaloneResourceManager rm = new TestingStandaloneResourceManager(
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
			startupPeriod,
			rmServices);

		rm.start();
		rmServices.grantLeadership();

		return rm;
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

	private static void assertNotHappens(
		SupplierWithException<Boolean, InterruptedException> condition,
		Deadline until) throws InterruptedException {
		while (!condition.get()) {
			if (!until.hasTimeLeft()) {
				return;
			}
			Thread.sleep(2);
		}
		fail("condition was fulfilled before the deadline");
	}

	private static class TestingStandaloneResourceManager extends StandaloneResourceManager {
		private final MockResourceManagerRuntimeServices rmServices;

		private TestingStandaloneResourceManager(
				RpcService rpcService,
				String resourceManagerEndpointId,
				ResourceID resourceId,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				SlotManager slotManager,
				MetricRegistry metricRegistry,
				JobLeaderIdService jobLeaderIdService,
				ClusterInformation clusterInformation,
				FatalErrorHandler fatalErrorHandler,
				JobManagerMetricGroup jobManagerMetricGroup,
				Time startupPeriodTime,
				MockResourceManagerRuntimeServices rmServices) {
			super(
				rpcService,
				resourceManagerEndpointId,
				resourceId,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				metricRegistry,
				jobLeaderIdService,
				clusterInformation,
				fatalErrorHandler,
				jobManagerMetricGroup,
				startupPeriodTime);
			this.rmServices = rmServices;
		}
	}
}
